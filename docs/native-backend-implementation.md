# Native Tantivy backend implementation

This document records the standalone native backend contract. RocksDB is not a Fulltext delivery
target. [Native checkpoint publication](native-checkpoint-publication.md) defines the durability
boundary.

## Intent

Implement `@harperfast/fulltext/native` as a usable standalone full-text index backed directly by
Tantivy's `MmapDirectory`.

The backend covers native index creation and reopen, batched document upsert/delete, explicit
commit and checkpoint publication, weighted BM25, phrase, bounded prefix/autocomplete, fuzzy and
fuzzy-prefix queries, score-neutral candidate filtering, current-record match tracing, status, and
deterministic close. Source-data projection, replication, and readiness policy remain outside the
package.

It implements the native portions of [the façade and lifecycle contract](https://github.com/HarperFast/fulltext/issues/14)
and [bounded native execution](https://github.com/HarperFast/fulltext/issues/17). The facade uses
versioned packed mutation and search frames, stable error codes, and explicit lifecycle operations.
Each index has one bounded writer actor and a bounded search executor, while the optional process
governor provides checked aggregate admission across indexes. Sustained work stays off JavaScript
and libuv, and search remains independent of write and commit latency. Shared cross-index executors,
multi-environment handles and cancellation of started Tantivy collectors remain outside this
contract.

## Invariant

The engine-facing schema, mutation, commit, search, and lifecycle contracts remain independent from
the consuming application. The Node package contributes canonical path handling and Tantivy
`MmapDirectory` construction; the application owns source projection, replay, and readiness.

## Verified constraints

- The package exports one hand-written `@harperfast/fulltext/native` facade for runtime information,
  index lifecycle, batched mutation, checkpoint publication, and search.
  `verify: ts/native.ts, package.json:6-11`
- The addon already contains a panic boundary and per-handle poison primitive.
  `verify: src/boundary.rs:8-40`
- The directory harness exercises Tantivy create, write, commit, query, and reopen behavior against
  `MmapDirectory`.
  `verify: src/directory_harness.rs`
- Tantivy 0.26.1 is pinned and compiled into the addon. Its `Index::open` wraps a supplied directory
  in `ManagedDirectory`, while `Index::writer_with_num_threads` acquires the writer lock and divides
  the supplied memory budget across the requested indexing threads.
  `verify: Cargo.toml:20-25; tantivy 0.26.1 src/index/index.rs:509-590`
- `MmapDirectory::open` requires an existing directory, canonicalizes it, and owns its mmap cache,
  watcher, filesystem access, and lock behavior.
  `verify: tantivy 0.26.1 src/directory/mmap_directory/mod.rs:166-175,232-295`
- The reader uses `ReloadPolicy::Manual`, which does not call `Directory::watch`; Tantivy's mmap
  watcher starts its polling thread only when `watch()` is called. The native backend therefore
  does not add a metadata-watcher thread per open index.
  `verify: src/engine.rs:148-154; tantivy 0.26.1 src/reader/mod.rs:80-98; src/directory/mmap_directory/file_watcher.rs:35-71`
- napi-rs `AsyncTask` executes on the shared libuv pool, so it is not the execution primitive for
  sustained indexing or search.
  `verify: napi 3.13.0 src/bindgen_runtime/js_values/task.rs:18-43; src/async_work.rs:181-195`

## Public slice

The hand-written TypeScript facade remains authoritative. Generated addon declarations remain
private.

```ts
interface NativeFullTextIndexOptions {
	path: string;
	indexId: string;
	generation: string;
	fields: Array<{ name: string; weight?: number }>;
	analyzer: 'english@2';
	stopWords?: boolean;
	positions?: boolean;
	surfaceTerms?: boolean;
	limits: {
		indexingThreads: number;
		writerMemoryBytes: number;
		maxQueuedCommands: number;
		maxQueuedBytes: number;
		maxBatchBytes: number;
		searchThreads: number;
	};
}

type NativeFullTextIndexInspectionOptions = Omit<NativeFullTextIndexOptions, 'limits'>;

interface FullTextMutationBatch {
	upserts?: Array<{ id: string; fields: Record<string, string | string[]> }>;
	deletes?: string[];
}

interface EncodeFullTextMutationBatchesOptions {
	maxTotalBytes?: number;
	allowPartial?: boolean;
}

interface SearchRequest {
	text: string;
	mode?: 'any' | 'all' | 'phrase' | 'prefix' | 'fuzzy' | 'fuzzy-prefix';
	operator?: 'any' | 'all';
	fields?: string[];
	candidateIds?: string[];
	offset?: number;
	limit?: number;
	exactTotal?: boolean;
}

interface SearchResult {
	total: number;
	totalRelation: 'exact' | 'lower-bound';
	hits: Array<{ id: string; score: number }>;
}

interface FullTextStatus {
	state: 'open' | 'closing' | 'closed' | 'poisoned';
	uncommittedMutations: bigint;
	writerQueuedCommands: bigint;
	writerQueuedBytes: bigint;
	searchQueuedCommands: bigint;
	searchQueuedBytes: bigint;
	commitOpstamp: bigint;
	metrics: {
		writerQueueNanoseconds: bigint;
		writerExecutionNanoseconds: bigint;
		searchQueueNanoseconds: bigint;
		searchExecutionNanoseconds: bigint;
	};
}

type NativeFullTextIndexInspection =
	| { state: 'missing' }
	| { state: 'cursorless' }
	| { state: 'checkpointed'; committedPayload: string }
	| {
			state: 'incompatible';
			code:
				| 'E_IDENTITY_MISMATCH'
				| 'E_INCOMPLETE_CREATE'
				| 'E_INDEX_CORRUPT'
				| 'E_INDEX_FORMAT_INCOMPATIBLE'
				| 'E_SCHEMA_MISMATCH';
	  };
```

These native-only per-index limits keep standalone measurements reproducible. The optional process
governor adds aggregate caps without moving policy into the shared engine. The application supplies
resolved per-index limits and configures the process budget once before opening indexes.

The exported flow is:

1. `inspectNativeFullTextIndex(options)` synchronously checks durable identity, schema, and commit
   payload without creating files, reserving a handle, starting an actor, or acquiring the writer.
   The application validates the opaque payload against its own replay-cursor contract.
2. `openNativeFullTextIndex(options)` creates the directory when absent, canonicalizes it, reserves
   the canonical path, and asynchronously creates or reopens Tantivy state.
   `openNativeFullTextReader(options)` opens existing state without acquiring a writer and may be
   called multiple times for the same path. Each reader uses manual reload and never polls files.
3. `index.encodeMutationBatches(batch)` validates one logical batch against the opened schema and
   greedily creates versioned packed requests within the opened byte limits. Invalid individual
   mutations are reported by operation and per-operation array index; they are never dropped.
   `apply(packedBatch)` copies one frame into Rust-owned memory, validates it in Rust, and enqueues
   one command. Upsert is delete-by-ID followed by add, so a committed ID has at most one live
   document. The low-level `encodeMutationBatch(batch, maxBytes)` remains available for callers that
   intentionally produce one frame.
4. `commit()` serializes behind earlier writer commands and publishes through Tantivy's ordinary
   commit path. It does not imply reader reload.
5. `reload()` crosses the writer barrier and then refreshes the reader on the search executor;
   `search()` uses one captured immutable searcher without waiting behind indexing or commit work.
6. `close()` rejects new work, settles admitted commands, shuts down the writer, releases the path
   reservation, and is idempotent.
7. `status()` reads bounded counters and state without entering either sustained-work queue.

Only the versioned `english@2` analysis contract is accepted. `positions` defaults to true and
selects frequencies with or without positions. Changing that default in a future release is an
index-format change, not a silent reinterpretation. `generation` is an opaque caller-owned identity
for this physical index generation; it is persisted in the engine fingerprint and must match on
reopen. It is not a Tantivy opstamp or an application transaction-log position.
`surfaceTerms` creates a separately indexed, unstemmed companion term field for prefix,
fuzzy-prefix, and current-record match tracing; it never stores source values. It defaults off
because of its storage cost. Field weights are query-time boosts and may change when reopening the
same physical index without rebuilding it.

`configureNativeFullTextRuntime()` optionally installs one immutable process budget before the first
successful open. Identical calls are idempotent; configuration while an open is pending returns
retryable `E_LOCK_BUSY`, while configuration after an unbudgeted open returns `E_RESOURCE_LIMIT`.
A failed first open releases its pending latch only after teardown proves native resources were
released; an unproven teardown latches the process as opened without a budget until restart. The
governor admits aggregate resident indexes, indexing/search threads,
writer memory, configured queue capacity, and expensive searches with checked shared accounting.
Each writer reserves its writer queue and shared search-queue capacity, so process queue accounting
charges twice the per-index `maxQueuedBytes`; each read-only handle reserves one search-queue share.
Exceeding an open-time admission cap returns
`E_RESOURCE_LIMIT`; it never evicts an active generation. Expensive searches wait on a condition
variable off the JavaScript thread until process capacity is available, the request deadline
expires, or close interrupts the wait. Callers that omit the governor retain per-index admission.
The deadline controls whether queued or completed work is accepted, not when its promise settles.
If every eligible worker is executing non-interruptible Tantivy work, an expired queued request is
rejected when a worker next examines it. An unproven teardown quarantines the path and keeps its
aggregate capacity charged until process restart; releasing an unverified reservation could
oversubscribe threads or memory still held by native actors.

The public search method accepts a small typed request and decodes a versioned native result buffer
into bounded result objects. The N-API boundary receives one operation per batch or search;
per-document native calls are not exposed. The benchmark reports engine execution separately from
N-API plus result-decoding time so storage and boundary costs cannot be confused.

Both encoders perform UTF-8 encoding synchronously on the caller's JavaScript thread.
`encodeMutationBatches()` binds frame size to the opened handle, validates IDs and field names,
requires IDs to be distinct across the logical batch, and defaults the total returned-byte ceiling
to 64 MiB. A caller can lower that ceiling with `maxTotalBytes`; exceeding it throws by default.
With `allowPartial: true`, the result reports the leading upsert and delete counts consumed under
that ceiling so a caller can apply the frames and continue with each array's remaining suffix
without re-encoding earlier records. Aggregate frame overflow creates more frames; a mutation that
cannot fit alone is reported to the caller. The call performs no native work. `apply()` then makes
one required copy into Rust-owned memory before asynchronous admission. Callers apply every frame and
commit or publish only after all succeed; otherwise they rollback-close the staged writer window.
One caller owns that complete apply-and-publication sequence per handle; the low-level frame API
does not provide a logical-batch fence between concurrent publishers. Searches remain concurrent.
The benchmark reports encoding separately and pre-encodes its engine-only corpus so directory
results exclude both JavaScript encoding and the N-API copy.

## Native architecture

```text
Node worker
  │ one packed mutation or small search request
  ▼
N-API handle registry ── canonical path reservation
  │
  ├─ write/commit/reload barrier ─► bounded writer queue ─► dedicated writer actor
  │                                                       └─ Tantivy IndexWriter
  │                                                          └─ indexing/merge workers
  └─ search/reload ─┬► bounded ordinary queue ─► reserved ordinary worker
                    │                         └► idle flexible workers
                    └► bounded expensive queue ─► flexible workers
                                                  └─ shared IndexReader/Searcher

writer actor ─► shared engine ─► MmapDirectory
```

The native addon owns these threads and queues; no sustained operation runs on the JavaScript event
loop or libuv pool. The writer actor is the sole owner of `IndexWriter`, making mutation, commit,
rollback, and shutdown ordering explicit. A small configurable search pool shares the
active immutable `Searcher`, so searches overlap each
other as well as indexing and commit. `reload()` first crosses the writer queue as a barrier. It
reloads a reusable staging reader, validates that reader against the persisted checkpoint, then
swaps its `Searcher` into the active slot. A failed validation leaves the prior aligned snapshot
active and returns retryable `E_RELOAD_FAILED`. Publication by the owning writer already knows the
checkpoint it committed, so that path reloads the staging reader and replaces the active searcher
without the external-reader alignment check. Tantivy remains free to use its configured indexing
and merge workers behind the writer actor. Independent indexes and their
searches may run concurrently. The process governor bounds their aggregate resources without
replacing the per-index search pools with a shared scheduler.

With two or more search threads, one worker consumes only the ordinary bounded BM25 lane. Every
other worker prioritizes phrase, prefix, fuzzy, unanchored-negation, exact-total, and trace requests,
then steals ordinary work when the expensive lane is idle. A negation intersected with a positive
ordinary clause stays in the ordinary lane. This preserves ordinary-query capacity during expensive
traffic without stranding half the pool during ordinary-only workloads. A one-thread configuration
uses one shared lane and provides no query-class isolation. In that configuration, a process-wide
expensive permit held by another index can delay ordinary work behind an expensive request. Closing
an index rejects an expensive request waiting for a permit with `E_CLOSED`.

Both queues are bounded by command count and retained bytes. A JS-owned buffer is copied once into a
Rust-owned `Vec<u8>` before admission; no native thread borrows memory owned by a Node environment.
Queue wait and engine execution time are reported separately in status/benchmark output. Effective
writer arena, indexing-thread, and search-thread values are printed in every benchmark record.

The registry rejects a second writer for the same canonical path and admits bounded read-only
handles alongside the writer. A reset requires both the writer and every reader to be closed.
Readers reserve a resident-index slot, search threads, and queue bytes from the process budget but
do not reserve indexing threads or writer memory. Publication coordination belongs to the caller: a reader calls
`reload()` only after learning that the writer published a newer revision.

Inspection is outside the handle registry and writer actor. It opens Tantivy's
managed directory read-only long enough to validate the persisted schema and read the current
commit payload, then drops it before returning. A small synchronous lifecycle check is preferable
to acquiring and closing an `IndexWriter`, but it is not a request-path API: callers cache the
result for their ownership epoch and repeat it only when lifecycle ownership changes. Tantivy's
`MmapDirectory` constructs a dormant file-watcher value, but no watcher thread starts unless
`Directory::watch` is invoked; inspection never invokes it.

## Schema and query behavior

Each Tantivy schema contains an internal indexed string fast field for the raw ID, using Tantivy's
raw tokenizer, and one declared text field per configured source field. At creation, the engine
atomically writes a small backend-neutral identity sidecar through `Directory::atomic_write()` and calls
`Directory::sync_directory()`. It contains a versioned fingerprint of the logical index ID, bounded
generation, analyzer identity, stop-word policy, positions, surface-term storage, canonical
single-token synonym rules, and structural schema. It excludes the Node wire ABI and Tantivy
package version: wire changes do not
change durable semantics, and Tantivy performs its own index-format compatibility check. Reopen
compares both the generated Tantivy schema and this fingerprint before creating a writer. The
immutable sidecar is separate from Tantivy's per-commit payload, which remains available for
standalone checkpoints and derived watermarks.

Unknown mutation fields, missing IDs, duplicate schema field names, unknown search fields, oversized
batches, and excessive result windows fail before search/index work. Blank and stop-word-only
queries return an exact empty result. Record IDs have a separate 4,096-byte UTF-8 ceiling; field
values retain the general 1 MiB packed-string ceiling. The tighter ID invariant bounds the sort keys
materialized for deterministic score-tie pagination.

Create treats `{sidecar, meta.json}` as a pair. If neither exists, it writes and syncs the sidecar
first and then creates the Tantivy index. If both exist, it reopens and verifies them. A sidecar-only
state is an interrupted empty create and is completed automatically after verifying the sidecar;
`meta.json` without the sidecar is rejected as incomplete because it may contain durable data whose
identity cannot be proven. Absent, unparseable, or mismatched identity is never accepted as
legacy-compatible. Tantivy's persisted schema and versioned tokenizer names independently verify
the structural and analyzer portions of the identity. Every declared count and length in packed
input is checked against the remaining bytes before arithmetic or allocation, the version tag is checked first, and
invalid UTF-8 is rejected on the writer actor rather than the JavaScript thread. Admission checks
the fixed header and structural bounds only. `indexId` and `generation` have fixed encoded-length
limits because they are persisted.

The English analyzer is versioned by name and starts with an NFKC-aware tokenizer so decomposed
words are normalized before token boundaries are selected. Already-normalized input follows a
zero-allocation iterator. Other input is compatibility-decomposed, canonically ordered, and
recomposed as a stream carrying original byte spans; it does not materialize a normalized copy or
one span per expanded character. Token text stops growing once it is guaranteed to be removed by
the downstream byte-length filter. The tokenizer follows Unicode Stream-Safe Text Process by
inserting a combining grapheme joiner before an input character whose compatibility decomposition
would exceed 30 consecutive non-starters, bounding normalization buffers. Possessive removal,
`LowerCaser`, `AsciiFoldingFilter`, `RemoveLongFilter`, optional English `StopWordFilter`, and English
`Stemmer` follow. Golden fixtures cover combining marks, cross-source-character composition,
full-width text, Latin diacritics, possessives, expansions, and adjacent emoji. The same analyzer
tokenizes indexed and search text.
The analyzer name and identity-sidecar version form the compatibility key; changing filter semantics
requires bumping at least one of them.

Synonym rules are bounded by count, replacement count, and encoded bytes in the native decoder.
Each source and replacement must yield exactly one normalized and analyzed term. Canonical rules are
sorted and fingerprinted in the identity sidecar. A bounded token filter streams replacements once at
the source position into both analyzed and surface fields; query text is not expanded. Tantivy counts
the alternatives in BM25 field length, so enabling a rule can affect unrelated-term ranking for a
document containing its source. Match tracing uses the same document-side expansion and maps every
replacement to the source token span. Crossing its 262,144-token-per-value ceiling marks the trace
incomplete instead of failing the request. Version 2 and 3 sidecars remain parseable for safe reset
but mismatch v4 open/inspection so the application can retire and rebuild them.

Search builds a typed Boolean query rather than exposing Tantivy's query-string syntax. Each
analyzed term is searched across the selected fields, applying configured field boosts. `any`
scores documents matching at least one term; `all` requires every analyzed term to match at least
one selected field. Tantivy's normal scorer supplies BM25. The default result reports a bounded
lower total (`offset + returned hits`, with `totalRelation: 'lower-bound'` when the page is full) and
runs `TopDocs::order_by_score()` alone. In pinned Tantivy 0.26.1 that collector invokes
`Weight::for_each_pruning`, and Boolean term unions select the block-WAND implementation. Exact
total is explicit per query, runs a separate `Count`, and is benchmarked separately at the same
concurrency because it must visit all matches. The initial schema resolves hit IDs through that fast
field once per result segment, avoiding stored-document decompression on every result. The ID is not
duplicated in Tantivy's document store.

Phrase queries preserve analyzer positions, including gaps left by removed stop words, and match
tracing uses the same positional rule. Prefix expansion is capped; exceeding the cap returns
`E_PREFIX_TOO_BROAD` rather than truncating the term set and silently biasing recall or rank. A
caller may catch that distinct code and choose a documented fallback. Fuzzy-prefix remains a
preview capability until the catalog-scale benchmark qualifies it. Request budgets cover
queue wait plus execution, are clamped to 30 seconds, and are checked during match tracing;
Tantivy's collector itself cannot be interrupted, so an expired search result is discarded after
the collector returns. If all eligible workers are executing, an expired queued request is rejected
when a worker next examines it rather than at the exact wall-clock deadline.

## Failure and lifecycle behavior

All N-API exports retain `catch_unwind`, and both native thread entries catch unwind around
initialization and every command. A panic poisons only the affected handle, drains and rejects every
queued promise, and leaves no admitted promise unsettled. Filesystem, incomplete-create, identity,
schema, query, resource, queue, closed, and native failures map to stable package error codes present
in the TypeScript allowlist while preserving the cause message. A competing process holding
Tantivy's filesystem writer lock maps to a distinct retryable lock-busy code. A source-parity test
compares the Rust error table with the TypeScript allowlist, while integration tests assert codes on
representative synchronous and asynchronous failures. No Rust type or Tantivy object crosses the
public API or Node worker.

Inspection returns metadata incompatibilities as data so a derived-index owner can choose a
rebuild. This includes corrupt metadata, unsupported Tantivy index formats, and persisted commit
payloads beyond Fulltext's 64 KiB bound. A `checkpointed` inspection result proves compatible
committed metadata, not that every referenced segment is readable. Callers must open successfully
before serving queries; initialization maps missing segment files and unreadable segment metadata
or footers to rebuildable codes. Permission, device, and other operational storage failures still
throw and are not silently reclassified as rebuilds. Errors after an index is open retain the
ordinary live-operation classification rather than triggering an automatic rebuild decision.
Missing storage returns `missing` without creating the requested directory. A read-only integration
test snapshots file names, bytes, sizes, and modification times before and after inspection.

Successful `commit()` delegates to Tantivy 0.26.1's ordinary commit path and resolves only after it
returns. The process-kill test verifies publication and process-crash recovery. A test directory
that reports an error after applying an atomic metadata write verifies conservative checkpoint
handling after an ambiguous commit. Checkpointed publication is implemented separately in
[native-checkpoint-publication.md](native-checkpoint-publication.md). A commit or post-validation
mutation failure poisons the writer generation; callers close and reopen from the last durable
commit rather than guessing which uncommitted opstamps survived.

`close()` defaults to require-clean: uncommitted mutations fail close rather than being silently
committed or discarded. That failure restores the open state so the caller can commit or call
`close({ mode: 'rollback' })`; it does not strand the path reservation in `closing`. A poisoned
handle always permits rollback close, and a default close after a terminal commit failure performs
the same forced teardown because there is no valid writer state left to preserve. A successful close
joins Tantivy's merge threads, stops the search executor, and only then releases the canonical path.
Closing transitions through open, closing, and closed states; it settles admitted commands, and
repeated successful close calls resolve. A post-quiescence operational cleanup failure also resolves
and is returned as `cleanupError`; rejection means quiescence was not proven or the caller must
explicitly resolve uncommitted data. Process exit does not promise an implicit final commit.

Each Node environment registers one N-API asynchronous cleanup hook, shared by every index it opens.
Environment teardown stops accepting work, detaches JavaScript completions, schedules rollback close
for every tracked runtime before waiting, and keeps the native environment alive until writer
shutdown, search-thread joins, and path release complete. The hook uses napi-rs's cleanup primitive
and returns only after the native actors stop, so hook completion and environment destruction stay
on Node's environment thread. All runtimes share one 30-second deadline rather than consuming that
budget serially per index, and a caught panic still returns control to napi-rs so worker/process
shutdown cannot hang indefinitely. A worker terminated while an index is still opening waits on the
same bounded completion signal. Completions detached during teardown leave their native thread-safe
function to Node's environment finalization instead of accessing that environment after the hook
returns. Handles and completions are never reused across workers.

## Performance experiment

The release-build benchmark generates a deterministic high-cardinality product-style corpus and
drives the public native API. It emits one versioned JSON record containing environment
metadata and:

- durable documents and packed MiB indexed per second by batch size and commit cadence;
- separately reported packing, apply, writer queue, and writer execution time;
- commit latency distribution and reload time;
- warm p50/p95/p99 and throughput for any/all, phrase, prefix, fuzzy, fuzzy-prefix, and candidate
  filtering at configurable concurrency, plus approximate/exact-total profiles;
- cold-after-reopen search p50/p95/p99;
- index bytes, periodically sampled peak RSS, and post-close RSS; and
- document count, field count, average packed bytes, thread/memory budgets, Tantivy version, and
  host metadata needed to interpret the numbers.

The default local profile is short enough for engineering iteration. A larger profile is selected
explicitly. Correctness assertions run before timing results are accepted: expected IDs must rank,
reopen must preserve results, and every operation count must match. The benchmark's
replacement-safe upserts emit delete terms, so `--commit-every` is an explicit workload dimension
rather than allowing an unbounded final commit to masquerade as a production ingestion profile.
The benchmark does not claim a 100-million-document or p99-under-50-ms release gate; those remain
fixed-host qualification work. This slice establishes the standalone native baseline.

CI runs correctness tests and an explicit small benchmark-smoke command that performs ranking,
commit, close, and reopen assertions before validating nonzero measurements. Shared runners enforce
no timing threshold. Performance thresholds require controlled hardware and release-over-release
history. Smoke JSON is retained as a GitHub Actions artifact. A published release also attaches its
JSON benchmark record to the GitHub release so results remain comparable after Actions artifacts
expire.

The inspection benchmark creates one committed native seed, clones it to configurable index counts,
and compares synchronous inspection with full writer-backed reopen. It reports first-pass and warm
p50/p95/p99/max latency for both operations plus synchronous wall time for each inspection sweep.
The comparison answers whether lifecycle inspection is cheap enough to remain synchronous; it is
not a query-throughput or OS-cold-storage benchmark. Every record includes the actual segment count
and metadata size so metadata-light runs are not presented as worst-case evidence. `--warm-rounds`
controls repeated passes, and requested index counts are capped at 10,000 total directories.

## Verification

- Rust unit tests: schema equality, analyzer behavior, batch decode bounds, upsert/delete ordering,
  query construction, close state, duplicate writer rejection, and queue saturation.
- Node tests through `@harperfast/fulltext/native`: create, apply, commit, reload, BM25 ranking,
  reopen, concurrent read-only handles, manual publication refresh, Boolean queries, hit versions,
  mutation validation, schema mismatch, close modes, and event-loop responsiveness.
- Process tests: kill the indexer immediately after a successful commit and verify the committed
  corpus after reopen; kill before commit and verify it is absent. A worker-thread test verifies
  promises settle only into their originating Node environment.
- Concurrency tests: duplicate canonical opens admit one writer, overload rejects without blocking
  JavaScript, indexing leaves the event loop responsive, and worker termination detaches
  completions and releases the writer.
- Decoder tests cover invalid counts, lengths, and UTF-8; randomized decoder fuzzing remains part of
  the hardening work.
- MmapDirectory contract tests remain unchanged.
- `npm run check`, benchmark smoke profiles, and package artifact verification run in CI.
- Release benchmarks run in release mode and retain versioned JSON for comparison on equivalent
  hardware.

End-to-end route: a Node integration test loads the built addon through the published native
subpath, creates an on-disk index, mutates and commits it, searches it, closes it, reopens it, and
repeats the search.

## Approaches considered

### Different layer: implement native storage only in a host application

The consuming application could own the engine and call a thin native filesystem binding. Rejected
because standalone users need the complete search and indexing behavior without depending on a
separate host or forking those semantics.

### Deeper cause: implement the complete storage-neutral runtime before either backend

The bad state to prevent is a backend opening durable data whose analyzer or engine semantics it
cannot interpret. The candidate is a backend-neutral persisted fingerprint and commit durability
contract before either backend ships. This is adopted in the chosen approach. Derived watermarks and
multi-environment sharing remain application responsibilities; the wrapper's optional process
governor only enforces local aggregate resource caps.

### Do less: expose Tantivy's existing filesystem API or query parser directly

Rejected because it would expose a third-party API, permit unbounded query syntax, and create a
public contract a host could not safely govern. A directory-only smoke also cannot provide the
performance baseline required for the wrapper.

### Do less on runtime: engine benchmark plus separate per-index writer and search executors

Partially adopted. The engine remains generic over `Directory`, and the Node slice keeps separate,
byte-bounded per-index writer and search executors. A small optional process governor accounts for
aggregate index, thread, writer-memory, queue-capacity, and expensive-search budgets; it does not
introduce a shared cross-index executor or scheduler.

### Identity sidecar versus repeating identity in every commit payload

The sidecar is chosen. Repeating identity in every commit payload couples immutable engine identity
to future checkpoint/watermark publication and lets any omitted `set_payload()` erase it. An
immutable sidecar written through the same `Directory` contract prevents that failure and works
with `MmapDirectory` without custom filesystem code.

### Chosen: one shared engine slice with a thin MmapDirectory constructor

This produces a usable standalone backend, avoids reimplementing Tantivy filesystem primitives,
and establishes bounded off-event-loop execution.

## Explicit deferrals

- Source-data projection, replay, replication, and readiness policy.
- Corpus-level query suggestions; bounded prefix search supplies text autocomplete.
- Shared handles across multiple Node worker environments.
- A handle-lifetime response dispatcher that replaces the initial per-operation thread-safe
  callback as part of a future shared multi-environment runtime.
- Fixed-host regression thresholds for representative application workloads.
- Cancellation of an already-running Tantivy collector and cursor-based deep pagination.
