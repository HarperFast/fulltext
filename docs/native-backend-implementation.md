# Native Tantivy backend implementation

This document records the native backend contract. Native Tantivy files are also the selected
storage for Harper's derived full-text indexes; RocksDB is not a Fulltext delivery target. See
[native checkpoint publication](native-checkpoint-publication.md) for its durability boundary.

## Intent

Implement `@harperfast/fulltext/native` as a usable standalone full-text index backed directly by
Tantivy's `MmapDirectory`. This backend provides the engine and persistence path that Harper will
also consume through its derived-index lifecycle.

This change covers the smallest end-to-end slice needed to measure real behavior: native index
creation and reopen, batched document upsert/delete, explicit commit and reload, BM25 search,
status, and deterministic close. Phrase, fuzzy, prefix, autocomplete,
suggestions, highlighting, and derived-index watermarks were outside this initial slice.

It deliberately implements narrow prerequisites from issues #14 and #17 without closing either
issue. From #14 it uses one versioned packed mutation request, one packed search result, stable error
codes, and the lifecycle shape required by this backend. From #17 it uses one bounded dedicated
writer actor and one bounded search executor per index so sustained work stays off JavaScript and
libuv while search remains independent of write/commit latency. It does not implement the final
process-wide governor, shared cross-index search pool, multi-environment handle registry,
cancellation, or derived nonblocking admission.

## Invariant

The engine-facing schema, mutation, commit, search, and lifecycle contracts remain independent from
Harper. The Node wrapper contributes canonical path handling and Tantivy `MmapDirectory`
construction; Harper owns source projection, replay, and derived-index readiness.

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
  `verify: napi 2.16.17 src/task.rs:6-14`

## Public slice

The hand-written TypeScript facade remains authoritative. Generated addon declarations remain
private.

```ts
interface NativeFullTextIndexOptions {
	path: string;
	indexId: string;
	generation: string;
	fields: Array<{ name: string; weight?: number }>;
	analyzer: 'english@1';
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
}

interface SearchRequest {
	text: string;
	operator?: 'any' | 'all';
	fields?: string[];
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

These native-only limits are required in the pre-1.0 standalone factory so measurements are
reproducible and memory is bounded without prematurely implementing #17's final process-wide budget
allocator. The shared engine accepts resolved limits; the future runtime governor will supply them,
and the Harper schema will never expose them.

The exported flow is:

1. `inspectNativeFullTextIndex(options)` synchronously checks durable identity, schema, and commit
   payload without creating files, reserving a handle, starting an actor, or acquiring the writer.
   Harper validates the opaque payload against its own replay-cursor contract.
2. `openNativeFullTextIndex(options)` creates the directory when absent, canonicalizes it, reserves
   the canonical path, and asynchronously creates or reopens Tantivy state.
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

Only English analysis is accepted. `positions` defaults to true and selects frequencies with or
without positions. Changing that default in a future release is an index-format change, not a
silent reinterpretation. `generation` is an opaque caller-owned identity for this physical index
generation; it is persisted in the engine fingerprint and must match on reopen. It is not a Tantivy
opstamp or a Harper transaction-log position.
`surfaceTerms` stores original field values in the same Tantivy field for later highlighting and
suggestion work; it defaults off because of its storage cost. Field weights are applied as query
boosts and therefore may change without rebuilding the physical schema.

The public search method accepts a small typed request and decodes a versioned native result buffer
into bounded result objects. The N-API boundary receives one operation per batch or search;
per-document native calls are not exposed. The benchmark reports engine execution separately from
N-API plus result-decoding time so storage and boundary costs cannot be confused.

Both encoders perform UTF-8 encoding synchronously on the caller's JavaScript thread.
`encodeMutationBatches()` binds frame size to the opened handle, validates IDs and field names,
requires IDs to be distinct across the logical batch, and defaults the total returned-byte ceiling
to 64 MiB. A caller can lower that ceiling with `maxTotalBytes`. The result reports the leading
mutation count consumed under that ceiling so a caller can apply the frames and continue with the
remaining suffix without re-encoding earlier records. Aggregate frame overflow creates
more frames; a mutation that cannot fit alone is reported to the caller. The call performs no native
work. `apply()` then makes one
required copy into Rust-owned memory before asynchronous admission. Callers apply every frame and
commit or publish only after all succeed; otherwise they rollback-close the staged writer window.
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
  └─ search/reload ─► bounded search queue ─► small search worker pool
                                             └─ shared IndexReader/Searcher

writer actor ─► shared engine ─► MmapDirectory
```

The native addon owns these threads and queues; no sustained operation runs on the JavaScript event
loop or libuv pool. The writer actor is the sole owner of `IndexWriter`, making mutation, commit,
rollback, and shutdown ordering explicit. A small configurable search pool shares the
`IndexReader`; each request captures its immutable `Searcher`, so searches overlap each other as
well as indexing and commit. `reload()` first crosses the writer queue as a barrier, reloads the
shared reader under a short coordination lock, and publishes the new searcher to subsequent
requests. Tantivy remains free to use its
configured indexing and merge workers behind the writer actor. Independent indexes and their
searches may run concurrently. #17 later replaces the per-index search pools with the bounded
process pool without changing engine behavior.

Both queues are bounded by command count and retained bytes. A JS-owned buffer is copied once into a
Rust-owned `Vec<u8>` before admission; no native thread borrows memory owned by a Node environment.
Queue wait and engine execution time are reported separately in status/benchmark output. Effective
writer arena, indexing-thread, and search-thread values are printed in every benchmark record.

The registry rejects a second live open of the same canonical path. This is narrower than the
eventual shared multi-environment registry, but it preserves the one-writer invariant without
pretending two JavaScript handles have coordinated close ownership. The later registry issue can
replace rejection with reference-counted shared handles without changing the index contract.

Inspection is deliberately outside the handle registry and writer actor. It opens Tantivy's
managed directory read-only long enough to validate the persisted schema and read the current
commit payload, then drops it before returning. A small synchronous lifecycle check is preferable
to acquiring and closing an `IndexWriter`, but it is not a request-path API: callers cache the
result for their ownership epoch and repeat it only when lifecycle ownership changes. Tantivy's
`MmapDirectory` constructs a dormant file-watcher value, but no watcher thread starts unless
`Directory::watch` is invoked; inspection never invokes it.

## Schema and query behavior

Each Tantivy schema contains an internal indexed string fast field for the raw ID using Tantivy's raw tokenizer
and one declared text field per configured source field. At creation, the engine atomically writes a
small backend-neutral identity sidecar through `Directory::atomic_write()` and calls
`Directory::sync_directory()`. It contains a versioned fingerprint of the logical index ID, bounded
generation, analyzer identity, stop-word policy, positions, surface-term storage, and structural
schema. It deliberately excludes the Node wire ABI and Tantivy package version: wire changes do not
change durable semantics, and Tantivy performs its own index-format compatibility check. Reopen
compares both the generated Tantivy schema and this fingerprint before creating a writer. The
immutable sidecar is separate from Tantivy's per-commit payload, which remains available for
standalone checkpoints and derived watermarks.
Unknown mutation fields, missing IDs, duplicate schema
field names, empty queries, unknown search fields, oversized batches, and excessive result windows
fail before search/index work.

Create treats `{sidecar, meta.json}` as a pair. If neither exists, it writes and syncs the sidecar
first and then creates the Tantivy index. If both exist, it reopens and verifies them. A sidecar-only
state is an interrupted empty create and is completed automatically after verifying the sidecar;
`meta.json` without the sidecar is rejected as incomplete because it may contain durable data whose
identity cannot be proven. Absent, unparseable, or mismatched identity is never accepted as
legacy-compatible. Tantivy's persisted schema and versioned tokenizer names independently verify
the structural and analyzer portions of the identity. Every declared count and length in packed input is checked
against the remaining bytes before arithmetic or allocation, the version tag is checked first, and
invalid UTF-8 is rejected on the writer actor rather than the JavaScript thread. Admission checks
the fixed header and structural bounds only. `indexId` and `generation` have fixed encoded-length
limits because they are persisted.

The English analyzer is versioned by name and composed from Tantivy's tokenizer primitives:
`SimpleTokenizer`, `RemoveLongFilter`, `LowerCaser`, optional built-in English
`StopWordFilter`, and English `Stemmer`. The same registered analyzer tokenizes indexed text and
search text.

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
repeated successful close calls resolve. Process exit does not promise an implicit final commit.

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

Add a release-build benchmark that generates a deterministic high-cardinality product-style corpus
and drives the public native API. It emits one versioned JSON record containing environment
metadata and:

- durable documents and packed MiB indexed per second by batch size and commit cadence;
- separately reported packing, apply, writer queue, and writer execution time;
- commit latency distribution and reload time;
- warm BM25 search p50/p95/p99 and throughput at configurable concurrency, using approximate totals
  by default and same-concurrency single-worker approximate/exact profiles;
- cold-after-reopen search p50/p95/p99;
- index bytes, periodically sampled peak RSS, and post-close RSS; and
- document count, field count, average packed bytes, thread/memory budgets, Tantivy version, and
  host metadata needed to interpret the numbers.

The default local profile is short enough for engineering iteration. A larger profile is selected
explicitly. Correctness assertions run before timing results are accepted: expected IDs must rank,
reopen must preserve results, and every operation count must match. The benchmark's
replacement-safe upserts emit delete terms, so `--commit-every` is an explicit workload dimension
rather than allowing an unbounded final commit to masquerade as a production ingestion profile.
The benchmark does not claim the 100-million-document or Harper p99-under-50-ms release gate; those
remain paired fixed-host work. This slice establishes the standalone native baseline for comparison
with the integrated Harper path and a no-index Harper control.

CI runs correctness tests and an explicit small benchmark-smoke command that performs ranking,
commit, close, and reopen assertions before validating nonzero measurements. Shared runners enforce
no timing threshold. Performance thresholds require controlled hardware and release-over-release
history.

The inspection benchmark creates one committed native seed, clones it to configurable index counts,
and compares synchronous inspection with full writer-backed reopen. It reports first-pass and warm
p50/p95/p99/max latency for both operations plus synchronous wall time for each inspection sweep.
The comparison answers whether lifecycle inspection is cheap enough to remain synchronous; it is
not a query-throughput or OS-cold-storage benchmark. Every record includes the actual segment count
and metadata size so metadata-light runs are not presented as worst-case evidence. `--warm-rounds`
controls repeated passes, and requested index counts are capped at 10,000 total directories.

## Verification

- Rust unit tests: schema equality, analyzer behavior, batch decode bounds, upsert/delete ordering,
  query construction, close state, duplicate path rejection, and queue saturation.
- Node tests through `@harperfast/fulltext/native`: create, apply, commit, reload, BM25 ranking,
  reopen, read-only inspection while a writer is held, mutation validation, schema mismatch, close
  modes, and event-loop responsiveness.
- Process tests: kill the indexer immediately after a successful commit and verify the committed
  corpus after reopen; kill before commit and verify it is absent. A worker-thread test verifies
  promises settle only into their originating Node environment.
- Concurrency tests: duplicate canonical opens admit one writer, overload rejects without blocking
  JavaScript, indexing leaves the event loop responsive, and worker termination detaches
  completions and releases the writer.
- Decoder tests cover invalid counts, lengths, and UTF-8; randomized decoder fuzzing remains part of
  the hardening work.
- MmapDirectory contract tests remain unchanged.
- `npm run check`, the inspection benchmark smoke profile, and package artifact verification run
  before review.
- The release benchmark runs locally at two dataset sizes and commit cadences; raw JSON is retained
  with the PR verification notes. It is built explicitly in release mode before measurements are
  taken.

End-to-end route: a Node integration test loads the built addon through the published native
subpath, creates an on-disk index, mutates and commits it, searches it, closes it, reopens it, and
repeats the search.

## Approaches considered

### Different layer: implement native storage only in Harper

The candidate layer is Harper's `DerivedIndexBackend`, which could own the engine and call a thin
native filesystem binding. Rejected because standalone users and Harper-derived delivery need the
same search and indexing behavior; putting the engine in Harper would force standalone use to
depend on Harper or fork those semantics.

### Deeper cause: implement the complete storage-neutral runtime before either backend

The bad state to prevent is a backend opening durable data whose analyzer or engine semantics it
cannot interpret. The candidate is a backend-neutral persisted fingerprint and commit durability
contract before either backend ships. This is adopted in the chosen approach. The complete #14/#17
surface remains rejected for this issue because derived watermarks, multi-environment sharing, and a
global scheduler are not needed to enforce that durable compatibility invariant.

### Do less: expose Tantivy's existing filesystem API or query parser directly

Rejected because it would expose a third-party API, permit unbounded query syntax, and create a
public contract Harper could not safely govern. A directory-only smoke also cannot provide the
performance baseline required for the wrapper.

### Do less on runtime: engine benchmark plus separate per-index writer and search executors

Adopted. The engine is generic over `Directory`, and the Node slice uses byte-bounded per-index
execution rather than implementing #17's process governor and shared cross-index search pool.
Keeping search separate from the writer is the minimum needed for a baseline that measures Tantivy
instead of temporary writer-queue head-of-line blocking.

### Identity sidecar versus repeating identity in every commit payload

The sidecar is chosen. Repeating identity in every commit payload couples immutable engine identity
to future checkpoint/watermark publication and lets any omitted `set_payload()` erase it. An
immutable sidecar written through the same `Directory` contract prevents that failure and works
with `MmapDirectory` without custom filesystem code.

### Chosen: one shared engine slice with a thin MmapDirectory constructor

This produces a usable standalone backend, keeps Tantivy storage mechanics out of Harper, avoids
reimplementing Tantivy filesystem primitives, establishes bounded off-event-loop execution, and
yields the reference implementation used by Harper's derived index.

## Explicit deferrals

- Derived-index delivery, replay, readiness, and Harper lifecycle hooks.
- Phrase, fuzzy, prefix, autocomplete, suggestions, highlighting, snippets, and filters.
- Shared handles across multiple Node worker environments.
- A handle-lifetime response dispatcher that replaces the initial per-operation thread-safe
  callback; this is part of the shared multi-environment runtime in issue #17.
- Durable benchmark publication and fixed-host regression thresholds comparing standalone native,
  Harper without full text, and Harper using the native derived index.
- Process-wide runtime budgets, cancellation, and cursor-based
  deep pagination.
