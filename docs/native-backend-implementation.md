# Native Tantivy backend implementation

## Intent

Implement `@harperfast/fulltext/native` as a usable standalone full-text index backed directly by
Tantivy's `MmapDirectory`. This backend is the behavioral and performance reference for the future
RocksDB-backed directory. Harper will never select it in a product release.

This change covers the smallest end-to-end slice needed to measure real behavior: native index
creation and reopen, batched document upsert/delete, explicit commit and reload, BM25 search,
status, and deterministic close. Phrase, fuzzy, prefix, autocomplete,
suggestions, highlighting, derived-index watermarks, and RocksDB storage remain separate work.

It deliberately implements narrow prerequisites from issues #14 and #17 without closing either
issue. From #14 it uses one versioned packed mutation request, one packed search result, stable error
codes, and the lifecycle shape required by this backend. From #17 it uses one bounded dedicated
actor per index so sustained work stays off JavaScript and libuv. It does not implement the final
process-wide governor, shared search pool, multi-environment handle registry, cancellation, or
derived nonblocking admission.

## Invariant

Every storage backend uses the same engine-facing schema, mutation, commit, search, and lifecycle
contracts; the native backend contributes only canonical path handling and Tantivy
`MmapDirectory` construction.

## Verified constraints

- The package currently exports only `runtimeInfo()` from the hand-written native facade.
  `verify: ts/native.ts:1-23, package.json:6-11`
- The addon already contains a panic boundary and per-handle poison primitive.
  `verify: src/boundary.rs:1-38, src/lib.rs:31-53`
- The committed Phase 0 work already exercises a complete Tantivy create, write, commit, query, and
  reopen lifecycle through the same `Directory` contract intended for RocksDB.
  `verify: src/directory_harness.rs:108-153`
- Tantivy 0.26.1 is pinned and compiled into the addon. Its `Index::open` wraps a supplied directory
  in `ManagedDirectory`, while `Index::writer_with_num_threads` acquires the writer lock and divides
  the supplied memory budget across the requested indexing threads.
  `verify: Cargo.toml:20-25; tantivy 0.26.1 src/index/index.rs:509-590`
- `MmapDirectory::open` requires an existing directory, canonicalizes it, and owns its mmap cache,
  watcher, filesystem access, and lock behavior.
  `verify: tantivy 0.26.1 src/directory/mmap_directory/mod.rs:166-175,232-295`
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
	};
}

interface FullTextMutationBatch {
	upserts?: Array<{ id: string; fields: Record<string, string | string[]> }>;
	deletes?: string[];
}

interface SearchRequest {
	text: string;
	operator?: 'any' | 'all';
	fields?: string[];
	offset?: number;
	limit?: number;
}

interface SearchResult {
	total: number;
	hits: Array<{ id: string; score: number }>;
}
```

These native-only limits are required in the pre-1.0 standalone factory so measurements are
reproducible and memory is bounded without prematurely implementing #17's final process-wide budget
allocator. The shared engine accepts resolved limits; the future runtime governor will supply them
for both storage backends, and the Harper schema will never expose them.

The exported flow is:

1. `openNativeFullTextIndex(options)` creates the directory when absent, canonicalizes it, reserves
   the canonical path, and asynchronously creates or reopens Tantivy state.
2. `encodeMutationBatch(batch)` creates the versioned packed request. `apply(packedBatch)` copies
   it once into Rust-owned memory, validates it once in Rust, and enqueues one command. Upsert is
   delete-by-ID followed by add, so a committed ID has at most one live document.
3. `commit()` serializes behind earlier writer commands and publishes through Tantivy's ordinary
   commit path. It does not imply reader reload.
4. `reload()` refreshes the reader after earlier commits; `search()` uses one captured immutable
   searcher.
5. `close()` rejects new work, settles admitted commands, shuts down the writer, releases the path
   reservation, and is idempotent.

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

## Native architecture

```text
Node worker
  │ one packed mutation or small search request
  ▼
N-API handle registry ── canonical path reservation
  │
  └─ command ─► one per-index queue bounded by commands and retained bytes
                                              └─ one dedicated actor
                                                 ├─ one Tantivy IndexWriter
                                                 │  └─ Tantivy indexing/merge workers
                                                 └─ IndexReader/Searcher

writer actor ─► shared engine ─► MmapDirectory (native)
                            later └─ RocksDbDirectory (same engine)
```

The native addon owns the actor thread and queue; no sustained operation runs on the JavaScript
event loop or libuv pool. The actor is the sole owner of `IndexWriter`, `IndexReader`, and searcher
publication, which makes mutation, commit, reload, search, rollback, and shutdown ordering explicit.
Tantivy remains free to use its configured indexing and merge workers behind that actor. Independent
indexes have independent actors and may run concurrently. Searches within one index are serialized
in this slice; replacing that limitation with the process-wide bounded search pool belongs to #17
and will not change engine semantics.

The queue is bounded by both command count and retained bytes. A JS-owned buffer is copied once into
a Rust-owned `Vec<u8>` before admission; no actor borrows memory owned by a Node environment. Queue
wait and engine execution time are reported separately in status/benchmark output. Effective writer
arena and indexing-thread values are printed in every benchmark record.

The registry rejects a second live open of the same canonical path. This is narrower than the
eventual shared multi-environment registry, but it preserves the one-writer invariant without
pretending two JavaScript handles have coordinated close ownership. The later registry issue can
replace rejection with reference-counted shared handles without changing the index contract.

## Schema and query behavior

Each Tantivy schema contains an internal stored/indexed raw ID field and one declared text field per
configured source field. At creation, the engine performs an initial metadata commit whose payload
contains a versioned fingerprint of the package ABI, Tantivy version, logical index ID, generation,
analyzer identity, stop-word policy, positions, surface-term storage, and structural schema. Reopen
compares both the generated Tantivy schema and this fingerprint before creating a writer. Unknown
mutation fields, missing IDs, duplicate schema
field names, empty queries, unknown search fields, oversized batches, and excessive result windows
fail before search/index work.

The English analyzer is versioned by name and composed from Tantivy's tokenizer primitives:
`SimpleTokenizer`, `RemoveLongFilter`, `LowerCaser`, optional built-in English
`StopWordFilter`, and English `Stemmer`. The same registered analyzer tokenizes indexed text and
search text.

Search builds a typed Boolean query rather than exposing Tantivy's query-string syntax. Each
analyzed term is searched across the selected fields, applying configured field boosts. `any`
scores documents matching at least one term; `all` requires every analyzed term to match at least
one selected field. Tantivy's normal scorer supplies BM25. Count and top-doc collection execute in
one traversal. The initial schema stores the ID and also indexes it as a string fast field; the
benchmark reports stored-field and fast-field hit resolution separately before one becomes the
fixed contract.

## Failure and lifecycle behavior

All N-API exports retain `catch_unwind`, and the actor catches unwind around initialization and every
command. A panic poisons only the affected handle, drains and rejects every queued promise, and
leaves no admitted promise unsettled. Filesystem, schema, query, resource, queue, closed, lock-busy,
and native failures map to stable package error codes present in the TypeScript allowlist while
preserving the cause message. No Rust type or Tantivy object crosses the public API or Node worker.

Successful `commit()` has Tantivy 0.26.1's documented persistence contract: all prior mutations are
published and persisted, and indexing can resume from that point after a process crash if the
storage device survives. The implementation uses `prepare_commit()`, installs the versioned engine
payload, and completes Tantivy's metadata write and directory sync before resolving. A commit error
poisons the writer generation; callers must close and reopen from the last durable commit rather
than guessing which uncommitted opstamps survived.

`close()` defaults to require-clean: uncommitted mutations fail close rather than being silently
committed or discarded. That failure restores the open state so the caller can commit or request an
explicit rollback; it does not strand the path reservation in `closing`. A successful close joins
Tantivy's merge threads before releasing the canonical path. Closing transitions through open,
closing, and closed states; it settles admitted commands, and repeated successful close calls
resolve. Process exit does not promise an implicit final commit.

## Performance experiment

Add a release-build benchmark that generates a deterministic product-style corpus and drives the
public native API. It emits one versioned JSON record containing environment metadata and:

- pure engine documents and UTF-8 MiB indexed per second by batch size;
- end-to-end apply throughput plus separately reported packing, queue, engine, N-API, and decode
  time;
- commit time and commit-plus-reload time;
- warm BM25 search p50/p95/p99 and throughput at configurable concurrency;
- cold-after-reopen search p50/p95/p99;
- stored-ID and fast-ID hit-resolution cost;
- index bytes, peak RSS, post-close RSS, and error counts; and
- document count, field count, average text bytes, query mix, thread/memory budgets, build profile,
  package revision, Tantivy version, and host fingerprint needed to interpret the numbers.

The default local profile is short enough for engineering iteration. A larger profile is selected
explicitly. Correctness assertions run before timing results are accepted: expected IDs must rank,
committed deletes must disappear, reopen must preserve results, and every operation count must
match. The benchmark does not claim the 100-million-document or Harper p99-under-50-ms release
gate; those remain the paired fixed-host work in issue #15. This slice establishes the native engine
and N-API baseline that issue #15 will compare against RocksDB.

CI runs correctness tests and an explicit small benchmark-smoke command that performs the same
ranking, delete, commit, close, and reopen assertions before validating the JSON schema and nonzero
measurements. Shared runners enforce no timing threshold. Performance thresholds require controlled
hardware and release-over-release history.

## Verification

- Rust unit tests: schema equality, analyzer behavior, batch decode bounds, upsert/delete ordering,
  query construction, close state, duplicate path rejection, and queue saturation.
- Node tests through `@harperfast/fulltext/native`: create, apply, commit, reload, BM25 ranking,
  reopen, mutation validation, schema mismatch, close modes, and event-loop responsiveness.
- Process tests: kill the indexer immediately after a successful commit and verify the committed
  corpus after reopen; kill before commit and verify it is absent. A worker-thread test verifies
  promises settle only into their originating Node environment.
- Concurrency tests: search during commit/merge, actor panic drains queued promises, concurrent
  canonical opens admit one writer, and merge files stop changing after close resolves.
- Existing Directory contract tests remain unchanged.
- `npm run check` and package artifact verification run before review.
- The release benchmark runs locally at two dataset sizes; raw JSON is retained with the PR
  verification notes.

End-to-end route: a Node integration test loads the built addon through the published native
subpath, creates an on-disk index, mutates and commits it, searches it, closes it, reopens it, and
repeats the search.

## Approaches considered

### Different layer: implement native storage only in Harper

The candidate layer is Harper's `DerivedIndexBackend`, which could own the engine and call a thin
native filesystem binding. Rejected because the invariant is one engine for standalone RocksDB,
native filesystem users, and Harper-derived delivery; putting the engine in Harper would force the
two standalone modes either to depend on Harper or to fork search/index behavior.

### Deeper cause: implement the complete storage-neutral runtime before either backend

The bad state to prevent is a backend opening durable data whose analyzer or engine semantics it
cannot interpret. The candidate is a backend-neutral persisted fingerprint and commit durability
contract before either backend ships. This is adopted in the chosen approach. The complete #14/#17
surface remains rejected for this issue because derived watermarks, multi-environment sharing, and a
global scheduler are not needed to enforce that durable compatibility invariant.

### Do less: expose Tantivy's existing filesystem API or query parser directly

Rejected because it would expose a third-party API, permit unbounded query syntax, and create a
public contract the Rocks and Harper integrations could not safely govern. A directory-only smoke
also cannot provide the performance baseline Kyle requested.

### Do less on runtime: engine benchmark plus one dedicated actor per index

Adopted. The engine is generic over `Directory`, the pure engine benchmark excludes N-API, and the
Node slice uses one byte-bounded actor rather than implementing #17's process governor and shared
search pool. This preserves a usable off-event-loop API while keeping the storage reference
measurable.

### Chosen: one shared engine slice with a thin MmapDirectory constructor

This is the only option that simultaneously produces a usable standalone backend, keeps native
storage out of Harper, avoids reimplementing Tantivy filesystem primitives, establishes bounded
off-event-loop execution, and yields an apples-to-apples reference for the Rocks directory.

## Explicit deferrals

- Derived-index delivery, checkpoints/watermarks, replay, and Harper lifecycle hooks.
- RocksDbDirectory and rocksdb-js lease use.
- Phrase, fuzzy, prefix, autocomplete, suggestions, highlighting, snippets, and filters.
- Shared handles across multiple Node worker environments.
- Durable benchmark publication, fixed-host regression thresholds, and Rocks/native comparison.
- Process-wide runtime budgets, concurrent per-index search, cancellation, and cursor-based deep
  pagination.
