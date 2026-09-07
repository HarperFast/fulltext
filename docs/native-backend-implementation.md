# Native Tantivy backend implementation

## Intent

Implement `@harperfast/fulltext/native` as a usable standalone full-text index backed directly by
Tantivy's `MmapDirectory`. This backend is the behavioral and performance reference for the future
RocksDB-backed directory. Harper will never select it in a product release.

This change covers the smallest end-to-end slice needed to measure real behavior: explicit runtime
initialization, native index creation and reopen, batched document upsert/delete, explicit commit and
reload, BM25 search, status, and deterministic close. Phrase, fuzzy, prefix, autocomplete,
suggestions, highlighting, derived-index watermarks, and RocksDB storage remain separate work.

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
interface FullTextRuntimeOptions {
	maxRegisteredIndexes: number;
	maxIndexingThreads: number;
	maxWriterMemoryBytes: number;
	searchThreads: number;
	maxQueuedSearches: number;
	maxQueuedWriterCommands: number;
}

interface NativeFullTextIndexOptions {
	path: string;
	indexId: string;
	generation: string;
	fields: Array<{ name: string; weight?: number }>;
	analyzer: 'english@1';
	stopWords?: boolean;
	positions?: boolean;
	surfaceTerms?: boolean;
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

The exported flow is:

1. `initializeFullTextRuntime(options)` freezes one process-wide resource configuration. An exact
   repeat is idempotent; a different repeat fails.
2. `openNativeFullTextIndex(options)` creates the directory when absent, canonicalizes it, reserves
   the canonical path, and asynchronously creates or reopens Tantivy state.
3. `apply(batch)` validates and packs the entire bounded batch in TypeScript, then performs one
   native call. Upsert is delete-by-ID followed by add, so a committed ID has at most one live
   document.
4. `commit()` serializes behind earlier writer commands and publishes through Tantivy's ordinary
   commit path. It does not imply reader reload.
5. `reload()` refreshes the reader after earlier commits; `search()` uses one captured immutable
   searcher.
6. `close()` rejects new work, settles admitted commands, shuts down the writer, releases the path
   reservation, and is idempotent.

Only English analysis is accepted. `positions` selects frequencies with or without positions.
`surfaceTerms` stores original field values in the same Tantivy field for later highlighting and
suggestion work; it defaults off because of its storage cost. Field weights are applied as query
boosts and therefore may change without rebuilding the physical schema.

The public API accepts typed objects. TypeScript encodes mutations and native code returns bounded
result objects for this slice. The N-API boundary still receives one operation per batch or search;
per-document native calls are not exposed. A versioned packed result protocol is deferred until
result metadata grows beyond IDs and scores, avoiding a speculative wire format while keeping the
hot ingestion boundary batched.

## Native architecture

```text
Node worker
  │ typed validation + one batch encoding
  ▼
N-API handle registry ── canonical path reservation
  │
  ├─ writer command ─► bounded per-index queue ─► one writer actor
  │                                            └─ one Tantivy IndexWriter
  │                                               └─ Tantivy indexing/merge workers
  │
  └─ search command ─► bounded process search queue ─► fixed search threads
                                                    └─ captured Searcher

writer actor ─► shared engine ─► MmapDirectory (native)
                            later └─ RocksDbDirectory (same engine)
```

The native addon owns these threads and queues; no sustained operation runs on the JavaScript event
loop or libuv pool. The writer actor is the sole owner of `IndexWriter`, which makes mutation,
commit, rollback, and shutdown ordering explicit. Tantivy remains free to use its configured
indexing and merge workers behind that actor. Search uses a separate fixed pool so long queries do
not head-of-line block commits and independent indexes can search concurrently.

The process runtime allocates indexing threads and writer memory to a newly opened writer from the
remaining global budget. The initial implementation reserves a deterministic equal share based on
`maxRegisteredIndexes`; it rejects an infeasible configuration rather than silently reducing
Tantivy below its supported per-thread arena. Search queue and writer command queue capacities are
hard bounds. Queue wait and execution time are reported separately in status/benchmark output.

The registry rejects a second live open of the same canonical path. This is narrower than the
eventual shared multi-environment registry, but it preserves the one-writer invariant without
pretending two JavaScript handles have coordinated close ownership. The later registry issue can
replace rejection with reference-counted shared handles without changing the index contract.

## Schema and query behavior

Each Tantivy schema contains an internal stored/indexed raw ID field and one declared text field per
configured source field. Reopen builds the expected schema and compares it with the persisted
Tantivy schema before creating a writer. Unknown mutation fields, missing IDs, duplicate schema
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
one traversal, and only the stored internal ID is loaded for returned hits.

## Failure and lifecycle behavior

All N-API exports retain `catch_unwind`. A panic poisons only the affected handle. Filesystem,
schema, query, resource, queue, closed, and native failures map to stable package error codes while
preserving the cause message. No Rust type or Tantivy object crosses the public API.

`close()` defaults to require-clean: uncommitted mutations fail close rather than being silently
committed or discarded. A caller may explicitly request rollback. Closing transitions through
open, closing, and closed states; it waits for admitted searches and writer commands, and repeated
successful close calls resolve. Process exit does not promise an implicit final commit.

## Performance experiment

Add a release-build benchmark that generates a deterministic product-style corpus and drives the
public native API. It emits one versioned JSON record containing environment metadata and:

- documents and UTF-8 MiB indexed per second by batch size;
- apply queue time, native apply time, commit time, and commit-plus-reload time;
- warm BM25 search p50/p95/p99 and throughput at configurable concurrency;
- cold-after-reopen search p50/p95/p99;
- index bytes, peak RSS, post-close RSS, and error counts; and
- document count, field count, average text bytes, query mix, thread/memory budgets, build profile,
  package revision, Tantivy version, and host fingerprint needed to interpret the numbers.

The default local profile is short enough for engineering iteration. A larger profile is selected
explicitly. Correctness assertions run before timing results are accepted: expected IDs must rank,
committed deletes must disappear, reopen must preserve results, and every operation count must
match. The benchmark does not claim the 100-million-document or Harper p99-under-50-ms release
gate; those remain the paired fixed-host work in issue #15. This slice establishes the native engine
and N-API baseline that issue #15 will compare against RocksDB.

CI runs only correctness tests and a small benchmark smoke that validates the JSON schema and
nonzero measurements without enforcing timing on shared runners. Performance thresholds require
controlled hardware and release-over-release history.

## Verification

- Rust unit tests: schema equality, analyzer behavior, batch decode bounds, upsert/delete ordering,
  query construction, close state, duplicate path rejection, and queue saturation.
- Node tests through `@harperfast/fulltext/native`: create, apply, commit, reload, BM25 ranking,
  reopen, mutation validation, schema mismatch, close modes, and event-loop responsiveness.
- Existing Directory contract tests remain unchanged.
- `npm run check` and package artifact verification run before review.
- The release benchmark runs locally at two dataset sizes; raw JSON is retained with the PR
  verification notes.

End-to-end route: a Node integration test loads the built addon through the published native
subpath, creates an on-disk index, mutates and commits it, searches it, closes it, reopens it, and
repeats the search.

## Approaches considered

### Different layer: implement native storage only in Harper

Rejected because Harper must never expose or select native storage, and doing so would prevent
standalone Node users and the Rocks adapter from sharing one behavioral reference.

### Deeper cause: implement the complete storage-neutral runtime before either backend

This is the final architecture, but implementing derived watermarks, Rocks leases, all query forms,
and every cancellation rule in one change would make backend correctness and performance impossible
to isolate. The native vertical slice establishes the engine boundary while preserving extension
points for those contracts.

### Do less: expose Tantivy's existing filesystem API or query parser directly

Rejected because it would expose a third-party API, permit unbounded query syntax, and create a
public contract the Rocks and Harper integrations could not safely govern. A directory-only smoke
also cannot provide the performance baseline Kyle requested.

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
- A final packed result protocol and cursor-based deep pagination.
