# Native Tantivy backend implementation

This document records the standalone native backend contract. RocksDB is not a Fulltext delivery
target. [Native checkpoint publication](native-checkpoint-publication.md) defines the durability
boundary.

## Intent

Implement `@harperfast/fulltext/native` as a usable standalone full-text index backed directly by
Tantivy's `MmapDirectory`.

The backend covers native index creation and reopen, batched document upsert/delete, explicit
commit and checkpoint publication, weighted BM25, phrase, bounded prefix/autocomplete, fuzzy and
fuzzy-prefix queries, score-neutral candidate filtering, current-record match tracing, status,
ranked snapshot traversal, and deterministic close. Source-data projection, replication, and
readiness policy remain outside the package.

It implements the native portions of [the façade and lifecycle contract](https://github.com/HarperFast/fulltext/issues/14)
and [bounded native execution](https://github.com/HarperFast/fulltext/issues/17). The facade uses
versioned packed mutation and search frames, stable error codes, and explicit lifecycle operations.
Each index has one bounded writer actor and a bounded search executor, while the optional process
governor provides checked aggregate admission across indexes. Native indexing and search execute on
owned threads, independently of write and commit latency. Large traversal-query identities hash
through WebCrypto on the shared libuv pool under separate bounded admission. Traversal checks its
deadline between document batches; ordinary collection and individual Tantivy operations remain
uninterruptible. Shared cross-index executors and multi-environment handles remain outside this
contract.

## Invariant

The engine-facing schema, mutation, commit, search, and lifecycle contracts remain independent from
the consuming application. The Node package contributes canonical path handling and Tantivy
`MmapDirectory` construction; the application owns source projection, replay, and readiness.

## Verified constraints

- The package exports one hand-written `@harperfast/fulltext/native` facade for runtime information,
  index lifecycle, batched mutation, checkpoint publication, search, and snapshot traversal.
  Sources: [`ts/native.ts`](../ts/native.ts), [`package.json`](../package.json).
- The addon already contains a panic boundary and per-handle poison primitive.
  Source: [`src/boundary.rs`](../src/boundary.rs).
- The directory harness exercises Tantivy create, write, commit, query, and reopen behavior against
  `MmapDirectory`.
  Source: [`src/directory_harness.rs`](../src/directory_harness.rs).
- Tantivy 0.27.0 is pinned and compiled into the addon. Its `Index::open` wraps a supplied directory
  in `ManagedDirectory`, while `Index::writer_with_num_threads` acquires the writer lock and divides
  the supplied memory budget across the requested indexing threads.
  Sources: [`Cargo.toml`](../Cargo.toml), pinned Tantivy `src/index/index.rs`.
- `MmapDirectory::open` requires an existing directory, canonicalizes it, and owns its mmap cache,
  watcher, filesystem access, and lock behavior.
  Source: pinned Tantivy `src/directory/mmap_directory/mod.rs`.
- The reader uses `ReloadPolicy::Manual`, which does not call `Directory::watch`; Tantivy's mmap
  watcher starts its polling thread only when `watch()` is called. The native backend therefore
  does not add a metadata-watcher thread per open index.
  Sources: [`src/engine.rs`](../src/engine.rs), pinned Tantivy `src/reader/mod.rs` and
  `src/directory/mmap_directory/file_watcher.rs`.
- napi-rs `AsyncTask` executes on the shared libuv pool, so it is not the execution primitive for
  sustained indexing or search.
  Source: pinned napi 3.13.0 `src/bindgen_runtime/js_values/task.rs` and `src/async_work.rs`.

## Tantivy ownership boundary

The wrapper composes Tantivy primitives instead of implementing a second search engine.

| Capability                                                                      | Owner                                                                                                 | Wrapper responsibility                                                                                                                                                                                                           |
| ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tantivy index files, memory maps, locks, segments, and metadata                 | Tantivy `MmapDirectory` and `Index`                                                                   | Canonicalize the caller's path, atomically maintain the wrapper identity sidecar, coordinate reset, and map failures to stable public errors.                                                                                    |
| Writing, deletion, commits, merges, and merge threads                           | Tantivy `IndexWriter` and `LogMergePolicy`                                                            | Serialize one writer actor per index, publish the opaque checkpoint in Tantivy's commit payload, and set only the configured deleted-document threshold.                                                                         |
| Postings, BM25, Boolean execution, phrases, fuzzy terms, and block-WAND pruning | Tantivy query and scorer types                                                                        | Translate the bounded typed request into native query objects; do not expose query-string syntax.                                                                                                                                |
| Result collection                                                               | Tantivy scorers, `TopDocs`, `Count`, `TopNComputer`, and columnar fast fields                         | Own the stable score-then-ID collector, its cross-segment merge, batched winning-ordinal resolution, and exact match count.                                                                                                      |
| Prefix autocomplete                                                             | Tantivy term dictionaries, `TermQuery`, and `FuzzyTermQuery`                                          | Bound dictionary expansion and implement completed-terms-plus-final-prefix semantics. Tantivy's `PhrasePrefixQuery` requires positional adjacency and cannot run on the frequency-only surface field.                            |
| Analysis                                                                        | Tantivy `TextAnalyzer` and built-in lowercase, ASCII-folding, length, stop-word, and stemming filters | Supply the versioned streaming NFKC tokenizer, English possessive handling, bounded index-time synonyms, and source-span tracking.                                                                                               |
| Highlighting and snippets                                                       | Wrapper                                                                                               | Match caller-supplied current values and return UTF-16 spans without storing source text or emitting HTML. Tantivy's snippet helper does not implement Fulltext's synonym, fuzzy/prefix, source-span, and completeness contract. |
| Node concurrency and admission                                                  | Wrapper                                                                                               | Keep sustained native work off JavaScript, bound queues and process resources, isolate expensive queries, and settle every admitted promise during close.                                                                        |

The custom stable collector reuses Tantivy's scorer, `TopNComputer`, segment readers, and string
fast-field ordinals; it does not calculate relevance or read postings itself. An optimized local
macOS arm64 probe against Tantivy 0.27 used 100,000 generated documents and 40 interleaved calls
per collector. At a 10-hit window the wrapper/native totals were 44.5/41.5 ms; at 1,000 hits they
were 72.0/1,237.4 ms, with matching returned IDs. This is one synthetic comparison, not a universal
speedup claim. Retain the wrapper for larger windows, batched dictionary resolution, exact match
count and fail-closed missing-ID behavior; the native composite collector is a possible future
small-window optimization. Re-run this comparison when string sorting or collection changes.
Prefix expansion remains bounded policy around Tantivy's dictionary and query objects.

### Final-page version hydration

Ordinary sorting retains document addresses and validates IDs over the same TopDocs window, then
resolves versions only for the returned page. Shared final hydration also serves the stable and
traversal collectors. Optional metadata never participates in ranking; skipped version metadata is
not proactively validated.

Run the optimized [ordinary-search driver](../benchmarks/ordinary-search.mjs) against either build:

```sh
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/ordinary-search.mjs /path/to/build label --hydration
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/ordinary-search.mjs /path/to/build label --hydration --approximate-totals
```

Historical measurements using the prior time-based warmup and upper-index percentile convention
(the retained driver now uses 500 fixed warmups and nearest-rank percentiles):
Observed on macOS arm64 / Node 24.16.0, 2026-10-27: three alternating runs per artifact, each with
10,000 documents, one-second warmup and 1,000 measured requests per case. Values below are medians
of the three run percentiles, comparing the
[pre-hydration build](https://github.com/HarperFast/fulltext/commit/e1cc2c160a32a5f0bdbf97f8f547942de11f2e2b)
with the [hydration build](https://github.com/HarperFast/fulltext/commit/b544e9c9e32ae5a80a237f5e90ec8d12be75c228).
The table uses exact totals, including any separate count pass; the second command qualifies
approximate totals independently. Both artifacts embed Tantivy 0.27. The shallow fixture has a score change after hit 20, exercising
the no-boundary-tie branch. Versions have either no value or 2,048 bytes of padding plus a short ID.

| Case                                    | Median before/after (ms) | p95 before/after (ms) | p99 before/after (ms) |
| --------------------------------------- | ------------------------ | --------------------- | --------------------- |
| Offset 9,990, ten hits, padded versions | 6.899 / 0.941            | 7.348 / 0.994         | 7.585 / 1.069         |
| Offset 9,990, ten hits, no versions     | 0.767 / 0.786            | 0.851 / 0.882         | 0.922 / 1.024         |
| First 20 hits, padded versions          | 0.087 / 0.082            | 0.119 / 0.096         | 0.185 / 0.136         |
| First 20 hits, no versions              | 0.046 / 0.048            | 0.056 / 0.062         | 0.078 / 0.087         |

By code trace, decoded versions fall from 10,000 to ten in the deep padded case, and from 21 to 20
in the shallow case; these are work-model counts, not runtime instrumentation. The driver checks
IDs, versions, totals and within-run score stability. Sub-millisecond tail variation, notably the
no-version controls, limits general performance claims; this synthetic comparison establishes no
production latency guarantee. Re-run on representative workloads when changing collection or
metadata hydration. The Linux release workflow runs both total profiles and retains their JSONL
results with its workflow artifacts; generic mixed-query results alone do not cover this path.

A separate local approximate-total probe using the same fixture and run counts observed median
latency 5.802 → 0.895 ms for deep padded versions, 0.729 → 0.717 ms for deep no-version requests,
and 0.081 → 0.079 ms for shallow padded versions. The no-version shallow control remained about
0.055 ms. These are additional local measurements, with the same limits as the table above.

### Native fuzzy-prefix trace matching

Fuzzy-prefix tracing reuses Tantivy's prefix automaton, prepared lazily after exact-prefix checks.
UTF-16 spans, eligibility and current-source matching remain wrapper policy. Full fuzzy tracing
retains its allocation-free predicate, checked against native dictionary automata: preparing up to
32 analyzed/surface DFAs regressed short sixteen-term traces in three paired optimized runs
(median p50 0.0554 → 0.1874 ms). That exceeded the preset small-request threshold of both 10% and
0.1 ms; broader reuse is deferred rather than adding a matcher cache.

```sh
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/trace-matching.mjs /path/to/build label
```

The retained driver produces eight cases: fuzzy and fuzzy-prefix requests over one matching token,
1,000 matching tokens, sixteen terms over 10,000 nonmatching tokens, and sixteen inflected terms
over twenty near-length nonmatching tokens. Prefix matching includes an edit inside the prefix;
it cannot pass through the exact-prefix shortcut. Each case records its first call separately,
then 50 fixed warmups and 200 measured calls with nearest-rank percentiles, asserting complete
output and expected record count. First-call values include case/process initialization effects;
they are not a portable cold-start guarantee. Linux release jobs retain JSONL and attach it to
release assets alongside hydration measurements.

Three alternating local optimized macOS arm64 / Node 24.16 runs on 2026-10-27 compared the
pre-reuse implementation with the final prefix-only implementation. Median-of-run p50 (ms):

| Case                                | Fuzzy control before / after | Fuzzy-prefix before / after |
| ----------------------------------- | ---------------------------- | --------------------------- |
| One token                           | 0.0206 / 0.0217              | 0.0163 / 0.0213             |
| 1,000 matching tokens               | 0.4360 / 0.4410              | 0.3964 / 0.3699             |
| 16 terms, 10,000 nonmatching tokens | 9.8034 / 9.9058              | 2.4623 / 1.9810             |
| 16 inflected terms, twenty tokens   | 0.0563 / 0.0568              | 0.0285 / 0.0329             |

Small prefix requests pay about 0.005 ms extra preparation in these fixtures; larger prefix inputs
improve or stay similar. Final p50/p95 comparisons stayed within the preset threshold (over 10%
and 0.1 ms for small requests, over 10% for large requests). These synthetic results are not a
production guarantee; keep the preparation-heavy controls when revisiting full fuzzy reuse.

## Public API

The public types in [`ts/native.ts`](../ts/native.ts) define the API. Generated declarations in
`ts/addon.d.ts` describe the private addon interface and are not consumer types.

Use the [README examples](../README.md#quick-start) for opening an index, mutations, Boolean
queries, structured filters, and traversal. Keeping the type definitions in one place avoids stale
copies when the API changes. Each open requires explicit per-index thread, writer-memory, and queue
limits; [limits and their effects](limits.md) explains their constraints.

Explicit per-index limits make standalone measurements reproducible. The optional process
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
   `search()` and `searchPage()` use one captured immutable searcher without waiting behind indexing
   or commit work. `searchPage()` returns an authenticated continuation for the next ranked page.
   Keep a dedicated reader open without reloading to retain the original snapshot across writer
   publications; changed-snapshot reload expires its continuations. The host bounds reader leases
   and continues enforcing current authorization and source-version checks.
6. `close()` rejects new work, settles admitted commands, shuts down the writer, releases the path
   reservation, and is idempotent.
7. `status()` reads bounded counters and state without entering either sustained-work queue.

Only the versioned `english@2` analysis contract is accepted. `positions` defaults to true and
selects frequencies with or without positions. Changing that default in a future release is an
index-format change, not a silent reinterpretation. `generation` is an opaque caller-owned identity
for this physical index generation; it is persisted in the engine fingerprint and must match on
reopen. It is not a Tantivy opstamp or an application transaction-log position.
`surfaceTerms` creates a separately indexed, unstemmed companion term field for prefix,
fuzzy-prefix, and current-record match tracing; it never stores source values or token positions.
Surface fields retain term frequencies for BM25 while primary fields follow `positions`. It defaults
off because of its storage cost. Field weights are query-time boosts and may change when reopening
the same physical index without rebuilding it.

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

The native addon owns these threads and queues; native indexing and search do not execute on the
JavaScript event loop or libuv pool. Traversal preparation separately uses a bounded synchronous
hash for identities up to 64 KiB and WebCrypto on libuv for larger identities. A dispatched hash
cannot be forcibly cancelled; deadline and close state are checked before native submission. The
writer actor is the sole owner of `IndexWriter`, making mutation, commit,
rollback, and shutdown ordering explicit. A small configurable search pool shares the active
immutable `Searcher`, so searches overlap each other as well as indexing and commit. `reload()`
first crosses the writer queue as a barrier. It
reloads a reusable staging reader, validates that reader against the persisted checkpoint, then
swaps its `Searcher` into the active slot. A failed validation leaves the prior aligned snapshot
active and returns retryable `E_RELOAD_FAILED`. Publication by the owning writer already knows the
checkpoint it committed, so that path reloads the staging reader and replaces the active searcher
without the external-reader alignment check. Tantivy remains free to use its configured indexing
and merge workers behind the writer actor. The writer uses Tantivy's log merge policy with its
deleted-document threshold set to 50%, preventing replacement-heavy indexes from retaining a
segment once more than half its documents are dead. Independent indexes and their searches may run
concurrently. The process governor bounds their aggregate resources without
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

Schema changes fail closed even when the logical identity is unchanged. Indexes made by versions
that stored positions in surface fields report `E_SCHEMA_MISMATCH` and must be rebuilt; they are
never opened under the smaller frequency-only surface schema.

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
incomplete instead of failing the request. Versions 1–5 remain parseable for safe reset
but mismatch current open/inspection so the application can retire and rebuild them. Filter-free
indexes use sidecar v6; indexes with structured filter fields use v7 so their field names and types
participate in compatibility checks alongside the current stemmer revision.

Search builds a typed Boolean query rather than exposing Tantivy's query-string syntax. Each
analyzed term is searched across the selected fields, applying configured field boosts. `any`
scores documents matching at least one term; `all` requires every analyzed term to match at least
one selected field. Tantivy's normal scorer supplies BM25. A bounded result reports the exact match
count when the collector exhausts the matches before filling `offset + limit + 1`; otherwise it
reports `offset + limit` with `totalRelation: 'lower-bound'`. Exact total is explicit per query and
is benchmarked separately at the same concurrency because it may need to visit all matches. TopDocs
paths run a separate `Count` only when the bounded pass does not exhaust the result set. The stable
multi-clause `any` collector already enumerates and counts every match, so that path reuses its count
instead of executing the query twice.

Single-clause `any`, `all`, phrase, prefix, fuzzy, and candidate-filtered searches use
`TopDocs::order_by_score()`. In pinned Tantivy 0.27.0 that collector invokes
`Weight::for_each_pruning`, and Boolean term unions can select the block-WAND implementation.
Candidate-free top-level `any` queries with multiple scoring clauses instead enumerate matches with
one stable score-then-raw-ID collector so every page shares one scoring topology. Its per-segment
collector reads the ID fast field for each match because raw UTF-8 ID order is the secondary key.
It resolves winning ID ordinals with one ordered dictionary traversal per segment. Flattening the
`any` scorer can change the least-significant bits of a returned `f32` score because floating-point
addition is not associative; ranking and pagination use the flattened topology consistently.
The ID fast field is an index invariant. If any matched document lacks an ID ordinal, the query
fails with `E_NATIVE_FAILURE` rather than returning an incomplete or incorrectly paginated result.

The initial schema resolves hit IDs through a fast field, avoiding stored-document decompression on
every result. The ID is not duplicated in Tantivy's document store.

Phrase queries preserve analyzer positions, including gaps left by removed stop words, and match
tracing uses the same positional rule. Prefix expansion is capped; exceeding the cap returns
`E_PREFIX_TOO_BROAD` rather than truncating the term set and silently biasing recall or rank. A
caller may catch that distinct code and choose a documented fallback. Fuzzy-prefix remains a
preview capability until the catalog-scale benchmark qualifies it. Request budgets cover
queue wait plus execution, are clamped to 30 seconds, and are checked during match tracing.
Traversal also charges preparation against that budget and checks its deadline every 1,024 scorer
advances. Ordinary collection, query construction, and individual Tantivy operations remain
uninterruptible; an expired result is discarded after control returns. If all eligible workers are
executing, an expired queued request is rejected
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

Successful `commit()` delegates to Tantivy 0.27.0's ordinary commit path and resolves only after it
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
fixed-host qualification work. This benchmark establishes the standalone native baseline.

CI runs correctness tests and an explicit small benchmark-smoke command that performs ranking,
commit, close, and reopen assertions before validating nonzero measurements. Shared runners enforce
no timing threshold. Performance thresholds require controlled hardware and release-over-release
history. Smoke JSON is retained as a GitHub Actions artifact. After a GitHub release is published, a
separate workflow runs the native and multi-index profiles on Linux x64 and Linux arm64, retains the
results as 90-day Actions artifacts, and attaches all four JSON records to the release. This
post-publish workflow preserves comparison history; it does not gate npm publication or calculate a
baseline delta.

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

Partially adopted. The engine remains generic over `Directory`, and the Node runtime keeps separate,
byte-bounded per-index writer and search executors. A small optional process governor accounts for
aggregate index, thread, writer-memory, queue-capacity, and expensive-search budgets; it does not
introduce a shared cross-index executor or scheduler.

### Identity sidecar versus repeating identity in every commit payload

The sidecar is chosen. Repeating identity in every commit payload couples immutable engine identity
to future checkpoint/watermark publication and lets any omitted `set_payload()` erase it. An
immutable sidecar written through the same `Directory` contract prevents that failure and works
with `MmapDirectory` without custom filesystem code.

### Chosen: shared engine with a thin MmapDirectory constructor

This produces a usable standalone backend, avoids reimplementing Tantivy filesystem primitives,
and establishes bounded off-event-loop execution.

## Explicit deferrals

- Source-data projection, replay, replication, and readiness policy.
- Corpus-level query suggestions; bounded prefix search supplies text autocomplete.
- Shared handles across multiple Node worker environments.
- A handle-lifetime response dispatcher that replaces the initial per-operation thread-safe
  callback as part of a future shared multi-environment runtime.
- Fixed-host regression thresholds for representative application workloads.
- Cancellation of ordinary offset-search collection and individual in-flight Tantivy operations.
