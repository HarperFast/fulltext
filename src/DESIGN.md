# Native search collection

Status: verified against Tantivy 0.27.0 source and local macOS arm64 tests · Owner: Harper
Engineering · Last verified: 2026-10-28

`TraversalScoreTieCollector` owns its scorer loop so it can return `E_TIMEOUT` between document
batches. Keep the ordinary `StableScoreTieCollector` independent: traversal-only boundary and
deadline checks must not enter its per-match path.

Traversal must use the same scoring path on every page. In pinned Tantivy 0.27.0,
`BooleanWeight::for_each` and `BooleanWeight::scorer` both construct `BufferedUnionScorer` for term
unions; `into_box_scorer` performs that construction for the latter. Preserve this equivalence when
upgrading Tantivy: a different floating-point summation order can change score ties and continuation
boundaries.

The test `traversal_retains_a_merged_snapshot_and_stops_collection_on_deadline` enforces
collection-stage expiry and preservation of an old searcher through a forced merge. Public traversal
tests compare complete scored sequences against ordinary search, including filtered, candidate,
Boolean, and zero-score queries.

The facade authenticates token version/handle and snapshot/score/ID bytes plus a fixed query digest
using HMAC-SHA256 and an object-local 32-byte key minted before asynchronous work. Version-3 tokens
reject boundary edits and splicing. Native handle numbers restart in a new process, so the number
and segment fingerprint alone cannot enforce reader/process binding; the cross-process replay test
uses handle 1 in both processes.

The scorer/`for_each` bit-equivalence regression covers multi-clause unions, intersections,
phrase, prefix and fuzzy queries across segments. The equal-score paging fixture also checks
ordinal ordering against raw UTF-8 IDs. Retain both gates across engine upgrades.

Result collection keeps document addresses until final page selection. All four search paths use
`search_hits` to decode versions only for returned hits. Ordinary score sorting still resolves and
validates IDs across its retained TopDocs window; skipped version metadata is not proactively
validated, matching stable collection and traversal. Keep optional version data out of ranking.

Fuzzy-prefix traces lazily reuse `FuzzyTermQuery::automaton()` after exact-prefix checks. Full
fuzzy traces retain the allocation-free predicate: preparing analyzed/surface DFAs regressed
short sixteen-term traces in paired optimized measurements. Native dictionary parity tests cover
both predicates. Trace preserves its current-source surface branch, including `running`/`runnin`,
and its UTF-16 spans independently of indexed stems.

## Persisted analyzer revision

Identities 6/7 preserve the layouts of 4/5 while marking Tantivy 0.27's changed English stemmer.
Open and inspection compare the complete identity before opening Tantivy. Reset parses both old
and new identities, allowing retirement without serving old tokens. `english@2` plus the sidecar
revision is the persisted analysis contract; never rewrite the sidecar to reuse an incompatible index.

## Writer close ownership

Native handle close must retain the original writer until its indexing and merge threads have joined. In pinned
Tantivy 0.27, `IndexWriter::rollback` replaces the writer; dropping the old writer joins indexing
workers but does not join its old merges. Waiting on the replacement cannot establish quiescence
for the original generation. Close therefore consumes the original writer through
`wait_merging_threads`, which does not publish uncommitted mutations. Dispatch still requires
explicit discard authorization for dirty handles. Reopen tests preserve committed records and
payloads while discarding pending upserts and deletes; the gated merge regression holds close
until the original merge can finish. Do not substitute delayed filesystem cleanup for this ownership
invariant: reset and retirement depend on completed native I/O.

Joining also drains indexing workers and finalizes pending segments. Discard close therefore pays
I/O proportional to outstanding work and can leave unpublished files for native garbage collection.
It needs storage headroom. If finalization fails, Tantivy may return before joining all workers or
merges; the native outcome must remain unquiesced, producing `E_QUIESCENCE_FAILED` and retaining the
path reservation until process restart. The indexing-write failure regression pins that conservative
classification. A failed `prepare_commit` can also detach indexing workers before close begins;
writer ownership uncertainty must remain sticky through later close attempts, even when joining
the remaining native handles succeeds. Avoiding this discard I/O while preserving the original merge join would require
an upstream native lifecycle hook.

The public Rust `Writer::rollback` remains a direct, reusable-writer Tantivy operation, not a
quiescence barrier. A Rust caller that explicitly rolls back has already replaced the writer;
a subsequent close cannot join the earlier merges. Terminal Rust shutdown should consume `close`
directly. Native handles no longer call that rollback primitive. Tracking a broader Rust rollback
ownership contract remains separate from this native-handle fix.

## Request preparation

Query identities up to 64 KiB hash synchronously; larger ones use WebCrypto. This threshold changes
where hashing runs, not how large a query can be. Preparation has separate command and retained-frame
limits capped by the configured search queues, 128 commands, and 8 MiB. Native status counters do not
include this JavaScript work.

Reserve capacity after encoding, including after any caller-supplied accessor has run: an accessor
can start another request. Small synchronous requests bypass these reservations. Release a hash
reservation before native submission, then recheck close state and the remaining execution budget.
A dispatched hash cannot be cancelled.

The protocol decoder and public Rust engine share `validate_search_window`: offset capacity and
traversal page capacity are separate contracts, and checked arithmetic must precede collector
allocation. Native/TypeScript boundary tests cover advertised page/filter limits; analyzer recall
and trace completeness are qualified in [the limits guide](../docs/limits.md).

Both search paths use `search_response_bytes` to reject non-finite computed scores before exposing
hits or continuations. Finite field weights can overflow BM25 scores; the recovery regression
reopens the same data with lower weights.

## Snapshot identity

`search_snapshot` hashes the captured searcher’s ordered segment/deletion map with SHA-256. The
preimage is `fulltext-snapshot-v1\0`, a little-endian u64 segment count, then each segment’s 32 ASCII
hex UUID bytes, a None/Some byte, and a little-endian u64 deletion opstamp for Some. Fixed native
vectors were checked with Python’s `hashlib.sha256`.

This works for independently retained Rust searchers without a cursor registry. SHA-256 makes
collisions impractical; it cannot rule them out mathematically. The snapshot is 64 lowercase hex
characters. Version-3 continuations can reach 5,602 base64url characters and reject earlier
unreleased tokens.

Traversal checks its segment/deletion snapshot before constructing a query against that snapshot.
Native query/schema checks follow that guard; wire/window/ID checks still precede it. A stale prefix
continuation must return `E_CURSOR_EXPIRED` even if newly indexed terms would make the original
prefix too broad; the public regression covers writer publication and reader reload.

## Candidate identity

Traversal reads native candidate capacities once per facade, then uses the existing frame encoder.
Capture each original candidate entry once so caller accessors cannot change values between checking
limits and removing duplicates. Check original count and UTF-8 bytes before sorting unique IDs;
removed bytes still count toward the original request limit. Only the normalized frame is retained
for hashing.

JavaScript’s UTF-16 sort establishes request identity. Native hit ordering uses UTF-8 bytes. Preserve
field and query-clause order because it can affect scores. Ordinary search keeps its existing
encoding; Rust does not need to normalize candidates again.

## Metadata collectors

Observed: Rust and public Node tests pass locally on macOS arm64, 2026-10-28.

`Engine::search_inner` validates catalog requests once before building the query on the supplied
reader snapshot. Only `validate_requests` constructs `ValidatedCatalog`; its private fields and
collection methods preserve that validation boundary. Sorting and recursive aggregation reuse the query;
aggregation never runs over the returned hit page. Numeric and string sorts share `collect_sorted`,
which composes native collectors rather than sorting wrapper-owned results.
`native-metadata.test.mjs` pins filtered/candidate counts, retained-reader behavior, and every string
sort direction/missing-order combination. Its sort-key memory regression checks native
`E_RESOURCE_LIMIT` rejection with 1,199 short values and one 60 KB value, then verifies smaller
sorted windows and ordinary searches still succeed.

Only string metadata gains a fast column. The writer removes repeated strings per document
as a storage optimization, preserving first occurrence. Native terms aggregation independently
deduplicates document contributions; the writer set is not required for correct facet counts. `identity_bytes`
emits v8 only for string configurations; numeric/Boolean-only v7 and text-only v6 remain
compatible. The public identity-migration regression checks open, inspection, and reset.

`catalog::DeadlineCollector` wraps metadata collectors and ordinary exact-total Count passes. Its deletion-aware loop keeps
native top-k/aggregation ownership and checks the shared deadline every 1,024 visited matches.
`deadline_interrupts_an_active_collection` verifies expiry after collection starts.
`count_matches` returns native errors so each caller retains its error policy;
`ordinary_exact_count_preserves_native_error_classification` pins ordinary storage, internal-query
and timeout codes through the production count branch.
The shared `error::index_error` maps storage failures consistently for ordinary and metadata
searches; `metadata_collection_preserves_storage_error_classification` exercises native sorts,
aggregation and combined count collection.

Native aggregation structure accounting excludes some decoded keys. `sorted_catalog` and
`aggregate` bound retained keys before creating collectors. Counts use segment live documents
or dictionary cardinality; a deadline-bound streaming scan can qualify actual maximum key lengths when
the supported-length estimate fails. Completed maxima use a 1,024-entry FIFO cache keyed by
immutable segment ID and field. Merges create new segment IDs; deletes leave a conservative
dictionary maximum, so neither invalidates a cached bound. A completed scan is cached before
the final timeout check; interrupted scans never populate the cache. Scans run outside the cache
mutex. A warm-cache qualification compares the mutex with an experimental reader/writer lock across
eight indexes and up to eight callers; the measured gain missed the declared replacement threshold,
so the mutex remains. Concurrent cold-miss amplification remains unqualified. Sorting retains the ID contract first,
measuring IDs only when necessary. If that segment-wide estimate still fails, a deadline-aware
native Count pass supplies a global matching count that bounds total retained hits across
segments; the fallback combines it with maximum measured key lengths for the sort budget. Native Searcher and
TopDocs still own collection. Terms retain native tenfold candidate oversampling.
Sorted hits, count and aggregates share a native tuple collector; unsorted aggregates preserve
the ordinary ranked-hits path; their exact totals share that aggregation pass.
`exact_total_with_aggregations_uses_two_native_query_passes` guards this with real native query
weights, contrasting two production passes with a separate three-pass control. Segment buffer
accounting follows the actual executor concurrency while decoded fruits remain budgeted across
all segments. Engine encodes bounded aggregates once, and native wire encoding
including its success envelope finishes inside the request permit/deadline;
`encode_results` bounds the response. Catalog resource tests pin rejection rather than
partial output. See [metadata resource policy](../docs/limits.md#metadata-collectors).

One- and two-field sorts use native typed computers; three and four fields use native
erased-type tuples. Four fields use nested pairs because the pinned native flat four-tuple
does not forward per-field comparators. Larger lists compose native per-field computers into a runtime list as the head of Tantivy's native ID/score tuple. Lazy threshold
comparison avoids allocating rejected keys. TopDocs owns collection and merging. All paths share
conservative admission, including retained decoded keys, at most two global threshold-key copies, and list-storage estimates.
Global merging moves segment keys rather than cloning a second full window. Bucket
aggregation children map directly to native `sub_aggregation` trees. Resource admission accounts
for collector construction and possible parent instances before collection. Histogram parents use
position spans, with native query-scoped probes when conservative admission fails. Histogram safety
checks require ancestor presence, so non-contributing documents cannot reject descendants. Independent depth
checks protect each recursive wire/API walker. Packed protocol 7 and query API 6 describe the expanded query contract; current ABI 12 additionally
requires query-capacity open support. Neither change alters persisted identity v8.

Sorted snapshot continuation and metadata-derived score adjustment remain outside this
increment. Production latency, process-memory residency, and Harper integration are untested.

## Remaining limits

Local verification covers macOS arm64; use the platform CI results for Linux and Windows. Individual
Tantivy operations and query construction remain uninterruptible.

## Sources

- [Metadata collectors and native tests](catalog.rs)
- [Public metadata regressions](../test/native-metadata.test.mjs)

- [Collector and native tests](engine.rs)
- [Facade and token identity](../ts/native.ts)
- [Public API tests](../test/native-traversal.test.mjs)
- [Pinned Tantivy version](../Cargo.toml)

## Query capacity policy

Operational open limits snapshot analyzed terms per leaf and compiled text clauses on each engine;
they are absent from persisted identity. Text structure uses that clause budget; depth, bytes and
structured-filter bounds remain fixed. The facade's frozen `queryLimits` getter reports the
snapshot, not mutable caller options. Native validation remains authoritative for raw frames and
direct Rust opens.

`SearchStructureBudget` charges `S = sum(AND/OR child counts) + 2 × NOT count` before descending.
The compiler charges those same edges plus nonnegative leaf costs, so `S <= compiled clauses`
and AST nodes are at most `S + 1`. This protects allocation without duplicating Tantivy analysis.
The Node normalizer charges before visiting children; the native admission scanner uses the actual
handle policy and rejects field counts above the schema before scanning names. The unchanged
`decode_search` API uses the supported 4,096-clause ceiling. Direct engine searches run the same
nonallocating Rust prewalk before analysis. Empty and stopword-only leaves still consume structural
edges. Conservative lexical admission estimates select a lane; they never reject as compiled cost.

Phrase construction charges positions × fields across the request; fuzzy construction charges
term-field automata across the tree. Their fixed bounds retain the default policy's implied upper
bounds even when configured term/clause limits rise. Exact trace membership uses short vector comparisons for up to eight terms and hash lookup
above that. Fuzzy traces compare at most 16 eligible alternatives. Branch-local found cardinality
makes completion checks constant-time. Boundary and construction regressions live in `query-capacity.test.mjs` and the
engine tests.

Raised-policy admission compares maximum per-leaf term cost and summed compiled-clause cost against the default envelope. Boolean child counts and NOT’s two synthetic clauses are included, matching native query construction; term counts across independent leaves are not added. Protocol boundary tests and the two-handle optimized admission test enforce this distinction.

Raised-policy admission estimates ordinary ASCII queries using ASCII alphanumeric runs and selected fields;
non-ASCII input conservatively uses the expensive lane because normalization can expand text.
Default-policy classification is unchanged. The optimized two-handle regression pins ordinary
isolation and shared expensive permits. The optional open-frame extension preserves version-7
mutation/search frames; ABI 12 requires a matching facade/addon.

The open-policy extension is exactly two u32 values; absent means default policy and a truncated or
additional tail rejects. A future extension needs explicit framing/versioning rather than appending
an ambiguous optional value.
