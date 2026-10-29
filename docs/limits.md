# Limits and their effects

The native library enforces bounded requests, query expansion, retained results, and runtime
resources. A limit is not a promise that every request below it will succeed: byte budgets, query
expansion, selected fields, deadlines, and available process capacity also apply. Use `(await runtimeInfo()).limits` from the installed addon for its advertised numeric bounds instead of copying
them into an integration. This metadata is a subset: expression depth, schema/encoding widths,
analyzer rules, trace-token and snippet policies, and operational tuning values are documented below
rather than returned by `runtimeInfo()`.

There is no fixed limit on the total matches traversable with `searchPage()`. Its page-size bound is
independent of the ordinary offset window. See [snapshot
traversal](../README.md#traversing-beyond-10000-matches) for snapshot ownership and costs. Each page
rescans the matching set, so traversing a large result set can still exhaust the execution budget.

## Query and result bounds

Unless explicitly configurable below, these are fixed safety bounds in this release. Requests that exceed
them fail; callers must not catch the error and substitute a smaller query while claiming equivalent
results.

| Bound                       | Value                                                         | Effect and recovery                                                                                                                                                                                                                                                     |
| --------------------------- | ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ordinary `offset + limit`   | 10,000                                                        | Rejects deeper windows. Use `searchPage()` rather than increasing offsets.                                                                                                                                                                                              |
| Traversal page `limit`      | 1–10,000                                                      | Returns at most `limit` hits after merging; retains at most `limit + 1` candidates per segment, independent of traversal depth. Continue until the continuation is absent.                                                                                              |
| Prefix/fuzzy-prefix hits    | 100, offset zero                                              | Applies to legacy `search()` only. Use `searchPage()` for subsequent prefix/fuzzy-prefix matches; its independent page and byte budgets apply.                                                                                                                          |
| Candidate IDs               | 1,024 IDs and 1 MiB combined UTF-8 ID bytes                   | Rejects larger candidate lists; it does not silently select the first 1,024. Use indexed structured filters where applicable. Splitting candidates requires caller-owned global ordering, totals, and merging; independent top-k batches are not a drop-in replacement. |
| Query text                  | 64 KiB total UTF-8 bytes                                      | Applies across Boolean leaf text. Narrow the query; do not truncate user text silently.                                                                                                                                                                                 |
| Analyzed query terms        | 64 default, configurable at open                              | Applies per analyzed query leaf, not to the whole Boolean tree. The compiled clause budget also applies. Analysis can remove stop words or produce different terms from raw whitespace splitting.                                                                       |
| Query/filter clauses        | 256 default compiled text clauses; structural bounds stay 256 | Query terms, selected fields, and expansions can multiply compiled clauses. A valid schema with 1,024 fields does not imply that an all-field query fits this budget. Select the fields needed by the query.                                                            |
| Expression depth            | 8 edges from the root                                         | Search and filter trees reject deeper nesting. Flatten equivalent Boolean groups where semantics permit.                                                                                                                                                                |
| Exact prefix expansion      | 50 distinct terms per selected field across segments          | Exceeding this rejects with `E_PREFIX_TOO_BROAD`; rankings are not silently truncated. A longer prefix or narrower field set may help. Candidate IDs and structured record filters do not narrow term-dictionary expansion.                                             |
| Fuzzy-eligible terms        | 16                                                            | Rejects too many fuzzy terms. Fuzzy expansion and compiled-clause costs still apply below this cap.                                                                                                                                                                     |
| Packed search/trace request | 8 MiB                                                         | Rejects oversized frames. Other text, candidate, and source limits usually apply first.                                                                                                                                                                                 |
| Search response             | 8 MiB                                                         | Fails with `E_RESULT_TOO_LARGE`. Reduce `limit` and retry. A legal 10,000-hit page can exceed this bound when IDs/versions are large.                                                                                                                                   |
| Queue plus execution budget | 30,000 ms maximum                                             | The facade defaults to this ceiling, clamps larger supplied budgets to it, rounds down fractional milliseconds, and uses at least 1 ms. Native frames outside 1–30,000 ms reject. `E_TIMEOUT` does not imply a hard callback-time bound.                                |

Traversal snapshots the candidate list, checks original count and combined UTF-8 bytes against
native-reported capacities, then sorts its unique IDs for identity. Duplicate count does not change
identity, but duplicates still count toward original input limits and the packed-request bound;
field order is unchanged.

Ordinary `search()` may return `totalRelation: 'lower-bound'`; do not interpret that total as an
exact match count. Request `exactTotal` when needed. Traversal returns an exact total on each page.
Neither a page-size limit nor `hits.length` alone is a completeness signal for ordinary search.

## Indexing, schema, and encoding

| Bound                                    | Value                                                                                                        | Effect and recovery                                                                                                                                                                                                                                                                                  |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Record ID and version                    | 4,096 UTF-8 bytes each                                                                                       | Rejects oversize values. IDs must be nonempty and well-formed UTF-16 at the JS boundary. Application keys that exceed the bound need a stable bounded mapping, not truncation.                                                                                                                       |
| `indexId` and `generation`               | Nonempty, at most 4,096 UTF-8 bytes each                                                                     | Opening or inspecting an invalid identity fails. Changing persisted identity requires the appropriate generation/rebuild flow.                                                                                                                                                                       |
| Text fields and structured filter fields | 1,024 entries in each list                                                                                   | Rejects larger schemas. Query complexity has a separate, smaller bound.                                                                                                                                                                                                                              |
| Individual packed string/document value  | 1 MiB UTF-8                                                                                                  | Rejects larger values. Divide source values deliberately before indexing; do not silently discard the rest of a document. Aggregate frame/queue limits still apply.                                                                                                                                  |
| Structured string filter term            | 65,530 UTF-8 bytes                                                                                           | Tantivy's raw-term ceiling; both indexed filter values and queried filter strings enforce it. Raw frames are checked by the engine after decoding; decoding a frame is not acceptance of its values. Use bounded exact filter values for long identifiers that are unsuitable for the text analyzer. |
| Multi-value field count                  | 65,535 values                                                                                                | Encoded as a u16 count. Frame bytes may bind first. This also applies to each record-field value list supplied for tracing.                                                                                                                                                                          |
| Synonyms                                 | 1,024 rules, 1–16 replacements per rule, 1 MiB combined source/replacement bytes                             | Rejects excess configuration. Each source/replacement must analyze to exactly one term. Changing synonyms requires a new generation/rebuild.                                                                                                                                                         |
| Single mutation frame                    | Configured `maxBatchBytes`, at least 24 bytes and no greater than `maxQueuedBytes`                           | Rejects oversized frames with `E_BATCH_TOO_LARGE`. Use the logical batch API to frame large operations; a single unencodable record is still rejected.                                                                                                                                               |
| Materialized partition output            | 64 MiB default, configurable with `maxTotalBytes`                                                            | `encodeMutationBatches()` fails on incomplete consumption by default. With explicit `allowPartial: true`, resume from `consumedUpserts`/`consumedDeletes`; the returned batches alone do not imply complete input consumption.                                                                       |
| Numeric encoding                         | u16/u32 counts; safe JS integers for applicable u64 values; finite f32 scores/weights and f64 filter numbers | Rejects out-of-range values rather than wrapping. JavaScript numbers cannot represent arbitrary large integer IDs precisely; use string IDs.                                                                                                                                                         |

Logical mutation application rejects unindexable records by default. Explicit `rejectedUpsert: 'delete'` removes a previously indexed version for an unindexable replacement; it is not a
skip-with-success policy. Read the [mutation/recovery
contract](../README.md#rejected-records-and-schema-errors) before changing that policy.

## Analyzer and ranking semantics

These values affect recall or ranking. They are not resource knobs that can safely be raised on an
existing index.

| Rule                           | Current behavior                                                                                               | Impact                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Token text retention           | At most 40 normalized alphanumeric characters retained before downstream filters                               | Bounds tokenizer memory. The entire source token is consumed for source offsets; its retained text is then processed by the analyzer.                                                                                                                                                                                                                                    |
| Long-token filter              | Keeps only terms shorter than 40 UTF-8 bytes after lowercase/folding/possessive processing                     | An ASCII token of 39 bytes is indexed; 40- and 41-byte ASCII tokens are omitted. Ordinary queries containing only omitted terms return no hits, not an oversize-query error. Use structured exact filters for identifiers.                                                                                                                                               |
| Unicode normalization boundary | 30 non-starters                                                                                                | Inserts the Unicode Stream-Safe joiner boundary for excessive combining sequences; this can change token boundaries.                                                                                                                                                                                                                                                     |
| Prefix minimum                 | 3 Unicode characters; fuzzy prefix minimum 4                                                                   | Short nonempty final prefixes reject. A trailing-space prefix query follows the completed-term path instead.                                                                                                                                                                                                                                                             |
| Fuzzy eligibility              | At least 4 characters, alphabetic content, no numeric characters or `-`, `_`, `/` in the analyzed surface term | Ineligible terms follow exact matching. Eligible terms use edit distance 1 with transpositions. This is a matching rule, not a user-selected fuzziness setting.                                                                                                                                                                                                          |
| Fuzzy score bias               | Fuzzy branch constant score 0.25 times field weight; exact bonus uses `max(field weight) * 0.25 + 1`           | Favors exact matches; field weights remain configurable. Changing these coefficients changes ranking and continuation boundaries. Finite weights can still overflow a computed f32 score: both search paths reject with `E_INVALID_ARGUMENT` before returning hits or a continuation. Close and reopen with lower field weights; rebuilding indexed data is unnecessary. |
| Analyzer identity              | `english@2`, optional stop words and index-time synonyms                                                       | Stop words, stemming, normalization, and synonym expansion affect recall and BM25 scores. An analyzer/filter-order change needs a new analyzer or persisted identity version and a rebuild; silently changing these constants would mix incompatible terms.                                                                                                              |

## Tracing and snippets

Tracing is explanation of supplied current source, not an independent proof of indexed-query completeness.

| Bound                            | Value                                                     | Effect and recovery                                                                                                                                                         |
| -------------------------------- | --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Source records                   | 100                                                       | Oversized trace requests reject. Partition source records for tracing.                                                                                                      |
| Combined source text             | 1 MiB UTF-8                                               | Oversized requests reject. Partition source requests without changing indexed-search semantics.                                                                             |
| Analyzed tokens per source value | 262,144                                                   | Stops analysis with `complete: false`, including when a later unmatched token might have matched. Do not treat an incomplete empty trace as proof of no match.              |
| Returned spans                   | 1,024 across the response, also bounded by response bytes | Signals incomplete tracing rather than silently claiming complete explanation. Inspect the response-level `complete` flag; there is no per-value truncation flag.           |
| Snippet length                   | Default 160; accepted 32–512 UTF-16 code units            | Invalid options reject. This is a target length: preserving surrogate pairs can extend fragment boundaries. Snippets intentionally show excerpts; they are not full source. |
| Fragments per value              | Default 3; accepted 1–5                                   | Invalid options reject. Fragment omission does not remove the underlying returned spans.                                                                                    |

## Runtime and operational values

Per-index indexing/search threads, writer memory, queued commands/bytes, and mutation-frame bytes
are caller configuration. Optional process-wide admission additionally bounds resident handles,
aggregate threads/writer memory/queue reservations, and simultaneous expensive searches. Configure
the [process budget](../README.md#process-wide-resource-limits) before opening indexes when capacity must be bounded
across them; there is no automatic global budget if it is omitted.

- Each indexing/search thread count must be 1–64. Tantivy writer memory per indexing thread must be at least 15,000,000 bytes and strictly below 4,294,967,295 bytes. Opening outside these bounds fails; increasing thread count without increasing memory can invalidate a previously sufficient memory setting.
- Two search threads are required for query-class isolation. One thread remains valid but shares ordinary and expensive work. More than 16 filter leaves or values, range filters, and the documented expensive query classes affect lane selection, not result completeness.
- Ordinary search checks its deadline around uninterruptible Tantivy work. Traversal checks every 1,024 scorer visits; tracing checks during tokenization/matching. These are cancellation checkpoints, not wall-clock guarantees for individual scorer operations or query construction.
- Reload/open snapshot alignment makes three attempts with short backoff. Persistent contention fails with retryable `E_RELOAD_FAILED`; a failed reload preserves the prior aligned snapshot. Opening retries lifecycle-lock contention for about two seconds at 5 ms intervals, then reports `E_LOCK_BUSY`.
- Environment cleanup allows 30 seconds to prove quiescence. A failure quarantines resources; increasing a timeout or deleting files is not a safe substitute for proving release. Reset's retired-path allocation tries 16 unique names and fails with `E_STORAGE` rather than overwriting an existing path. JS retired-directory removal retries three times with 100 ms delay.
- The deleted-document merge ratio is 0.5. It influences compaction, disk retention, and indexing work rather than the number of searchable results. A retained traversal reader can keep old segments alive independently of that ratio; close it when finished.
- ABI/query/protocol versions, enum tags, frame widths, filenames, and reserved field names are compatibility identifiers. They are intentionally fixed. They must be changed through a coordinated format/API upgrade, not exposed as arbitrary configuration.

Tests and benchmarks also contain dataset sizes, timing fixtures, and panic/delay hooks. Those are
verification inputs, not production policy. This audit covers the repository's production wrapper;
pinned Tantivy has its own format and allocation constraints, so the table is not a claim that every
possible corpus below these bounds is indexable or executable within budget.

## Capacity and integration implications

The response-byte limit is checked after collection; it is not a peak-memory limit. Each segment can
retain up to page-size-plus-one winners, including resolved IDs, before the global merge. Larger
pages reduce repeated scans but increase intermediate memory. Optional process admission accounts
for configured resource reservations, not total RSS, mmap/cache residency, or disk retained by
snapshots.

Fuzzy matching has no equivalent of the exact-prefix 50-term dictionary guard. Tantivy builds a
per-segment document bitset and walks matching terms/postings while constructing its scorer. That
construction is not interruptible by the traversal checkpoints. A deadline therefore cannot
substitute for concurrency limits and workload qualification.

Host-side eligibility checks must continue past rejected raw hits. Keep raw batch size separate from
visible page size, retain unconsumed raw hits, and bound cumulative request work with an explicit
timeout/resume outcome. Exact native totals count snapshot matches, not currently authorized source
records or complete source-table coverage.

Harper encodes table identity and primary keys into native IDs using base64url. Validate the encoded
UTF-8 size, not merely the original key size. Rejected oversized or invalid source values can remove
a derived record during replacement; processed indexing progress alone does not prove complete
coverage. The host must expose rejection health and a recovery path.

Retained readers need bounded lifetimes and counts, cleanup on every exit path, owner-process
routing, and explicit expiration on restart/rebuild. Native snapshots freeze indexed hits, not
current source values or permissions. Current authorization and version checks remain necessary.

The configurable 64-term/256-clause default policy is described in
[query capacity](#query-capacity-policy).
Long-token omission and language/analyzer changes require a versioned analyzer and rebuild rather
than silently raising a constant.

### Local autocomplete qualification

Observed on 2026-10-18 UTC, macOS arm64 / Node 24, optimized native build: 100,000 equal-score
`widget` records, one selected field, one expensive-search permit, and a retained reader. Every
traversal returned all IDs in order; an illustrative host predicate accepting one in 1,000 found all
100 eligible records.

| Raw page size | Calls | Exact prefix, accumulated page time | Fuzzy prefix, accumulated page time |
| ------------- | ----: | ----------------------------------: | ----------------------------------: |
| 20            | 5,000 |                            2,924 ms |                            9,026 ms |
| 200           |   500 |                              338 ms |                              938 ms |
| 1,000         |   100 |                              107 ms |                              229 ms |

A separate finite batch of six ordinary and six fuzzy-prefix searches all completed within 15 ms
under that governor. This demonstrates progress for this sample, not starvation freedom or a
production SLO. Process RSS sampled after traversals was approximately 305–309 MB, including the
fixture, writer, reader, and caches; this is not collector-only memory or a worst-case bound. The
corpus has a small vocabulary and does not qualify arbitrary fuzzy expansion, many segments, large
IDs, or large source documents. The regression suite separately accepts 50 exact-prefix dictionary
terms and rejects 51 even with restrictive candidate/filter inputs.

Choose raw batch sizes from measured latency and byte/memory costs; a universal minimum of 1,000
would be inappropriate for maximum-size IDs/versions. Budget the entire host operation, not only
each native page call.

A second optimized sample used `benchmarks/traversal.mjs`: 16,384 records, 32 seed commits yielding
12 actual segments, 3,906-byte IDs, 4,096-byte versions, and 76 surface terms within one edit of
`widget`. A retained reader traversed all 16,384 ordinary/fuzzy-prefix hits across 128 pages while a
writer published updates and an ordinary search completed alongside every page. Accumulated
paired-call time was approximately 281 ms ordinary and 314 ms fuzzy-prefix; exact prefix returned
its expected 216 matches. Process RSS sampled after modes was approximately 413–423 MB, not peak or
collector-only memory. An oversized 2,000-hit response rejected and a 128-hit retry succeeded.
Snapshot, sequence, version, total and byte-recovery assertions are repeated by the smaller smoke
profile in the platform CI matrix; remote CI results remain a release prerequisite.

These native-only samples exclude Harper source projection, its JavaScript heap workload,
authorization, and RocksDB cache contention. Production capacity must be qualified in the host
environment. Smoke CI reports the actual segment count without requiring a merge-scheduler timing
outcome; the full qualification requires multiple segments, and the Rust collector tests retain
deterministic multi-segment coverage.

## Traversal token integrity and preparation

Version-3 continuations authenticate the header and full snapshot/score/ID boundary together with
the query digest. Contents are not encrypted; the boundary ID and score are readable. Valid-looking
finite boundary edits, token splicing and older tokens reject with `E_INVALID_ARGUMENT`; tokens
still do not grant authorization. Page size and execution budget remain changeable.

Query identities up to 64 KiB hash on the JavaScript thread. Larger accepted identities use
asynchronous WebCrypto. The threshold changes scheduling, not accepted input size.

Each object limits concurrent large-request preparation by its configured search queue limits, with
ceilings of 128 commands and 8 MiB of retained frames. These limits exclude WebCrypto copies and
caller-owned input. Native status and process reservation counters also exclude preparation. Small
synchronous pages bypass preparation capacity. Encoding happens before reservation, so a rejected
request can briefly allocate a request-sized frame.

`E_QUEUE_FULL` identifies preparation overload in its message. A frame larger than the configured
byte capacity cannot fit even when the handle is idle; retrying it unchanged cannot succeed. Native
queue admission has the same single-frame rule. Reduce the request or raise capacity within the
process budget, and keep retries bounded.

Reservations release after hashing settles and before native submission. The facade then rechecks
close state and remaining time. WebCrypto shares the libuv pool, so other pool work can delay it;
close does not cancel a dispatched hash.

One optimized macOS arm64 / Node 24 before/after pair using `benchmarks/traversal-preparation.mjs`
(30 measured calls after warmup) observed large-query synchronous submission median 5.72 → 3.12 ms
and total completion median 321.08 → 324.98 ms. This sample shows reduced synchronous preparation,
not an overall search speedup. Encoding/copying and native work remain; these observations do not
establish production latency bounds.

On 2026-10-17, three alternating optimized pairs on macOS arm64 / Node v24.16.0 used
`benchmarks/ordinary-search.mjs` to compare baseline `a162f80890cd7516ba8e4fd21a7d329531ac58d0`
(0.5.0) with the pre-copy-removal traversal implementation, using 10,000 records and limits 20 /
1,000. Empty-query median times were 14.00 / 10.92 µs per call on the baseline and 14.18 / 11.58 µs
on traversal. The relative empty-query changes (+1.3% / +6.0%) vary despite equivalent zero-hit
work; sparse changes also cross zero (−2.7% / +1.2%), and broad changes were −0.1% / −0.8%. These
samples do not resolve the cost of the facade allocation and are not production latency bounds. The
ordinary path now passes its existing packed request directly to the encoder, removing the
unnecessary spread/copy; no post-removal speedup is claimed.

The shared encoder still returns one small frame-metadata object on ordinary searches. This
measured-path tradeoff is accepted; the samples above do not justify specializing the encoder API
solely to remove it.

### Local traversal driver comparison

On 2026-10-18, optimized macOS arm64 / Node v24.16.0 runs compared ordinary `search` and the first
`searchPage` on one retained snapshot: 65,536 equal-length two-term records, query `widget catalog`,
exact totals and 128 hits. Each run alternated method order for 60 calls per method after 10 warmup
pairs, asserting identical scored hits. `benchmarks/traversal-driver.mjs` reproduces this profile.

Three pre-cleanup runs at `0f3bf3f` observed ordinary medians 0.63–0.68 ms and traversal medians
0.74–0.79 ms (ratios 1.17–1.18). Three runs after moving the snapshot check and hoisting the
alive-bitset accessor observed 0.56–0.61 ms and 0.65–0.70 ms (ratios 1.13–1.17). These sequential
local samples include all facade and native work; they do not isolate dispatch cost or prove a
causal speedup from the hoist. Traversal still owns its interruptible boxed-scorer loop, and full
traversal still rescans matches per page. Broader query reuse and pruning remain separately tracked;
no production latency bound is established.

### SHA-256 snapshot qualification

The version-3 response body reserves 82 bytes before hit payloads (`13 + 4 + 64 + 1`), including its
64-character snapshot. This is 48 bytes more than the earlier unreleased version-2 traversal header;
the same 8 MiB body ceiling applies. Reduce page size and retry the original continuation when large
IDs or versions exceed it.

An optimized native Rust qualification on 2026-10-18, macOS arm64 / Rust 1.90.0, compared pre-SHA
`f9ec4a4` with the SHA-256 engine in `61fd1ad`. Both used the identical 512-record, two-term fixture
with `NoMergePolicy`, exactly 1 or 256 segments, and page sizes 1 or 32. Each of three alternating
before/after process pairs performed two warmup walks and five timed full walks per shape; every
walk asserted exact totals and the complete scored sequence against ordinary search. This excludes
JavaScript, N-API, libuv and host projection.

| Actual segments | Page size | Before: median full-walk range | SHA-256: median full-walk range | Paired change range |
| --------------- | --------- | ------------------------------ | ------------------------------- | ------------------- |
| 1               | 1         | 5.83–7.63 ms                   | 5.77–7.78 ms                    | −1.0% to +2.0%      |
| 1               | 32        | 0.210–0.245 ms                 | 0.227–0.247 ms                  | +0.7% to +9.0%      |
| 256             | 1         | 288.7–294.4 ms                 | 296.6–296.9 ms                  | +0.8% to +2.8%      |
| 256             | 32        | 9.28–9.41 ms                   | 9.50–9.57 ms                    | +1.3% to +2.3%      |

These are local whole-engine observations, including per-segment UUID formatting and SHA-256, not
isolated hashing timings or production bounds. The small fixed-work samples vary; no causal claim or
statistical latency guarantee follows from three pairs. The measured many-segment cost is accepted
to strengthen snapshot identity; repeated full scans remain the larger separately tracked scaling
cost. Reproduce the fixture with `cargo test --release --lib --no-default-features traversal_many_segment_qualification -- --ignored --nocapture`.

The post-SHA public Node first-page driver also passed the existing 65,536-record profile: one
sample observed 0.565 ms ordinary and 0.658 ms traversal medians (ratio 1.165). It includes the full
facade/native path but does not qualify the many-segment case above.

## Direct Rust API compatibility in 0.6

The public Rust `SearchRequest` adds required `traversal` and `after` fields. Existing ordinary
requests initialize `traversal: false` and `after: None`. `Engine::search` now enforces the same
checked `offset + limit <= 10,000` window as the packed API; direct Rust requests that previously
exceeded it return `E_INVALID_ARGUMENT`.

For deeper ranked traversal, Rust callers use `Engine::search` with `traversal: true` and offset
zero, retaining their original Tantivy `Searcher`. When `SearchResult.traversal` reports more
results, construct the next `SearchBoundary` from that snapshot string and the last hit's score/ID.
The native many-segment qualification demonstrates this flow. These Rust source/API changes
accompany the pre-1.0 package change from 0.5 to 0.6; they do not introduce a `searchPage` method on
`Engine`.

## Direct Rust API compatibility for the unreleased metadata APIs

Direct Rust `protocol::Limits` literals now also supply `max_query_terms: 64` and
`max_query_clauses: 256` for default policy, or validated configured values. They are operational
settings and do not alter persisted identity.

When migrating from released 0.7.x, ordinary `protocol::SearchRequest` literals add `sort: None`
and `aggregations: Vec::new()`. Catalog callers use `catalog::SortRequest` and
`catalog::AggregationRequest` (with child `aggregations: Vec::new()` for leaves). Sorted requests
use `sort: Some(vec![sort_request, ...])`; `Engine::search` returns named results in
`SearchResult.aggregations`. Obtain `SearchResult` through the engine rather than constructing
it: its cached wire representation is crate-private.

Exhaustive matches must handle `FilterExpression::Not(Box<FilterExpression>)` and
`FilterComparator::{Exists, Missing}`. Presence clauses carry `values: Vec::new()`.
`SearchResult` and the filter enums are source changes to the pre-1.0 Rust API; the JavaScript
facade uses packed protocol 7, native ABI 12, and query API 6 with its matching addon.

## Source reference

- [Native bounds and request decoding](../src/protocol.rs)
- [Advertised runtime limits](../src/lib.rs)
- [Analyzer, query, and collector behavior](../src/engine.rs)
- [JavaScript preparation and mutation framing](../ts/native.ts)

## Metadata collectors

`runtimeInfo().limits` exposes aggregation depth, bucket count, native aggregation memory, sort-key
bytes, and term-key bytes. The unreleased `maxAggregations` and `maxTermsSize` fields are removed
with query API 6; request shape is governed by resource budgets rather than those fixed counts. Key preflights
first use supported maximum lengths and per-segment document/dictionary counts. When that estimate
exceeds its budget, a streaming dictionary scan measures maximum key lengths and retries the bound.
Completed measurements are cached by immutable segment and field, with at most 1,024 entries per
process. A completed scan populates the cache before its final deadline check, so a request that
times out at that check can still help a later request. Cache eviction affects performance, not results. Cold scans check the request deadline every 1,024 keys and at completion,
and retain only a maximum length; high cardinality alone does not exceed a qualification-count
limit. Sorting first tries the contractual ID bound, measuring IDs only if that still cannot
qualify the request. If the full-segment sort estimate still fails, a deadline-aware native Count
pass measures the global matching count and uses it as an upper bound on retained hits across
segments, together with maximum measured key lengths, to retry the sort budget.
The sort continues to use native TopDocs. Reduce the window or terms size when retained keys exceed their budget;
independent top-k requests do not produce a globally equivalent answer.

| Resource                           | Bound                                                   | Enforcement                                                                                                                                                                                                                                                     |
| ---------------------------------- | ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sort fields / aggregation siblings | Encoded request budget; no small fixed count cap        | Sort keys must be distinct. Aggregation names are 1–128 UTF-8 bytes and unique among siblings.                                                                                                                                                                  |
| Aggregation depth                  | 32 levels                                               | Independent facade, wire, validation, collector and result walkers reject deeper trees.                                                                                                                                                                         |
| Terms size / explicit range count  | Positive u32 size; nonempty ranges within request bytes | Checked tenfold native candidate oversampling; terms remain approximate across segments. Large requested sizes can succeed on low-cardinality fields.                                                                                                           |
| Returned aggregation buckets       | 1,024 across the entire tree                            | Native global bucket guard; overflow rejects with `E_RESOURCE_LIMIT`. No partial histogram/range response.                                                                                                                                                      |
| Collector construction             | 16 MiB conservative admission estimate                  | Charges request nodes, range setup and segment count before constructing collectors. Nested state also charges possible parent instances, including stats vectors that Tantivy does not charge to its native guard.                                             |
| Native aggregation structures      | 16 MiB estimated, shared across segment collectors      | Tantivy guard; not total process memory and not every decoded key.                                                                                                                                                                                              |
| Decoded term keys                  | 16 MiB conservative preflight                           | Per-segment candidates are bounded by oversampling, dictionary size and observed values. Descendants multiply by possible parent instances; each candidate charges maximum key bytes plus overhead. Dictionary scans refine maximum lengths under the deadline. |
| Retained metadata sort keys        | 64 MiB conservative preflight                           | Charges every requested key, vector storage, native top-k buffers, IDs and global decoded keys across segments. Existing 10,000-window and prefix limits apply.                                                                                                 |
| Request / response                 | Existing 8 MiB each                                     | Input normalization charges bytes incrementally; result encoding charges all descendants. A conservative per-node output estimate and construction cost are checked before collection. Oversized output rejects with `E_RESULT_TOO_LARGE`.                      |
| Numeric arithmetic                 | Finite input/output; representable histogram positions  | Invalid intervals/ranges reject with `E_INVALID_ARGUMENT`; arithmetic/capacity overflow rejects with `E_RESOURCE_LIMIT`.                                                                                                                                        |

Sort dispatch uses native typed computers for one/two fields, native erased-type tuples for
three/four fields, and runtime lists for five or more. Four-field native keys use nested pairs
to retain configured comparators. All paths share the 64 MiB sort budget. Admission charges
retained decoded keys across segments, fixed top-k/merge slots at actual executor concurrency,
and dynamic scratch/key-vector storage. Global merging moves retained payloads; only up to two
additional threshold payload copies are charged, independently of the result window. Dynamic
scratch and threshold vectors are charged per active segment. This is a conservative admission
estimate, not a process-residency bound.

Histogram-parent admission first bounds actual possible bucket positions rather than charging every
numeric value as a separate bucket. If dictionary and column bounds still cannot qualify a tree,
bounded native histogram probes can refine those bounds using the original query and native
ancestor-presence predicates. Probes share the deadline and native memory/bucket guards; they add
collection work only on this admission fallback. An inconclusive probe retains the conservative
bound. Nested histogram validation ignores documents missing an ancestor bucket field.

Static construction costs, conservative per-node result estimates and known range-only bucket counts are checked
before query construction or ranking. Actual bucket cardinality and serialized key bytes depend
on matching data: native guards and encoding enforce those limits during/after collection. A high
requested terms size is not itself a rejection reason; it can return a few categories or none.
Pre-rejecting every possible oversized output would reinstate feature caps or reject empty/selective
queries. These are bounded failures, not a promise that every rejected request avoids collection.

The metadata collector checks the original request deadline before setup, at segment boundaries,
every 1,024 visited matches, and around final merge. Native scorer setup, individual dictionary
conversion, and native aggregation merge remain synchronous within those checkpoints. Sorted hits,
exact totals and requested aggregates share one collection pass. Aggregation-only requests retain
the ordinary ranked-hits path and use a separate full-set pass. When exact totals need counting,
that pass also counts matches instead of running a third pass. Wire encoding, including the success envelope, stays inside the
deadline and expensive-search permit, with a final deadline check before returning. This is a
cooperative deadline, not a hard wall-clock or memory-residency guarantee. Deleted documents are
excluded using the snapshot's deletion mask. Ordinary exact-total Count passes also use the deadline wrapper.

String metadata adds a raw fast column. The writer stores repeated strings once per document
to avoid redundant fast-column values, preserving first occurrence for sorting. Native terms
aggregation independently counts each distinct string once per document. Upgrading a released index containing string metadata requires a
rebuild to sidecar v8. Numeric/Boolean-only metadata keeps sidecar v7; text-only keeps v6.

## Query capacity policy

`limits.maxQueryTerms` accepts integers 1–1,024 and `limits.maxQueryClauses` accepts integers
1–4,096, defaulting to 64/256 when omitted. These are host-selected operational settings, not
persisted schema identity or search-request overrides. `queryLimits` reports each handle's frozen
policy. Runtime capability metadata retains the default bounds and advertises supported ceilings
separately. Apply one policy consistently across workers; reopen to change it without rebuilding.

Terms are counted after analysis per text leaf: ordinary ANY/ALL deduplicates analyzed terms;
phrase/prefix position analysis retains positions as before. Compiled text-clause accounting
includes selected fields, fuzzy branches, prefix expansion and Boolean overhead. Structural search
and filter trees independently retain 256 leaves/children and filter values, with depth eight.
The configurable compiled-text limit does not combine text and structured-filter costs. Tracing
applies analyzed-term limits to its simple text clause, without compiling indexed clauses.

Whole-query construction additionally permits at most 16,384 phrase term-field positions and
256 fuzzy term-field automata across all branches. These fixed guards preserve the default policy's
implied construction bounds; raising term/clause policy does not raise them. Construction overflow
rejects explicitly. Individual Tantivy setup operations remain synchronous between deadline checks.
Phrase tracing checks its deadline while matching anchors/positions; its cost still depends on the
source and phrase shape. Finite qualification does not make a hard callback-time guarantee.

On higher-policy handles, the native scanner keeps conservatively small ASCII queries ordinary;
ASCII token-run/selected-field estimates exceeding default bounds, and non-ASCII text, use expensive
admission without analyzing on the event loop. Default-policy classification is unchanged. The
query-capacity qualification driver reports process CPU, sampled RSS, latency and recovery at
1/4/8 callers; this is not a production service objective or sustained-memory qualification.
