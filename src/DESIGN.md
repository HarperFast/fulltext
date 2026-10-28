# Native search collection

Status: verified against Tantivy 0.27.0 source and local macOS arm64 tests · Owner: Harper
Engineering · Last verified: 2026-10-27

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

Observed: Rust and public Node tests pass locally on macOS arm64, 2026-10-27.

`Engine::search_inner` builds the query on the supplied reader snapshot. Sorting and flat
aggregation reuse that query; aggregation never runs over the returned hit page.
`native-metadata.test.mjs` pins filtered/candidate counts and retained-reader behavior.

Only string metadata gains a fast column and per-document deduplication. `identity_bytes`
emits v8 only for string configurations; numeric/Boolean-only v7 and text-only v6 remain
compatible. The public identity-migration regression checks open, inspection, and reset.

`catalog::DeadlineCollector` wraps only metadata collectors. Its deletion-aware loop keeps
native top-k/aggregation ownership and checks the shared deadline every 1,024 visited matches.
`deadline_interrupts_an_active_collection` verifies expiry after collection starts.

Native aggregation structure accounting excludes some decoded keys. `sorted_hits` and
`aggregate` perform separate worst-case key preflights before creating collectors;
`encode_results` bounds the response. Catalog resource tests pin rejection rather than
partial output. See [metadata resource policy](../docs/limits.md#metadata-collectors).

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
