# Native search collection

Status: verified against Tantivy 0.26.2 source and local macOS arm64 tests · Owner: Harper Engineering · Last verified: 2026-10-18

`TraversalScoreTieCollector` owns its scorer loop so it can return `E_TIMEOUT` between document batches. Keep the ordinary `StableScoreTieCollector` independent: traversal-only boundary and deadline checks must not enter its per-match path.

Traversal must use the same scoring topology on every page. In pinned Tantivy 0.26.2, `BooleanWeight::for_each` and `BooleanWeight::scorer` both construct `BufferedUnionScorer` for term unions; `into_box_scorer` performs that construction for the latter. Preserve this equivalence when upgrading Tantivy: a different floating-point summation order can change score ties and continuation boundaries.

The test `traversal_retains_a_merged_snapshot_and_stops_collection_on_deadline` enforces collection-stage expiry and preservation of an old searcher through a forced merge. Public traversal tests compare complete scored sequences against ordinary search, including filtered, candidate, Boolean, and zero-score queries.

The facade authenticates token version/handle and snapshot/score/ID bytes plus a fixed query digest using HMAC-SHA256 and an object-local 32-byte key minted before asynchronous work. Version-2 tokens reject boundary edits and splicing. Native handle numbers restart in a new process, so the number and segment fingerprint alone cannot enforce reader/process binding; the cross-process replay test uses handle 1 in both processes.

Query identities up to 64 KiB hash synchronously; larger identities use WebCrypto under bounded preparation admission. The threshold selects execution, not accepted capacity. Preparation frame bytes are capped by the configured search queue bytes and the existing request-frame ceiling; command capacity also respects configuration and a frame/threshold-derived ceiling. Large-hash admission is checked at reservation after encoding, including after caller-supplied accessors can reenter; small synchronous pages do not consume or wait for this capacity. Hash reservations release before native submission. After asynchronous work, closing/closed state and the remaining monotonic budget must be rechecked before invoking native. Native status counters exclude this JavaScript stage; preparation overload errors identify it explicitly.

The protocol decoder and public Rust engine share `validate_search_window`: offset capacity and traversal page capacity are separate contracts, and checked arithmetic must precede collector allocation. Native/TypeScript boundary tests cover advertised page/filter limits; analyzer recall and trace completeness are qualified in [the limits guide](../docs/limits.md).

Both search paths use `search_response_bytes` to reject non-finite computed scores before exposing hits or continuations. Finite field weights can overflow BM25 scores; the recovery regression reopens the same data with lower weights.

Traversal checks its segment/deletion snapshot before constructing a query against that snapshot. A stale prefix continuation must return `E_CURSOR_EXPIRED` even if newly indexed terms would make the original prefix too broad; the public regression covers writer publication and reader reload.

The facade lazily reads native candidate capacities once per object and passes them to the existing frame encoder for traversal. Original list count and UTF-8 bytes are bounded before sorting a copy; duplicate multiplicity and frame size remain unchanged. JS UTF-16 ordering is only the deterministic request-identity convention, not hit ordering. Fields and query clauses retain their scoring order; ordinary search keeps its existing encoding. No Rust-side candidate canonicalization is required.

## Open items

Linux and Windows behavior is for CI; local verification covers macOS arm64. Individual Tantivy operations and query construction remain uninterruptible.

## Sources

- [Collector and native tests](engine.rs)
- [Facade and token identity](../ts/native.ts)
- [Public API tests](../test/native-traversal.test.mjs)
- [Pinned Tantivy version](../Cargo.toml)
