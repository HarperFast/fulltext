# Native search collection

Status: verified against Tantivy 0.26.2 source and local macOS arm64 tests · Owner: Harper Engineering · Last verified: 2026-10-18

`TraversalScoreTieCollector` owns its scorer loop so it can return `E_TIMEOUT` between document batches. Keep the ordinary `StableScoreTieCollector` independent: traversal-only boundary and deadline checks must not enter its per-match path.

Traversal must use the same scoring topology on every page. In pinned Tantivy 0.26.2, `BooleanWeight::for_each` and `BooleanWeight::scorer` both construct `BufferedUnionScorer` for term unions; `into_box_scorer` performs that construction for the latter. Preserve this equivalence when upgrading Tantivy: a different floating-point summation order can change score ties and continuation boundaries.

The test `traversal_retains_a_merged_snapshot_and_stops_collection_on_deadline` enforces collection-stage expiry and preservation of an old searcher through a forced merge. Public traversal tests compare complete scored sequences against ordinary search, including filtered, candidate, Boolean, and zero-score queries.

## Open items

Linux and Windows behavior is for CI; local verification covers macOS arm64. Individual Tantivy operations and query construction remain uninterruptible.

## Sources

- [Collector and native tests](engine.rs)
- [Public API tests](../test/native-traversal.test.mjs)
- [Pinned Tantivy version](../Cargo.toml)
