# Design notes

- [Native search collection](src/DESIGN.md) — traversal deadlines, scoring topology, and ordinary-search isolation.

- [Metadata collectors](src/DESIGN.md#metadata-collectors) — full-match snapshot ownership, native collector reuse, resource limits, and string-only schema migration.

- [Writer close ownership](src/DESIGN.md#writer-close-ownership) — join original native merges before releasing a generation or allowing reset.

- [Qualification measurement](benchmarks/DESIGN.md) — fixed-generation reader leases and bounded-memory soak diagnostics.

- [Query capacity policy](src/DESIGN.md#query-capacity-policy) — immutable operational limits, construction guards, and raised-profile admission.
