---
layers: universal, repo-type/native-addon
---

## Repo-specific checks (@harperfast/fulltext)

This is a Rust/Tantivy N-API addon (`napi-rs`, npm-only, Node.js-only) with a
hand-written TypeScript facade. The `repo-type/native-addon` layer carries the
common native-addon checks (its C++/`binding.gyp`, pnpm, Bun/Deno,
`test:stress`, `pnpm bench`, and `MaybePromise`/msgpackr items do not apply
here). On top of it:

- **Use the repository's real gates.** `npm run check` covers Prettier,
  TypeScript, Clippy, Rust tests, Node tests, examples, and package checks.
  Dependency changes must preserve locked builds, `cargo deny check`,
  `npm audit --audit-level=high`, and panic containment.
- **Keep generated declarations private.** Binding changes regenerate and
  commit `ts/addon.d.ts`; consumers use only the hand-written public facade.
- **Test source modules directly.** New source modules need direct tests;
  exercise changed behavior through the public Node boundary where practical.
- **Keep sustained native work off JavaScript.** Search, indexing, commit,
  and request-path storage stay off the Node event loop. Check actor ownership,
  teardown, bounded queues, resource budgets, and one writer per index. The
  documented one-time addon load, process-budget configuration, and bounded
  lifecycle inspection are allowed synchronous exceptions.
- **Tantivy owns the search engine.** Flag duplicated scoring, postings,
  query execution, collection, merge, or storage behavior unless a wrapper
  contract requires it and measured evidence justifies the specialization.
- **Native filesystem storage only.** RocksDB and Harper-specific projection,
  replication, readiness, and authorization do not belong in this package.
- **Preserve search and durability contracts.** Check stable score-then-ID
  pagination, exact and bounded totals, checkpoints, reopen/reset behavior,
  schema compatibility, and fail-closed index invariants.
- **Preserve release packaging.** Versions stay in lockstep across Cargo,
  the root package, and platform packages. Linux x64/ARM64, macOS ARM64,
  and Windows x64 clean-consumer tests remain covered. Do not add install
  scripts or build-from-source fallback.
- **Qualify performance claims.** Compare the same workload, architecture,
  build mode, and controlled hardware. Shared-runner timing is not a gate.
