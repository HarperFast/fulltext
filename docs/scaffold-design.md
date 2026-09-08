# Fulltext package architecture

## Scope and implementation status

The scaffold and native Tantivy backend are implemented. The library delivers standalone
`@harperfast/fulltext/native` and a planned `@harperfast/fulltext/harper` integration using Harper's
existing RocksDB storage APIs. The Harper export is not implemented yet.

One Rust addon and shared engine serve both modes. Native mode delegates file I/O and locks to
Tantivy MmapDirectory. Harper owns records, schema, derived delivery/replay, storage registration,
authorization and lifecycle. The integration adapts Tantivy Directory operations to supported
Harper storage access through a bounded transport.

[Harper storage integration](harper-storage-integration.md) defines the current storage proof,
architecture, publication/recovery invariants, performance plan and remaining decisions.
The prior native-lease prototype is recorded in
[the historical Phase 0 experiment](phase-0-rocks-bridge-plan.md); it is not a production dependency.

## Package boundaries

- Keep the existing independent fulltext Rust/npm release lifecycle, following HNSW and Symphony
  packaging patterns where applicable.
- Share engine, query construction, codec, native scheduling, commit/reload and error handling.
- Keep generated bindings private behind TypeScript façades. Add exports only with tested behavior.
- Native mode remains independently usable. Harper has no native-file fallback.
- No public `/rocks` factory, rocksdb-js peer or second linked RocksDB runtime is planned.
- Preserve Apache-2.0 metadata, exact dependency pins and reproducible builds.
- Do not add placeholder modules or a separate backend artifact matrix.

## Native implementation

[Native backend implementation](native-backend-implementation.md) describes the current actor,
bounded search execution, batch codec, schema, close behavior and reference benchmark.
The native API and implemented limits in README and source remain the user-facing contract.
The broader designs describe future work and do not imply that all query capabilities have shipped.

## Verification

Use the repository's existing Rust, Node, formatting, lint, packed-install and platform-loading
checks. Retain the backend-parameterized Directory harness and its negative controls. Harper
qualification adds real storage, crash/reopen, backup/restore, multi-worker lifecycle and derived
replay tests, not a mandatory test against a patched rocksdb-js build.

Performance comparisons use the implemented native reference and the actual Harper integration,
with engine/storage and product scopes reported separately. PR CI runs correctness and smoke
profiles; stable hardware supplies comparable scheduled and release evidence retained in GitHub.
Developer documentation and examples are tested against packed artifacts and supported Harper
versions.

## Approaches considered

- Different layer: placing the engine inside Harper would tie native builds and standalone usage to
  Harper's release lifecycle. Keep the independent library and put Harper policy in the host.
- Deeper cause: a new native rocksdb-js lease would solve one proposed transport design, but it is
  not approved. Prove the synchronous-native to supported-host-storage boundary instead.
- Do less: reuse the shipped native backend and omit standalone Rocks support and the mandatory
  third benchmark arm.
- Chosen: one engine, native storage and a Harper-specific storage/delivery integration, with the
  actual Harper vertical slice establishing feasibility before the factory is frozen.
