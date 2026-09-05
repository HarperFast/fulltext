# Fulltext package scaffold

- **Issue:** [HarperFast/fulltext#9](https://github.com/HarperFast/fulltext/issues/9)
- **Status:** implementation plan
- **Base:** `origin/main`
- **Initial engine:** Tantivy 0.26.1

## Objective

Establish the repository, build, package, and test architecture for `@harperfast/fulltext` without
prematurely implementing the search engine or committing to unresolved rocksdb-js bridge details.
The target architecture supports one shared Rust addon behind two explicit package entry points:

- `@harperfast/fulltext/native`, backed by Tantivy `MmapDirectory`;
- `@harperfast/fulltext/rocks`, backed by a future `RocksDbDirectory` over a caller-owned
  rocksdb-js lease.

Harper will consume only the Rocks entry point. The native entry point exists for standalone use,
directory conformance, and comparative performance measurement. Issue #9 exports only the tested
native entry point. The Rocks entry point is added after Phase 0 defines and tests its lease.

## Design assessment

This work establishes a public package API, a JavaScript/Rust boundary, and a future
fulltext/rocksdb-js native boundary. Those are durable interfaces, so the scaffold requires a
reviewed design and must keep unresolved storage details behind explicit seams.

The invariant is: **backend selection is explicit and never falls back, all indexing and search
behavior is implemented once, the addon links no RocksDB symbols, no native panic crosses Node-API,
and no opaque native lease is used before its identity, layout, ABI, capabilities, and lifetime are
validated.**

## Source-grounded baseline

The plan was checked against:

- HarperFast/hnsw at `42c71850536afce0b72d7511158fe600cd0879c6`: Rust `cdylib` plus
  `rlib`, a narrow Node binding, Rust tests, Node smoke coverage, and platform artifacts;
- HarperFast/symphony at `eb2c1b6760605f908475345672f9cab3ae5548cc`: napi-rs artifact
  packaging, generated low-level declarations, a TypeScript façade, multi-platform CI, and
  integration testing;
- Tantivy 0.26.1 at `d8f4c0b703120ed98f06297724dc1522df6019b9`: the pinned engine and
  `Directory` contract;
- rocksdb-js 2.8.0 at `7ab102ca3e9600343bcefe6f19204b111836ec52`: the current caller-owned
  database and column-family baseline.

The scaffold reuses those patterns selectively. HNSW's hand-written loader is not copied when
napi-rs artifact packaging supplies the same function. Symphony's generated native declarations
remain private rather than becoming the user API. Neither reference determines the unresolved
Rocks lease or durability contract.

## Repository shape

```text
.
├── Cargo.toml
├── Cargo.lock
├── build.rs
├── package.json
├── tsconfig.json
├── rust-toolchain.toml
├── rustfmt.toml
├── src/
│   ├── lib.rs
│   ├── boundary.rs
│   └── directory_harness.rs
├── ts/
│   ├── addon.d.ts
│   ├── shared.ts
│   └── native.ts
├── test/
├── docs/
└── .github/workflows/
```

The first scaffold commit creates only enough source to build and smoke-test the package. Empty
architectural directories are not added. Modules appear with the behavior and unit tests that
justify them.

## Package and API boundaries

`package.json` initially exports only the backend exercised end to end:

```json
{
	"exports": {
		"./native": {
			"types": "./dist/native.d.ts",
			"import": "./dist/native.js"
		}
	}
}
```

The TypeScript façade is the public API. Generated napi-rs declarations describe the low-level
addon only. Public types must not expose Tantivy, napi-rs, or RocksDB implementation types.

The native entry point loads the shared artifact and calls only the native factory. A later Rocks
entry point loads the same artifact and calls only the Rocks factory. There is no generic public
factory, storage option bag, or fallback between them. Issue #9 neither exports the unimplemented
Rocks subpath nor invents the lease shape before Phase 0 establishes it.

The future lease has non-negotiable safety constraints now: it is a type-tagged N-API `External`
carrying a fixed magic value, integer ABI version, structure size, capability set, and a token
generated for each loaded rocksdb-js addon instance. The wrapper validates every field before
calling through the table; a
mismatch returns a stable coded JavaScript error. The lease exposes an opaque handle and C-ABI
function table owned by rocksdb-js, never a `rocksdb::DB*`, numeric pointer, untagged external, or
Rust/C++ implementation type. It retains rocksdb-js lifetime ownership for every admitted operation;
database close rejects new work and joins or safely revokes all holders before releasing handles.

## Rust boundary

The crate builds as:

- `rlib` so engine and storage behavior can be tested directly in Rust;
- `cdylib` for the Node-API addon. The initial artifact contains only implemented native code; the
  same artifact gains RocksDbDirectory only when that backend is tested and exported.

The initial N-API surface is deliberately small: typed package/version/ABI capability reporting
proves loading and generated declarations without JSON serialization. The public façade exposes it
as a Promise so no future work API inherits a synchronous public signature. The first call may
synchronously load the native addon before performing constant-time introspection; sustained
blocking, `block_on`, and CPU- or I/O-bearing synchronous exports are forbidden. Issue #17 introduces the bounded
package-owned executor before any search, indexing, commit, or storage operation is exposed.
Packed operations use borrowed request buffers and transfer owned `Vec<u8>` responses rather than
JSON strings. Batch operations are the default shape; the façade never invokes native code once per
document. Work is admitted to a bounded package-owned pool and never creates a runtime task or
thread per call.

The release and benchmark profiles use thin LTO, one codegen unit, and `panic = "unwind"`.
The addon does not install a global allocator because doing so could change allocation behavior for
the host Node process. Every package-owned N-API boundary invokes a shared `catch_unwind` adapter
that converts a panic into a JavaScript error with a stable native `code`; every exported function
and method, including constructors, also enables napi-rs 2.16's generated `catch_unwind` wrapper so
argument and result conversion remain inside an unwind boundary. A source-level CI gate rejects new
Node-API functions without that wrapper. The outer wrapper is a process-safety net: conversion-time
panics use napi-rs error mapping and do not participate in handle poisoning. A test-only Cargo
feature exposes a handle-level panic probe to the Node
smoke suite, but the probe and feature are absent from published artifacts. All public errors carry
stable codes from the first release.

Before implementing the adapter, the pinned napi-rs behavior is verified so the wrapper neither
double-wraps panics nor replaces useful native error mapping. Every package-owned worker entry
catches and records its panic before completing its promise with a coded failure. Query depth and
complexity are bounded before native execution. Allocation failure, stack overflow, double panic
during unwinding, and a panic wholly inside an upstream-owned thread can still abort the process;
those paths are explicitly out of scope for `catch_unwind` and remain targets for input bounds,
upstream qualification, and process supervision rather than false error-containment claims.

Any panic caught by a stateful handle's inner boundary poisons that handle. It and all work derived from it fail fast
with `E_POISONED`; callers cannot retry through potentially corrupted Tantivy or adapter state.
Stateless capability inspection remains available, and an unrelated handle remains healthy. The
panic smoke route verifies the original coded failure, terminal handle poisoning, and isolation
from another handle. An operation completing after a concurrent panic also returns `E_POISONED`.
The poison flag is not a substitute for synchronization: an owning writer or other mutable handle
must serialize its state-changing operations before entering this boundary. Concurrent operations
are permitted only over immutable or independently synchronized state, so no successful operation
can observe a sibling mutation while that sibling is unwinding.
Caught panics still invoke Rust's process-wide panic hook before returning the coded error; Harper
logging must classify the subsequent error as contained rather than treating the hook output alone
as evidence of process failure. CI also asserts that the effective release profile retains
`panic = "unwind"`.

Tantivy is pinned exactly to 0.26.1. napi-rs is a build and binding dependency, not part of the
public API. The Cargo feature layout must allow Rust unit tests to exercise engine code without
requiring a Node environment.

## Build and packaging

The scaffold uses npm and committed `package-lock.json`, matching current Symphony practice and
Harper CI. Scripts cover:

- TypeScript build and type checking;
- debug and release native builds;
- Rust unit tests and Clippy;
- Node smoke tests;
- formatting checks;
- napi-rs artifact assembly.

The initial scaffold qualifies only the targets exercised end to end in CI:

- Linux x64 glibc;
- macOS arm64;
- Windows x64.

One addon artifact is built per platform rather than one per backend. Every matrix artifact is
loaded and smoke-tested on its target architecture; producing a file is
not sufficient. A missing or mismatched artifact fails with the resolved platform triple and never
falls back to an install-time source build or download. Linux arm64, Linux musl, and macOS x64 are
added to the manifest only with target-native loading coverage under the dedicated release issue.
Platform packages and publishing automation are completed there as well. No consumer install script
compiles or downloads code silently in this scaffold. `prepack` creates the host release artifact
and rejects any additional native artifact in the package root, preventing a stale or test-feature
cross-build from entering the tarball.

## Testing and end-to-end route

Every introduced source module has a direct test. The scaffold gates:

1. `cargo test`;
2. `cargo clippy --all-targets -- -D warnings`;
3. TypeScript type checking;
4. native addon build;
5. a Node smoke test that imports the public native entry point, awaits typed capability reporting,
   and proves a test-only native panic becomes a stable coded JavaScript error;
6. a backend-parameterized Rust `Directory` baseline harness, initially run against Tantivy
   `MmapDirectory`, covering concurrent atomic metadata visibility, missing-file error variants,
   in-process writer exclusion, backend-neutral open-handle deletion semantics, synchronous and
   asynchronous boundary reads, write termination, `meta.json` watch notification, and
   `sync_directory`; logical read-call and requested-byte counters prove the instrumentation and a
   fixed baseline for the harness's own operations, while the Rocks adapter adds physical
   fetched/copied-byte accounting around real indexing and search;
7. negative controls proving the harness rejects always-successful locks, failed partial metadata,
   and partial metadata exposed during a successful replacement;
8. a smoke test installed from `npm pack` output rather than the repository tree, proving the
   exports map, packaged files, addon resolution, stable errors, and platform artifact together;
9. package-content and loaded-addon inspection proving private generated bindings, unintended
   source artifacts, unlisted subpaths, test-only probes, consumer lifecycle scripts, and stale
   native artifacts are absent; CI also verifies committed generated declarations against a release
   build;
10. `cargo deny` gates for advisories, licenses, sources, bans, and duplicate native libraries,
    plus a policy-scoped npm audit.

All Cargo operations use `--locked`; npm CI operations use `npm ci`. The scaffold gates loading on
the executing architecture, not merely cross-compilation. Cross-process Rocks writer exclusion,
crash durability, physical read amplification, chunk publication, and bounded watch latency require
the concrete Rocks lease and remain mandatory Phase 0 tests. The scaffold does not claim to
exercise indexing, search, Rocks durability, cross-process concurrency, or catalog performance.

The end-to-end route for this issue is the Node smoke test against the built addon through the
published `./native` entry point. Rocks behavior is intentionally not observable end to end until
the Phase 0 bridge is selected.

## Dependency decisions

Production dependencies are limited to Tantivy and the napi-rs binding crates needed to build the
addon. TypeScript, Node types, napi-rs CLI, and formatting/lint tooling are development-only.
rocksdb-js is not installed by the native entry point and is not bundled into the addon. The future
Rocks entry point uses an optional peer relationship so the caller owns the qualified version.

The Cargo dependency graph must contain no RocksDB crate or linked RocksDB native library. CI checks
`cargo tree` and built-artifact linkage. All Rocks operations eventually call the versioned
function table supplied and owned by rocksdb-js.

Every dependency added by the scaffold is recorded in `dependencies.md` with its purpose and
whether it appears in the runtime, build, or development graph. CI checks the documented direct
dependency names against Cargo and npm manifests so the ledger cannot silently drift.

Before the first npm release, the release work reserves the qualified platform package names,
publishes them with provenance, and references each at the exact root-package version. The scaffold
does not resolve undeclared platform packages.

## Approaches considered

### Different layer: implement the wrapper inside Harper

Rejected because it prevents standalone rocksdb-js use, couples native artifact releases to Harper,
and makes the native reference backend a Harper concern. The Node wrapper is an independently
versioned deliverable with Harper as one consumer.

### Deeper cause: add fulltext behavior to the rocksdb-js addon

Rejected because it would make rocksdb-js own Tantivy, query behavior, and the fulltext release
cadence. That removes the cross-addon boundary but collapses two independently useful libraries
into one. The narrower fix is to keep all RocksDB calls and handle ownership in rocksdb-js while
exposing only the minimum versioned C-ABI capability table to fulltext.

### Do less: ship only the native entry point in issue #9

Chosen for this issue. The scaffold proves package loading, generated bindings, error containment,
and the Tantivy `Directory` conformance harness without exporting placeholder Rocks APIs or lease
types. The Rocks entry point is added only after Phase 0 selects and tests the bridge contract.

### Chosen: one shared addon with tested subpath façades

The long-term package uses one platform artifact and explicit `./native` and `./rocks` façades.
This keeps indexing, search, scheduling, errors, and lifecycle single-sourced; permits native
conformance and performance comparison; leaves Rocks ownership with rocksdb-js; and avoids a doubled
artifact matrix. The artifact links no RocksDB symbols. Harper imports and exposes only the Rocks
façade. The presence of compiled `MmapDirectory` code is not a storage fallback: no Harper code
calls its factory, and no generic factory can select it.

### Rejected bridge: link RocksDB into the fulltext addon

A second statically linked RocksDB runtime cannot safely operate on handles created by rocksdb-js
and can introduce incompatible vtables, allocators, static state, and teardown. The build therefore
enforces a zero-RocksDB dependency graph; all future Rocks operations call the versioned function
table supplied and owned by rocksdb-js.

### Alternative: compile one native artifact per backend

Rejected because it doubles every platform build and release artifact and creates a mode where one
backend compiles while the other silently drifts. Once the addon links zero RocksDB symbols and
backend choice is available only through explicit factories, binary separation does not prevent an
additional unsafe operation.

### Alternative: place fulltext behind a process boundary

Rejected because a sidecar cannot use the caller-owned RocksDB handle: the database is already open
for writing inside Harper's process. IPC would isolate panics but cannot satisfy the single-owner
storage requirement without adding a second store or moving Harper's primary database ownership.

## Storage seam considered

### Chosen: implement Tantivy Directory directly over RocksDB

Tantivy keeps its segment and metadata formats while the adapter maps logical files to RocksDB
objects. This avoids a second persistent store and permits bounded recovery from the caller-owned
database.

### Alternative: materialize Rocks-backed segments into a local MmapDirectory

Rejected because every activation and recovery would require a second physical copy, introduce
local-filesystem capacity and cleanup semantics into Harper, and add a checkpoint/materialization
protocol whose correctness is separate from Tantivy publication.

### Alternative: persist immutable segments in RocksDB and use local mmap files as a read cache

Rejected for the Harper release because each node needs local capacity proportional to the index,
startup requires cache hydration, and correctness acquires an additional cache lifecycle. Phase 0
may retain this as a comparative benchmark because it avoids a RocksDB lookup and copy on every
Tantivy range read, but it cannot become an implicit fallback.

### Alternative: checkpoint RamDirectory into RocksDB

Rejected because index residency and rebuild memory would scale with index size. Product catalogs
with hundreds of millions of records cannot use whole-index RAM as their persistence staging
contract.

The direct Directory remains a Phase 0 proof obligation rather than an assumed success. If it
cannot satisfy Tantivy semantics and the performance gates, Harper does not release the feature.
Phase 0 must also assign durability ownership: whether objects use WAL, which operation implements
`sync_directory`, what makes object bytes durable before metadata publication, and which
capability reports that guarantee. A commit cannot be reported durable until that sequence is
proven by crash tests. The reserved durability capability must express the invariant that all
segment bytes become durable before atomic `meta.json` publication; successful commit reporting
comes only after that publication is durable.

## Sequencing

1. Land the minimal buildable Rust/Node scaffold and native smoke route.
2. Define the public façade and packed operation ABI in
   [#14](https://github.com/HarperFast/fulltext/issues/14).
3. Implement bounded runtime and writer coordination in
   [#17](https://github.com/HarperFast/fulltext/issues/17).
4. Implement the native reference backend in
   [#16](https://github.com/HarperFast/fulltext/issues/16).
5. Resolve the minimum Rocks bridge through
   [#7](https://github.com/HarperFast/fulltext/issues/7) and
   [HarperFast/rocksdb-js#834](https://github.com/HarperFast/rocksdb-js/issues/834) before fixing
   its public lease surface.

## Out of scope

- Search/index implementation.
- A concrete Rocks lease ABI.
- Harper schema or query integration.
- Performance claims beyond build and smoke overhead.
- npm publication.
- Treating native storage as a Harper fallback.
