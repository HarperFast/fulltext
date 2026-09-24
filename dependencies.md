# Dependencies

Direct dependency versions are exact so the native artifact is reproducible and upgrades are
reviewed deliberately.

## Rust runtime and build graph

| Dependency          | Scope                           | Purpose                                                                    |
| ------------------- | ------------------------------- | -------------------------------------------------------------------------- |
| `tantivy` 0.26.1    | runtime                         | Full-text indexing and search engine, including native filesystem storage. |
| `napi` 3.13.0       | optional runtime                | Node-API values, error conversion, and addon-image retention.              |
| `napi-derive` 3.6.9 | optional build/runtime boundary | Generates Node-API exports.                                                |
| `napi-build` 2.5.0  | build                           | Configures platform-specific addon linking.                                |

The Rust dependency graph must not include RocksDB. Harper uses the native filesystem backend for
its rebuildable derived index rather than linking a second RocksDB runtime into this addon.

napi-rs generates an outer unwind boundary only for exports marked `catch_unwind`. Every
fulltext function, method, and constructor uses that option to contain argument and result
conversion panics. That outer boundary uses napi-rs error mapping; the inner boundary adds stable
error codes and per-handle poison state for package-owned operations. The runtime also retains the
addon image before native actors can outlive a Node environment, preventing worker teardown from
unmapping code that those actors can still execute.

## JavaScript development graph

| Dependency            | Scope       | Purpose                                                               |
| --------------------- | ----------- | --------------------------------------------------------------------- |
| `@napi-rs/cli` 3.10.5 | development | Builds and names native artifacts and generates private declarations. |
| `@types/node` 24.10.0 | development | Type information for supported Node.js APIs.                          |
| `prettier` 3.6.2      | development | Repository formatting checks.                                         |
| `typescript` 5.9.3    | development | Compiles the public façade and declarations.                          |

The public entry point has no JavaScript runtime dependencies and does not depend on rocksdb-js.
Its optional native packages are exact-version platform artifacts for Linux x64 glibc, macOS arm64,
and Windows x64. They contain only the compiled addon and package metadata; keeping them separate
prevents consumers from downloading binaries for other platforms and preserves installation without
lifecycle scripts or a Rust toolchain.

| Dependency                            | Scope            | Purpose                                        |
| ------------------------------------- | ---------------- | ---------------------------------------------- |
| `@harperfast/fulltext-darwin-arm64`   | optional runtime | macOS arm64 native addon for this version.     |
| `@harperfast/fulltext-linux-x64-gnu`  | optional runtime | Linux x64 glibc native addon for this version. |
| `@harperfast/fulltext-win32-x64-msvc` | optional runtime | Windows x64 native addon for this version.     |
