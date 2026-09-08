# Dependencies

Direct dependency versions are exact so the native artifact is reproducible and upgrades are
reviewed deliberately.

## Rust runtime and build graph

| Dependency                 | Scope                            | Purpose                                                                                  |
| -------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------- |
| `async-trait` 0.1.92       | optional Phase 0 runtime + tests | Implements Tantivy's asynchronous `FileHandle` read contract.                            |
| `tantivy` 0.26.1           | runtime                          | Full-text indexing and search engine, including the `Directory` contract.                |
| `napi` 2.16.17             | optional runtime                 | Node-API values and error conversion for the addon build.                                |
| `napi-derive` 2.16.13      | optional build/runtime boundary  | Generates Node-API exports.                                                              |
| `napi-build` 2.4.1         | build                            | Configures platform-specific addon linking.                                              |
| `libloading` 0.8.9         | Windows runtime                  | Resolves the Node-API type-tag check from the host, matching napi-rs's Windows strategy. |
| `stable_deref_trait` 1.2.1 | optional Phase 0 runtime         | Lets Tantivy `OwnedBytes` retain and directly read provider-owned buffers.               |

The Rust dependency graph must not include RocksDB. The planned Harper integration uses supported
Harper storage APIs through a bounded transport. It does not require a rocksdb-js native lease.
The optional Phase 0 dependencies above describe retained experimental code, not the production
Harper storage contract; reassess them when that integration is implemented.

napi-rs 2.16 generates an outer unwind boundary only for exports marked `catch_unwind`. Every
fulltext function, method, and constructor uses that option to contain argument and result
conversion panics. That outer boundary uses napi-rs error mapping; the inner boundary adds stable
error codes and per-handle poison state for package-owned operations.

## JavaScript development graph

| Dependency            | Scope       | Purpose                                                               |
| --------------------- | ----------- | --------------------------------------------------------------------- |
| `@napi-rs/cli` 2.18.4 | development | Builds and names native artifacts and generates private declarations. |
| `@types/node` 24.10.0 | development | Type information for supported Node.js APIs.                          |
| `prettier` 3.6.2      | development | Repository formatting checks.                                         |
| `typescript` 5.9.3    | development | Compiles the public façade and declarations.                          |

The native entry point has no production npm dependencies. No rocksdb-js peer dependency is planned.
Harper owns its rocksdb-js version and supplies the qualified integration context. Tests qualify the
actual Harper/fulltext/storage stack. See [Harper storage integration](docs/harper-storage-integration.md).
