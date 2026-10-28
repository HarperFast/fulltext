# Dependencies

Direct dependency versions are exact so the native artifact is reproducible and every upgrade is
reviewed before merge.

## Rust runtime and build graph

| Dependency                     | Scope                           | Purpose                                                                                                 |
| ------------------------------ | ------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `tantivy` 0.27.0               | runtime                         | Full-text indexing and search engine, including native filesystem storage.                              |
| `tantivy-fst` 0.5.0            | runtime                         | Matches Tantivy fuzzy-prefix automata against current-source trace tokens; already in the engine graph. |
| `sha2` 0.10.9                  | runtime                         | SHA-256 snapshot identity over Tantivy segment IDs and deletion generations.                            |
| `unicode-normalization` 0.1.24 | runtime                         | Applies the versioned English analyzer's NFKC token normalization.                                      |
| `napi` 3.13.0                  | optional runtime                | Node-API values, error conversion, and addon-image retention.                                           |
| `napi-derive` 3.6.9            | optional build/runtime boundary | Generates Node-API exports.                                                                             |
| `napi-build` 2.5.0             | build                           | Configures platform-specific addon linking.                                                             |

Snapshot identity is computed inside the engine for both direct Rust and Node callers. The exact-pinned, no-default-features `sha2` implementation avoids hand-written cryptography and per-reader lease state; no third-party type enters the public API.

The Rust dependency graph must not include RocksDB. The library uses Tantivy's native filesystem
backend and does not link another database runtime into the addon.

### Updating Tantivy

A Dependabot pull request announces a new Tantivy release; it is not a merge-ready upgrade. Before
merging it:

- Update `TANTIVY_VERSION` and the release-package assertion to match the bot-managed exact pin.
- Review Tantivy's release notes for index-format, analyzer, schema, scoring, and query changes.
- Check the manifest and CI Rust version floors and the exact-version exceptions in `deny.toml`.
- Align the exact `tantivy-fst` pin with Tantivy's resolved version; the matcher trait must come from the same crate version.
- Refresh the dependency table and every version-specific Tantivy citation in the design documents.
- Run the full test suite. Manually dispatch the `Release benchmark` workflow on the update branch,
  then compare its Linux x64 and Linux arm64 results with the matching assets attached to the
  previous release.

Scheduled Cargo version updates are limited to Tantivy. Dependabot security updates remain enabled
for the full Cargo graph; they share the `dependencies` label used for Slack notifications. The
per-dependency `version-update:` filters preserve that distinction and must not be replaced with a
bare dependency-name ignore rule.

### Tantivy 0.27 compatibility

Tantivy 0.27 replaces the English stemmer with `frostem`. Persisted identities 6 (text only),
7 (numeric/Boolean-only filters), and 8 (string filters) distinguish that output from identities
4/5; identity 8 also records raw string fast columns. The public `english@2` configuration remains
unchanged. Older incompatible indexes must be retired and rebuilt from authoritative records.
This build recognizes identities 1–8 for reset. Retire a string-metadata index with this build
before downgrading: released 0.7.x reset parsers do not recognize identity 8. Before downgrading
to 0.6, retire identities 6–8 with this build too; its reset parser cannot recognize them.
Never copy an old sidecar over a new index to bypass inspection.

The exact `getrandom` 0.3.4 and `r-efi` 5.3.0 duplicate exceptions are required by
`tantivy-columnar` 0.27 → `rand` 0.9, alongside `getrandom` 0.4 used by `tempfile`/`uuid`.
Remove both exceptions when those upstream branches converge on one random-backend version.

The locked `frostem` revision is part of persisted analyzer semantics even though it is transitive.
The two `frostem` bans in `deny.toml` reject any revision other than 1.20260821.8.
Review every stemmer update for token changes before updating those bounds; changed output requires another analyzer/sidecar
revision and rebuild. The upgrade regression pins representative changed English terms, not an
exhaustive language corpus.

Metadata-hydration changes require targeted versioned/offset cases alongside generic mixed-query
measurements. The release workflow runs both total profiles and attaches hydration JSONL assets
to the release. Keep per-head qualification evidence in the upgrade PR and its tracking issue.

### Updating Unicode semantics

`unicode-normalization` data and Rust's `char` classification determine durable token output. Any
change to `unicode-normalization` or the Rust toolchain floor requires reviewing normalization and
token-boundary behavior. If indexed output can change, bump the public analyzer name or identity
sidecar version so an existing index fails closed and is rebuilt. Run the differential Unicode
normalization test and the Unicode-aware native benchmarks before merging the update.

napi-rs generates an outer unwind boundary only for exports marked `catch_unwind`. Every
fulltext function, method, and constructor uses that option to contain argument and result
conversion panics. That outer boundary uses napi-rs error mapping; the inner boundary adds stable
error codes and per-handle poison state for package-owned operations. The runtime also retains the
addon image before native actors can outlive a Node environment, preventing worker teardown from
unmapping code that those actors can still execute.

Completion callbacks retain the one-buffer, non-callee-handled contract used by the JavaScript
facade. If JavaScript buffer allocation fails, napi-rs escalates through `napi_fatal_exception`;
refusal of external buffers falls back to a copy. Changing this behavior requires an error-first
private addon callback and a matching facade change.

## JavaScript development graph

| Dependency            | Scope       | Purpose                                                               |
| --------------------- | ----------- | --------------------------------------------------------------------- |
| `@napi-rs/cli` 3.10.5 | development | Builds and names native artifacts and generates private declarations. |
| `@types/node` 24.10.0 | development | Type information for supported Node.js APIs.                          |
| `prettier` 3.6.2      | development | Repository formatting checks.                                         |
| `typescript` 5.9.3    | development | Compiles the public façade and declarations.                          |

The public entry point has no JavaScript runtime dependencies and does not depend on rocksdb-js.
Its optional native packages are exact-version platform artifacts for Linux x64 glibc, Linux arm64
glibc, macOS arm64, and Windows x64. They contain only the compiled addon and package metadata;
keeping them separate prevents consumers from downloading binaries for other platforms and
preserves installation without lifecycle scripts or a Rust toolchain.

| Dependency                             | Scope            | Purpose                                        |
| -------------------------------------- | ---------------- | ---------------------------------------------- |
| `@harperfast/fulltext-darwin-arm64`    | optional runtime | macOS arm64 native addon for this version.     |
| `@harperfast/fulltext-linux-arm64-gnu` | optional runtime | Linux arm64 glibc addon for this version.      |
| `@harperfast/fulltext-linux-x64-gnu`   | optional runtime | Linux x64 glibc native addon for this version. |
| `@harperfast/fulltext-win32-x64-msvc`  | optional runtime | Windows x64 native addon for this version.     |

### Metadata collectors

Sorting reuses Tantivy's composable `TopDocs` sort keys. Flat aggregations use its typed
`AggregationCollector` API without another dependency or public upstream request types.
String-metadata configurations use identity 8 for raw fast columns; numeric/Boolean-only configurations
retain identity 7. Native terms aggregation counts each distinct string once per document;
the writer additionally removes repeated strings as a storage optimization, preserving the
caller’s first value for sorting. The library guards
decoded sort/term keys separately from Tantivy's estimated aggregation structure memory.
