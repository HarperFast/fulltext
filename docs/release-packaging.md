# Release packaging

## TL;DR

- Publish a binary-free `@harperfast/fulltext` facade at `0.1.2`.
- Publish one exact-version optional package for each supported platform.
- Load the platform package by default; repository tests explicitly prefer the local build.
- Reject missing, unloadable, ABI-incompatible, capability-incompatible, or version-skewed addons
  before opening an index.
- Build, pack, install, and load every platform artifact before publishing the root package.
- Publish with npm provenance; make workflow retries verify and skip identical published tarballs.

## Intent

Publish `@harperfast/fulltext` as a reproducible native dependency that works without a Rust
toolchain or install script on every supported platform. Harper must be able to pin one exact
wrapper version and either load the matching native artifact or receive an actionable unsupported
platform error before an index is opened.

## Invariant

A published root version resolves only a native artifact with the same package version for the
current supported platform; a missing, unsupported, or ABI-incompatible artifact fails before any
index mutation.

## Chosen design

Use the established HNSW packaging model:

1. `@harperfast/fulltext` contains the JavaScript and TypeScript facade.
2. It declares exact-version optional dependencies for the supported native packages:
   `@harperfast/fulltext-linux-x64-gnu`, `@harperfast/fulltext-linux-arm64-gnu`,
   `@harperfast/fulltext-darwin-arm64`, and `@harperfast/fulltext-win32-x64-msvc`.
3. The loader selects the platform package first in an installed consumer and falls back to an
   adjacent local artifact when no package is installed. Repository tests and benchmarks set
   `FULLTEXT_PREFER_LOCAL_BUILD=1` so a previously published package cannot shadow current source.
   Both paths run the same ABI and capability validation. Validation also requires
   `runtimeInfo().packageVersion` to equal the facade version; ABI compatibility alone cannot
   detect a skewed platform package.
4. A release workflow builds and tests on each native runner, assembles platform-package tarballs,
   validates their manifests and loadability, then publishes the platform packages before the root
   package. The root tarball contains no native artifact. Release tags must match both the npm and
   Cargo manifest versions, prereleases use the `next` dist-tag, and a retry skips an already
   published package only after its registry integrity matches the local tarball byte-for-byte.
5. The release workflow installs the packed root and matching packed platform package into a clean
   consumer with lifecycle scripts disabled, validates `runtimeInfo()`, then runs an
   open/apply/commit/reload/search/close round trip through the public
   `@harperfast/fulltext/native` export.
6. The first published version is `0.1.1`. The `0.1.0` GitHub release failed before npm
   publication because its platform-package path was parsed as a GitHub shorthand instead of a
   local directory. Version `0.1.1` successfully published the facade and three original platform
   packages on 2026-09-24. Version `0.1.2` adds the Linux arm64 artifact.
7. The publish workflow reports success or failure to Slack after the release pipeline settles.
   Success links the npm package and GitHub release; failure links the workflow run and observes
   failures from packaging, packed-consumer verification, or publication.

The support matrix is Linux x64 glibc, Linux arm64 glibc, macOS arm64, and Windows x64. Adding a
target requires its own native runner, packed-artifact load test, and platform package.
Linux artifacts are built on Ubuntu 22.04 and require glibc 2.35 or newer.

The root package's explicit `files` allowlist excludes native artifacts. Its `prepack` compiles only
the TypeScript facade and verifies that the manifest does not include a native artifact. The packed
consumer test verifies the resulting root tarball. Native compilation belongs to the platform build
jobs; this prevents a release runner's local artifact from masking a failed optional dependency
installation.

## Failure behavior

- Unsupported platform or libc: `E_NATIVE_ADDON_NOT_FOUND` names the computed platform triple and
  the expected optional package.
- Missing optional package: the same error is raised before opening an index; npm installation of
  Harper itself remains possible. A package that resolves but cannot be loaded is wrapped as
  `E_NATIVE_LOAD_FAILED` with its original error as the cause rather than leaking a raw `dlopen`
  error.
- Wrong native ABI or capability set: existing loader validation rejects the artifact.
- Partial release: platform packages are published first, so the root version is never published
  until every supported native artifact has been built, packed, and validated. A failed root
  publish leaves unreachable platform packages but no installable root version that references an
  incomplete set.
- Retry a failed publish job with the original workflow artifacts. Do not restart the complete
  workflow after any package has published: a rebuilt native artifact may not be byte-identical,
  and the publisher intentionally rejects different bytes for an existing version.

## Verification

- Unit-test platform package naming, local fallback, missing-package errors, and ABI validation.
- Verify the installed platform package wins over an adjacent development artifact, package-version
  skew is rejected at load time, missing addons return a typed error, and the unsupported musl
  target maps to its expected package name.
- Test package assembly rejects a missing target, an unexpected target, or version skew.
- On each platform, install the packed root and packed platform tarballs in a clean temporary
  consumer with `--ignore-scripts`, validate `runtimeInfo()`, then open an index, apply and commit a
  document, reload and search it, and close the index.
- Before merge, the existing PR CI runs formatting, lint, Rust, Node, supply-chain, and
  benchmark-smoke gates. The release workflow reruns Rust and Node tests and inspects each exact
  release artifact for RocksDB dependencies and linkage.
- Re-run the no-RocksDB dependency and native-linkage checks on release artifacts before publish;
  publish with npm provenance.
- **untested:** after publishing, install `@harperfast/fulltext@0.1.2` into Harper and run the
  derived-index lifecycle suite against the real package rather than an injected binding.

## Approaches considered

### Different layer: bundle Fulltext binaries in Harper

Rejected because it makes Harper responsible for building and distributing a standalone wrapper,
and standalone Node consumers would still have no supported package. The native artifact belongs
to the wrapper version whose ABI it implements.

### Deeper cause: publish one universal package containing every native binary

Rejected because every consumer would download all supported native binaries. The platform
selection problem is already solved by exact-version optional packages in HNSW and rocksdb-js, and
using that mechanism keeps one artifact per installation without an install script.

### Do less: publish the runner-local artifact or compile during installation

Rejected because a single tarball built on one runner cannot serve the supported matrix. Compiling
at installation would require Rust on Harper hosts and violate the package's existing no-lifecycle-
script installation contract.

### Chosen: exact-version platform optional packages

This preserves the existing public entry point and native ABI validation while reusing Harper's
established native-package distribution model. It is the only option considered that supports the
standalone wrapper, avoids install-time compilation, and does not download irrelevant binaries.

## Open items

- **untested:** run the release workflow and full benchmark on the Linux arm64 runner.
- **untested:** publish the new Linux arm64 package with npm provenance and verify an identical-
  tarball retry.
- **configuration:** grant `HarperFast/fulltext` access to the organization `SLACK_BOT_TOKEN`
  secret and set the repository `SLACK_CHANNEL_ID` secret before the next release.
- **untested:** confirm both Slack notification paths against the configured channel.
- After `0.1.2` is published, refresh `package-lock.json` so every platform package carries its
  registry URL and integrity hash.
- Publish `0.1.2` before updating Harper's exact optional dependency; the Harper integration test
  must use the registry artifact rather than an injected binding.

## Sources

- [Build multi-platform CI, prebuild packaging, provenance, and release automation](https://github.com/HarperFast/fulltext/issues/10)
- [`package.json`](../package.json)
- [`ts/load-addon.ts`](../ts/load-addon.ts)
- [`scripts/platform-packages.mjs`](../scripts/platform-packages.mjs)
- [`scripts/publish-release-packages.mjs`](../scripts/publish-release-packages.mjs)
- [`.github/workflows/publish.yml`](../.github/workflows/publish.yml)
- [`HarperFast/hnsw` package manifest](https://github.com/HarperFast/hnsw/blob/main/package.json)
- [`HarperFast/hnsw` publish workflow](https://github.com/HarperFast/hnsw/blob/main/.github/workflows/publish.yml)
