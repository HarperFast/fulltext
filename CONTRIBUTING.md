# Contributing

## Setup

Install Node.js 22.18 or newer in the 22.x line, or Node.js 24 or newer, and Rust 1.90, then install
JavaScript dependencies without running package lifecycle scripts:

```bash
npm ci --ignore-scripts
```

## Checks

Run the same primary checks used in CI:

```bash
npm run format:check
npm run lint
npm test
```

Rust tests exercise the engine and directory contract without Node.js. Node tests build the addon,
load it through the public `./native` entry point, verify panic containment, and install the output
of `npm pack` into a temporary consumer project.

CI runs the primary checks on Node.js 22.18, 24, and 26 across Linux x64/arm64, macOS arm64,
and Windows x64. Node.js 26 adds four matrix jobs, approximately 20–30 runner-minutes per run
based on recent job durations. The public-repository hosted runner pool has no execution charge.

CI retains synthetic soak manifests, failure reports, and memory/queue samples for 14 days after
failure. Native index files and memory maps are not uploaded. Set `FULLTEXT_TEST_ARTIFACT_ROOT`
to an empty scratch directory to retain these test fixtures locally; otherwise the tests remove
their temporary directories. An artifact with an injected failure is expected in negative tests;
use the failed test name and error to identify unexpected failures.

## Automated review

Maintainers can apply the `claude-review` or `gemini-review` label to run the corresponding
AI-assisted pull-request review. The workflows use the shared, commit-pinned HarperFast review
prompts and default to opt-in. Remove and reapply a review label after a new push to request another
review. Maintainers can deliberately enable trusted-author automatic reviews with the
`CLAUDE_ALWAYS_ON` or `GEMINI_ALWAYS_ON` repository variable. Fork and Dependabot pull requests
cannot run these reviews because GitHub withholds the required provider secrets for those events.

## Design constraints

- Keep search, indexing, scheduling, and lifecycle behavior independent from any host application.
- Keep the package on Tantivy's native filesystem storage; do not add storage fallback.
- Source-data projection, replication, and readiness policy belong in the consuming application.
- Do not expose generated Node-API declarations as the public TypeScript API.
- Keep sustained native CPU and I/O work off the Node.js event loop. Request encoding and small
  query-identity hashes run synchronously in JavaScript; large identity hashes use WebCrypto.
  Runtime information and process-budget configuration may synchronously load the addon once; budget configuration performs
  no storage I/O. Bounded, synchronous native index inspection is permitted only for lifecycle
  ownership changes; search, indexing, commit, and request-path storage remain async.
- Add a direct test for each source module.

Open an issue before changing a public package entry point, native ABI, persistence contract, or
supported-platform matrix.

## Release packaging

The root package is binary-free. Each supported native runner stages one exact-version platform
package, and CI installs the packed root and matching native tarball before a release can publish.
Keep `package.json`, `Cargo.toml`, the root optional dependencies, and platform-package versions in
lockstep. Repository tests and benchmarks set `FULLTEXT_PREFER_LOCAL_BUILD=1` so a previously
published optional package cannot shadow the addon just built from the checkout. Do not add install
scripts or build-from-source installation fallback to the published package contract.
