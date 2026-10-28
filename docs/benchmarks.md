# Benchmarking the wrapper

Run these commands from the repository root. Benchmarks measure the wrapper’s native path;
source-record retrieval and authorization remain application work. Build optimized artifacts for
performance comparisons and compare repeated runs under the same conditions.

## Standard workloads

```bash
npm run benchmark:native -- --documents 100000 --concurrency 4 --commit-every 25000
npm run benchmark:multi-index -- --indexes 8 --documents 100000 --concurrency 16
npm run benchmark:native -- --documents 100000 --revision v0.1.0 --output benchmark-native.json
npm run benchmark:native -- --documents 100000 --concurrency 4 --commit-every 25000 --mutation-driver low-level
npm run benchmark:inspect -- --indexes 1,10,100,1000 --commits 64 --warm-rounds 10
```

## Hydration and trace matching

After an optimized `npm run build`, run the targeted hydration and trace benchmarks:

```bash
node scripts/run-local.mjs benchmarks/ordinary-search.mjs . current --hydration
node scripts/run-local.mjs benchmarks/ordinary-search.mjs . current --hydration --approximate-totals
node scripts/run-local.mjs benchmarks/trace-matching.mjs . current
```

Hydration covers shallow/deep pages with and without padded versions under exact and approximate
totals. Tracing covers small and large fuzzy/fuzzy-prefix inputs, including preparation-heavy
requests. The Release benchmark workflow retains eight hydration and eight trace JSONL records
per Linux architecture. See [implementation and measurement limits](native-backend-implementation.md#tantivy-ownership-boundary)
for final-page version decoding, lazy native fuzzy-prefix matching, the retained full-fuzzy predicate,
and the benchmark methodology. These implementation changes preserve the public matching and
pagination contracts; synthetic timings do not establish a general production speedup.

## Metadata sorting and aggregation

`npm run benchmark:metadata` builds optimized artifacts, then runs the deterministic metadata
driver with 10,000 documents and 100 measured requests per case. For a correctness smoke run,
build first with `npm run build`, then run `npm run benchmark:metadata:smoke` (300 documents,
10 requests per case). The driver itself never builds. Override workload sizes with
`--documents` and `--queries`. Select scalar strings, eight distinct strings, or eight repeated
strings with `--string-values scalar|unique|repeated` (default `scalar`):

```bash
npm run benchmark:metadata -- --documents 10000 --queries 100 --string-values repeated
```

The JSON report includes Node, platform, architecture and native versions, the complete index
configuration, durable write throughput, total filesystem bytes after close, and query
p50/p95/p99. Query cases cover ordinary search, numeric/string sorting, terms, statistics,
histograms, and a combined filtered query. Each case warms three requests before measurement;
all requests run sequentially with exact totals and ten-hit windows. The footprint includes
metadata and segment files; it does not isolate the cost of string fast fields. Resource-limit,
response-size and timeout rejections appear in the JSON and on stderr.

Compare baseline and current using this **same driver** and identical optimized builds. Build
both worktrees first, then run from the current worktree:

```bash
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/metadata.mjs --module /absolute/baseline/dist/native.js --documents 10000 --queries 100 --revision baseline > metadata-baseline.json
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/metadata.mjs --documents 10000 --queries 100 --revision current > metadata-current.json
```

The `--module` argument loads the selected worktree's JavaScript facade and matching native
addon. A query API older than version 5 measures writes, footprint and ordinary search, and marks
metadata cases skipped. Alternate process order over repeated pairs on idle hardware. Compare
ingestion, footprint and ordinary search across revisions; compare new collector costs against
ordinary search within the current revision. These synthetic observations are not production
latency guarantees.

### Observed string-metadata cost

Three alternating optimized arm64 pairs compared [the baseline](https://github.com/HarperFast/fulltext/commit/0b5608778b2d80ed2a8dfbea79565d509837e7ff)
with [the metadata writer](https://github.com/HarperFast/fulltext/commit/1adb7511a587521bf581f65b01de35f03aba89b3), using 10,000 documents,
32 brands and 1,000 queries per case on the same host. Later changes preserve the writer behavior and
schema for this scalar-string workload; repeated-string array ingestion was not measured. Median persisted bytes increased **7.0%**; median durable ingestion throughput
was flat at the reported precision. Ordinary exact-total p50 increased **0.6%** and p99 **1.5%**.
These small local samples varied and do not establish a production latency or throughput bound.
They measure the complete branch, not the fast column in isolation; raw host-specific reports
remain local.

Every configured string filter gains the fast column so the same metadata supports presence,
sorting and facets without another capability flag. This deliberately makes equality-only string
indexes pay the rebuild and storage cost too. Numeric/Boolean-only indexes retain their existing
schema and identity.

### Writer string deduplication experiment

Three alternating optimized pairs used 10,000 documents and 30 measured queries per case to
compare the retained writer set with an experimental build that removed it. Repeat the paired
worktree commands above with identical `--string-values scalar`, `unique`, or `repeated` arguments.
The same driver loads each build's matching native addon.

With eight repeated strings per document, removing the set increased median persisted bytes by
**16.6%** and terms p50 by about **2.12×**. Durable ingestion changed by **−0.6%**. Scalar and unique-array
footprints were unchanged; ingestion showed no consistent gain across the three workloads.
The removal was rejected: the set reduces redundant stored values and downstream work, while
native aggregation already deduplicates facet contributions. These local synthetic observations
are not production latency or throughput guarantees.

## Native search and ingestion

The native benchmark generates a deterministic, high-cardinality product catalog and emits one versioned
JSON record. It reports packing, apply, durable end-to-end ingestion, actor queue and execution
time, logical-batch p50/p95/p99, frame count, commit distributions, reload cost, warm and cold query
p50/p95/p99, per-mode term/phrase/prefix/fuzzy/candidate performance, exact-total overhead, index
bytes, and periodically sampled process RSS. The default
`--mutation-driver logical` exercises `applyMutationBatch()`; rerun the identical command with
`--mutation-driver low-level` for the prior encode-plus-apply path. Compare
`mutationDriverMilliseconds`, durable end-to-end throughput, logical-batch percentiles, and peak
RSS. The component `packingMilliseconds` and `applyMilliseconds` fields are not comparable because
the logical API performs both inside one call. `--commit-every` sets the target number of mutations
between durability points; it materially affects throughput and peak memory because
replacement-safe upserts include delete terms. CI runs only the correctness smoke profile; timing
comparisons require controlled hardware.

### Paired native collector qualification

Run `cargo test --release --all-features metadata_collector_qualification -- --ignored --nocapture`
for a same-process comparison against Tantivy's native collectors. The in-memory fixture contains
one million matching documents across ten segments and one unmatched extreme numeric outlier.
After warmup, five alternating pairs compare identical hit ordering, exact counts and aggregate
values. It also compares cold and cached dictionary qualification across one million ID keys.

In this synthetic fixture, the deadline-aware path added approximately **18%** for numeric sorting,
**1%** for terms/statistics, **2%** for a safe histogram, and **14%** for the histogram fallback that
checks matching values when segment extrema are unsafe. Completed dictionary qualification was reused on the warm lookup; cold scans and cache hits
are reported separately rather than treated as a production latency target. Unscored collection uses native block reads; deadline checkpoints occur
at most 1,024 documents apart. The extreme-value fallback deliberately pays for validating live
matches so unrelated or deleted extremes do not reject an otherwise valid request.

The expanded fixture then deletes half the documents, reloads the retained ten segments, and
compares Boolean AND/OR/NOT sorting and aggregation plus fuzzy aggregation over multivalue
numeric fields. Independent assertions check live counts, value-based statistics and per-document
bucket deduplication. Five alternating pairs observed deadline/native ratios of **0.94–0.99×**
for these additional cases; small apparent improvements are local timing noise, not a speedup claim.

The qualification also compares bare native `Count` with the cooperative count used by ordinary
exact-total searches. In a one-million-document fixture, dense AND counting added approximately
**11%** and dense OR **37%**. A single-term native count uses a constant-time shortcut; the
cooperative version enumerates blocks (about **0.13 ms** in this run). These count-only costs are
not represented by the aggregation tuple comparisons above. Native count remains in use when no
deadline is supplied; with a deadline, bounded interruption is chosen over uninterruptible count
shortcuts.

For the public exact-total route, run the paired metadata commands with
`--distinct-scores --ordinary-only --documents 100000 --queries 300`. The fixture verifies that
ranked hits do not tie at the filtered window boundary, ensuring the final Count path is used.
Three alternating pairs observed median p50 changes of **+6.3%** for ordinary exact totals and
**+1.1%** with the Boolean filter. These small local samples provide no production latency bound.

Dictionary results are cached only after a complete scan. Concurrent cold misses may duplicate
that bounded-memory scan: avoiding an in-flight lock keeps one request's deadline from blocking
another. These measurements cover collector CPU work and result comparison, not peak RSS,
production storage, concurrent cold-miss amplification or end-to-end application latency.

## Traversal

`node scripts/run-local.mjs benchmarks/traversal.mjs` qualifies traversal with large IDs/versions,
multiple actual segments, a broader fuzzy vocabulary, concurrent writes and ordinary searches,
and response-size recovery. Build first with `npm run build`; use `--smoke` for the smaller CI
correctness profile. Output reports actual segment counts, workload sizes, timings and process RSS.
It does not enforce production latency targets or claim a peak-memory bound.

`node scripts/run-local.mjs benchmarks/traversal-preparation.mjs current` measures synchronous
submission time, completion time and event-loop delay for a legal large candidate/filter identity.
Build first; compare repeated runs on the same idle hardware. It measures encoding/copying and
native work as well as hashing, so it does not isolate crypto cost or establish a production latency
target.

For a paired ordinary-search comparison, build baseline and current worktrees with the same optimized
native profile, then run `FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/ordinary-search.mjs /absolute/worktree baseline`
and repeat with the current worktree and label. The fixture checks totals and hit counts, warms each
query, and emits timings for empty, sparse, and broad exact searches at limits 20 and 1,000.
Alternate process order across repeated pairs on idle hardware; these observations are not a latency gate.

## Multiple indexes and release artifacts

`benchmark:multi-index` exercises independent writers in parallel under the process budget. It
includes heavy-tail field sizes, bounded synonyms, update/delete churn, resource-cap rejection,
warm concurrent queries, close/reopen, and cold queries. CI runs the two-index smoke profile; the
release workflow records the larger eight-index profile per architecture. These results measure
the library's native path, not source projection, authorization, or record retrieval performed by
an application.

`--revision` labels a result and `--output` writes the same JSON record printed to stdout. Pull
requests keep smoke records as GitHub Actions artifacts for 30 days. After a GitHub release is
published, the release benchmark keeps 90-day Actions artifacts and attaches native and multi-index
JSON results for Linux x64 and Linux arm64 to that release. These assets provide a permanent
per-architecture release-over-release history, but the post-publish workflow does not gate npm
publication, calculate a baseline delta, or fail a build on timing. Shared-runner numbers are
evidence that the workload still runs, not a latency gate. Compare performance only with the same
benchmark format, workload, architecture, and controlled hardware. Record observed numbers in the
pull request or release notes; keep this guide focused on the reproducible method rather than
environment-specific targets.

## Inspection and first-page comparisons

The inspection benchmark compares synchronous read-only inspection with full writer reopen across
multiple index counts. It reports equivalent first-pass and warm p50/p95/p99/max latency,
synchronous wall time per inspection sweep, metadata size, and the actual segment count produced by
the seed workload. It does not claim to model OS-cold storage. `--indexes` accepts configurations
that create at most 10,000 temporary index directories across both benchmark paths.

For a controlled same-query comparison of ordinary and traversal first-page overhead, run
`npm run build && node scripts/run-local.mjs benchmarks/traversal-driver.mjs`. It alternates
60 calls per method after warmup on one retained snapshot, with identical exact totals and
page size. This manual benchmark has no timing assertion or CI performance threshold.
