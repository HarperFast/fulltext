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

Three alternating optimized arm64 pairs compared [the baseline (0b56087)](https://github.com/HarperFast/fulltext/commit/0b5608778b2d80ed2a8dfbea79565d509837e7ff)
with [the metadata writer (1adb751)](https://github.com/HarperFast/fulltext/pull/75/commits), using 10,000 documents,
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
Use the reviewed pre-Count-fix baseline [e7f5a07](https://github.com/HarperFast/fulltext/pull/75/commits)
with its matching addon for this comparison. Ordinary filtered queries also run against query API 4
baselines; only metadata cases require API 5. Three alternating pairs observed median p50 changes
of **+6.3%** for ordinary exact totals and
**+1.1%** with the Boolean filter. These small local samples provide no production latency bound.

### Concurrent dictionary-cache qualification

```bash
cargo test --release --all-features dictionary_cache_concurrency_qualification -- --ignored --nocapture
```

The fixture creates eight real in-memory indexes with 40 segments of 128 documents each and one
string field. It asserts that all 320 dictionary maxima are cached before measurement. At one,
four and eight concurrent callers, five samples per process report per-request p50/p95 and median
wall time for warm dictionary preflight and full `aggregate_count` requests. Full requests assert
native counts and terms results.

Three alternating optimized macOS arm64 pairs compared the preserved mutex baseline
[03df642](https://github.com/HarperFast/fulltext/pull/75/commits), with this qualification added,
against a candidate changing only the cache lock to `RwLock`. Eight-caller preflight median p50 was
**0.172958 ms** for both. Full aggregation changed from **0.739375 ms** to **0.710292 ms** (**−3.93%**).
One-caller preflight changed from **0.046375 ms** to **0.046959 ms** (**+1.26%**).
The retained mutex baseline rises **3.73×** from one to eight callers. The comparison does not
attribute that increase to lock waits, fast-field work, memory contention or scheduling; it rules
out the proposed lock substitution as a demonstrated fix for this workload.

Retain the mutex: the candidate missed the declared threshold of at least **10%** improvement in
eight-caller preflight and **2%** in full aggregation. This fixture qualifies warm concurrent access;
it neither establishes absence of contention nor predicts production or cross-platform performance.

Dictionary results are cached only after a complete scan. Concurrent cold misses may duplicate
that bounded-memory scan: avoiding an in-flight lock keeps one request's deadline from blocking
another. The cache retains at most 1,024 entries in FIFO order, publishes only complete scans, and performs
scanning outside its guard. The qualifications cover collector CPU work, result comparison and
warm concurrent cache access, not peak RSS, production storage, concurrent cold-miss amplification
or end-to-end application latency.

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

## Concurrent exact-count and cold-start qualification

Build optimized artifacts first. Run both revisions through the same count driver in alternating
process order; `--module` must select each revision's matching facade and addon:

```sh
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/metadata-count-load.mjs --module /path/to/checkout/dist/native.js --scratch /path/to/scratch --documents 100000 --queries 120 --concurrency 1,4,8 --revision candidate
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/metadata-cold-start.mjs --scratch /path/to/scratch --documents 65536 --rounds 3 --concurrency 1,4,8
```

The count driver asserts exact totals and distinct scores at the ranked page boundary. Its filtered
dense OR bypasses the stable-score-tie shortcut. It measures normal requests and 1 ms budgets,
then requires full-budget recovery and zero retained queue commands/bytes. Timeouts can occur in
queueing or hit collection before Count; this public experiment does not isolate Count cancellation.
The direct native cancellation regression remains the proof for interruption inside Count.

The cold-start driver writes eight indexes with 65,536 unique strings each. Every measured cohort
runs in a fresh child process, with either one shared index or independent indexes. Sixteen
candidate IDs keep matching work small; requesting 100 terms forces the dictionary-key admission
scan despite the selective query. Each cohort runs a first wave, a recovery wave, then 20 warm waves.
A separate fresh process exercises a 1 ms first-wave budget and subsequent recovery. CPU covers
Node and native threads; sampled RSS includes the reader/process baseline and is not an allocation
or leak measurement. The dictionary maximum cache is cold; filesystem caches are uncontrolled and
likely warm. This does not measure disk-cold startup.

### Observed exact-count load results

Three alternating optimized macOS arm64 process pairs compared the
[previous implementation (0b56087)](https://github.com/HarperFast/fulltext/commit/0b5608778b2d80ed2a8dfbea79565d509837e7ff)
with the [qualified runtime (a471b58)](https://github.com/HarperFast/fulltext/pull/75/commits)
on 2026-10-28: 100,000 documents, two native search threads and 120 requests per cell.
The table shows medians of three per-run statistics; p99 has few tail observations.
This compares the whole branch, not the Count stage in isolation.

| Workload / callers    | p50 base → current ms | p95 base → current ms | p99 base → current ms | Success/s base → current |
| --------------------- | --------------------- | --------------------- | --------------------- | ------------------------ |
| AND / 1               | 0.070 → 0.072         | 0.112 → 0.107         | 0.262 → 0.122         | 12,058 → 12,792          |
| AND / 4               | 0.187 → 0.203         | 0.214 → 0.213         | 0.308 → 0.221         | 20,319 → 19,514          |
| AND / 8               | 0.365 → 0.395         | 0.404 → 0.422         | 0.420 → 0.451         | 21,331 → 19,575          |
| Filtered dense OR / 1 | 2.110 → 2.080         | 2.274 → 2.137         | 2.454 → 2.422         | 470 → 477                |
| Filtered dense OR / 4 | 8.282 → 8.263         | 8.460 → 8.535         | 8.528 → 8.594         | 482 → 482                |
| Filtered dense OR / 8 | 16.560 → 16.813       | 16.681 → 17.311       | 16.720 → 17.460       | 483 → 474                |

Per version, all 2,160 normal requests and 1,080 budgeted AND requests returned exact results;
1,080 budgeted OR requests returned `E_TIMEOUT`. Each cell then recovered at full caller width
with exact totals and zero queued commands/bytes. At eight callers, CPU per normal 120-request cell
changed from 9.5 to 9.4 ms for AND, and 253.8 to 260.2 ms for OR. Median peak RSS across runs was
about 2 MiB higher on the branch; current peaks ranged from 274.4 to 275.6 MiB across normal cells.
Sampled RSS and short runs cannot establish long-term memory stability.

Decision: retain cooperative Count. At eight callers the AND throughput fell 8.2% and p99 rose
7.3%; OR throughput fell 1.7% and p99 rose 4.4%. These local differences do not justify changing
cancellation or adding a new collector mechanism. A consuming application with a tighter CPU or
latency budget must rerun its own workload before adopting. Production concurrency, sustained
runs, and other platforms remain open qualification work, not guarantees from this experiment.

### Observed cold-start results

Three optimized macOS arm64 rounds on 2026-10-28 used the
[qualified runtime (a471b58)](https://github.com/HarperFast/fulltext/pull/75/commits).
Cold percentiles pool only three waves (3–24 observations per row); warm rows pool 60 waves.
They are local observations, not production latency guarantees.

| Callers | Indexes     | Cold p50 / p95 ms | Warm p50 / p95 ms | Cold median CPU ms per wave | Highest cold RSS MiB |
| ------- | ----------- | ----------------- | ----------------- | --------------------------- | -------------------- |
| 1       | Shared      | 1.649 / 3.714     | 0.413 / 0.485     | 1.889                       | 58.6                 |
| 1       | Independent | 1.549 / 1.643     | 0.414 / 0.493     | 1.798                       | 58.5                 |
| 4       | Shared      | 1.542 / 1.873     | 0.939 / 1.354     | 3.734                       | 58.7                 |
| 4       | Independent | 1.512 / 2.116     | 0.509 / 0.648     | 6.402                       | 67.0                 |
| 8       | Shared      | 2.059 / 3.114     | 1.501 / 2.547     | 6.101                       | 59.3                 |
| 8       | Independent | 1.481 / 1.996     | 0.531 / 0.624     | 11.739                      | 74.1                 |

Every full-budget cold/recovery/warm request returned the expected exact total of 16 and 16 one-document
buckets. Of 78 requests with 1 ms budgets, 62 timed out; successful requests had exact results.
The largest timeout response was 1.692 ms; the largest subsequent recovery latency was 2.710 ms.
All cohorts ended with zero queued commands/bytes. These observations do not identify duplicate
scans or attribute CPU scaling to the mutex; native admission also limits shared-index concurrency.

Decision: retain the existing cache and admission behavior. The measured cold cost is visible but
provides no evidence that scan coordination would improve the release materially. Revisit with a
reproducing workload if first-search latency, CPU, or recovery violates a consuming application's
budget. Linux/Windows, larger dictionaries/segment counts, disk-cold startup and sustained memory
behavior remain unqualified; see [Qualify remaining native metadata collector resource tradeoffs](https://github.com/HarperFast/fulltext/issues/76).

## Runtime sort-list qualification

The ignored `runtime_production_sort_qualification` test compares native statically typed tuples
with production dispatch: typed computers for one/two fields, native erased-type tuples for
three/four fields, and a runtime-length adapter for five or more. Four fields use nested pairs
to preserve each configured comparator. Both measured paths use production Count, ID/score
collectors and an active 30-second cooperative deadline on 50,000 documents, 50 segments and a
top-10 window. Run:

```bash
cargo test --locked --release runtime_production_sort_qualification -- --ignored --nocapture
```

Observed locally on 2026-10-28: three fresh processes ran nine alternating native/candidate pairs
for each case. Increasing keys with descending ordering force accepted replacements after the
threshold fills; the randomized case exercises rejection and ties. All hits, scores, addresses
and totals matched. Pooled 27-sample medians:

| Metadata keys | Data        | Native tuple | Production path | Change |
| ------------- | ----------- | ------------ | --------------- | ------ |
| 2             | Adversarial | 4.848 ms     | 4.831 ms        | -0.3%  |
| 2             | Random      | 1.678 ms     | 1.671 ms        | -0.4%  |
| 3             | Adversarial | 5.051 ms     | 5.341 ms        | +5.7%  |
| 3             | Random      | 1.780 ms     | 1.985 ms        | +11.5% |
| 4             | Adversarial | 5.261 ms     | 5.463 ms        | +3.8%  |
| 4             | Random      | 1.837 ms     | 1.956 ms        | +6.5%  |
| 6             | Adversarial | 5.923 ms     | 6.629 ms        | +11.9% |
| 6             | Random      | 1.951 ms     | 2.182 ms        | +11.8% |

Two-field dispatch stayed within local measurement variation. Erased native tuples and dynamic
lists retain measured conversion/comparison costs. The dynamic adapter computes accepted keys
once using per-segment scratch storage and allocates no vector for threshold rejections.
Native TopDocs owns every heap and merge. Global merging moves retained key payloads; admission
charges at most two additional threshold payload copies, independently of the result window,
alongside fixed merge slots, scratch storage and retained key vectors.

These timings include collector construction, scoring, Count, deadline checks and merging; they
exclude index creation, admission and the JavaScript/native boundary. They are collector-path
samples, not public API latency, a production service guarantee or cross-platform qualification.
Larger key lists and sustained concurrent loads remain unqualified.

## Public synthetic catalog acceptance

See the [premerge qualification](metadata-premerge.md) for the public-addon workload driver,
predeclared latency/throughput/RSS gates, three alternating baseline/current pairs and a
six-minute expanded workload observation. The guide retains an unexplained earlier RSS-span
flag alongside all subsequent passing diagnostic and acceptance runs. These are local synthetic
observations, not production performance objectives.
