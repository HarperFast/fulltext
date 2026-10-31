# Observability qualification

## Initial paired matrix

This historical run compared [candidate `32a9401`](https://github.com/HarperFast/fulltext/commit/32a94018d7f02f7c10c3e3ed228d0fa74b51b7a2)
with unchanged [baseline `bb4fac1`](https://github.com/HarperFast/fulltext/commit/bb4fac150bae3f17e16a18c106b959966ba1bd83) on Node.js
24.16.0, Darwin arm64. It predates the subsequent state, teardown and accounting-efficiency fixes;
it is **not qualification of the final runtime**. The original raw report remains preserved
unchanged locally; it is not a publicly available artifact. Its SHA-256 identity is
`21b264434761c2f03fb8a93dd75e7872817f327f38006742063b5086356bd156`.

The matrix used 2,000 synthetic documents per index, 4,000 timed queries per process, seven
alternating-order pairs per cell, one/four callers and one/two indexes. Ordinary searches used
`any`; expensive searches used `prefix`, both with exact totals. All 448 fresh child processes
completed: **1,792,000 timed queries passed correctness checks**, plus untimed warmup/final checks.
Each process also indexed its corpus and performed 20 mutation/publication cycles. This finite
local fixture does not qualify production capacity or sustained memory.

The tight targets remain **unestablished**: at most 1% median throughput loss with histograms off,
or 3% throughput loss and 5% p99 increase with histograms on. Short query phases and substantial
pair-to-pair variation prevent a reliable overhead bound. Negative values below are observed
improvements, not proof that instrumentation makes execution faster. No outliers were discarded
and no gate was relaxed.

All values are percentages: median paired change followed by the full paired range. Throughput
loss is `1 − candidate / baseline`; p99 increase is `candidate / baseline − 1`. Ranges are
observations, not confidence intervals.

| Search    | Callers | Indexes | Histograms | Throughput loss: median [range] | p99 increase: median [range] |
| --------- | ------- | ------- | ---------- | ------------------------------- | ---------------------------- |
| ordinary  | 1       | 1       | off        | -3.71 [-9.98, 0.64]             | -4.60 [-19.24, 34.67]        |
| ordinary  | 1       | 1       | on         | -4.31 [-12.03, 2.62]            | 1.78 [-13.98, 16.21]         |
| expensive | 1       | 1       | off        | -4.44 [-11.13, 2.65]            | -2.60 [-16.56, 17.77]        |
| expensive | 1       | 1       | on         | -2.74 [-5.80, -0.59]            | -4.59 [-9.89, 6.80]          |
| ordinary  | 1       | 2       | off        | -10.79 [-433.19, -2.59]         | -2.90 [-97.65, 6.52]         |
| ordinary  | 1       | 2       | on         | -2.91 [-16.53, 2.97]            | -14.99 [-45.57, 29.92]       |
| expensive | 1       | 2       | off        | -2.09 [-4.41, 0.52]             | -0.88 [-20.78, 14.77]        |
| expensive | 1       | 2       | on         | -0.96 [-13.32, 10.24]           | -7.56 [-15.05, 27.78]        |
| ordinary  | 4       | 1       | off        | 8.96 [-9.01, 33.33]             | 26.86 [-14.02, 364.92]       |
| ordinary  | 4       | 1       | on         | -2.09 [-5.08, 27.33]            | 1.40 [-6.36, 46.74]          |
| expensive | 4       | 1       | off        | -3.11 [-4.73, 3.05]             | -3.51 [-14.89, 12.97]        |
| expensive | 4       | 1       | on         | -1.89 [-9.94, 10.10]            | -0.12 [-7.56, 5.18]          |
| ordinary  | 4       | 2       | off        | 0.06 [-2.66, 0.34]              | -1.11 [-9.49, 18.24]         |
| ordinary  | 4       | 2       | on         | -2.35 [-7.79, 9.44]             | 6.53 [-19.08, 15.67]         |
| expensive | 4       | 2       | off        | 2.37 [-20.32, 4.02]             | 1.89 [-37.15, 19.01]         |
| expensive | 4       | 2       | on         | -4.50 [-71.06, 2.88]            | -8.06 [-69.32, 7.92]         |

Four off-profile cells were within the throughput target in every observed pair; the other
12 off/on cells were inconclusive under the conservative per-pair classifier. This does not
establish the complete tight-target qualification. In particular, four-caller/one-index ordinary
search with histograms off observed 8.96% median throughput loss and 26.86% median p99 increase;
its wide ranges remain visible rather than being waived.

### CPU, memory and write costs

Each entry below is the range of **eight cell medians**, calculated from seven paired changes
per cell; it is neither the full raw range nor a confidence interval. Positive percentages mean
more candidate cost. The mixed-phase CPU interval includes timed queries, correctness checks,
20 mutation/publication cycles and periodic RSS sampling; it is not search-only or native-only
CPU. Setup CPU is separate and includes opening/indexing the corpus and initial publication.

| Observation                                   | Histograms off     | Histograms on      |
| --------------------------------------------- | ------------------ | ------------------ |
| Mixed query/write process CPU change          | −4.40% to +5.99%   | −4.36% to +5.19%   |
| Setup process CPU change                      | −5.87% to +12.33%  | −4.41% to +3.02%   |
| Open/index/initial-publication elapsed change | −0.74% to +6.12%   | −2.37% to +7.26%   |
| Publication p50 callback latency change       | −1.62% to +2.88%   | −2.50% to +6.36%   |
| Publication p99 callback latency change       | −2.80% to +2.90%   | −3.52% to +7.91%   |
| Mutation-plus-publication p50 change          | −1.70% to +3.00%   | −2.49% to +6.39%   |
| Mutation-plus-publication p99 change          | −2.79% to +2.93%   | −3.49% to +7.97%   |
| Sampled peak process RSS difference           | −1.08 to +2.59 MiB | −2.77 to +0.77 MiB |

Peak RSS uses 25 ms sampling during the mixed query/write interval plus its terminal observation;
it excludes setup peaks and can miss spikes between samples. It describes the entire process,
including the driver, JavaScript and native engine. These finite differences do not establish
per-index memory attribution, retained-memory growth or production headroom. The CPU/write-cost
ranges also remain observations rather than additional passed gates.

## Optional observation costs

The initial polling profile enabled histograms and scraped every 10 ms, so it measured their
**combined** cost. With four callers its median p99 increases ranged from 24.37% to 29.37%;
it cannot isolate scraping overhead. The original raw report applied the histogram thresholds
to these polling cells. Those labels are preserved as historical output, but are not an accepted
polling gate: the reviewed tight targets apply to the off/on profiles only.

Snapshot-byte observations were a separate profile with histograms disabled. Its median paired
mutation-plus-publication p50 changes ranged from −4.50% to −0.03%. This noisy result does not prove
zero observation cost. Byte work runs after publication settles and may delay the next writer
command, so publication callback latency alone cannot measure its cost.

The current [driver](../benchmarks/observability.mjs) leaves histograms **off** while polling and
reports polling/byte profiles as `cost-observation-only`, with no borrowed throughput/p99 target.
That revised polling profile was **not measured by this initial run**. A longer off/on matrix
with 40,000 queries per process was planned after the runtime gates; its later results are recorded
below. See [the protocol](observability.md#qualifying-overhead) for module
selection, raw reporting and scope.

## Refined 40,000-query matrix

The longer comparison measured [candidate `8ebe625`](https://github.com/HarperFast/fulltext/commit/8ebe6257e97187980cc4741a0f4dcf4763e146ce)
against the same baseline `bb4fac1`, on Node.js 24.16.0/Darwin arm64. The 16 off/on cells used
seven pairs, 2,000 documents per index and 40,000 timed queries per process. All **224 fresh
children and 8,960,000 timed correctness checks completed**. This identifies the measured commit,
not any subsequent runtime refinements. Raw SHA-256:
`de66be505b4929ebc86b3c39b385704c6be1cd203e0206081f0fbcd548b5c5ad`.

The percentage definitions and full-pair-range interpretation remain the same as above. Three
cells were within target in every pair; 13 were inconclusive. **The complete 1%/3%/5% targets remain
unestablished.** No threshold changed. The histograms-off/four-caller/one-index ordinary cell again
had a positive median throughput loss, now 5.37% (full range −11.33% to +18.05%). Histograms-on in
that cell observed −1.53% median loss. That disagreement prompted the same-process diagnostic below;
it does not justify discarding either matrix result.

| Search    | Callers | Indexes | Histograms | Throughput loss: median [range] | p99 increase: median [range] |
| --------- | ------- | ------- | ---------- | ------------------------------- | ---------------------------- |
| ordinary  | 1       | 1       | off        | -3.57 [-8.74, 7.04]             | 1.63 [-28.26, 56.10]         |
| ordinary  | 1       | 1       | on         | -6.45 [-14.18, 1.44]            | -8.10 [-50.83, 17.63]        |
| expensive | 1       | 1       | off        | -4.35 [-9.55, 1.41]             | -4.44 [-14.15, 22.57]        |
| expensive | 1       | 1       | on         | -5.55 [-7.43, -1.59]            | -5.78 [-22.47, 2.64]         |
| ordinary  | 1       | 2       | off        | -2.83 [-6.79, 2.37]             | -3.44 [-9.34, 8.40]          |
| ordinary  | 1       | 2       | on         | -4.81 [-7.23, -1.77]            | -2.14 [-17.48, 7.64]         |
| expensive | 1       | 2       | off        | -4.51 [-11.79, 7.74]            | -8.70 [-17.22, 3.00]         |
| expensive | 1       | 2       | on         | -5.21 [-9.95, -2.71]            | -2.03 [-23.02, 12.47]        |
| ordinary  | 4       | 1       | off        | 5.37 [-11.33, 18.05]            | 3.37 [-19.21, 25.86]         |
| ordinary  | 4       | 1       | on         | -1.53 [-6.39, 8.44]             | 3.91 [-4.15, 29.04]          |
| expensive | 4       | 1       | off        | -0.07 [-11.06, 3.35]            | -0.89 [-3.68, 15.65]         |
| expensive | 4       | 1       | on         | -4.27 [-13.72, 0.35]            | 0.18 [-5.39, 2.96]           |
| ordinary  | 4       | 2       | off        | -1.75 [-27.47, 1.33]            | -3.22 [-35.46, 5.20]         |
| ordinary  | 4       | 2       | on         | -4.34 [-11.17, 5.68]            | -1.79 [-9.56, 7.35]          |
| expensive | 4       | 2       | off        | -4.21 [-12.08, -2.01]           | 1.11 [-3.60, 7.37]           |
| expensive | 4       | 2       | on         | -4.85 [-12.35, -0.29]           | -0.74 [-32.38, 62.95]        |

CPU, setup, publication and sampled-RSS ranges below use the same eight-cell-median calculation
and measurement scopes as the initial matrix. Publication p99 differences remain visible; they
are not a separate passed gate.

| Observation                                   | Histograms off     | Histograms on      |
| --------------------------------------------- | ------------------ | ------------------ |
| Mixed query/write process CPU change          | −4.30% to +3.49%   | −6.10% to −0.02%   |
| Setup process CPU change                      | −2.38% to +11.97%  | −6.44% to +0.37%   |
| Open/index/initial-publication elapsed change | −14.41% to +6.57%  | −5.97% to +1.66%   |
| Publication p50 callback latency change       | −12.07% to +6.82%  | −8.63% to +5.06%   |
| Publication p99 callback latency change       | −23.27% to +24.25% | −18.11% to +39.34% |
| Mutation-plus-publication p50 change          | −12.03% to +6.82%  | −8.62% to +5.08%   |
| Mutation-plus-publication p99 change          | −23.26% to +24.19% | −18.11% to +39.32% |
| Sampled peak process RSS difference           | −1.11 to +4.81 MiB | −2.52 to +1.31 MiB |

## Same-process phase control

Four private diagnostic processes used the same measured candidate/baseline packages, corpus,
ordinary exact-total request and correctness oracle, with four callers and one index. Each made
one initial publication, then eight alternating off/on phases, each with 2,000 untimed warmup
queries and 100,000 timed queries. Normal and reversed order were run for each package.
**All 32 timed phases, 3,200,000 timed queries and 64,000 warmup queries completed correctly.**

Baseline phases used the same labels without configuration calls because that package lacks
telemetry configuration. Candidate phases changed the real histogram flag. There was no forced
GC, polling timer, tuning or binary swap; final telemetry was read only after all timed phases.
A phase median over four values uses the mean of the middle two. These exploratory diagnostics
have no pass/fail classification.

| Process                  | Label | Throughput (queries/s) | Query process CPU (ms) | Legacy search queue sum (ms) | Legacy search execution sum (ms) |
| ------------------------ | ----- | ---------------------- | ---------------------- | ---------------------------- | -------------------------------- |
| Baseline, normal order   | off   | 81,114                 | 4,354.9                | 567.0                        | 3,227.5                          |
| Baseline, normal order   | on    | 83,338                 | 4,250.8                | 535.8                        | 3,145.0                          |
| Baseline, reverse order  | off   | 85,590                 | 4,159.3                | 503.4                        | 3,075.2                          |
| Baseline, reverse order  | on    | 84,005                 | 4,257.8                | 515.2                        | 3,121.9                          |
| Candidate, normal order  | off   | 86,652                 | 4,097.7                | 499.0                        | 2,951.6                          |
| Candidate, normal order  | on    | 89,476                 | 3,998.9                | 466.9                        | 2,895.0                          |
| Candidate, reverse order | off   | 85,851                 | 4,171.4                | 498.5                        | 3,031.0                          |
| Candidate, reverse order | on    | 89,234                 | 4,009.5                | 465.6                        | 2,895.3                          |

Legacy spans are sums across concurrent operations, not elapsed wall time; CPU includes the
JavaScript driver, oracle and native work. Candidate on/off phase differences moved with CPU and
legacy queue/execution sums, but their ranges overlapped and the unconfigured baseline labels
also varied. Adjacent-pair on-versus-off throughput changes were:

| Process                  | Median change | Full four-pair range |
| ------------------------ | ------------- | -------------------- |
| Baseline, normal order   | +3.42%        | −3.82% to +8.60%     |
| Baseline, reverse order  | −1.85%        | −10.90% to +9.09%    |
| Candidate, normal order  | +2.14%        | −1.27% to +11.39%    |
| Candidate, reverse order | +4.12%        | −0.83% to +15.92%    |

The same-process runs did not reproduce a stable candidate-off slowdown relative to baseline:
candidate-off phase medians were 85.9–86.7k queries/s, baseline-off 81.1–85.6k. This does **not**
prove a speedup, show that enabling histograms causes better performance, explain the separate-
process matrix discrepancy, or establish the tight targets. There are only two processes per
package with fixed alternating phase order, so process/JIT/GC/scheduling effects remain confounded.
RSS endpoints across phases ranged roughly 85–199 MiB; retained latency arrays and normal GC are
part of the driver, and this is not a retained-native-memory diagnosis.

All four raw diagnostic reports are preserved privately with these SHA-256 identities:

| Report                   | SHA-256                                                            |
| ------------------------ | ------------------------------------------------------------------ |
| Baseline, normal order   | `f4d0f8aebc26a46cb5af55664795272b41bd956aeaf94ec16a5517cdf1f02aae` |
| Candidate, reverse order | `3c1d2d3001d79dc1dfb2ef54e9cefba28b66323b698b4f65ac765b24a7b5e0b5` |
| Candidate, normal order  | `5843199dff7861bd6d3b0ffc6030f852bb5bba45f52f70eea9e1104301df8c8e` |
| Baseline, reverse order  | `49f1d53c50134f47759e67f1912b2e69486c40acee42795f14ba800a2ed0ee28` |

The adjudication is limited: preserve the repeatably positive matrix cell as an unresolved
performance qualification concern, while treating its cause and any configuration-specific
regression as unproven. Subsequent timing/accounting refinements need their own measured evidence;
these runs cannot qualify changes made after `8ebe625`.

## Refined native recorder microbenchmark

At runtime commit [`8ebe625`](https://github.com/HarperFast/fulltext/commit/8ebe6257e97187980cc4741a0f4dcf4763e146ce), the optimized ignored Rust microbenchmark completed 100,000 operations per thread, including ticket ownership, admission, dispatch, queue/execution clocks and settlement. These are single local trials, not confidence intervals or end-to-end overhead percentages. Four threads share the same metrics, deliberately exercising counter contention.

| Threads | Histograms | Operations | Elapsed milliseconds | Wall nanoseconds per operation |
| ------- | ---------- | ---------- | -------------------- | ------------------------------ |
| 1       | off        | 100,000    | 9.325250             | 93.25                          |
| 1       | on         | 100,000    | 9.634834             | 96.35                          |
| 4       | off        | 400,000    | 74.081334            | 185.20                         |
| 4       | on         | 400,000    | 102.700208           | 256.75                         |

The refined recorder removes the redundant completed-counter update and constructor clock. It preserves independent admission attempts; completed totals derive from success plus failure. This microbenchmark does not establish the whole-wrapper 1%/3% targets.
