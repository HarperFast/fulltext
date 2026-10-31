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
with 40,000 queries per process is planned after the final runtime gates; no results from that
planned run are claimed here. See [the protocol](observability.md#qualifying-overhead) for module
selection, raw reporting and scope.

## Refined native recorder microbenchmark

At runtime commit [`8ebe625`](https://github.com/HarperFast/fulltext/commit/8ebe6257e97187980cc4741a0f4dcf4763e146ce), the optimized ignored Rust microbenchmark completed 100,000 operations per thread, including ticket ownership, admission, dispatch, queue/execution clocks and settlement. These are single local trials, not confidence intervals or end-to-end overhead percentages. Four threads share the same metrics, deliberately exercising counter contention.

| Threads | Histograms | Operations | Elapsed milliseconds | Wall nanoseconds per operation |
| ------- | ---------- | ---------- | -------------------- | ------------------------------ |
| 1       | off        | 100,000    | 9.325250             | 93.25                          |
| 1       | on         | 100,000    | 9.634834             | 96.35                          |
| 4       | off        | 400,000    | 74.081334            | 185.20                         |
| 4       | on         | 400,000    | 102.700208           | 256.75                         |

The refined recorder removes the redundant completed-counter update and constructor clock. It preserves independent admission attempts; completed totals derive from success plus failure. This microbenchmark does not establish the whole-wrapper 1%/3% targets.
