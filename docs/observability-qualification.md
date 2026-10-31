# Observability qualification

## Classified catalog and prefix comparison

This comparison measured runtime [`62bb041`](https://github.com/HarperFast/fulltext/commit/62bb041d65b6726344b16b810a1650dab536f8a4)
against unchanged [baseline `bb4fac1`](https://github.com/HarperFast/fulltext/commit/bb4fac150bae3f17e16a18c106b959966ba1bd83),
on Node.js 24.16.0/Darwin arm64. The 16 off/on cells used seven alternating-order pairs,
2,000 documents per index, 40,000 timed queries per process, one/four callers and one/two indexes.
All **224 fresh children and 8,960,000 timed correctness checks completed**. Every candidate
verified its selected native search-operation counter and absence of the other class during
warmup. No runtime after the named measured commit is qualified by these observations.

Ordinary `catalog`/`any` requests used bounded totals and the native ordinary queue. Prefix
requests retained exact totals and used the expensive queue. The catalog fixture gives all
documents tied scores at the page boundary, invoking the stable tie fallback after TopDocs.
It therefore measures a two-pass ordinary collector case, **not the cheapest ordinary path**.
The single-hit scenario is recorded separately below.

The complete tight targets remain **unestablished**: at most 1% median throughput loss with
histograms off, or 3% throughput loss/5% p99 increase with histograms on. Four cells were within
target in every observed pair and 12 were inconclusive; none was outside target in every pair.
The wide ranges are retained, including outliers, without relaxing a target. Neither a negative
median nor an individually successful cell proves a general overhead bound.

All values below are percentages: median paired change followed by the full seven-pair range.
Throughput loss is `1 − candidate / baseline`; p99 increase is `candidate / baseline − 1`.
These ranges are not confidence intervals.

| Search    | Callers | Indexes | Histograms | Throughput loss: median [range] | p99 increase: median [range] |
| --------- | ------- | ------- | ---------- | ------------------------------- | ---------------------------- |
| ordinary  | 1       | 1       | off        | -3.71 [-4.96, 2.17]             | 5.15 [-7.04, 24.68]          |
| ordinary  | 1       | 1       | on         | -5.48 [-58.34, 29.84]           | 0.00 [-85.40, 198.84]        |
| expensive | 1       | 1       | off        | -4.02 [-7.21, 4.52]             | 3.53 [-1.78, 22.79]          |
| expensive | 1       | 1       | on         | -3.08 [-28.19, 62.70]           | -2.31 [-36.12, 2113.69]      |
| ordinary  | 1       | 2       | off        | -3.82 [-6.02, -0.36]            | 1.16 [-4.51, 49.68]          |
| ordinary  | 1       | 2       | on         | -7.17 [-48.49, 1.82]            | -8.55 [-76.98, 0.16]         |
| expensive | 1       | 2       | off        | -7.19 [-9.65, -1.81]            | -2.71 [-21.02, 12.85]        |
| expensive | 1       | 2       | on         | -5.58 [-8.35, -1.31]            | -3.73 [-30.77, 24.31]        |
| ordinary  | 4       | 1       | off        | -3.15 [-16.44, 15.35]           | -0.36 [-20.77, 100.68]       |
| ordinary  | 4       | 1       | on         | 0.04 [-20.74, 2.71]             | -2.41 [-24.00, 51.84]        |
| expensive | 4       | 1       | off        | -1.44 [-8.78, 17.83]            | 5.09 [2.10, 42.40]           |
| expensive | 4       | 1       | on         | -5.31 [-16.30, 5.96]            | -4.88 [-17.25, 24.99]        |
| ordinary  | 4       | 2       | off        | -0.06 [-2.75, 44.81]            | 7.90 [-15.16, 230.35]        |
| ordinary  | 4       | 2       | on         | -2.49 [-11.31, 5.08]            | 0.00 [-18.18, 7.94]          |
| expensive | 4       | 2       | off        | -22.70 [-97.37, -2.42]          | -40.92 [-61.87, 1.84]        |
| expensive | 4       | 2       | on         | -4.39 [-8.23, 1.82]             | -1.49 [-7.27, 8.80]          |

The CPU/write/memory summary is the range of eight cell medians per profile, not the full raw
range. Mixed process CPU includes timed queries, correctness checks, 20 mutation/publication
cycles and RSS sampling. Setup CPU separately covers open, indexing and initial publication.
Publication p99 changes are observations, not an additional passed gate.

| Observation                                   | Histograms off     | Histograms on      |
| --------------------------------------------- | ------------------ | ------------------ |
| Mixed query/write process CPU change          | −4.50% to +0.48%   | −7.42% to −2.17%   |
| Setup process CPU change                      | −4.11% to +7.72%   | −4.23% to +4.43%   |
| Open/index/initial-publication elapsed change | −13.44% to +16.39% | −25.52% to +7.21%  |
| Publication p50 callback latency change       | −4.65% to +19.30%  | −9.83% to +8.89%   |
| Publication p99 callback latency change       | −12.46% to +11.89% | −6.96% to +32.17%  |
| Mutation-plus-publication p50 change          | −4.73% to +19.24%  | −9.80% to +8.86%   |
| Mutation-plus-publication p99 change          | −12.36% to +11.90% | −7.00% to +32.15%  |
| Sampled peak process RSS difference           | −1.78 to +3.22 MiB | −3.48 to +2.89 MiB |

Peak RSS uses 25 ms mixed-phase sampling plus the terminal observation, excludes setup peaks,
and can miss inter-sample spikes. It includes JavaScript/driver/native allocations and provides
no per-index attribution or sustained-retention/headroom guarantee. These are finite local
synthetic measurements, not production qualification.

The complete raw report remains private and unchanged. SHA-256:
`b1efa7ea648fab6f1bd1ceafcbd2a2906c9772bf5bb2d358d64754230aba5c0d`.

## Single-hit ordinary comparison

The additional `ordinary-cheap` scenario uses unique term `product7`, `any`, bounded totals and
limit 10 against the same corpus. Its oracle requires exactly ID7, total1 and exact relation;
publication cycles mutate only ID0. One result avoids the page-boundary tie fallback. Production
smokes exercised baseline with eight documents and candidate off/on/polling/bytes with 100
documents; invalid operation filters and an undersized cheap corpus were rejected.

Full paired results measured benchmark commit [`bff340d`](https://github.com/HarperFast/fulltext/commit/bff340dbf0fe0481548c3e35e9e9f9dd13b8b3c6)
with unchanged `62bb041` runtime against baseline `bb4fac1`, on the same Node.js 24.16.0/Darwin arm64
host. All **eight cells, 112 fresh children and 4,480,000 timed correctness checks completed**,
using seven pairs and 40,000 queries per process. Every candidate verified ordinary-class routing
through native counters before timing. The actual request and single-hit oracle were also checked
across every raw process report.

All eight cells were inconclusive under the unchanged targets. Several medians exceed their
numeric targets: off/one-caller/one-index throughput loss 1.54%, off/four-callers/two-indexes 3.26%,
and on/four-callers/one-index throughput loss 5.42% with p99 increase 5.07%. Their full paired
ranges cross target boundaries; this is neither a passed tight bound nor a demonstrated stable
regression with an identified cause. No result was discarded or gate relaxed.

| Callers | Indexes | Histograms | Throughput loss: median [range] | p99 increase: median [range] |
| ------- | ------- | ---------- | ------------------------------- | ---------------------------- |
| 1       | 1       | off        | 1.54 [-13.71, 8.21]             | 0.49 [-15.38, 54.96]         |
| 1       | 1       | on         | 1.57 [-25.09, 20.84]            | -2.98 [-4.26, 11.82]         |
| 1       | 2       | off        | -0.66 [-15.35, 17.82]           | 3.31 [-23.29, 9.64]          |
| 1       | 2       | on         | -0.06 [-12.04, 12.34]           | -4.53 [-8.82, 5.61]          |
| 4       | 1       | off        | 0.26 [-8.51, 7.46]              | -1.98 [-11.16, 13.06]        |
| 4       | 1       | on         | 5.42 [-8.77, 13.22]             | 5.07 [-9.69, 84.28]          |
| 4       | 2       | off        | 3.26 [-2.74, 12.53]             | 1.97 [-7.00, 24.67]          |
| 4       | 2       | on         | -1.66 [-11.49, 0.53]            | -3.80 [-5.98, 5.05]          |

CPU/write/memory ranges use four cell medians per profile and the same measurement scope as
the classified catalog matrix. The single-hit query is cheaper than that tie-heavy query; its
measurements must not be replaced by the catalog results.

| Observation                                   | Histograms off     | Histograms on      |
| --------------------------------------------- | ------------------ | ------------------ |
| Mixed query/write process CPU change          | −1.19% to +3.59%   | −1.29% to +3.64%   |
| Setup process CPU change                      | −3.16% to +7.54%   | −1.46% to +2.51%   |
| Open/index/initial-publication elapsed change | −6.72% to +29.67%  | −3.01% to +2.56%   |
| Publication p50 callback latency change       | −4.78% to +12.56%  | −5.30% to +2.20%   |
| Publication p99 callback latency change       | −15.36% to +14.05% | −12.37% to +7.61%  |
| Mutation-plus-publication p50 change          | −4.93% to +12.55%  | −5.28% to +2.25%   |
| Mutation-plus-publication p99 change          | −15.33% to +14.04% | −12.37% to +7.63%  |
| Sampled peak process RSS difference           | −1.28 to +2.05 MiB | +0.05 to +2.22 MiB |

Raw report SHA-256:
`246a9eeb7f2c6a4a434770d6d02ae4c178bbce547d70e398716a10f4a1220b1c`.
The artifact remains private and unchanged. Across the classified and single-hit matrices,
13,440,000 timed queries were correct; **the complete 1%/3%/5% overhead qualification remains
unestablished**. These measurements do not establish production capacity or sustained memory.

## Isolated optional observation costs

This final optional-cost control measured [`b3f9be1`](https://github.com/HarperFast/fulltext/commit/b3f9be1178fb4c1e4120551f65d88203605fc2c2)
against baseline `bb4fac1`: four callers, two indexes, 2,000 documents per index, seven alternating
pairs per cell and 40,000 queries per child. All **four cells, 56 fresh children and 2,240,000 timed
queries completed correctly**. Candidate native class counters and disabled-histogram flags were
verified in every report. All 28 final reader snapshots in the byte profile had non-null bytes,
no pending observation and no observation error.

Polling scraped every **10 ms as a stress control, not a recommended export interval**, with
histograms disabled to isolate off-plus-polling cost. Byte observations also ran with histograms
disabled. Both profiles are `cost-observation-only` with null targets; the histogram off/on gates
are neither applied nor relaxed. All values are median [full seven-pair range], in percentages
except sampled RSS differences in MiB.

| Search         | Profile | Throughput loss       | p99 increase           | Mixed process CPU change | Mutation/publication p50 change |
| -------------- | ------- | --------------------- | ---------------------- | ------------------------ | ------------------------------- |
| ordinary-cheap | polling | 9.03 [4.76, 19.87]    | 36.24 [19.65, 109.57]  | 11.38 [4.24, 14.00]      | -5.36 [-35.57, 70.81]           |
| ordinary-cheap | bytes   | 2.25 [-21.99, 40.03]  | 9.53 [-38.34, 309.50]  | -0.06 [-5.83, 9.61]      | -23.48 [-51.06, 76.31]          |
| expensive      | polling | 2.30 [-17.21, 25.16]  | 49.31 [-12.54, 139.47] | 3.71 [-2.82, 18.37]      | -21.89 [-38.07, 107.67]         |
| expensive      | bytes   | -4.53 [-20.94, 20.25] | 1.28 [-23.73, 105.40]  | -4.88 [-9.33, 4.48]      | 32.82 [-29.67, 59.10]           |

| Search         | Profile | Sampled peak process RSS difference (MiB) | Mixed elapsed change (%) |
| -------------- | ------- | ----------------------------------------- | ------------------------ |
| ordinary-cheap | polling | 3.36 [-3.17, 7.00]                        | -6.60 [-18.86, 58.04]    |
| ordinary-cheap | bytes   | -2.63 [-11.13, 5.75]                      | -23.11 [-40.28, 55.69]   |
| expensive      | polling | 1.78 [-3.03, 7.22]                        | -5.38 [-28.02, 63.36]    |
| expensive      | bytes   | -1.30 [-5.17, 4.95]                       | 11.53 [-17.90, 41.68]    |

The polling single-hit case observed 9.03% median throughput loss, 36.24% p99 increase and 11.38%
CPU increase; this is measurable cost, not zero-overhead telemetry. The expensive polling case
observed 49.31% median p99 increase with a range crossing zero. Publication and byte-cost results
are noisy. CPU and elapsed include queries, oracle checks and 20 mutation/publication cycles;
RSS has the same sampled process-wide limitations described above.

Elapsed measurement ends before close and does not explicitly await the final byte-observer
tail. The preceding 19 publication cycles can carry the prior observation's queue wait into the
next mutation/publication cycle. Consequently, neither publication callback timing nor mixed
elapsed is a full-drain byte-cost bound. Final successful snapshot values verify availability,
not that the full observation tail was included in those timings.

This closes the isolated polling-cost evidence gap only for the measured four-caller/two-index
topology and query shapes. It does not establish a broad polling/byte-cost bound, production
capacity or sustained memory. Raw report SHA-256:
`f44a58c5c6efc616a3757e68268487aabaf1dd3edf7b1e7630863d4102f3c975`.
The full raw reports remain private and unchanged.

<details>
<summary>Historical exact-total-only measurements and recorder microbenchmark</summary>

> **Historical classification correction:** every matrix and phase-control run recorded below used
> `exactTotal: true`. The native classifier routes exact-total requests to the expensive-search
> queue, including the raw reports' `ordinary`/`any` labels. Both historical query shapes therefore
> measured expensive-queue work. They provide **no ordinary-queue or ordinary-contention evidence**.
> Raw reports and measurements remain unchanged. The corrected driver uses `any` with bounded
> totals for ordinary work and verifies candidate routing through native operation counters;
> none of the results within this historical section measures that corrected workload.

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
`any` exact-total request and correctness oracle, with four callers and one index. Despite its
historical ordinary label, that request used the expensive-search queue. Each made
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
performance qualification concern for its **expensive-queue exact-total query shape**, while
treating its cause and any configuration-specific regression as unproven. It cannot establish an
ordinary-path regression. Subsequent timing/accounting refinements and the corrected ordinary
workload need their own measured evidence; these runs cannot qualify changes after `8ebe625`.

## Refined native recorder microbenchmark

At runtime commit [`8ebe625`](https://github.com/HarperFast/fulltext/commit/8ebe6257e97187980cc4741a0f4dcf4763e146ce), the optimized ignored Rust microbenchmark completed 100,000 operations per thread, including ticket ownership, admission, dispatch, queue/execution clocks and settlement. These are single local trials, not confidence intervals or end-to-end overhead percentages. Four threads share the same metrics, deliberately exercising counter contention.

| Threads | Histograms | Operations | Elapsed milliseconds | Aggregate wall ns/operation | Run wall ns ÷ 100,000 per-thread operations |
| ------- | ---------- | ---------- | -------------------- | --------------------------- | ------------------------------------------- |
| 1       | off        | 100,000    | 9.325250             | 93.25                       | 93.25                                       |
| 1       | on         | 100,000    | 9.634834             | 96.35                       | 96.35                                       |
| 4       | off        | 400,000    | 74.081334            | 185.20                      | 740.81                                      |
| 4       | on         | 400,000    | 102.700208           | 256.75                      | 1,027.00                                    |

Elapsed time includes thread startup, joining and scheduling. Dividing run wall time by each
thread's 100,000 operations is a normalization, not measured per-thread CPU time or a formal upper
bound. Four-thread admission contention differs from production admission by one owning JavaScript
thread, and the microbenchmark uses a standalone clock path. This is an artificial stress fixture,
not a production execution model or performance guarantee.

The refined recorder removes the redundant completed-counter update and constructor clock. It preserves independent admission attempts; completed totals derive from success plus failure. This microbenchmark does not establish the whole-wrapper 1%/3% targets.

</details>
