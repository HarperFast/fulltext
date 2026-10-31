# Qualification measurement

## Mixed metadata soak

`metadata-soak.mjs` gives each in-flight query a lease on a reader that is never reloaded. Its
source oracle is prepared for that reader's published checkpoint. A publication swaps the current
lease; retirement closes the old reader only after its queries finish. This prevents comparing a
legitimate old-snapshot result with newer source rows.

Latency histograms have fixed bins. Recent memory samples retain only the final five-minute
window; the sampling interval is at least 100 ms. Full samples stream to disk. Do not retain every
query or latency in memory: that makes the harness itself a growing part of the RSS measurement.
Source-oracle preparation and validation costs remain part of observed process behavior; this
fixture does not attribute RSS growth solely to native allocations. Mixed-phase query latency
also includes event-loop stalls while the next source oracle is prepared.

A resolved close result carrying `cleanupError` is a qualification failure, even when native resources were released. Both drivers inspect every native close result before claiming cleanup. Sampling captures the phase and phase start with the memory observation, before asynchronous diagnostics. Sort-key encoding is prepared once per distinct string per oracle preparation, outside comparisons; it remains part of measured process behavior. Historical driver/oracle hashes distinguish prior allocation behavior.

Successful reporting follows writer drain, query completion, reader close, final checkpoint reopen
and source verification, then index-directory removal. Operational failures before cleanup retain the index and diagnostics, including close errors.
A reporting failure after cleanup records that the index is absent; it never emits a successful
report. Setup failures before the manifest cannot promise artifacts. Do not substitute cleanup retries for
native quiescence.

Failure reports retain the first failing operation independently of later abort/close errors.
Commit errors distinguish Tantivy preparation from final publication while retaining their error
code and original message. These labels localize failures; they do not identify the native file
or establish an operating-system cause. The optional test artifact root retains synthetic
fixtures, and CI uploads only the manifest, failure JSON and streamed samples after a failed job.

Segment observations read the pinned Tantivy build's committed `meta.json`; they are diagnostics,
not a public storage interface. Disk size observations can race file retirement and are sampled
rather than exact high-water guarantees. No forced garbage collection or memory release is part
of the default qualification.

## Workload smoke diagnostics

`metadata-workload.mjs` writes coarse stage-begin events and elapsed time to stderr, outside timed
rounds. The event count depends on the fixed case/caller-width list and pressure checkpoints, not
query count or duration. Progress includes no native status and never polls a closing handle.
Only verified cleanup permits an accepted stdout report. The child-process test preserves progress
on timeout and prints successful profiles too, so platform differences can be localized without
changing the workload or deadline. A killed-child regression verifies named-stage retention.

The Node test runner reserves a separate serial batch for the workload and soak qualification
files. Their fixed child deadlines must not compete with other native test files. An exact-name
count guard fails on a missing or duplicated qualification file; release and optimized phases
remain separate. Native-worker tests retain their existing shared-process isolation batch.
Keep successful stage profiles: a green isolated run alone does not establish why a concurrent
run exceeded its deadline. Compare ingestion and close durations before attributing contention.

## Fuzzy controls

The `benchmark:query-controls` command builds production artifacts before running. Direct driver
invocation is reserved for explicitly prepared artifacts, including paired baseline comparisons.

`query-controls.mjs` uses source-derived edit families, not the engine's returned matches, as its
correctness oracle. The distance-one and distance-two profiles run in fresh processes at equal
construction work-unit admission; field counts differ deliberately. They are not equivalent
native allocation workloads. Atomic progress snapshots precede latency/RSS assertions, and
post-setup failure snapshots retain completed rows, observed memory, runtime and hashes before reporting
failure. A failed resource gate cannot become an accepted stdout report. Preserve failures and
compare unchanged-default controls before attributing a regression to the new options.

## Writer stall evidence

The opt-in writer-diagnostics helper emits five-second process CPU/RSS and existing native status snapshots. It uses an unref timer, contains sampling/output errors, serializes counters as decimal strings and detaches native status before close while continuing process CPU/RSS sampling until close settles. Native test-addon events use a separate prefix so progress JSON parsers ignore them. Diagnostic overhead and separate wall-clock samples are observations; neither changes workload acceptance or establishes native CPU attribution.

## Query relevance scoring

`npm run benchmark:query-scoring` builds production artifacts and runs the local addon. Direct driver invocation assumes an already-built optimized addon. The workload indexes 10,000 documents across four commits, with 7,500 Any/OR matches and 5,000 All/AND matches. A subset of required matches also contains the optional term, and optional-only records must stay excluded. Before timing, paired full-result checks verify membership, exact totals, doubled boost scores and positive optional bonuses.

Twenty warm sequential searches per row use limit 20, with exact totals disabled and enabled separately. The paired root-boost OR/AND rows measure composite wrapping; optional with required All measures native Must/Should composition against its required-only baseline. These costs remain accepted rather than introducing custom scorer or composite boost distribution semantics.

A same-process optimized run on 2026-10-31, macOS arm64, Node 24.16.0, Tantivy 0.27.0, native ABI 14 / query API 8 produced these local p50 milliseconds:

| Query                      | Exact totals off | Exact totals on |
| -------------------------- | ---------------: | --------------: |
| Any baseline               |            0.133 |           0.112 |
| Boosted Any                |            0.108 |           0.111 |
| Optional with required Any |            0.250 |           0.238 |
| OR baseline                |            0.123 |           0.120 |
| Root boost over OR         |            0.178 |           0.169 |
| AND baseline               |            0.118 |           0.120 |
| Root boost over AND        |            0.158 |           0.154 |
| All baseline               |            0.124 |           0.108 |
| Optional with required All |            0.217 |           0.204 |

The composite root boost and required-All optional paths cost more in this corpus. Direct Any lowering does not imply zero overhead for other query shapes. This small warm fixture contains repeated terms and ties; timings include the JavaScript/native boundary and are not a throughput or catalog-scale qualification. Re-run paired rows on representative customer distributions before drawing broader performance conclusions.
