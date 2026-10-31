# Qualification measurement

## Observability comparisons

`observability.mjs` uses fresh child processes and separate baseline/candidate module paths, with
alternating order inside each pair. It cannot silently swap an addon under an already loaded
facade. Keep workload/correctness identical across profiles; baseline deliberately makes no
telemetry API calls. `--baseline-profile=match` instead compares matching telemetry settings;
that output is explicitly optimization-only and carries no feature-cost budget classification.
Optional snapshot-byte cost and polling are separate profiles. Preserve raw
pairs and mark noisy measurements inconclusive instead of treating a rounded median as proof of
the 1%/3% throughput targets. Finite local CPU/RSS observations are not sustained-memory evidence.

Default scenarios cover tie-heavy ordinary collection, single-hit `ordinary-cheap`, and expensive
prefix/exact-total work. `--operations` filters distinct known scenario names without changing
other matrix dimensions. Both ordinary shapes use bounded totals; requesting exact totals routes
to the expensive queue. Candidate warmup asserts actual native class counters. The catalog tie
case invokes the stable fallback after TopDocs, so it cannot bound the cheapest ordinary path.
The unique product7 case requires ID7/total1/exact, and ID0-only publications cannot change it.

Per-query timers stop when the search API resolves, before correctness validation. The report
labels API latency separately from query-window throughput/time (`measurementVersion: 2`). Each
bounded window (default 1,024 results) drains every caller before validating every result outside
its timer. CPU and event-loop utilization are sampled over those same windows; mixed CPU/elapsed
still include validation and 20 mutation/publication cycles. Polling defaults to 1,000 ms per handle,
one handle per timer turn, and reports actual counts and synchronous scrape durations. A short
run with few scrapes cannot qualify that schedule; retain 10 ms only as an explicit stress setting.
The default ten-poll/handle descriptive floor labels undersampled cells insufficient; it is not
an acceptance threshold or statistical confidence statement.

`observability-scrape-cost.mjs` retains the separate idle-handle protocol: immutable baseline and
candidate modules, serial fresh children, 100 searches/handle, default 25 warmup rounds and 120
measured calls/handle with event-loop turns between them. The five selected topologies cap total
search workers at 64 and handles at 16. Explicit aggregate queue budget covers writer, ordinary
and expensive queues. Results pool per-call quantiles per child and retain every per-handle
sample; they do not bound active workload stalls. Emit the child report only after verified close
and directory cleanup; preserve a primary measurement failure with any secondary cleanup errors,
and retain the directory whenever quiescence is unproven. Test fixtures verify configuration, warmup and
sample counts without treating a fake clock as native performance evidence.
Historical reports used validation-inclusive percentiles; preserve and label them rather than
reinterpreting their p99 values as API completion latency.

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
