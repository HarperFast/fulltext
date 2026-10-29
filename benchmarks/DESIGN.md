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

Successful reporting follows writer drain, query completion, reader close, final checkpoint reopen
and source verification, then index-directory removal. Operational failures before cleanup retain the index and diagnostics, including close errors.
A reporting failure after cleanup records that the index is absent; it never emits a successful
report. Setup failures before the manifest cannot promise artifacts. Do not substitute cleanup retries for
native quiescence.

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
