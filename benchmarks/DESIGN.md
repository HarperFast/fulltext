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
fixture does not attribute RSS growth solely to native allocations.

Successful reporting follows writer drain, query completion, reader close, final checkpoint reopen
and source verification, then index-directory removal. A failure retains the index and diagnostics,
including close errors; it never emits a successful report. Do not substitute cleanup retries for
native quiescence.

Segment observations read the pinned Tantivy build's committed `meta.json`; they are diagnostics,
not a public storage interface. Disk size observations can race file retirement and are sampled
rather than exact high-water guarantees. No forced garbage collection or memory release is part
of the default qualification.
