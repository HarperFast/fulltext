# Wrapper observability

`index.telemetry()` and `reader.telemetry()` return bounded, process-local observations. They do
not execute a search, walk a directory, read source records, or perform network export. Applications
own the export interval and monitoring backend. See the executable
[vendor-neutral JSON exporter](../examples/observability.mjs).

```js
index.configureTelemetry({ histograms: true });
const snapshot = index.telemetry();
console.log(JSON.stringify(snapshot, (_key, value) => (typeof value === 'bigint' ? value.toString() : value)));
```

Counters, nanoseconds and bytes use `bigint`; convert them explicitly before JSON serialization.
Do not convert long-running counters to JavaScript `number` without checking safe-integer bounds.
Metric names and error-code labels are fixed. Requests, document IDs, field values, paths and error
messages are not metric labels. Use a bounded application-owned index label when exporting many
handles; avoid creating a monitoring series for every transient generation.

## Operations and admission

`native.operations` separates `applyFrame`, `commit`, `publish`, `reload`, `close`, `search`,
`expensiveSearch` and `trace`. Each operation exposes attempts, accepted/rejected admission,
dispatched work, completed/successful/failed outcomes, errors, request bytes, mutations and
queue/execution timing. Request bytes count admitted payloads; native mutation totals count mutations reported by successful apply frames, not unknown partial storage work after failure. A native attempt starts at a validated owning handle. Unknown handles
cannot be attributed to a handle; facade preparation failures appear separately under
`preparationRejections` and do not become native attempts.

Failures before the native search class is known, including classifier panic, are attributed to
`search`, not expensive-search admission. Public closed-facade search/trace guards count as
preparation rejections. Direct calls with unknown or released native handles remain outside
attributed per-handle counters; the wrapper does not retain tombstones to infer their owner.

At quiescence, attempted equals accepted plus rejected. Native `completed` is derived from succeeded
plus failed; attempts remain independently recorded. Accepted work is completed, queued or dispatched. `dispatched` is a current gauge that decreases on settlement, not a cumulative dispatch counter. It includes any wait for
a permit after dequeue; its wait contributes to queue time, not execution. Scrapes read relaxed
observations and may temporarily disagree across fields during a transition. Native cumulative
counters wrap modulo 2^64; exporters should handle a decrease as a wrap/restart boundary rather
than calculate a negative rate.

`native.queues` reports writer, ordinary-search and expensive-search occupancy, admitted bytes,
oldest age, capacity and whether capacity is shared. Queue `bytes` is current occupancy; operation `requestBytes` is cumulative admitted payload. A process-wide permit pool is not a separate
handle-owned capacity. Legacy `status().metrics.searchQueueNanoseconds` already includes post-dequeue permit wait, as does telemetry queue timing. The observations overlap; do not add them together. An empty/unavailable queue has no oldest age. There is no active native
cancellation API: deadline timeouts are errors, not cancellation counts.

`native.phases` reports commit, reader-reload and teardown success/failure counts and timing
independently of whole-operation timing. A successful commit phase does not imply a reader-visible publication. Explicit close
records its result before resolving the callback. Final telemetry remains readable after close;
configuring a closed handle rejects. The retained telemetry token owns scalar observations, not
an index, searcher, mapping or execution permit, so keeping it cannot keep an index generation open.

## Histograms and logical batches

Operation counters and timing totals are always collected. Histograms start disabled. Enable them
with `configureTelemetry({ histograms: true })`; omitted settings retain their prior values.
Configure through the owning writer/reader facade; directly configuring internal native handles or constructing duplicate facade owners is unsupported. Disabling freezes accumulated histogram data; re-enabling continues it. There is no reset API.
Histogram `count` and `sumNanoseconds` describe only enabled samples, independently of always-on
timing totals. Buckets are **noncumulative**; each `upperBoundsNanoseconds` entry is an inclusive
upper bound, with `null` for infinity. Convert to cumulative buckets when required by an exporter.
These buckets support approximate distributions, not exact p95/p99 values.

`logicalBatches` counts JavaScript batch attempts and outcomes, validated requested mutations,
processed mutations, rejected records, successful encoded transport frames/bytes. One logical batch can span several native frames; a failed batch can retain successful
partial progress. Direct low-level frames do not become logical batches. Requested-mutation and
frame-byte distributions sample only while histograms are enabled. Use the native `applyFrame`
metrics to inspect native transport work rather than summing logical batches and frames together.

## Reader observations and their limits

`native.reader` describes the currently installed searcher-visible snapshot: live/deleted document
counts, segment count, reader-local generation, observation age, duration and availability/error.
Standalone commit does not replace that snapshot. `generationId` is a reader-local identifier;
it is neither a durable content identity nor a freshness timestamp or cross-process cursor.
Retained previous values after an observation failure must be interpreted with their error/age.

Expensive byte observations start disabled:

```js
index.configureTelemetry({ snapshotBytes: true });
await index.reload();
const snapshot = index.telemetry();
// Native byte observation can still be pending after reload resolves.
```

Enabling alone queues no work. A subsequent snapshot replacement triggers observation. Tantivy
`Searcher::space_usage()` supplies the size of the large files referenced by that reader snapshot.
This excludes small metadata, older pinned snapshots, uncommitted files and in-progress merge
files; it is **not total directory size or resident memory**. Work runs after command settlement,
outside reader/cache locks, and can increase subsequent queue wait. Unchanged segment/deletion
sets reuse the last successful byte observation. `snapshotBytesAgeNanoseconds` tracks that measurement separately from the cheap reader observation age. Pending/error/duration fields distinguish missing,
stale and completed measurements. Byte duration describes the last attempt; disabling observations preserves cached values, so exporters should also inspect `snapshotBytesEnabled` and measurement age. Scraping never initiates I/O. Observation failures do not change
the result of publication or reload.

`native.unsupported` explicitly explains unavailable total-directory bytes, automatic-merge
statistics, per-index RSS and native cancellation. Process CPU/RSS belongs to OS monitoring.
Harper owns request latency, authorization, record hydration, replication and source-log lag;
this API makes no changes to Harper's public surfaces. If telemetry token acquisition fails,
opening the index still succeeds and `telemetry()` returns `available: false` with a bounded
reason. Operations continue; telemetry configuration then reports capability failure.

## Qualifying overhead

[The paired driver](../benchmarks/observability.mjs) loads separately prepared baseline and
candidate packages by absolute module path. It never swaps their native binaries. Each sample
runs in a fresh process; pair order alternates to reduce time-order bias. The default matrix covers
three search scenarios, one/four callers, one/two indexes, histograms off/on, 10 ms polling and
optional snapshot bytes. It records correctness checks, throughput, latency, indexing/publication
cost, process CPU/RSS, runtime versions and caller-supplied source revisions.

The default scenarios are `ordinary` (tie-heavy catalog/`any` with bounded totals), `ordinary-cheap`
(unique product7/`any`, one exact ID7 hit), and `expensive` (prefix with exact totals). Both ordinary
scenarios set `exactTotal: false`; the single-hit case avoids the tie fallback's second collector
pass. Prefix requests use the expensive-search queue. `--operations=ordinary-cheap` selects one
scenario; the comma-separated filter accepts only distinct known names. Candidate warmup
verifies both the selected native operation counter and the absence of attempts in the other
class before timing starts. Baseline routing follows the same request classifier. Historical
results used exact totals for both labels and therefore did not measure the ordinary queue.

```sh
npm run benchmark:observability -- \
  --baseline=/absolute/baseline/dist/native.js \
  --candidate=/absolute/candidate/dist/native.js \
  --baseline-revision=BASE_SHA --candidate-revision=HEAD_SHA \
  --pairs=7 --output=/absolute/observability-results.json
```

Predeclared targets are at most 1% median throughput loss with histograms off, and 3% throughput
loss/5% p99 increase with histograms enabled. Raw pairs and their ranges remain in the report.
The driver's conservative per-pair classification is not a statistical confidence interval;
mixed results are inconclusive, and a median alone does not establish a tight overhead bound.
Polling and snapshot-byte costs must be inspected separately. Byte work starts after publication
settles, so inspect mutation-plus-publication cycles and elapsed time as well as publication
callback latency; later writer commands can wait for the prior observation. Native accounting
microbenchmarks complement these end-to-end measurements; neither proves production capacity,
sustained memory behavior or per-index native memory attribution. No forced GC or allocator/OS
tuning is part of the protocol.

See [recorded qualification results](observability-qualification.md) for classified fixture-specific
comparisons and historical evidence. The current polling profile leaves histograms disabled to isolate
scraping cost; the initial recorded matrix used histograms enabled while polling.
