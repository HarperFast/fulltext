# Metadata premerge acceptance

Status: local acceptance runs passed; historical RSS observation remains unexplained and requires review before merge. Owner: Harper Engineering · Last reviewed: 2026-10-28.

Merge qualification covers the public metadata contract, a representative local synthetic workload,
and a local Harper Table source-journal reconstruction exercise. It does not qualify a production
service, deployed fulltext adapter, distributed replication, or a production latency objective.

## Acceptance matrix

| Area                 | Required behavior                                                                                                                                              | Evidence / status                                                                              |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Consumer sorting     | Use the first supplied array value; honor per-field missing placement and ordered keys; resolve equal keys by raw UTF-8 ID.                                    | Verified locally: public/native regressions and Harper consumer harness.                       |
| Nested results       | Collect children within the matching parent bucket; preserve named request order; omit the `aggregations` property when no children were requested.            | Verified locally: public/native regressions and Harper consumer harness.                       |
| Resource rejection   | Return a stable resource error, return no partial results, and allow a valid request afterward.                                                                | Verified locally: public/native regressions and Harper consumer harness.                       |
| Pagination           | Reject metadata sort/aggregation requests through `searchPage`; use bounded offset pagination.                                                                 | Verified locally: public/native regressions and Harper consumer harness.                       |
| Terms accuracy       | Expose approximate candidate-selection diagnostics; exact document totals do not make top terms an exact census.                                               | Contract acceptance; not a billing/census guarantee.                                           |
| Timeout recovery     | Real 1 ms budgets may expire cooperatively; full-budget requests afterward return exact totals and drain queues.                                               | Verified: final count-load run; details below.                                                 |
| Workload correctness | Compare metadata results with fixture-derived expected values at every tested caller width; normal requests have zero errors; timed failures recover.          | Verified: final quiet flat pairs and expanded observation passed the declared gates.           |
| Local source handoff | Rebuild from actual Harper Table records, catch up the source journal, verify checkpoint-aligned cutover, and reconstruct rollback from retained source state. | Verified locally: checkpoints 7, 10 and 11, child-process reopen and old-build reconstruction. |

## Declared workload gates

Use a synthetic product catalog. Fix the workload and gates before recording results. At every
case/caller-width cell, the premeasurement local acceptance thresholds were:

- Zero normal-request errors, exact fixture-derived results, and drained queues.
- Normal-request p95 at most 250 ms, p99 at most 5,000 ms, and successful throughput at least
  10 requests/second.
- Sampled peak RSS growth at most 256 MiB above the post-index baseline; the last five round-end
  RSS samples span at most 32 MiB. This bounded short observation is not a leak or sustained-memory proof.
- After a resource rejection or timeout batch, a full-budget request returns the expected results.
  Expected pressure errors are exempt from the normal-request latency gate.
- For each flat API-compatible case/caller-width cell, pool three alternating baseline/current
  pairs: current median p95 must be at most 1.20 times baseline, and median successful throughput
  must be at least 0.80 times baseline. Compare the same feature and fixture in both builds.
  Expanded-only features have no previous-version equivalent; report their measurements against
  the absolute local gates without claiming a comparative speedup.

These gates trigger investigation of this fixture. They are not product performance promises.
Record workload sizes, caller widths, timings, sampling method, and deviations alongside results;
do not label a failed gate as passed because another workload was faster.

## Reproduce from source checkouts

These drivers are source-checkout tools, not installed-package files. Build the new fulltext
checkout with `npm run build`; build the previous fulltext and Harper checkouts separately.
Each module path must select its matching facade and addon. Use an existing isolated scratch parent.

```sh
FULLTEXT_PREFER_LOCAL_BUILD=1 node scripts/qualify-harper-metadata.mjs --harper-root /built/harper --module /new/dist/native.js --old-module /old/dist/native.js --scratch-dir /existing/scratch
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/metadata-workload.mjs --module /new/dist/native.js --scratch /existing/scratch --documents 50000 --queries 60 --rounds 10 --concurrency 1,4,8 --features flat
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/metadata-workload.mjs --module /new/dist/native.js --scratch /existing/scratch --documents 50000 --queries 60 --concurrency 1,4,8 --features expanded --duration 30
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/metadata-count-load.mjs --module /new/dist/native.js --scratch /existing/scratch --documents 100000 --queries 60 --concurrency 1,4,8 --budget-ms 1
```

Run flat mode three times per revision in alternating process order, using the same driver with
[the pre-expansion baseline (f808c58)](https://github.com/HarperFast/fulltext/pull/75/commits) and current
module paths. Compare the same case/caller-width cells. Expanded mode runs 12 cells for at least
30 seconds each, approximately six minutes plus setup and pressure checks.

## Flat workload acceptance and retained RSS observation

Three final quiet alternating pairs compared the same three flat cases at 1/4/8 callers, using
50,000 records and ten rounds of 60 requests per cell. All nine cells passed the unchanged paired
thresholds. The table reports medians of three per-process pooled p95s and throughputs as current /
baseline ratios; absolute gates also passed in these final acceptance runs.

| Case                   | Callers | p95 ratio | Throughput ratio |
| ---------------------- | ------- | --------- | ---------------- |
| ordinary-exact         | 1       | 0.983     | 1.009            |
| ordinary-exact         | 4       | 0.984     | 1.012            |
| ordinary-exact         | 8       | 0.998     | 1.000            |
| flat-price-sort        | 1       | 0.950     | 1.053            |
| flat-price-sort        | 4       | 0.925     | 1.061            |
| flat-price-sort        | 8       | 0.938     | 1.052            |
| flat-filter-candidates | 1       | 1.002     | 0.998            |
| flat-filter-candidates | 4       | 0.999     | 0.997            |
| flat-filter-candidates | 8       | 1.003     | 0.995            |

An earlier current-build flat run failed the unchanged 32 MiB last-five-round RSS-span gate.
That observation remains part of acceptance evidence; its growth/decrease direction and cause
are unknown because sufficient historical samples were not retained. An exact diagnostic rerun,
ten fresh GC-traced processes and ten fresh untraced processes subsequently passed unchanged
gates. Across the latter 20 processes, maximum tail span was 0.969 MiB and maximum sampled peak
RSS growth was 10.984 MiB. This bounded investigation is complete without reproducing or explaining
the original failure. Passing final runs do not erase it or establish leak absence; review the
residual observation before merge and retain it in
[Qualify remaining native metadata collector resource tradeoffs](https://github.com/HarperFast/fulltext/issues/76).

## Verified expanded workload observation

Observed final-addon run: 50,000 synthetic product records, four cases at 1/4/8 callers, each cell
running at least 30 seconds. All 895,680 normal requests passed the source oracle and absolute
local gates across 12 cells. The worst pooled p95 was 10.143 ms; maximum sampled RSS growth was
45.125 MiB above the post-index baseline. The largest last-five-round RSS span was 2.969 MiB,
below the 32 MiB gate. These sampled short-run results do not establish leak freedom.

| Nested multi-sort case callers | Completed requests | p95 ms | Successes/second |
| ------------------------------ | ------------------ | ------ | ---------------- |
| 1                              | 23,520             | 1.304  | 783.4            |
| 4                              | 23,400             | 5.742  | 779.4            |
| 8                              | 24,600             | 10.143 | 820.0            |

High-cardinality histogram and long-term-key pressure both returned `E_RESOURCE_LIMIT`. Subsequent
full-budget requests on the same handles recovered exact totals of 50,000 and 300 respectively,
with the expected first IDs and drained queues. There is no old-API equivalent for the nested case;
these observations make no previous-version speedup claim. The unexplained earlier flat RSS
observation remains recorded above despite passing final acceptance runs.

## Verified consumer and lifecycle evidence

Observed on 2026-10-28: the final full gate passed 126 Rust tests (five ignored), 421 Node tests
across four groups, and nine optimized expensive-lane tests. These runs cover first-value sorting,
missing placement, UTF-8 ties, nested aggregation scope, omitted child properties, resource rejection
and recovery. This is local macOS arm64 evidence, not platform-wide qualification.

The exercise exposed a close-quiescence defect: rollback replaced the original native writer
before its merge work was joined. The [close fix (33399a6)](https://github.com/HarperFast/fulltext/pull/75/commits)
joins the original writer, discards unpublished changes and preserves the published checkpoint.
Discard may flush unpublished segments while joining, so it requires I/O headroom. A natural
storage-error regression verifies that an unsuccessful shutdown is not marked quiesced: native
handle close reports `E_QUIESCENCE_FAILED`, requiring process restart before reset.
A deterministic blocked-merge regression failed before the fix and passed afterward; it also passes
in the full gate. A separate two-worker regression proves a failed native commit preparation can
detach another still-running worker. The wrapper now retains that ownership uncertainty through
later close attempts; a successful join of remaining handles cannot clear the restart requirement.
This regression also fails before and passes after the fix. These tests verify ownership rather
than relying on cleanup retries.

The installed previous 0.7.0 facade/addon migration check passed: numeric/Boolean reuse and reverse
writes, unchanged-file rejection for incompatible strings, new-generation rebuild, and old-build
rollback reconstruction. See the [migration contract](metadata-migration.md) for the rebuild boundary.

## Verified timeout recovery

Observed count-load run: 100,000 documents, 60 requests per case/caller-width cell,
and 1/4/8 callers. All 360 normal requests returned exact results. With a 1 ms budget, 180 AND
requests succeeded and 180 filtered dense-OR requests returned `E_TIMEOUT`. All 12 measured cells
then passed full-budget recovery with exact totals and zero queued commands/bytes. The largest
reported timeout p99 was approximately 2.088 ms: the budget is cooperative, not a hard wall-clock
limit. This public run does not identify whether expiry occurred in queueing, hits or Count;
the deterministic native interruption regression covers active collection.

The native handle-close regression verifies quiescence on that lifecycle path. Direct Rust
`Writer::rollback` remains a pre-existing native primitive without that quiescence contract;
direct Rust callers should close the original writer when they require safe resource shutdown.
This distinction is not an outstanding JavaScript handle-close failure.

## Local source-journal procedure

1. Write the authoritative fixture through actual built Harper Table CRUD and record its source
   checkpoint in the harness-owned durable journal.
2. Build a separate derived generation from a captured source boundary while recording later changes.
3. Apply the journal through a declared checkpoint and verify derived results against source records.
4. Switch local reads only after publication and verification; exercise a later source mutation.
5. Reconstruct the rollback generation from retained authoritative records/journal and verify it before
   switching back. Preserve enough source history to make this reconstruction possible.

Observed local harness result: actual built Harper 5.2.5 at
[the recorded source revision](https://github.com/HarperFast/harper/commit/9f469c079a0624a27ff66165078b98e9a4e1ec35)
seeded seven records at checkpoint 7. Three source mutations were interleaved sequentially before and during
snapshot indexing; replay reached the quiesced source checkpoint 10 before publication and
cutover. A separate child reopened source and derived reader and verified checkpoint and results.
Resumed ingestion advanced to checkpoint 11 and passed another reopen. Reset with the new build
followed by old-build reconstruction reopened all seven latest records at checkpoint 11. The
retired old artifact remained present through successful new publication.

The harness verifies source-tenant candidate restriction before aggregate collection, ordered
first-value/missing/UTF-8 sorting, parent-scoped numeric children, resource errors and same-handle
recovery. Its Table, Resource and database entry files have recorded combined SHA-256 digest
`5276a08ec97347b40d8a895e6f9d170050edac2d5bf516e8144e9ac6acd95b50`.
This fingerprints those loaded entry files, not the entire Harper build.

These are sequential local journal interleavings, not a concurrent external writer, HTTP workload,
distributed replication, or production fulltext adapter. Stop qualification if a source change is
missing, a checkpoint advances before its mutations are indexed, or a reopened generation disagrees
with source records.

## Longer mixed read/write soak procedure

The source-only `benchmarks/metadata-soak.mjs` driver reuses the workload oracle. Defaults run
50,000 synthetic catalog records with eight concurrent readers, 100 upserts plus ten deletes
approximately every two seconds, and restoration of the previous publication's deleted records.
Thirty minutes of mixed reads/writes are followed by writer drain and ten minutes of reads.
Queries cover exact ranked hits, price sorting, nested histogram/statistics buckets and a five-key
runtime sort. Every response is checked against its leased reader's fixed published generation.

Declare gates before measurement: per-phase/case p95 at most 250 ms, p99 at most 5,000 ms,
overall throughput at least ten successful requests/second, sampled peak RSS growth at most
256 MiB above the post-initialization baseline, and a final-five-minute RSS span at most 32 MiB
in each phase. These are the existing local acceptance thresholds, extended in duration; they
are not a production performance objective. Short CI smoke uses explicitly relaxed gates and
checks correctness, evidence retention and cleanup.

```sh
FULLTEXT_PREFER_LOCAL_BUILD=1 node benchmarks/metadata-soak.mjs --artifacts /existing/scratch/new-soak-run
```

The artifact directory must be new. It retains `manifest.json`, streamed `samples.ndjson`, and
`report.json` on success or `failure.json` on failure. The manifest records the tested revision,
driver/oracle hashes, loaded-addon identities where reported by Node, machine and runtime details,
workload and gates. Samples include RSS, heap/external memory, queues, committed segment counts
and periodic disk observations. Histograms and recent samples stay bounded in memory. No forced
GC is performed. Instrumentation and source-oracle allocations remain part of process RSS. Mixed-phase query
latency includes event-loop stalls from source-oracle preparation; it does not isolate native latency.

A pass is emitted only after close, final-checkpoint reopen/verification and native index cleanup.
Operational failures before cleanup retain the native index and close diagnostics. Reporting
failures after cleanup record that the index is absent; initialization failures before the manifest
may have no artifact. Preserve every run, including failures;
a passing rerun cannot explain an earlier failure. See [measurement invariants](../benchmarks/DESIGN.md).

## Open items

- Review the retained unexplained RSS observation before merge; final acceptance runs passed.
  Earlier timings overlapping compilation are excluded from acceptance.
- Qualify a consuming application's adapter, retention policy, downtime tolerance, distributed
  catch-up, and production cutover/rollback separately. No existing main/core/pro fulltext adapter
  is assumed by this local exercise.
- Before publication, choose a release version and align root/Cargo/platform package versions.
  This separate release task is tracked in [Qualify remaining native metadata collector resource tradeoffs](https://github.com/HarperFast/fulltext/issues/76);
  this acceptance work does not select or authorize a version.

## Sources

- [Executable Harper Table qualification](../scripts/qualify-harper-metadata.mjs).
- [Public count-load timeout driver](../benchmarks/metadata-count-load.mjs).
- [Synthetic workload driver](../benchmarks/metadata-workload.mjs).
- [Native close lifecycle and regression](../src/engine.rs).
- [Public metadata contract](../README.md#native-metadata-sorting-and-aggregations).
- [Resource policy](limits.md#metadata-collectors).
- [Index migration procedure and prior local qualification](metadata-migration.md).
