# Metadata premerge acceptance

Status: draft — consumer, lifecycle and Harper Table checks verified locally; workload acceptance pending. Owner: Harper Engineering · Last reviewed: 2026-10-28.

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
| Workload correctness | Compare metadata results with fixture-derived expected values at every tested caller width; normal requests have zero errors; timed failures recover.          | Untested; targets below.                                                                       |
| Local source handoff | Rebuild from actual Harper Table records, catch up the source journal, verify checkpoint-aligned cutover, and reconstruct rollback from retained source state. | Verified locally: checkpoints 7, 10 and 11, child-process reopen and old-build reconstruction. |

## Planned workload gates

Use a synthetic product catalog. Fix the workload and gates before recording results. At every
case/caller-width cell, the planned local acceptance thresholds are:

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
[the pre-expansion baseline](https://github.com/HarperFast/fulltext/commit/f808c58) and current
module paths. Compare the same case/caller-width cells. Expanded mode runs 12 cells for at least
30 seconds each, approximately six minutes plus setup and pressure checks.

## Workload acceptance still pending

One current-build flat run failed the unchanged 32 MiB last-five-round RSS-span gate. The failure
must remain part of acceptance evidence. A diagnostic rerun passed with the same gates; that
rerun does not explain or erase the failure. Historical samples sufficient to determine whether
the original span came from growth or a decrease were not retained. The expanded six-minute run
and investigation of this flag remain pending. No memory acceptance is claimed yet.

## Verified consumer and lifecycle evidence

Observed on 2026-10-28: the final full gate passed 124 Rust tests (five ignored), 416 Node tests
across four groups, and nine optimized expensive-lane tests. These runs cover first-value sorting,
missing placement, UTF-8 ties, nested aggregation scope, omitted child properties, resource rejection
and recovery. This is local macOS arm64 evidence, not platform-wide qualification.

The exercise exposed a close-quiescence defect: rollback replaced the original native writer
before its merge work was joined. The [close fix](https://github.com/HarperFast/fulltext/commit/33399a6)
joins the original writer, discards unpublished changes and preserves the published checkpoint.
A deterministic blocked-merge regression failed before the fix and passed afterward; it also passes
in the full gate. This verifies the close invariant rather than relying on cleanup retries.

The installed previous 0.7.0 facade/addon migration check passed: numeric/Boolean reuse and reverse
writes, unchanged-file rejection for incompatible strings, new-generation rebuild, and old-build
rollback reconstruction. See the [migration contract](metadata-migration.md) for the rebuild boundary.

## Verified timeout recovery

Observed final-addon count-load run: 100,000 documents, 60 requests per case/caller-width cell,
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
seeded seven records at checkpoint 7. Three source mutations were interleaved sequentially between
snapshot indexing chunks; replay reached the quiesced source checkpoint 10 before publication and
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

## Open items

- Complete the quiet paired workload rerun and expanded six-minute observation. Earlier timings overlapping compilation are excluded from acceptance.
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
