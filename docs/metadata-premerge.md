# Metadata premerge acceptance

Status: draft — acceptance targets planned; no results claimed. Owner: Harper Engineering · Last reviewed: 2026-10-28.

Merge qualification covers the public metadata contract, a representative local synthetic workload,
and a local Harper Table source-journal reconstruction exercise. It does not qualify a production
service, deployed fulltext adapter, distributed replication, or a production latency objective.

## Acceptance matrix

| Area                 | Required behavior                                                                                                                                              | Evidence / status                                    |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Consumer sorting     | Use the first supplied array value; honor per-field missing placement and ordered keys; resolve equal keys by raw UTF-8 ID.                                    | Untested in this acceptance run.                     |
| Nested results       | Collect children within the matching parent bucket; preserve named request order; omit empty child arrays.                                                     | Untested in this acceptance run.                     |
| Resource rejection   | Return a stable resource error, return no partial results, and allow a valid request afterward.                                                                | Untested in this acceptance run.                     |
| Pagination           | Reject metadata sort/aggregation requests through `searchPage`; use bounded offset pagination.                                                                 | Untested in this acceptance run.                     |
| Terms accuracy       | Expose approximate candidate-selection diagnostics; exact document totals do not make top terms an exact census.                                               | Contract acceptance; not a billing/census guarantee. |
| Workload correctness | Compare metadata results with fixture-derived expected values at every tested caller width; normal requests have zero errors; timed failures recover.          | Untested; targets below.                             |
| Local source handoff | Rebuild from actual Harper Table records, catch up the source journal, verify checkpoint-aligned cutover, and reconstruct rollback from retained source state. | Untested; local harness under construction.          |

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

## Local source-journal procedure

1. Write the authoritative fixture through actual built Harper Table CRUD and record its source
   checkpoint in the harness-owned durable journal.
2. Build a separate derived generation from a captured source boundary while recording later changes.
3. Apply the journal through a declared checkpoint and verify derived results against source records.
4. Switch local reads only after publication and verification; exercise a later source mutation.
5. Reconstruct the rollback generation from retained authoritative records/journal and verify it before
   switching back. Preserve enough source history to make this reconstruction possible.

The local harness uses child-process reopen, journal catch-up, cutover, and rollback. It does not
exercise HTTP, distributed replication, or a production fulltext adapter. It must disclose whether
mutations overlap reconstruction and exactly which checkpoint
boundary it verifies. Stop qualification if a source change is missing, a checkpoint advances before
its mutations are indexed, or a reopened generation disagrees with source records.

## Open items

- Run and review the consumer, workload, and Harper Table harnesses; replace planned rows only with
  verified outcomes and their artifacts.
- Qualify a consuming application's adapter, retention policy, downtime tolerance, distributed
  catch-up, and production cutover/rollback separately. No existing main/core/pro fulltext adapter
  is assumed by this local exercise.
- Before publication, choose a release version and align root/Cargo/platform package versions.
  This separate release task is tracked in [Qualify remaining native metadata collector resource tradeoffs](https://github.com/HarperFast/fulltext/issues/76);
  this acceptance work does not select or authorize a version.

## Sources

- [Public metadata contract](../README.md#native-metadata-sorting-and-aggregations).
- [Resource policy](limits.md#metadata-collectors).
- [Index migration procedure and prior local qualification](metadata-migration.md).
