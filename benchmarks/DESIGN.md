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

`npm run benchmark:query-scoring` builds production artifacts and runs the local addon. Direct driver invocation assumes an already-built optimized addon. The original sample indexed 10,000 documents across four commits, with 7,500 Any/OR matches and 5,000 All/AND matches. A subset of required matches also contains the optional term, and optional-only records must stay excluded. Before timing, paired full-result checks verify membership, exact totals, doubled boost scores and positive optional bonuses.

The original sample used twenty warm sequential searches per row, limit 20, with exact totals disabled and enabled separately. The paired root-boost OR/AND rows compare complete request latency with composite wrapping; optional with required All compares native Must/Should composition against its required-only baseline. Repeated score ties can force the unboosted top-docs collector to rerun through stable collection, so these rows dilute specialized-pruning differences and do not isolate the cost of losing block-WAND. These costs remain accepted rather than introducing custom scorer or composite boost distribution semantics.

The original sample's two-term Any baseline and boosted Any rows both used stable collection, so they did not measure the root leaf lowering's specialized pruning. That sample lacked a boosted-All row; the current driver and measurements below add two-, three- and four-term All comparisons.

A same-process optimized run of the original sample on 2026-10-31, macOS arm64, Node 24.16.0, Tantivy 0.27.0, native ABI 14 / query API 8 produced these local p50 milliseconds:

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

The composite root boost and required-All optional paths cost more in this corpus. Lowering a root boost directly over an Any leaf does not imply zero overhead for other query shapes. Recursive boosts retain native wrappers to preserve outer-first float32 multiplication. This small warm fixture contains repeated terms and ties; timings include the JavaScript/native boundary and are not a throughput or catalog-scale qualification. The varied-score mode below removes the boundary-tie rerun confound for its OR/AND pairs. Re-run paired rows on representative customer distributions before drawing broader performance conclusions.

### Varied-score collector boundary

The earlier varied-score sample kept the same membership distribution while varying term frequency and document length. It checked that ranks 20 and 21 differed for each OR/AND pair before timing; the current driver extends that check to every non-optional row. With these unfiltered Boolean queries, that check rules out the stable-collector boundary-tie rerun in `Engine::collect_search`. Output records the corpus and `distinctBoundary` for each row. This does not measure how many blocks Tantivy prunes.

After the nested-boost multiplication-order fix, an optimized run on the same machine/runtime produced these p50 milliseconds (10,000 documents, 20 warm sequential samples, limit 20):

| Query               | Exact totals off | Exact totals on |
| ------------------- | ---------------: | --------------: |
| OR baseline         |            0.132 |           0.158 |
| Root boost over OR  |            0.108 |           0.133 |
| AND baseline        |            0.072 |           0.094 |
| Root boost over AND |            0.101 |           0.126 |

All paired membership, total and boost-score checks passed, as did the four distinct-boundary assertions in both total modes. The OR cost difference changes sign relative to the tied corpus; neither sample establishes a general speedup or slowdown. Generic composite boosting remains an accepted native-library tradeoff. These small warm samples do not qualify throughput, memory at catalog scale, or other document/score distributions.

### Stable routing for wider term sums

The current driver adds a single-term baseline and root-lowered/native-wrapped comparisons, plus two-, three- and four-term All families. `boost(2, boost(1, clause))` is the native-wrapped control: its inner identity boost disappears, while the outer native wrapper remains. Complete membership, scaled scores and native-wrapped score bits must agree before any timing. All 40 rows completed in each of the tied and varied modes (20 query shapes across exact totals off/on). Both runs observed four committed segments; each All term has 5,000 postings spread across the committed batches, beyond one 128-document block. Committed `meta.json` observations are benchmark diagnostics, not a public storage API.

In the varied corpus, ranks 20 and 21 must differ for every non-optional row. The three/four-term All baseline and lowered rows now use stable collection; the native-wrapped controls use generic native TopDocs iteration. Two-term All starts with native pruning, while tied cutoffs can trigger the existing stable rerun. The tied mode deliberately retains that confound.

An optimized 2026-10-31 run after moving the stable-score fact into query construction, on macOS arm64, Node 24.16.0, Tantivy 0.27.0, native ABI 14 / query API 8 measured these p50 milliseconds (20 warm sequential samples, limit 20). Native artifact SHA256: `ce875b5a1ee0bea5486c5f3195c29925e62301dded248023113e7b6f2e219bd8`. Both modes passed all 40 correctness rows and observed four committed segments.

| Query                         | Tied, exact off | Tied, exact on | Varied, exact off | Varied, exact on |
| ----------------------------- | --------------: | -------------: | ----------------: | ---------------: |
| Single term baseline          |           0.101 |          0.110 |             0.094 |            0.099 |
| Single term root lowered      |           0.110 |          0.108 |             0.091 |            0.100 |
| Single term native wrapped    |           0.111 |          0.111 |             0.091 |            0.101 |
| Two-term All baseline         |           0.133 |          0.123 |             0.080 |            0.106 |
| Two-term All root lowered     |           0.131 |          0.129 |             0.081 |            0.107 |
| Two-term All native wrapped   |           0.139 |          0.134 |             0.100 |            0.131 |
| Three-term All baseline       |           0.108 |          0.112 |             0.157 |            0.144 |
| Three-term All root lowered   |           0.111 |          0.103 |             0.158 |            0.147 |
| Three-term All native wrapped |           0.166 |          0.159 |             0.128 |            0.168 |
| Four-term All baseline        |           0.131 |          0.130 |             0.168 |            0.171 |
| Four-term All root lowered    |           0.129 |          0.131 |             0.173 |            0.171 |
| Four-term All native wrapped  |           0.203 |          0.204 |             0.151 |            0.195 |

These are total-request comparisons. The wrapper control adds an AST node, stable collection resolves ID ordinals, exact totals can add a separate native count, and tied TopDocs can rerun collection. Those differences prevent attributing the timing delta solely to pruning or ordinal lookup. The small single-term differences do not establish an optimization benefit; the two-term and wider-All rows expose costs on this corpus only. Historical tables above describe earlier fixtures/builds and are not a matched before/after speedup comparison.

Correctness evidence is separate: the old installed addon fails the permanent 300-document, two-commit four-term All/AND/empty-body regressions with a distinct cutoff; the three-term control passes on that addon. The native canary independently compares raw TopDocs with scorer iteration at four terms. The new route passes both. Three-term routing is a conservative mitigation, not a demonstrated three-term failure or a claim of universal safety for smaller native queries. Ordinary stable collection checks deadlines after collection; these measurements do not establish strict latency bounds, customer-distribution throughput, or catalog-scale memory limits.
