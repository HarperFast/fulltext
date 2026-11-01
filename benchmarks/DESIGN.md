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

The composite root boost and required-All optional paths cost more in this corpus. Lowering a root boost directly over an Any leaf does not imply zero overhead for other query shapes. Recursive boosts retain native wrappers to preserve outer-first float32 multiplication. This small warm fixture contains repeated terms and ties; timings include the JavaScript/native boundary and are not a throughput or catalog-scale qualification. The historical varied-score measurements below excluded boundary-tie reruns for their OR/AND pairs. Re-run paired rows on representative customer distributions before drawing broader performance conclusions.

### Varied-score collector boundary

The earlier varied-score sample kept the same membership distribution while varying term frequency and document length. It checked that ranks 20 and 21 differed for each OR/AND pair before timing; the historical driver later extended that assertion to every non-optional row. The current configurable driver reports `distinctBoundary` and `boundaryTie` without requiring distinct scores; tied rows can include the stable fallback. Historically, that distinct-boundary check ruled out the stable-collector boundary-tie rerun for those unfiltered Boolean queries in `Engine::collect_search`. Output records the corpus and `distinctBoundary` for each row. This does not measure how many blocks Tantivy prunes.

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

For these historical measurements, the driver asserted that varied-corpus ranks 20 and 21 differed for every non-optional row. The three/four-term All baseline and lowered rows now use stable collection; the native-wrapped controls use generic native TopDocs iteration. Two-term All starts with native pruning, while tied cutoffs can trigger the existing stable rerun. The tied mode deliberately retains that confound.

An optimized 2026-10-31 run after moving the stable-score fact into query construction and adding shared deadline checks to ordinary stable collection, on macOS arm64, Node 24.16.0, Tantivy 0.27.0, native ABI 14 / query API 8 measured these p50 milliseconds (20 warm sequential samples, limit 20). Native artifact SHA256: `652d1aaafd045641c1880a6c08e1a52099df5aa599de91fabf64f1ec6191cd6b`. Both modes passed all 40 correctness rows and observed four committed segments.

| Query                         | Tied, exact off | Tied, exact on | Varied, exact off | Varied, exact on |
| ----------------------------- | --------------: | -------------: | ----------------: | ---------------: |
| Single term baseline          |           0.116 |          0.109 |             0.093 |            0.092 |
| Single term root lowered      |           0.119 |          0.110 |             0.091 |            0.096 |
| Single term native wrapped    |           0.115 |          0.113 |             0.094 |            0.099 |
| Two-term All baseline         |           0.135 |          0.131 |             0.077 |            0.104 |
| Two-term All root lowered     |           0.140 |          0.132 |             0.078 |            0.104 |
| Two-term All native wrapped   |           0.143 |          0.139 |             0.102 |            0.126 |
| Three-term All baseline       |           0.119 |          0.119 |             0.163 |            0.153 |
| Three-term All root lowered   |           0.120 |          0.118 |             0.162 |            0.158 |
| Three-term All native wrapped |           0.179 |          0.171 |             0.127 |            0.162 |
| Four-term All baseline        |           0.140 |          0.131 |             0.177 |            0.176 |
| Four-term All root lowered    |           0.139 |          0.133 |             0.177 |            0.173 |
| Four-term All native wrapped  |           0.210 |          0.208 |             0.143 |            0.195 |

These are total-request comparisons. The wrapper control adds an AST node, stable collection resolves ID ordinals, exact totals can add a separate native count, and tied TopDocs can rerun collection. Those differences prevent attributing the timing delta solely to pruning or ordinal lookup. The small single-term differences do not establish an optimization benefit; the two-term and wider-All rows expose costs on this corpus only. Historical tables above describe earlier fixtures/builds and are not a matched before/after speedup comparison.

Correctness evidence is separate: the old installed addon fails the permanent 300-document, two-commit four-term All/AND/empty-body regressions with a distinct cutoff; the three-term control passes on that addon. The native canary independently compares raw TopDocs with scorer iteration at four terms. The new route passes both. Three-term routing is a conservative mitigation, not a demonstrated three-term failure or a claim of universal safety for smaller native queries. Ordinary stable collection cooperatively checks deadlines every 1,024 visited documents; these measurements do not establish strict latency bounds, customer-distribution throughput, or catalog-scale memory limits.

### Pre-deadline baseline retained for comparison

The [complete pre-deadline tables](https://github.com/HarperFast/fulltext/blob/89c55246a74b34141b3a188977eed29f6211b16c/benchmarks/DESIGN.md#L109-L162) retain artifact `ce875b5a` measurements. The deadline-aware artifact is `652d1aaa`. Both used the same driver/corpora, Node 24.16.0/macOS arm64 and 20 warm sequential samples. Selected stable baseline p50 milliseconds, varied scores and exact totals off:

| Query                        | Before deadlines | Deadline-aware |
| ---------------------------- | ---------------: | -------------: |
| Three-term All               |            0.157 |          0.163 |
| Four-term All                |            0.168 |          0.177 |
| OR, routing corpus           |            0.137 |          0.152 |
| Exact singleton prefix       |            0.142 |          0.159 |
| Two short Fuzzy terms        |            0.129 |          0.156 |
| One-token Phrase, two fields |            0.131 |          0.156 |
| Three-term All, two fields   |            0.240 |          0.251 |

The deadline-aware run was slower in these stable baseline rows. These are separate warm runs, not a controlled isolation of deadline-check overhead; generic wrapped controls also varied. Preserve both artifacts' tables when assessing the cost of cooperative cancellation. The diagnostic-only `5394ecf` changes validation errors, not collection, and was not re-benchmarked.

### Additional compiler routing shapes

Use `node scripts/run-local.mjs benchmarks/query-scoring.mjs --routing-shapes` and add `--varied-scores` for the corresponding varied run. For the historical measurements below, this opt-in mode left the default 20 shapes and corpus unchanged and used 10,000 documents, four 2,500-document commit boundaries, 20 warm samples and limit 20. The current configurable driver uses five warmup rounds and 100 measured rounds by default; see [current controls](../docs/benchmarks.md#query-scoring-and-restricted-collection). Only this mode adds short `xy`/`zz` tokens correlated with amber/stone membership, copies title into a second body field, enables positions/surface terms, and uses 125-document mutation batches. Both runs observed four committed segments.

Each family has an explicit expected total: OR, short-Fuzzy and one-token two-field Phrase match 7,500 documents; exact prefix and three-term two-field All match 5,000. The exact prefix has two completed terms and a singleton final `ruby` expansion. Those historical correctness reads used 100-hit traversal pages because ordinary prefix search rejects limits above 100; timed requests remained at limit 20. The current driver uses 1,000-hit traversal pages for complete routing retrieval and rejects routing-mode ordinary limits above 100 before creating the index. Complete membership, totals and exact doubled baseline scores must agree with `boost(2, boost(1, query))` before timing. The native wrapper retains generic scoring. The historical varied run asserted distinct ranks 20/21 for all 20 rows; the tied run deliberately retained boundary ties. Current runs report ties explicitly instead of assuming the varied corpus is distinct at every configurable cutoff.

On the same unchanged optimized artifact `652d1aaafd045641c1880a6c08e1a52099df5aa599de91fabf64f1ec6191cd6b`, Node 24.16.0/macOS arm64, all 20 rows passed in each mode. P50 milliseconds:

| Query                                       | Tied, exact off | Tied, exact on | Varied, exact off | Varied, exact on |
| ------------------------------------------- | --------------: | -------------: | ----------------: | ---------------: |
| OR baseline                                 |           0.125 |          0.117 |             0.152 |            0.156 |
| OR native wrapped                           |           0.169 |          0.166 |             0.110 |            0.130 |
| Exact singleton prefix baseline             |           0.149 |          0.118 |             0.159 |            0.150 |
| Exact singleton prefix native wrapped       |           0.209 |          0.176 |             0.129 |            0.166 |
| Two short Fuzzy terms baseline              |           0.189 |          0.117 |             0.156 |            0.145 |
| Two short Fuzzy terms native wrapped        |           0.235 |          0.160 |             0.112 |            0.131 |
| One-token Phrase, two fields baseline       |           0.121 |          0.123 |             0.156 |            0.155 |
| One-token Phrase, two fields native wrapped |           0.180 |          0.177 |             0.114 |            0.146 |
| Three-term All, two fields baseline         |           0.226 |          0.223 |             0.251 |            0.261 |
| Three-term All, two fields native wrapped   |           0.372 |          0.413 |             0.227 |            0.349 |

The default driver also measured its original OR pair on this artifact: tied baseline/root boost p50 was 0.126/0.172 ms with exact totals off and 0.123/0.175 ms on; varied was 0.156/0.108 ms off and 0.158/0.118 ms on. The routing-mode OR rows use the augmented corpus, so they are not matched before/after comparisons with those default rows.

These samples measure total requests, including JavaScript/native calls, query construction, stable ID-ordinal resolution and possible separate exact counts. Wrapped controls add AST nodes and double scores; tied cutoffs can trigger a stable rerun. None of these pairs isolates pruning or compiler overhead, and timing differences change with totals and corpus. The added families make the small-workload costs visible; they establish no general speedup, strict deadline bound, catalog-scale throughput or memory qualification.

### Restricted collection measurements, 2026-11-01

Three matched 100,000-document varied-score configurations used Node 24.16.0/macOS arm64.
Both artifacts used optimized builds with test-panic features enabled and hooks inactive, without
per-hit instrumentation. These are not timings of the separately qualified production binary.
The identical driver SHA-256 was `bb14a9c81f725f46bc4170d11e1a9ff79117c098998e4ad62711beb8286c1453`;
loaded baseline/candidate addon SHA-256 values were
`81cde7067bf8b56e1e3dffb112f076912a189fe64524cb06449bd6ed8041d66d` /
`e226d3f7439d8a636ccc80f7dfdae3201bd6411758b80c3358861fab6b3e596e`.
The candidate corresponds to `697fde4` production code; the subsequent test-arm correction did
not change that code. The separate production artifact was
`84974a6e1e1262ccba866e2d01021a038c63c15553e9531671b8887126efb41a`, qualified with two packed
and six production-worker checks.

Each configuration retained 56 rows, with 100 raw warmed samples per row, exact totals off/on
separate and query rows interleaved within each process. Baseline and candidate were separate
build/process runs, not interleaved artifact pairs. Raw local reports are
`~/WebstormProjects/tmp/query-execution-{baseline,candidate}-final3-{100k-4seg-50pct,100k-4seg-1pct,100k-1seg-50pct-window1000}.json`.
Every warmed timing request passed score/ID/order and total/relation checks. Selected exact-total-off
metadata-filter baseline rows, milliseconds (p50 / p95):

| Segments / All density / limit | Query |      Baseline |     Candidate |
| ------------------------------ | ----- | ------------: | ------------: |
| 4 / 50% / 20                   | All4  | 0.779 / 0.799 | 0.815 / 0.838 |
| 4 / 50% / 20                   | OR    | 0.723 / 0.748 | 0.781 / 0.799 |
| 4 / 1% / 20                    | All4  | 0.170 / 0.183 | 0.170 / 0.182 |
| 4 / 1% / 20                    | OR    | 0.488 / 0.500 | 0.528 / 0.544 |
| 1 / 50% / 1,000                | All4  | 1.429 / 1.465 | 1.407 / 1.491 |
| 1 / 50% / 1,000                | OR    | 2.292 / 2.329 | 2.237 / 2.346 |

The four-segment runs used 30 MiB writer memory; the one-segment run needed 128 MiB to prevent
an automatic flush. All observed segment counts matched. Candidate-ID All4/OR baseline-row
p50 ranged from 0.999–1.797 ms before and 1.011–1.840 ms after. The one-segment OR filter
row had a boundary tie in both builds, so its cost includes stable fallback. Other displayed
filter rows had distinct boundaries. Candidate rows with too few hits have no boundary to check.

The public-worker 1/2/5 ms sweep recorded outcomes and recovery, not native-loop work. Several
filter medians were below the minimum one-millisecond budget, so success or observed overshoot
cannot prove mid-loop cancellation. Separate deterministic Rust test-only work counters showed
candidate collection stopping at 512 rather than 1,024 visits and generic boosted collection at
1,024 rather than 4,096. Those are cancellation canaries, not native block-WAND/pruning counters.
Synchronous native operations between cooperative checks remain unchecked. These local observations
establish neither a causal speedup, a hard deadline, nor clearance of the separate pruning gate.
