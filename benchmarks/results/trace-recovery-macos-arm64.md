# Trace recovery: macOS arm64 qualification

Status: observed local results; report checked against the raw samples by a second reader.

Observed: all 90 measured policy runs completed without terminal errors. Both recovery
policies matched singleton membership and complete source evidence in 590 comparisons.
The five oversized-singleton page runs consumed the input once, retained 1024 spans,
and reported `analysisComplete: true`, `spansComplete: false`.

## Reproduce and read the results

Build the production addon, then run:

```sh
node scripts/run-local.mjs benchmarks/trace-recovery.mjs --smoke
node scripts/run-local.mjs benchmarks/trace-recovery.mjs
```

[Raw samples](trace-recovery-macos-arm64.jsonl) contain the native, facade, protocol,
lockfile and driver SHA-256 fingerprints. [Driver](../trace-recovery.mjs) assertions check progress, decreasing shared budgets,
ordered states, terminal singleton policy and singleton evidence equivalence.

Observed environment: Apple M4 Pro, macOS kernel 25.6.0, Node 24.16.0, Tantivy 0.27.0,
native application binary interface (ABI) 17/query application programming interface (API) 10. Each case has one unreported warmup per policy and five
measured runs; the first policy rotates each round. The index is empty: this measures
supplied-source projection, without catalog or index-size qualification.

Each cell below is **calls / p50 ms / p95 ms** for consumer wall time. Percentiles use
nearest rank over five samples; p95 is the largest sample. Time includes correctness
checks and recovery bookkeeping. These are local observations, without a speedup claim.

| Case                       | Pages               | Split on incomplete | Singletons          |
| -------------------------- | ------------------- | ------------------- | ------------------- |
| easy-batch                 | 1 / 0.208 / 0.245   | 1 / 0.204 / 0.216   | 24 / 0.685 / 1.211  |
| late-nonmatches            | 1 / 0.113 / 0.121   | 1 / 0.099 / 0.123   | 24 / 0.553 / 0.574  |
| spans-600x3                | 3 / 0.888 / 0.934   | 5 / 1.463 / 1.733   | 3 / 0.620 / 0.688   |
| singleton-1100             | 1 / 0.343 / 0.368   | 1 / 0.356 / 0.365   | 1 / 0.349 / 0.368   |
| multivalue-unicode-boolean | 1 / 0.066 / 0.077   | 1 / 0.063 / 0.073   | 4 / 0.142 / 0.202   |
| work-bound-late            | 2 / 33.812 / 34.432 | 3 / 58.000 / 70.432 | 4 / 32.379 / 33.090 |

## Submission and memory scope

Observed per run:

- `spans-600x3`: pages submitted 6 inputs/21,594 source bytes, splitting 8/28,792,
  singletons 3/10,797. All three inputs completed. Repeated-ID submissions were 3, 5 and 0.
- `work-bound-late`: pages submitted 5 inputs/600,006 bytes, splitting 8/960,006,
  singletons 4/480,003. All four inputs completed; the late input was reported as a match.
- Submitted/repeated inputs count API arguments. Consumed page statuses and accepted
  consumer inputs measure progress. Native provisional analysis/reprocessing is
  unobservable and explicitly recorded as `null`.
- Sampled resident set size (RSS) is whole-process memory, including retained inputs
  and comparison outcomes. Within-policy sampled growth ranged from 0 to 1.52 MiB;
  this does not measure isolated allocations or a per-request native memory peak.

## Open items

Untested here: other operating systems/CPUs, concurrent consumers, and terminal-error
retention under an actual timeout. This run observed no errors; the driver retains
prior accepted evidence and stops that policy on errors, with a decreasing 30-second
shared budget. Incomplete legacy singletons cannot distinguish analysis truncation
from span truncation; the benchmark consumes them once and reports that limitation.
