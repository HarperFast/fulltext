# Trace recovery: macOS arm64 qualification

Status: observed local results; report checked against the raw samples by a second reader.

Observed: all 90 measured policy runs completed without terminal errors. Both recovery
policies matched singleton membership and complete source evidence in 590 comparisons.
The five oversized-singleton page runs consumed the input once, retained 1024 spans,
and reported `analysisComplete: true`, `spansComplete: false`.

## Reproduce and read the results

Measured base: [4dc89f3](https://github.com/HarperFast/fulltext/commit/4dc89f37d53e22ade232025b4179dfee9e979513)
plus the uncommitted work/span early-exit changes. Use the raw source/native hashes
and final verification row to identify the measured implementation.

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
| easy-batch                 | 1 / 0.305 / 0.586   | 1 / 0.256 / 0.273   | 24 / 0.907 / 1.054  |
| late-nonmatches            | 1 / 0.115 / 0.120   | 1 / 0.105 / 0.134   | 24 / 0.594 / 0.629  |
| spans-600x3                | 3 / 0.924 / 1.047   | 5 / 1.469 / 1.968   | 3 / 0.696 / 0.794   |
| singleton-1100             | 1 / 0.341 / 0.356   | 1 / 0.349 / 0.358   | 1 / 0.341 / 0.386   |
| multivalue-unicode-boolean | 1 / 0.076 / 0.091   | 1 / 0.065 / 0.102   | 4 / 0.189 / 0.466   |
| work-bound-late            | 2 / 32.920 / 33.303 | 3 / 55.503 / 56.066 | 4 / 30.780 / 31.193 |

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
  and comparison outcomes. Within-policy sampled growth ranged from 0 to 2.84 MiB;
  this does not measure isolated allocations or a per-request native memory peak.

## Open items

Untested here: other operating systems/CPUs, concurrent consumers, and terminal-error
retention under an actual timeout. This run observed no errors; the driver retains
prior accepted evidence and stops that policy on errors, with a decreasing 30-second
shared budget. Incomplete legacy singletons cannot distinguish analysis truncation
from span truncation; the benchmark consumes them once and reports that limitation.
