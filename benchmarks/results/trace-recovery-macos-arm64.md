# Trace recovery: macOS arm64 qualification

Status: observed local results; raw measurements and detailed report are archived separately.

All 90 measured policy runs finished without terminal errors, with 590 comparisons against
complete singleton evidence. The five oversized-singleton page runs consumed once and retained
1,024 spans with complete analysis and explicitly incomplete spans.

| Case                       | Page calls | Split calls | Singleton calls |
| -------------------------- | ---------- | ----------- | --------------- |
| easy-batch                 | 1          | 1           | 24              |
| late-nonmatches            | 1          | 1           | 24              |
| spans-600x3                | 3          | 5           | 3               |
| singleton-1100             | 1          | 1           | 1               |
| multivalue-unicode-boolean | 1          | 1           | 4               |
| work-bound-late            | 2          | 3           | 4               |

For `spans-600x3`, median consumer time was 0.924/1.469/0.696 ms for pages/splitting/singletons;
for `work-bound-late`, it was 32.920/55.503/30.780 ms. Recovery uses fewer calls than splitting
on those inputs, but singletons were faster. No general speedup is established.

## Evidence and reproduction

[Download the original evidence archive](https://github.com/user-attachments/files/34379739/fulltext-pr85-trace-recovery-evidence.zip)
for the unchanged JSONL samples, detailed report, source/native/driver fingerprints and
`SHA256SUMS`. This is a GitHub attachment, not an expiring CI artifact; downloading may require
repository access. Its authenticated download and file checksums were verified before the
repository copy was removed.

Measured source was [4dc89f3](https://github.com/HarperFast/fulltext/commit/4dc89f37d53e22ade232025b4179dfee9e979513)
plus uncommitted work/span early-exit changes; identify it by the archived fingerprints.

Raw JSONL SHA-256: `8dbab2d31ce5742644a0695fc81ecf48ffa7a7882f47ceffb1fa15a7bcdc1e13`.

After building the production addon, run the [driver](../trace-recovery.mjs):

```sh
node scripts/run-local.mjs benchmarks/trace-recovery.mjs --smoke
node scripts/run-local.mjs benchmarks/trace-recovery.mjs
```

## Scope and limits

Observed on Apple M4 Pro, macOS kernel 25.6.0, Node 24.16.0 and Tantivy 0.27.0, with native
ABI 17/query API 10. Each case had one warmup and five measured runs per policy, rotating order.
The empty index qualifies supplied-source projection only. Timings include consumer validation
and bookkeeping. Submitted bytes can repeat; native reprocessing is unobservable. Memory samples
cover the whole process, not isolated allocations. Other platforms, concurrent consumers and
retention after an actual timeout were not measured here. Incomplete legacy singleton results cannot
distinguish analysis truncation from span truncation.
