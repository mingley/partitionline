# Buffered classification: placebo and test-binary composition

[42475240981](https://github.com/mingley/partitionline/actions/runs/42475240981)
followed up the [initial experiment](README.md). The performance draft remains
**unapplied**. The earlier plain TCP 256-KiB run measured 854.0 → 671.9 MiB/s
(-21.3%); this follow-up reproduces the large regression in the same plain
binary composition. It does not identify its cause or justify a general
speedup claim.

## Control

The run built baseline transport blob `a36cf086c3d683a5f24b818705b15d8ce54245f4`
and draft blob `95a7e220ae8a6e34a0ae41936c3b36c7bc596ade` in two test-binary
compositions. “Plain” appends the same test-only TCP/TLS fixture used in the
earlier run. “Extended” appends that fixture plus an additional ignored
ready-reader benchmark module; the extra test is not run during these
measurements. The placebo is a byte-for-byte copy of the plain baseline binary;
the workflow verifies it with `cmp`. Output labels differ between the baseline
and placebo, and samples are printed after each timed interval, which may
affect subsequent measurements.

Workflow-only commit `007b40416be9864a2187bdfd38cd82913ea04120` used fixture
blobs `0e95d63eb41a33b0be87dca4ea6ee073b7892db0` and
`54172b6741ee2201894539ea58f230ed848942fa`. Binary SHA-256 values were:

| Binary | SHA-256 |
| --- | --- |
| plain baseline and identical placebo | `b2be5a217b99883c94001f8bc6733c1f51d33e5e4cd44c394f932c2fccceed95` |
| plain draft | `8fd0e48e83dbd8a819e276202bd3fd09a82d62a997441e7a90d4a45230e16d2d` |
| extended baseline | `77cd1f18da863d04f3adbb8aa2ddf4bde28b6eac61aa6a20c04e4b8306565802` |
| extended draft | `868fef736883d6e2fa0ebeecff65cfb0c171a6b4de890f90a3f0cfa205c51f87` |

The plain hashes exactly match the original network run. All five binaries
were measured in six rotated orders on one pinned logical CPU of a shared AMD
EPYC 7763 runner, stable rustc 1.99.1 and LLVM 23.1.1. The fixture sends one
framed request at a time and waits for an acknowledgement on loopback TCP or
TLS/TCP, with the wire prefix placed in ReadAhead. Client and server share a
current-thread executor. The 4-KiB case uses an intentional one-byte read
adapter, not one-byte packets. Each case uses 8 warmups and 128 or 256 measured
requests per round.

The [120 round summaries](layout-rounds.csv) and [23,040 individual RTTs](layout-rtt-samples.csv)
are retained. Each group has the expected sequential indexes and sample count;
all 120 emitted p50/p95/p99 values were independently recomputed from the
individual RTTs and matched. CSV timestamps are sample output times, not
request timestamps.

## Results

Throughput is calculated from the median of six per-round elapsed times.
Latencies are medians of six per-round percentile estimates; they are not
pooled or fleet-level percentiles.

| Case | Plain baseline / draft / placebo MiB/s | Extended baseline / draft MiB/s |
| --- | ---: | ---: |
| TCP, 256 KiB | 864.5 / 668.2 / 806.9 | 836.1 / 853.6 |
| TLS/TCP, 256 KiB | 510.6 / 497.9 / 539.6 | 511.4 / 537.3 |
| TCP, 4 KiB one-byte adapter | 1.079 / 1.105 / 1.077 | 1.098 / 1.095 |
| TLS/TCP, 4 KiB one-byte adapter | 11.86 / 12.08 / 12.04 | 11.93 / 12.56 |

For TCP 256 KiB, the plain draft's elapsed time exceeded baseline in all six
paired rounds by 22.9%–32.7%. The same-binary placebo's paired elapsed time
ranged from -0.4% to +15.6%, with median throughput 6.7% below baseline. Its
median p50 RTT was nearly identical to baseline (282.38 vs 282.25 µs), while
median p99 was 505.2 vs 326.2 µs. In the extended composition, draft paired
elapsed time ranged from -7.9% to +1.7%; median p50 was 283.6 vs 281.5 µs.

The large plain-binary regression therefore persisted in the repeated plain
measurement but did not reproduce after adding the unused test module. The
placebo shows that scheduling, ordering or other run noise can affect elapsed
time, especially tails. Different test-binary composition can change code
generation, layout and other runtime conditions; these controls do not isolate
an LLVM mechanism. They do not measure a full broker, multi-client fairness,
or behavior across machines, and they do not resolve the performance concern.
