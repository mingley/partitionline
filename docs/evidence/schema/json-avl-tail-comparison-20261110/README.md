# Incremental AVL tail comparison

This compares the unapplied [32-key-tail candidate](../json-progressive-avl-20261110/README.md)
with the same algorithm using a 64-key linear tail. Both progressively
migrate one earlier key per accepted field into a balanced index of entry
slots. The index retains no copy of key strings. With a valid AVL invariant,
the fixed suffix and tree bound the number of key comparisons to logarithmic
per indexed operation plus a fixed suffix scan. Key length, allocation,
synchronous execution and RSS are not bounded by that count.

The 64-key variant remains unapplied. On the recorded host it did not remove
the 64/65-key slowdowns versus baseline, and it was consistently slower than
the 32-key variant on the larger tested objects.

## Checks

The exact [64-key candidate](https://github.com/mingley/partitionline/commit/7fcca1385bb49b85163db14556f26486fa54e6f3)
passed [run 42867532041](https://github.com/mingley/partitionline/actions/runs/42867532041)
on stable Rust 1.99.1. Three focused controls, 111 default, 152 all-feature
and 20 no-default tests passed, including doctests; formatting, strict
Clippy and rustdoc passed. These controls cover sorted, reverse and shuffled
tree insertion, duplicate keys in the indexed prefix and recent suffix,
nested scope, escaped-equivalent keys, malformed-value precedence and a
public mock response. They do not prove balancing invariants for every
possible sequence. No live Registry or external TLS peer was tested.

The retained [64-key patch](candidate64.patch) reproduced source blob
`ff5832d7f86e35a621ed1ebd46e85f5753b1d949` from baseline blob
`2e54868346d05cafff0369358703a033cce0c501` in a text comparison. Only
the tail constant differs from the previously tested source, apart from
test-only observation changes already present in that candidate.

## Three-arm measurements

The same run compiled three separate release binaries, then timed 12 rounds
with all six arm orders repeated twice, pinned to one logical CPU on a shared
x86_64 runner. It parsed prepared, successful JSON bodies in-process; there
was no HTTP, Kafka or live Registry. The [684 raw samples](pairs.csv) retain
all totals and iteration counts. Times below are per-parse medians in
microseconds. Percentages are paired medians with the minimum and maximum
pair changes in parentheses; they need not equal ratios of time medians.

| Shape | Baseline µs | 32-tail µs | 64-tail µs | 32 vs baseline | 64 vs baseline | 64 vs 32 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| typical | 0.860 | 0.861 | 0.855 | 0.05% (-2.31 to 4.34%) | -0.79% (-2.74 to 2.80%) | -0.44% (-5.05 to 2.62%) |
| versioned | 0.696 | 0.696 | 0.695 | -0.02% (-0.92 to 9.87%) | -0.17% (-1.71 to 4.26%) | -0.10% (-7.64 to 5.23%) |
| k5 | 0.474 | 0.458 | 0.459 | -2.89% (-5.69 to 14.80%) | -3.11% (-4.79 to 7.19%) | -0.08% (-13.12 to 11.06%) |
| k16 | 1.538 | 1.548 | 1.540 | 1.19% (-3.35 to 12.61%) | 0.69% (-3.44 to 28.67%) | -0.96% (-11.31 to 27.21%) |
| k32 | 3.688 | 3.799 | 3.789 | 5.73% (-1.04 to 46.35%) | 3.80% (-1.40 to 11.21%) | -0.59% (-29.23 to 12.05%) |
| k33 | 3.867 | 4.009 | 3.984 | 3.08% (-0.48 to 47.83%) | 2.81% (-1.65 to 13.13%) | -1.47% (-30.50 to 8.99%) |
| k34 | 4.017 | 4.170 | 4.157 | 4.87% (-0.77 to 30.55%) | 4.19% (-0.47 to 9.86%) | -0.42% (-21.12 to 8.92%) |
| k64 | 9.862 | 10.494 | 10.726 | 8.51% (-0.21 to 21.15%) | 9.80% (1.66 to 14.56%) | 1.99% (-5.44 to 9.29%) |
| k65 | 10.344 | 10.847 | 11.102 | 5.93% (-0.84 to 20.96%) | 10.09% (1.88 to 14.53%) | 2.49% (-5.79 to 9.00%) |
| k96 | 19.269 | 17.844 | 20.082 | -6.69% (-16.53 to 1.98%) | 4.18% (-5.42 to 12.38%) | 12.48% (2.66 to 19.83%) |
| k128 | 31.247 | 25.749 | 29.863 | -18.21% (-23.90 to -6.28%) | -4.74% (-11.02 to 2.67%) | 16.84% (6.02 to 20.94%) |
| k129 | 34.414 | 29.101 | 33.500 | -16.39% (-19.86 to -5.75%) | -2.75% (-8.20 to 0.82%) | 15.05% (5.21 to 21.23%) |
| k256 | 112.203 | 63.260 | 77.154 | -44.15% (-47.75 to -37.55%) | -31.34% (-36.09 to -28.57%) | 22.01% (11.38 to 28.62%) |
| k512 | 404.483 | 135.646 | 169.853 | -66.70% (-69.54 to -63.32%) | -58.21% (-61.35 to -56.54%) | 25.08% (13.87 to 35.58%) |
| k512_prefix256 | 995.388 | 411.828 | 522.884 | -58.60% (-60.34 to -58.01%) | -47.48% (-49.75 to -45.19%) | 26.83% (24.93 to 30.53%) |
| k4097 | 23336.463 | 1280.297 | 1595.139 | -94.57% (-95.01 to -93.98%) | -93.26% (-93.83 to -92.93%) | 23.79% (13.94 to 28.39%) |
| k8193 | 92631.962 | 2669.285 | 3315.382 | -97.13% (-97.39 to -96.84%) | -96.45% (-96.74 to -96.30%) | 24.22% (14.79 to 28.17%) |
| k8193_desc | 92632.178 | 2562.175 | 3225.675 | -97.24% (-97.50 to -96.93%) | -96.54% (-96.82 to -96.17%) | 26.01% (15.98 to 31.49%) |
| k8193_shuffled | 93386.891 | 4546.596 | 5192.685 | -95.15% (-95.58 to -94.84%) | -94.44% (-94.92 to -94.28%) | 14.29% (8.25 to 20.60%) |

The small named cases are ordinary mock Registry response bodies.
`k512_prefix256` includes one schema field and 512 extra names sharing a
256-byte prefix; `k8193` shapes have 8,193 total keys. The prior 32-key
results on two other hosts showed different ordinary-case costs, so these
single-host samples do not establish a no-regression result.

The same run recorded [separate direct-child resource samples](resources.txt)
with a 30-second deadline per child and Linux `wait4` accounting. For
16,384 extra keys, all 15 peak-RSS readings were 16,952 KiB and do not resolve
the index's incremental memory. For a prepared single-key 64 MiB body,
direct synchronous parse times were 117.57–122.69 ms baseline,
117.30–120.05 ms for 32 and 117.43–119.17 ms for 64, three samples each.
These are not first-poll, deadline or process-memory bounds. Earlier denied
process-descriptor measurements were not retried.

## Reproduction pins

The [test injection](benchmark.rs.txt) and [comparison script](comparison.py.txt)
are retained; the [workflow](https://github.com/mingley/partitionline/blob/7fcca1385bb49b85163db14556f26486fa54e6f3/.github/workflows/schema-json-progressive-avl.yml)
records the commands. The comparison strips candidate-only controls and
injects the same benchmark into each source. Git blob IDs for the benchmark
sources were `2690fc31a5c8e79bfa94dafeb5273dcb1dd5b91a` baseline,
`0090f35c60b809cc95ff8410fd8cbca556f7eb43` 32 and
`4f258e598c7c53c174a597527f18ed118d61de27` 64. Executable SHA-256 values
were `c0eb22f23f6379543f4d203f4fadfe86942360d882240e35c4b26e0bb420295c`,
`c55d65cd33ef2e2780977defdb0138779b5f8c4efb509e02581cf7e73188a548` and
`8ffd463310a776b12f6589d677bb1ed9dadcece198b96fcff3f865f30482e99a`
respectively. Manifest and lockfile blobs were
`077a5f729d4e2c2accc2a138f872dd87e8f20a0b` and
`4cc5fc2a7502df2d4cb1a7785a63fe97650b5fa8`.
