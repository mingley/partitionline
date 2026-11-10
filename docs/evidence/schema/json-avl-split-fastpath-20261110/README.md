# Split fast-path Registry JSON key index

This experiment keeps small JSON objects on the original linear scan. After
the configured 32- or 64-key threshold and a following comma, it enters a
separate parser with a progressively built AVL index of entry positions and a
fixed-size unindexed suffix. It preserves object order and parses a value
before checking its key for duplication. No key string is copied into the
index.

For a valid AVL invariant, each indexed search or insertion requires
logarithmic key comparisons plus at most 32 or 64 comparisons in the suffix.
The byte cost also depends on key lengths and common prefixes. The tree tests
check ordering and balance for deterministic sorted, reverse and shuffled
inputs; they are not a formal proof. The helper adds another call frame for
each nested large object. The JSON depth limit remains 32, but stack use across
platforms has not been measured. Allocator behavior, total RSS, scheduling and
synchronous poll time are not bounded by the comparison count.

The experiments did not apply either variant. The repository now includes
the 64-tail implementation; the 32-tail and adaptive variants remain
experiments. The 64-tail version reduces
the 64/65-key transition costs seen with 32, but the 96-key fixture slowed on
two runs where every pair was slower. The ordinary cases varied between
runners. This is a correctness/resource tradeoff, not a general no-regression
or fastest-performance result.

## Correctness

The [8f288f2 candidate](https://github.com/mingley/partitionline/commit/8f288f2c4c52b354e873e5899a0d5480d002437f)
passed four focused controls and 112 default, 153 all-feature and 20
no-default tests, including doctests, for each threshold in
[run 42869378221](https://github.com/mingley/partitionline/actions/runs/42869378221).
Formatting, all-target/all-feature Clippy and strict rustdoc passed. The
controls cover sorted, reverse and shuffled keys; duplicates in indexed and
tail regions; malformed-value precedence; escaping in a small object; and
objects crossing the threshold at every level of the depth limit. In that
last test each parent had exactly the threshold number of preceding fields,
so its index was not yet populated when its child began parsing.

The first split run [42867911128](https://github.com/mingley/partitionline/actions/runs/42867911128)
passed the 64-tail controls but its copied 32-tail full suites and Clippy
failed because three external fixture directories were missing. The
[corrected setup](https://github.com/mingley/partitionline/actions/runs/42869104379)
staged those fixtures and passed both thresholds before the depth control was
added. Those failures remain part of the record.

The later reviewed fallback
[`b27a3d0`](https://github.com/mingley/partitionline/commit/b27a3d072a407fa82ae9efdd48b0c118a0b7d813)
passed [run 42872601027](https://github.com/mingley/partitionline/actions/runs/42872601027)
for both thresholds: four focused controls; 112 default, 153 all-feature and
20 no-default tests, including doctests; format, strict Clippy and rustdoc.
It adds escaped/unescaped duplicate names in indexed and tail positions
around the threshold, and tests nested objects with 128 preceding fields at
every level so parent indexes are populated during child parsing. The first
expanded-control [run 42872052613](https://github.com/mingley/partitionline/actions/runs/42872052613)
failed formatting on both thresholds; the later run corrected it. The tested
64-tail source blob is `74d8721405e32cc6939ad22378893b11a82ce625`; the
32-tail source was `d97fa84d47a85b116ed3b10763aa73d45f18aab7`.

A separately executed 104-case baseline-versus-candidate differential corpus
also matched; see the [adaptive comparison](../json-adaptive-avl-20261110/README.md).
These are finite authored controls, not an independent JSON implementation,
a live Registry test or a platform-stack qualification.

The [64-tail patch](split64.patch) was text-checked against baseline source
blob `2e54868346d05cafff0369358703a033cce0c501`. The tests use local
fixtures and mock responses, not a live Registry or external TLS peer.

## Paired in-process parsing

Three [runs](https://github.com/mingley/partitionline/actions/runs/42867911128),
[42869104379](https://github.com/mingley/partitionline/actions/runs/42869104379) and
[42869378221](https://github.com/mingley/partitionline/actions/runs/42869378221) used stable
Rust 1.99.1 and separate release binaries on shared x86_64 runners. Each
recorded 12 rounds with all six arm orders repeated twice, pinned to one
logical CPU. Inputs were prepared successful JSON bodies parsed in-process;
there was no HTTP, Kafka or live Registry. The runner CPU model was not
retained. The [first](pairs-1.csv), [second](pairs-2.csv) and
[third](pairs-3.csv) CSVs retain all 684 samples per run.

The benchmark sources are byte-identical across runs after candidate-only
test controls are removed and the same benchmark test is injected. Their
independently rebuilt executable SHA-256 values matched. Times are per-parse
medians in microseconds. Percentages are medians of paired changes against
baseline, with minimum and maximum pair changes in parentheses; they need
not equal the ratio of time medians.

### Run 42867911128

| Shape | Baseline µs | 32-tail µs | 64-tail µs | 32 vs baseline | 64 vs baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| typical | 1.132 | 1.141 | 1.135 | 1.08% (-3.83 to 2.70%) | 0.12% (-1.11 to 1.51%) |
| versioned | 0.931 | 0.946 | 0.938 | 2.10% (-2.17 to 4.51%) | 1.43% (0.20 to 7.12%) |
| k5 | 0.642 | 0.639 | 0.634 | -0.91% (-4.23 to 1.09%) | -0.92% (-4.74 to 3.48%) |
| k16 | 2.007 | 2.042 | 2.025 | 1.61% (-0.42 to 3.30%) | 1.18% (-1.00 to 2.60%) |
| k32 | 5.100 | 5.167 | 5.144 | 1.44% (-5.37 to 3.25%) | 1.09% (-4.48 to 2.40%) |
| k33 | 5.314 | 5.419 | 5.364 | 2.40% (-3.88 to 3.83%) | 0.92% (-3.16 to 3.20%) |
| k34 | 5.633 | 5.771 | 5.661 | 2.28% (-3.26 to 4.18%) | 0.53% (-2.61 to 1.50%) |
| k64 | 15.089 | 16.780 | 15.130 | 11.30% (4.17 to 13.73%) | 0.14% (-4.26 to 1.10%) |
| k65 | 15.662 | 17.351 | 15.734 | 10.76% (5.24 to 15.14%) | 0.41% (-3.69 to 1.09%) |
| k96 | 28.735 | 28.645 | 30.012 | -0.31% (-5.71 to 1.46%) | 4.31% (0.51 to 6.19%) |
| k128 | 45.778 | 40.507 | 45.070 | -11.66% (-15.80 to -6.62%) | -1.51% (-5.25 to -0.23%) |
| k129 | 48.291 | 42.830 | 47.363 | -11.29% (-15.26 to -10.04%) | -1.79% (-6.68 to -0.19%) |
| k256 | 164.849 | 93.008 | 113.122 | -43.69% (-46.57 to -43.01%) | -31.37% (-35.17 to -30.68%) |
| k512 | 603.385 | 204.034 | 256.314 | -66.16% (-68.36 to -65.94%) | -57.54% (-60.23 to -57.32%) |
| k512_prefix256 | 1264.568 | 563.613 | 674.469 | -55.40% (-56.92 to -54.42%) | -46.71% (-48.18 to -45.52%) |
| k4097 | 33384.184 | 1846.186 | 2296.573 | -94.48% (-94.86 to -94.36%) | -93.13% (-93.61 to -93.01%) |
| k8193 | 137169.527 | 3972.981 | 4963.459 | -97.11% (-97.53 to -96.89%) | -96.38% (-96.93 to -96.33%) |
| k8193_desc | 136580.921 | 3509.952 | 4470.717 | -97.43% (-97.60 to -97.33%) | -96.73% (-96.91 to -96.66%) |
| k8193_shuffled | 140910.745 | 5786.562 | 6807.369 | -95.90% (-95.94 to -95.81%) | -95.17% (-95.22 to -95.11%) |

### Run 42869104379

| Shape | Baseline µs | 32-tail µs | 64-tail µs | 32 vs baseline | 64 vs baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| typical | 0.665 | 0.618 | 0.616 | -7.04% (-13.58 to -2.16%) | -6.90% (-12.33 to -3.04%) |
| versioned | 0.577 | 0.538 | 0.527 | -6.85% (-13.15 to -2.86%) | -8.31% (-12.90 to -4.44%) |
| k5 | 0.319 | 0.319 | 0.324 | 0.02% (-4.74 to 5.00%) | 1.95% (-6.23 to 17.06%) |
| k16 | 1.046 | 1.036 | 1.041 | -2.14% (-5.15 to 3.86%) | -0.92% (-5.34 to 37.46%) |
| k32 | 2.482 | 2.485 | 2.486 | -0.29% (-5.58 to 36.06%) | 0.48% (-5.37 to 18.73%) |
| k33 | 2.611 | 2.592 | 2.590 | -1.04% (-9.09 to 6.57%) | -1.82% (-8.25 to 6.50%) |
| k34 | 2.718 | 2.689 | 2.705 | -1.52% (-9.07 to 6.39%) | 0.31% (-8.94 to 5.69%) |
| k64 | 6.570 | 7.513 | 6.706 | 14.07% (-0.86 to 18.55%) | 1.38% (-13.51 to 10.67%) |
| k65 | 6.853 | 7.801 | 7.039 | 13.00% (-0.49 to 19.67%) | 0.25% (-13.82 to 9.85%) |
| k96 | 12.422 | 13.194 | 13.625 | 4.56% (-7.79 to 12.18%) | 6.82% (-7.95 to 15.83%) |
| k128 | 19.868 | 19.467 | 21.103 | -4.00% (-15.26 to 4.23%) | 2.78% (-10.15 to 12.15%) |
| k129 | 23.177 | 22.663 | 23.834 | -3.04% (-11.45 to 8.20%) | 3.13% (-4.72 to 14.94%) |
| k256 | 73.373 | 50.121 | 57.785 | -30.06% (-39.05 to -23.98%) | -21.27% (-27.28 to -9.77%) |
| k512 | 257.046 | 108.033 | 128.199 | -56.85% (-63.59 to -52.00%) | -49.88% (-55.14 to -42.11%) |
| k512_prefix256 | 750.213 | 338.970 | 387.731 | -55.11% (-55.77 to -51.10%) | -47.89% (-53.13 to -45.99%) |
| k4097 | 14431.956 | 1005.671 | 1211.627 | -93.00% (-94.33 to -92.58%) | -91.64% (-92.86 to -91.19%) |
| k8193 | 57236.641 | 2116.133 | 2497.381 | -96.31% (-96.94 to -96.14%) | -95.65% (-96.19 to -95.42%) |
| k8193_desc | 57238.209 | 1777.153 | 2247.865 | -96.89% (-97.33 to -96.61%) | -96.08% (-96.58 to -95.89%) |
| k8193_shuffled | 57225.319 | 3542.512 | 3959.606 | -93.78% (-94.68 to -93.23%) | -93.06% (-93.99 to -92.65%) |

### Run 42869378221

| Shape | Baseline µs | 32-tail µs | 64-tail µs | 32 vs baseline | 64 vs baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| typical | 1.127 | 1.129 | 1.126 | 0.15% (-2.52 to 1.67%) | 0.05% (-3.35 to 0.69%) |
| versioned | 0.925 | 0.948 | 0.946 | 2.24% (1.16 to 4.50%) | 2.29% (-0.34 to 4.22%) |
| k5 | 0.633 | 0.631 | 0.640 | -0.10% (-1.34 to 3.73%) | 1.19% (0.11 to 4.03%) |
| k16 | 2.011 | 2.027 | 2.049 | 1.24% (-1.20 to 4.16%) | 1.89% (-0.39 to 2.97%) |
| k32 | 5.101 | 5.179 | 5.168 | 1.55% (-3.55 to 3.42%) | 1.38% (-4.04 to 4.86%) |
| k33 | 5.321 | 5.415 | 5.393 | 2.20% (-1.13 to 3.45%) | 1.29% (-1.68 to 3.51%) |
| k34 | 5.634 | 5.768 | 5.723 | 2.58% (0.10 to 3.68%) | 1.65% (-1.16 to 3.79%) |
| k64 | 15.043 | 16.688 | 15.208 | 10.96% (8.32 to 12.02%) | 1.23% (-1.66 to 3.01%) |
| k65 | 15.655 | 17.207 | 15.780 | 9.96% (8.09 to 13.86%) | 0.70% (-1.42 to 2.63%) |
| k96 | 28.641 | 28.378 | 30.159 | -1.02% (-2.74 to 0.12%) | 5.23% (3.23 to 6.32%) |
| k128 | 45.670 | 40.252 | 45.296 | -11.88% (-13.54 to -11.29%) | -0.78% (-2.75 to -0.34%) |
| k129 | 48.105 | 42.753 | 47.661 | -11.14% (-12.05 to -10.32%) | -1.11% (-1.70 to 1.23%) |
| k256 | 164.601 | 92.694 | 113.463 | -43.74% (-44.06 to -42.19%) | -31.16% (-31.36 to -28.19%) |
| k512 | 601.996 | 203.453 | 256.999 | -66.24% (-66.46 to -65.30%) | -57.29% (-57.70 to -52.36%) |
| k512_prefix256 | 1262.658 | 567.923 | 677.078 | -55.11% (-56.66 to -54.39%) | -46.36% (-47.75 to -45.76%) |
| k4097 | 33348.487 | 1836.268 | 2296.536 | -94.48% (-94.56 to -94.35%) | -93.11% (-93.19 to -93.02%) |
| k8193 | 136642.305 | 3954.337 | 4951.815 | -97.10% (-97.12 to -97.04%) | -96.37% (-96.38 to -96.32%) |
| k8193_desc | 136375.265 | 3462.264 | 4469.180 | -97.46% (-97.49 to -97.39%) | -96.72% (-96.75 to -96.68%) |
| k8193_shuffled | 140934.511 | 5743.832 | 6804.873 | -95.92% (-95.96 to -95.79%) | -95.17% (-95.20 to -95.11%) |

The named typical and versioned cases are small mock Registry responses.
`k512_prefix256` contains one schema field and 512 extra keys with a
256-byte common prefix; `k8193` shapes contain 8,193 total keys.

## Synchronous parsing and memory

The [first](resources-1.txt), [second](resources-2.txt) and
[third](resources-3.txt) resource samples used separate direct children with
a 30-second deadline each and Linux `wait4` accounting. For the 16,384
extra-key workload, all 15 readings within each run were identical, at
16,812, 16,712 and 16,832 KiB respectively; they do not resolve incremental
index RSS and may reflect a launcher/pre-exec high-water floor.

In a direct index control 8,192 nodes occupied capacity for 8,192 nodes at
12 bytes each: 98,304 bytes of nominal vector storage. Entries, keys, values,
allocation metadata and process state are additional. Across these runs a
prepared single-key 64 MiB body took 122.84–199.38 ms baseline and
124.33–194.72 ms at 64-tail in direct synchronous parsing. That body does not
exercise the index. The samples are not a first-poll, deadline, stack or RSS
bound, and earlier denied process-descriptor measurements were not retried.

## Reproduction

The [benchmark injection](benchmark.rs.txt), [comparison script](comparison.py.txt)
and [workflow](https://github.com/mingley/partitionline/blob/8f288f2c4c52b354e873e5899a0d5480d002437f/.github/workflows/schema-json-progressive-avl.yml)
are retained. Baseline and 32-/64-tail benchmark source blobs after test
injection were `2690fc31a5c8e79bfa94dafeb5273dcb1dd5b91a`,
`38f6b6a6a1f4334a1f53fd18a89c41583d7718d4` and
`bdd333c91b5db0ef08c34ba3505d9896c6b1a5a2`. Their independently rebuilt
executable SHA-256 values matched across the three runs:
`c0eb22f23f6379543f4d203f4fadfe86942360d882240e35c4b26e0bb420295c`,
`fff6ac4050f7ff7c203d407d36df331be6d177149dad7e13c91c520f51c5f873`,
and `fc4994219f86b126b3f08039bd359a47e001ec1f3f285645463961db487cc357`.
The manifest and lockfile Git blobs were
`077a5f729d4e2c2accc2a138f872dd87e8f20a0b` and
`4cc5fc2a7502df2d4cb1a7785a63fe97650b5fa8`.
