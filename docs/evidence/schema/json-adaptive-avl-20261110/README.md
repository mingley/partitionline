# Registry JSON index: adaptive promotion comparison

This records finite in-process experiments on untrusted JSON object parsing.
It does not qualify a Registry deployment, bound response processing time or
establish a Kafka performance ranking. The two index candidates differ in how
many recent keys remain in a linear tail: split64 keeps up to 64; adaptive64
starts at 64, promotes at most two older keys per accepted field until the
tail reaches 32, then promotes one. Split32 starts the index after 32 keys.
They preserve input order and compare decoded keys. An indexed lookup uses
logarithmic key comparisons plus a bounded linear tail if the tree invariant
holds. Key length and common prefixes affect each comparison's byte cost.

## Correctness and source

[Run 42871930235](https://github.com/mingley/partitionline/actions/runs/42871930235)
at `775c0eba9fd42dbdce4282e926135df7fcedd316` completed its four-arm
comparison successfully. Its adaptive and split32 correctness jobs passed
behavioral profiles, Clippy and rustdoc but failed formatting. The formatted
successor `1d876f02059708cd7176512c7e1327e1fb4d4592`
passed all jobs in
[run 42872561322](https://github.com/mingley/partitionline/actions/runs/42872561322):
focused index controls, default, all-feature and no-default-feature schema
suites, formatting, strict all-target/all-feature Clippy, rustdoc and the
four-arm comparison. The earlier formatting failures remain part of the
record.

The separately reviewed split64 source in
[`b27a3d0`](https://github.com/mingley/partitionline/commit/b27a3d072a407fa82ae9efdd48b0c118a0b7d813)
passed [run 42872601027](https://github.com/mingley/partitionline/actions/runs/42872601027)
including the same three schema profiles, expanded duplicate and nesting
controls, formatting, strict Clippy and rustdoc. Its complete
`registry.rs` Git blob is `74d8721405e32cc6939ad22378893b11a82ce625`.
Removing only its marked candidate-specific test-control block produces
`3f26f8d5cfa83b4b98aea944b7212028923b272e`, the split64 production-source
blob recorded in both four-arm comparisons. The baseline source blob is
`2e54868346d05cafff0369358703a033cce0c501`. The split64 tests and the
independent review are described in the
[split record](../json-avl-split-fastpath-20261110/README.md).

Each comparison ran a 104-case authored differential corpus against separate
baseline, split32, split64 and adaptive64 release-test executables. Their full
results matched before the runner emitted the retained transcripts:
[run 1](differential-1.txt), [run 2](differential-2.txt). Successful outputs
are recorded as byte lengths and SHA-256 digests; error outputs are retained
as printed. The transcripts match byte-for-byte. These finite cases cover
transition sizes, escaped names, duplicate locations, malformed values and
delimiters, and nested wide objects. They are not an independent JSON
implementation, a live Registry, or complete proof of equivalent behavior.

## Paired samples

Both comparisons used Rust 1.99.1 on hosted x86_64 runners, with separate
target directories, release test binaries, one warmup and 16 rotating-order
rounds pinned to one logical CPU. Each round measured 19 prepared successful
JSON shapes in all four arms. The CPU model was not recorded. Executable
SHA-256 values matched across the independently rebuilt runs; exact source and
binary identifiers appear in [run 1 details](details-1.txt) and
[run 2 details](details-2.txt). Raw sample rows are in
[run 1 CSV](pairs-1.csv) and [run 2 CSV](pairs-2.csv). The test binaries
inject benchmark-only code and are not shipped artifacts. No HTTP, live
Registry or Kafka workload was measured.

The tables give median paired percentage changes against the baseline, with
the minimum and maximum paired change in parentheses. Negative is faster.
They describe the samples rather than a controlled production regression
bound; small samples and host effects can move results.

### Run 42871930235

| Shape | 32-tail | 64-tail | Adaptive |
| --- | ---: | ---: | ---: |
| typical | 1.23% (-1.49 to 7.78%) | 1.05% (-2.12 to 3.83%) | -1.66% (-3.59 to 2.77%) |
| versioned | -0.23% (-1.60 to 6.04%) | 0.35% (-1.65 to 3.00%) | -1.46% (-2.75 to 4.13%) |
| k64 | 14.63% (8.86 to 17.61%) | 8.32% (1.82 to 18.60%) | 9.16% (3.06 to 14.60%) |
| k65 | 10.12% (8.39 to 19.54%) | 4.87% (0.04 to 19.46%) | 6.95% (3.23 to 11.95%) |
| k96 | -2.35% (-3.15 to 7.75%) | 4.01% (2.28 to 14.87%) | 9.97% (7.81 to 26.79%) |
| k128 | -13.99% (-14.51 to -6.90%) | -2.01% (-3.23 to 8.22%) | -5.64% (-8.11 to 11.79%) |
| k256 | -40.41% (-42.10 to -38.09%) | -28.11% (-29.52 to -26.53%) | -36.95% (-39.05 to -35.55%) |
| k512 | -65.00% (-65.25 to -58.67%) | -56.63% (-57.78 to -52.91%) | -63.66% (-64.48 to -59.22%) |
| k8193 | -97.12% (-97.25 to -96.96%) | -96.45% (-96.65 to -96.24%) | -96.97% (-97.14 to -95.74%) |

### Run 42872561322

| Shape | 32-tail | 64-tail | Adaptive |
| --- | ---: | ---: | ---: |
| typical | 2.59% (0.47 to 7.97%) | 1.82% (-0.44 to 4.12%) | 0.23% (-0.68 to 2.43%) |
| versioned | -0.70% (-8.70 to 0.66%) | -1.20% (-7.87 to 0.29%) | 0.80% (-5.92 to 1.74%) |
| k64 | 6.93% (4.81 to 8.19%) | -4.93% (-7.18 to -4.09%) | 0.14% (-2.13 to 2.53%) |
| k65 | 6.61% (5.06 to 7.20%) | -3.86% (-5.32 to -2.08%) | 0.94% (-0.82 to 2.36%) |
| k96 | -3.45% (-4.56 to -1.49%) | 5.56% (4.44 to 10.40%) | 8.46% (7.75 to 12.89%) |
| k128 | -14.10% (-14.54 to -13.84%) | 1.27% (0.33 to 6.60%) | -7.33% (-8.11 to -6.54%) |
| k256 | -41.62% (-42.03 to -40.73%) | -25.00% (-25.67 to -20.13%) | -40.18% (-40.81 to -39.61%) |
| k512 | -65.20% (-65.45 to -64.77%) | -53.78% (-54.04 to -53.33%) | -64.95% (-65.25 to -63.23%) |
| k8193 | -96.91% (-96.97 to -96.86%) | -95.96% (-96.00 to -95.52%) | -96.93% (-96.98 to -96.87%) |

For adaptive64 versus split64, the paired median was +5.39% at 96 keys and
−14.46% at 8,193 keys in run 1; run 2 reported +2.77% and −24.19%,
respectively. Their pair ranges, plus all other shapes, appear in the detail
files. The 64- and 65-key results vary substantially across runs, while the
96-key split64 sample is slower in both. The indexed variants add allocations,
and adaptive64 can grow its node vector sooner than split64. These results do
not show that any one variant wins on every workload.

## Synchronous work and memory

The [resource observations](details-1.txt) and [repeat](details-2.txt) use
`wait4` for separate direct child processes and do not establish a process
RSS bound. All many-key readings within a run had the same high-water mark,
which does not resolve incremental index memory and may reflect a prior
launcher high-water floor. Single-key 64 MiB body tests do not exercise the
index; parsing remained synchronous. The response-body limit does not bound
RSS, a single string comparison, allocator latency, scheduling, nested stack
use or synchronous poll duration. No denied process-descriptor sample was
repeated or replaced by these observations.

The [comparison script](comparison.py.txt), [injected benchmark](benchmark.rs.txt)
and [workflow](workflow.yml.txt) are retained. The pinned
[source commit](https://github.com/mingley/partitionline/tree/1d876f02059708cd7176512c7e1327e1fb4d4592)
contains the baseline file and full experiment setup. Source hashes and
recorded outputs support reproduction; uploaded artifacts were not
independently inspected.
