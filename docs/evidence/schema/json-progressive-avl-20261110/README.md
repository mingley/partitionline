# Progressive AVL index for Registry JSON keys

This isolated 32-key-tail candidate preserves JSON object order and checks
a new key after its value has been parsed. It retains the first part of an
object in an AVL index of positions in the existing entry vector; the most
recent 32 keys remain a linear suffix. After each accepted field beyond the
threshold, one key moves into the tree. No key string is copied into the
index and no hash-collision chain is used.

For a valid index, the suffix is fixed at 32 keys and AVL height bounds
key comparisons per lookup and insertion to logarithmic in indexed keys.
Comparison cost in bytes also depends on key lengths and common prefixes.
The tests check balance and ordering for sorted, reverse and deterministically
shuffled sequences through 8,192 keys, including parser-level duplicates in
the indexed prefix and recent suffix. They are not a formal proof. Vector
allocation, string comparison, synchronous parse time, scheduler behavior and
process RSS do not have a bound established by those tests.

**The candidate is unapplied.** On a second paired run it slowed both ordinary
mock Registry shapes and the 64/65-key fixtures consistently. It improves
large-object parse time in these workloads, but those costs must be weighed
against the correctness and resource benefit.

## Correctness

The formatted [candidate](https://github.com/mingley/partitionline/commit/e6b1f17753fdd2521981f4e340da7124c4eb3309)
passed [run 42865002005](https://github.com/mingley/partitionline/actions/runs/42865002005):
three focused controls; 111 default, 152 all-feature and 20 no-default tests
including doctests; format, all-target/all-feature Clippy and strict rustdoc.
The controls include duplicates on either side of the indexed boundary,
escaped-equivalent keys, nested scopes, malformed duplicate values, and a
public mock response with 4,096 extra fields. There was no live Registry or
TLS interoperability test.

The earlier [first attempt](https://github.com/mingley/partitionline/actions/runs/42864463458)
passed its behavioral and lint/doc checks but failed formatting; its benchmark
failed before measurement because the copied crate was inside another
workspace. [Run 42864631991](https://github.com/mingley/partitionline/actions/runs/42864631991)
also failed formatting and the same benchmark setup. The comparison was moved
to standalone temporary directories before later measurements. Earlier
[hash index](../json-duplicate-index-20261109/README.md),
[compact](../json-compact-index-20261109/README.md),
[balanced tree](../json-balanced-index-20261109/README.md) and
[incremental hash](../json-incremental-index-20261109/README.md) attempts
remain recorded and unapplied.

The [candidate patch](candidate32.patch) is the formatted source change from
main `19979b3a22e56b37a1a03d60fe8f67b746a1f047`. Applying its hunks to
baseline source blob `2e54868346d05cafff0369358703a033cce0c501` reproduced
candidate blob `7acce1e3e14204e1fc499afba6d9b77ffe1eb14a` in a text
comparison. The evidence patch is not applied to main.

## Paired in-process parsing

[Run 42865002005](https://github.com/mingley/partitionline/actions/runs/42865002005)
used Rust 1.99.1 on a shared x86_64 runner, separate release target directories
and 12 alternating pairs pinned to one logical CPU. It timed prepared
successful Registry JSON bodies, without HTTP, Kafka or a live Registry.
The [456 raw samples](formatted-pairs.csv) retain each total and iteration
count. Table times are per-parse medians in microseconds; percentage values
are medians of paired candidate/baseline changes, with minimum and maximum
pair changes. The percentage median need not equal the ratio of time medians.

| Shape | Baseline µs | Candidate µs | Paired median | Pair range |
| --- | ---: | ---: | ---: | ---: |
| typical | 1.022 | 1.053 | 3.28% | -0.23% to 7.77% |
| versioned | 0.858 | 0.856 | -0.10% | -3.70% to 1.31% |
| k5 | 0.544 | 0.527 | -3.51% | -4.55% to -1.64% |
| k16 | 1.856 | 1.810 | -2.64% | -4.65% to -1.31% |
| k32 | 4.527 | 4.345 | -3.75% | -7.67% to -2.11% |
| k33 | 4.693 | 4.461 | -5.23% | -8.32% to -3.75% |
| k34 | 4.888 | 4.676 | -4.78% | -8.55% to -2.92% |
| k64 | 11.853 | 12.299 | 3.93% | 1.43% to 4.45% |
| k65 | 12.343 | 12.714 | 2.98% | -0.30% to 3.97% |
| k96 | 22.535 | 20.891 | -7.42% | -9.94% to -6.34% |
| k128 | 35.968 | 29.830 | -17.36% | -19.14% to -16.09% |
| k129 | 38.786 | 32.269 | -16.77% | -17.98% to -15.88% |
| k256 | 125.031 | 70.101 | -43.87% | -45.07% to -43.51% |
| k512 | 446.936 | 151.225 | -66.18% | -66.39% to -65.80% |
| k512_prefix256 | 1172.286 | 468.245 | -60.09% | -60.89% to -59.22% |
| k4097 | 25777.697 | 1473.863 | -94.30% | -94.34% to -94.24% |
| k8193 | 102502.361 | 3105.862 | -96.97% | -97.00% to -96.93% |
| k8193_desc | 102265.239 | 2787.302 | -97.28% | -97.34% to -97.24% |
| k8193_shuffled | 102592.296 | 5495.142 | -94.66% | -94.76% to -94.56% |

The two named typical cases are prepared small Registry response bodies.
The `k512_prefix256` shape has one schema field and 512 distinct extra
keys sharing a 256-byte prefix. Ascending, descending and shuffled
`k8193` shapes use 8,193 total keys.

The [second formatted run](https://github.com/mingley/partitionline/actions/runs/42865846371)
used the same production source on another shared x86 runner. Its independently
rebuilt executables had the same SHA-256 values as the first run.
Typical and versioned responses were +3.96% and +4.21% paired medians; all
pairs were +3.14% to +7.66% and +2.81% to +6.15%, respectively. The 64-key
case was +11.02% (+6.98% to +14.53%). Its [raw samples](second-pairs.csv)
are retained; run-to-run differences are not explained. The
[earlier unformatted comparison](https://github.com/mingley/partitionline/actions/runs/42864838174)
is retained [separately](unformatted-pairs.csv).

## Synchronous and memory observations

[Run 42866901283](https://github.com/mingley/partitionline/actions/runs/42866901283)
used the same production implementation, with a test-only index-capacity
print and additional benchmark observations. Each separate child had a
30-second deadline and peak RSS was reported by normal direct-child `wait4`
accounting. No denied process descriptor was accessed. The
[raw observations](resource-observations.txt) are retained.

For a 262,159-byte body with 16,384 extra keys, five synchronous parses took
217.46–228.98 ms baseline and 4.73–5.19 ms candidate. In a direct index
control, 8,192 nodes occupied capacity for 8,192 nodes at 12 bytes each:
98,304 bytes of logical vector storage. Allocator metadata, capacity elsewhere,
entry storage, values and strings are additional. The ten many-key peak-RSS
readings were all 16,348 KiB, so they do not resolve incremental index RSS;
launcher/pre-exec high-water effects may dominate.

For a prepared single-key body at the configured 64 MiB maximum, three direct
synchronous parses took 116.64–119.03 ms baseline and 88.03–91.66 ms
candidate. A 1 MiB body took 1.83–1.88 ms and 1.38–1.43 ms respectively.
The 64 MiB peak-RSS observations ranged from 200,268 to 200,488 KiB across
both arms and include the input, parsed strings, output and process state.
The single-key case does not exercise the index; compiler/layout effects and
scheduler conditions were not isolated. These samples do not establish a
first-poll maximum, cancellation responsiveness, deadline guarantee or RSS cap.

## Reproduction pins

At `e6b1f17753fdd2521981f4e340da7124c4eb3309`, the baseline and candidate
source blobs were `2e54868346d05cafff0369358703a033cce0c501` and
`7acce1e3e14204e1fc499afba6d9b77ffe1eb14a`. Benchmark builds stripped
candidate-only controls and injected the same
[benchmark test](benchmark.rs.txt) into both arms; their remaining test
modules were checked equal. The [comparison script](comparison.py.txt), [resource-observation script](resource-comparison.py.txt)
and its [test injection](resource-benchmark.rs.txt) are retained. The
[workflow](https://github.com/mingley/partitionline/blob/0b8a00459e58abecf6750aa2171bc81e7fbe7bbb/.github/workflows/schema-json-progressive-avl.yml)
shows the executed commands.
Manifest and lockfile blobs were `077a5f729d4e2c2accc2a138f872dd87e8f20a0b`
and `4cc5fc2a7502df2d4cb1a7785a63fe97650b5fa8`. The formatted run's
baseline and candidate executable SHA-256 values were
`f8413b92cf4a6eec82c48d93c72a40a6b1d8d122e6619621d1db5e5ca8189d09`
and `385372d60ab73ff29f3115b5c43aafa930569e1b358bbdcd7190a36456b423a1`.
