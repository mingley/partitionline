# Incremental Registry JSON key index experiment

9 November 2026

The Registry JSON parser currently scans every earlier object key when it
accepts a new one. This experiment retains that scan through 64 keys. For
larger objects it indexes at most two previously accepted keys per new field,
checks the indexed prefix by a randomized hash and full string equality,
and scans the remaining suffix. This avoids a bulk index rebuild at the
threshold. It keeps the original object order and copies no key strings into
the index.

The experiment remains unapplied. The 96-key case was consistently slower
in the recorded comparison, strict Clippy failed, and the index has no
nonquadratic worst-case guarantee: full-hash collisions can force a linear
scan of the collision list. The response-size limit bounds the input, but
neither CPU time nor process RSS has been qualified.

## Controls

[Run 42689743606](https://github.com/mingley/partitionline/actions/runs/42689743606)
passed focused controls for 64, 65, 96, 128 and 129 keys; duplicates in the
indexed prefix and unindexed suffix; forced full-hash collisions; input
order; malformed duplicate values; nested duplicates; and a public mock
4,096-field response. Full default, all-feature and no-default-feature
schema profiles passed. The default and all-feature library profiles each
passed 66 tests. Format passed. Strict Clippy failed
`clippy::needless-range-loop` in the migration loop, so warning-denied
rustdoc did not run. The formatted source blob was
`266857065fa39e5da1e2b2ac9c94a3d749fd0709`; the unformatted candidate
commit is [5014a4c](https://github.com/mingley/partitionline/commit/5014a4cd8af8ee50912474e1c067e00dc4456f7d).

In one 16,384-name index control, the primary table held 16,384 hashes with
capacity 28,672 and a 16-byte hash/slot tuple. No full-hash collision was
observed in that sample; a separate constant-hash control passed. Hash-table
metadata, collision storage, allocator overhead, strings, values and input
are additional. This is a capacity observation, not a process-memory bound.

## Measurement

[Run 42689670705](https://github.com/mingley/partitionline/actions/runs/42689670705)
used Rust stable on a hosted x86_64 runner and built baseline and candidate
in separate release target directories with
`--no-default-features --features registry`. It timed 12 alternating pairs
pinned to one CPU on prepared, successful in-process Registry JSON bodies.
The [384 raw samples](pairs.csv) retain each elapsed time and iteration
count. Percentages are paired median changes; ranges are minimum and maximum
paired changes. This does not measure HTTP, Kafka or a live registry.

| Shape | Candidate vs baseline | Range |
| --- | ---: | ---: |
| Schema by ID | −0.58% | −2.07% to +8.95% |
| Subject/version | −2.28% | −3.49% to +0.53% |
| Five keys | −3.45% | −4.62% to −2.21% |
| 16 keys | −2.82% | −4.06% to +3.13% |
| 32 keys | −2.11% | −4.53% to +9.03% |
| 64 keys | −1.55% | −3.76% to +2.24% |
| 65 keys | −1.30% | −5.75% to +29.16% |
| 96 keys | +6.42% | +4.23% to +11.72% |
| 128 keys | −6.52% | −7.77% to −1.53% |
| 129 keys | −6.19% | −8.39% to +1.20% |
| 256 keys | −53.06% | −53.77% to −49.80% |
| 512 keys | −76.92% | −77.21% to −75.24% |
| 4,097 keys | −97.13% | −97.17% to −96.88% |
| 8,193 ascending | −98.56% | −98.59% to −98.46% |
| 8,193 descending | −98.59% | −98.60% to −98.47% |
| 8,193 shuffled | −98.58% | −98.61% to −98.46% |

The two named ordinary cases are small explicit mock Registry responses,
including a nested reference. Other cases contain a schema field and
generated unique names.

The same run observed process peak RSS with `/usr/bin/time` on separate
16,385-key parse processes, three per arm. Baseline observations were
5,428, 5,524 and 5,416 KiB; candidate observations were 6,076 KiB in all
three trials. These include the whole process and allocator state, not
just the index; they are not an RSS bound. The [raw observations](rss.csv)
retain the values. Parsing is synchronous and no live registry was used.

## Reproduction pins

The qualification started from main
`f1be3052aac5ec70a424ac951fe1189c27a31417`, with Registry source
`d97c106775976bac06808bdcb113488ccfe71148`. Benchmark source columns are
Git blob IDs including shared test-only instrumentation; executables are
SHA-256.

| Arm | Source blob | Executable SHA-256 |
| --- | --- | --- |
| Baseline | `af40a41a7b4c76cc638ccbe3cb9d993de624e11a` | `94bcffb0936c652c5d841e04cedfd2aea0c6f3430032f74e1fb5f42680d92e77` |
| Candidate | `6e9802551a860553ba84d0f203840a837305370e` | `009ae37f45d12c1ca8d6055cc5036400497315b0d736056e7f000ebcb4e80b1b` |

The candidate benchmark source matches the unformatted control source
`1a4bbbbbc18e48537b1511192c2cec79daa3d686` byte-for-byte before
`#[cfg(test)] mod tests`; benchmark instrumentation replaces the new
control tests in that module. The separate control run applied rustfmt
before testing and reported the Clippy failure above.
