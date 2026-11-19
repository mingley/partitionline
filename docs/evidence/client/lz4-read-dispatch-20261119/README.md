# Out-of-line LZ4 read dispatch

Rejected. The 100-byte and 1 KiB LZ4 text fixtures fell 5.18% and
7.31%. The frozen gate measured 6.31% more LZ4 decode instructions
than the recorded baseline.

The synchronous decoder kept the slice-backed frame decoder and the
existing bounded read loop. A single out-of-line function called the loop
through a mutable `Read` trait object. Declared and actual output limits stayed in place.
The cooperative decoder kept its original reader and yield schedule.
[Candidate source](https://github.com/mingley/partitionline/blob/03c96584d1e227713a12710fa0dc42eac664138a/src/protocol/records.rs)

## Paired synchronous decode

Twelve pairs alternated two locked release builds on one virtual CPU
of a hosted Linux x86_64 runner. Both binaries verified all records and
frame consumption before timing. All 108 pairs matched work, wire sizes
and SHA-256 digests. Reported changes use the median paired throughput
ratio and its 96.14% sign interval. Wire sizes include Kafka batch framing.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44635940440/job/135954337461)

| Fixture | Wire bytes | Baseline million records/s | Candidate million records/s | Change | Interval |
| --- | ---: | ---: | ---: | ---: | ---: |
| `gzip-100` | 15,498 | 4.779 | 4.764 | -0.44% | -0.65% to -0.02% |
| `lz4-100-text` | 28,710 | 7.098 | 6.723 | -5.18% | -5.88% to -5.07% |
| `lz4-1024-h2-text` | 225,641 | 1.026 | 0.954 | -7.31% | -7.55% to -6.60% |
| `lz4-64k-random` | 2,098,228 | 0.064 | 0.064 | +0.17% | -0.28% to +0.74% |
| `lz4-9000-random` | 577,844 | 0.445 | 0.448 | +0.33% | -0.08% to +0.82% |
| `lz4-small` | 947 | 8.834 | 8.942 | +1.14% | +0.22% to +1.83% |
| `none-100` | 63,433 | 12.884 | 12.915 | +0.10% | -0.47% to +0.58% |
| `snappy-100` | 24,265 | 6.482 | 6.573 | +1.38% | +0.96% to +1.56% |
| `zstd-100` | 17,153 | 3.272 | 3.240 | -0.91% | -1.44% to -0.44% |

## Checks

Formatting and strict Clippy passed on Rust 1.100.0. Default and all-feature
builds each passed 78 record tests. The Produce wire fixture and all three
allocation checks passed.
[Protocol checks](https://github.com/mingley/partitionline/actions/runs/44635940440/job/135954337149) [Allocation checks](https://github.com/mingley/partitionline/actions/runs/44635940440/job/135954337763)

The LZ4 decode benchmark measured 960,618 instructions against
903,610 recorded (+6.31%). The LZ4 case exceeded 1%. The other nine cases passed.
[Instruction gate](https://github.com/mingley/partitionline/actions/runs/44635940440/job/135954337491)

## Inputs and results

- Exact source, binary, runner and compiler identities. [Provenance](provenance.json)
- All 108 paired work counts, timings and wire digests. [Pairs](pairs.jsonl)
- Measured and committed instruction totals. [Counts](instructions.tsv)
- Timed fixtures and build procedure. [Rust bench](https://github.com/mingley/partitionline/blob/03c96584d1e227713a12710fa0dc42eac664138a/benchmarks/codec/src/bin/lz4-decode-timing.rs) [Pairing script](https://github.com/mingley/partitionline/blob/03c96584d1e227713a12710fa0dc42eac664138a/benchmarks/codec/lz4-decode-compare.py) [Workflow](https://github.com/mingley/partitionline/blob/03c96584d1e227713a12710fa0dc42eac664138a/.github/workflows/lz4-bounded-read-dispatch-qualification.yml)
