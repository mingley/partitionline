# LZ4 large-block copy out of line

Rejected by the instruction gate and timing controls. The 9 KiB and
64 KiB random LZ4 fixtures gained 8.63% and 9.41%. The 100-byte
and 1 KiB text fixtures fell 2.64% and 3.53%.

The synchronous decoder used a cursor and the existing bounded read for
compressed frame input below 256 KiB, equal to four usual Kafka 64 KiB
blocks. Larger inputs copied the frame decoder's buffered blocks
directly into output. Only the large block-copy helper stayed out of line.
The cooperative decoder kept its original reader and yield schedule.
[Candidate source](https://github.com/mingley/partitionline/blob/17852104d643f6b7ea50a06a37dfadd075140d1e/src/protocol/records.rs)

## Paired synchronous decode

Twelve pairs alternated two locked release builds on one virtual CPU
of a hosted Linux x86_64 runner. Both binaries verified all records and
frame consumption before timing. All 108 pairs matched work, wire sizes
and SHA-256 digests. Reported changes use the median paired throughput
ratio and its 96.14% sign interval. Wire sizes include Kafka batch framing.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44635013130/job/135951163541)

| Fixture | Wire bytes | Baseline million records/s | Candidate million records/s | Change | Interval |
| --- | ---: | ---: | ---: | ---: | ---: |
| `gzip-100` | 15,498 | 5.305 | 5.395 | +1.79% | +1.36% to +1.86% |
| `lz4-100-text` | 28,710 | 9.654 | 9.451 | -2.64% | -2.86% to -1.84% |
| `lz4-1024-h2-text` | 225,641 | 1.594 | 1.539 | -3.53% | -3.92% to -3.12% |
| `lz4-64k-random` | 2,098,228 | 0.101 | 0.111 | +9.41% | +7.82% to +9.72% |
| `lz4-9000-random` | 577,844 | 0.876 | 0.953 | +8.63% | +7.74% to +11.10% |
| `lz4-small` | 947 | 9.168 | 9.281 | +1.09% | +0.70% to +2.04% |
| `none-100` | 63,433 | 16.871 | 17.341 | +1.88% | +1.25% to +3.32% |
| `snappy-100` | 24,265 | 7.903 | 7.775 | -1.77% | -2.24% to -1.30% |
| `zstd-100` | 17,153 | 4.282 | 4.303 | +0.49% | -0.00% to +0.68% |

## Checks

Formatting and strict Clippy passed on Rust 1.100.0. Default and all-feature
builds each passed 79 record tests, including exact byte limits and
first-frame termination for checksummed linked and independent frames
on both sides of the split. The Produce wire fixture and all three
allocation checks passed.
[Protocol checks](https://github.com/mingley/partitionline/actions/runs/44635013130/job/135951163371) [Allocation checks](https://github.com/mingley/partitionline/actions/runs/44635013130/job/135951164003)

The LZ4 decode benchmark measured 960,806 instructions against
903,610 recorded (+6.33%). The LZ4 case exceeded 1%. The other nine cases passed.
[Instruction gate](https://github.com/mingley/partitionline/actions/runs/44635013130/job/135951163042)

## Inputs and results

- Exact source, binary, runner and compiler identities. [Provenance](provenance.json)
- All 108 paired work counts, timings and wire digests. [Pairs](pairs.jsonl)
- Measured and committed instruction totals. [Counts](instructions.tsv)
- Timed fixtures and build procedure. [Rust bench](https://github.com/mingley/partitionline/blob/17852104d643f6b7ea50a06a37dfadd075140d1e/benchmarks/codec/src/bin/lz4-decode-timing.rs) [Pairing script](https://github.com/mingley/partitionline/blob/17852104d643f6b7ea50a06a37dfadd075140d1e/benchmarks/codec/lz4-decode-compare.py) [Workflow](https://github.com/mingley/partitionline/blob/17852104d643f6b7ea50a06a37dfadd075140d1e/.github/workflows/lz4-large-block-boundary-qualification.yml)
