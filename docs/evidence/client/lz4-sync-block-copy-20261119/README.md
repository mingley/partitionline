# Synchronous LZ4 block copying

Rejected. The 9 KiB and 64 KiB random cases gained 3.17% and 3.53%.
The 100-byte and 1 KiB text cases fell 4.82% and 7.04%. The frozen gate
also measured 11.01% more instructions for LZ4 decode.

The synchronous decoder copied each decoded block from the existing
slice-backed frame decoder to the output buffer. It checks the
remaining output budget before copying and stops on the same frame
boundary. The declared size and allocation checks stayed in place.
The cooperative decoder kept its original reader and yield schedule.
[Candidate source](https://github.com/mingley/partitionline/blob/ba7c29bacce00e385f3e6c60a2c72a9b98b46336/src/protocol/records.rs)

## Paired synchronous decode

Twelve pairs alternated two locked release builds on one virtual CPU
of a hosted Linux x86_64 runner. Both binaries verified all records and
frame consumption before timing. All 108 pairs matched work, wire sizes
and SHA-256 digests. Reported changes use the median paired throughput
ratio and its 96.14% sign interval. Wire sizes include Kafka batch framing.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44638806846/job/135964184246)

| Fixture | Wire bytes | Baseline million records/s | Candidate million records/s | Change | Interval |
| --- | ---: | ---: | ---: | ---: | ---: |
| `gzip-100` | 15,498 | 4.778 | 4.778 | -0.00% | -0.15% to +0.38% |
| `lz4-100-text` | 28,710 | 7.131 | 6.781 | -4.82% | -5.53% to -4.29% |
| `lz4-1024-h2-text` | 225,641 | 1.023 | 0.951 | -7.04% | -7.24% to -6.48% |
| `lz4-64k-random` | 2,098,228 | 0.063 | 0.065 | +3.53% | +2.80% to +4.46% |
| `lz4-9000-random` | 577,844 | 0.440 | 0.453 | +3.17% | +2.85% to +3.78% |
| `lz4-small` | 947 | 8.907 | 9.026 | +2.07% | +0.11% to +3.22% |
| `none-100` | 63,433 | 12.940 | 12.883 | -0.53% | -1.52% to +0.35% |
| `snappy-100` | 24,265 | 6.493 | 6.481 | -0.17% | -0.57% to +0.73% |
| `zstd-100` | 17,153 | 3.276 | 3.268 | -0.43% | -1.06% to +0.24% |

## Checks

Formatting and strict Clippy passed on Rust 1.100.0. Default and all-feature
builds each passed 79 record tests, including exact byte limits and
first-frame termination for checksummed linked and independent frames. The Produce wire fixture and all three allocation checks passed.
[Protocol checks](https://github.com/mingley/partitionline/actions/runs/44638806846/job/135964184177) [Allocation checks](https://github.com/mingley/partitionline/actions/runs/44638806846/job/135964183309)

The LZ4 decode benchmark measured 1,003,104 instructions against
903,610 recorded (+11.01%). The LZ4 case exceeded 1%. The other nine cases passed.
[Instruction gate](https://github.com/mingley/partitionline/actions/runs/44638806846/job/135964184232)

## Inputs and results

- Exact source, binary, runner and compiler identities. [Provenance](provenance.json)
- All 108 paired work counts, timings and wire digests. [Pairs](pairs.jsonl)
- Measured and committed instruction totals. [Counts](instructions.tsv)
- Timed fixtures and build procedure. [Rust bench](https://github.com/mingley/partitionline/blob/ba7c29bacce00e385f3e6c60a2c72a9b98b46336/benchmarks/codec/src/bin/lz4-decode-timing.rs) [Pairing script](https://github.com/mingley/partitionline/blob/ba7c29bacce00e385f3e6c60a2c72a9b98b46336/benchmarks/codec/lz4-decode-compare.py) [Workflow](https://github.com/mingley/partitionline/blob/ba7c29bacce00e385f3e6c60a2c72a9b98b46336/.github/workflows/lz4-sync-block-copy-qualification.yml)
