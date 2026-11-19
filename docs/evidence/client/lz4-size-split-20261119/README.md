# Size-based LZ4 sync decode

Rejected by the instruction gate. All five LZ4 timing medians improved
2.45% to 4.48%. The uncompressed control fell 1.96% and the Snappy
control fell 0.94%.

The synchronous decoder used a cursor and the existing bounded read for
compressed frame input below 256 KiB, equal to four usual Kafka 64 KiB
blocks. Larger inputs copied the frame decoder's buffered blocks
directly into output. The sync dispatcher stayed out of line.
The cooperative decoder kept its original reader and yield schedule.
[Candidate source](https://github.com/mingley/partitionline/blob/ee1ae52b90cc46d3c32529ee939a92a04b8042d0/src/protocol/records.rs)

## Paired synchronous decode

Twelve pairs alternated two locked release builds on one virtual CPU
of a hosted Linux x86_64 runner. Both binaries verified all records and
frame consumption before timing. All 108 pairs matched work, wire sizes
and SHA-256 digests. Reported changes use the median paired throughput
ratio and its 96.14% sign interval. Wire sizes include Kafka batch framing.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44634116493/job/135948117073)

| Fixture | Wire bytes | Baseline million records/s | Candidate million records/s | Change | Interval |
| --- | ---: | ---: | ---: | ---: | ---: |
| `gzip-100` | 15,498 | 6.429 | 6.399 | -0.64% | -1.23% to +0.97% |
| `lz4-100-text` | 28,710 | 9.075 | 9.306 | +2.53% | +1.01% to +3.09% |
| `lz4-1024-h2-text` | 225,641 | 1.365 | 1.423 | +4.48% | +2.92% to +5.21% |
| `lz4-64k-random` | 2,098,228 | 0.076 | 0.078 | +3.24% | +2.77% to +3.55% |
| `lz4-9000-random` | 577,844 | 0.540 | 0.556 | +3.01% | +2.45% to +3.19% |
| `lz4-small` | 947 | 10.785 | 11.074 | +2.45% | +1.15% to +2.81% |
| `none-100` | 63,433 | 15.354 | 15.050 | -1.96% | -2.26% to -1.46% |
| `snappy-100` | 24,265 | 8.208 | 8.126 | -0.94% | -1.06% to -0.68% |
| `zstd-100` | 17,153 | 4.065 | 4.048 | -0.39% | -1.04% to +0.08% |

## Checks

Formatting and strict Clippy passed on Rust 1.100.0. Default and all-feature
builds each passed 79 record tests, including exact byte limits and
first-frame termination for checksummed linked and independent frames
on both sides of the split. The Produce wire fixture and all three
allocation checks passed.
[Protocol checks](https://github.com/mingley/partitionline/actions/runs/44634116493/job/135948117086) [Allocation checks](https://github.com/mingley/partitionline/actions/runs/44634116493/job/135948117038)

The LZ4 decode benchmark measured 960,805 instructions against
903,610 recorded (+6.33%). The LZ4 case exceeded 1%. The other nine cases passed.
[Instruction gate](https://github.com/mingley/partitionline/actions/runs/44634116493/job/135948116642)

## Inputs and results

- Exact source, binary, runner and compiler identities. [Provenance](provenance.json)
- All 108 paired work counts, timings and wire digests. [Pairs](pairs.jsonl)
- Measured and committed instruction totals. [Counts](instructions.tsv)
- Timed fixtures and build procedure. [Rust bench](https://github.com/mingley/partitionline/blob/ee1ae52b90cc46d3c32529ee939a92a04b8042d0/benchmarks/codec/src/bin/lz4-decode-timing.rs) [Pairing script](https://github.com/mingley/partitionline/blob/ee1ae52b90cc46d3c32529ee939a92a04b8042d0/benchmarks/codec/lz4-decode-compare.py) [Workflow](https://github.com/mingley/partitionline/blob/ee1ae52b90cc46d3c32529ee939a92a04b8042d0/.github/workflows/lz4-size-split-qualification.yml)
