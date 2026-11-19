# LZ4 sync decode with a slice cursor

Rejected for throughput regressions. Using a slice cursor restored the
unchanged instruction gate and improved the 100-byte and 1 KiB LZ4 text
cases by 1.17% and 1.50%. The 9 KiB and 64 KiB random LZ4 cases fell
5.45% and 5.60%. The uncompressed case fell 3.13%.

The sync decoder continued using the bounded read loop, with a cursor
tracking input position for the frame decoder. The cooperative path
kept the original reader and yield behavior.
[Candidate source](https://github.com/mingley/partitionline/blob/a8c47aa0dfa9c54e590f496c4af79b53e17712d7/src/protocol/records.rs)

## Paired synchronous decode

Twelve pairs alternated locked release builds on one virtual CPU
of a hosted Linux x86_64 runner. Each binary validated the complete
records and consumed the generated frame before timing. All 108 pairs
matched work, encoded lengths and SHA-256 digests. Reported changes
use the median paired throughput ratio and its 96.14% sign interval.
Wire sizes include the Kafka batch framing.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44632985049/job/135944210889)

| Fixture | Wire bytes | Baseline million records/s | Candidate million records/s | Change | Interval |
| --- | ---: | ---: | ---: | ---: | ---: |
| `gzip-100` | 15,498 | 4.784 | 4.862 | +1.74% | +0.61% to +2.23% |
| `lz4-100-text` | 28,710 | 7.107 | 7.184 | +1.17% | +0.52% to +1.63% |
| `lz4-1024-h2-text` | 225,641 | 1.023 | 1.039 | +1.50% | +1.30% to +2.19% |
| `lz4-64k-random` | 2,098,228 | 0.063 | 0.060 | -5.60% | -6.33% to -5.08% |
| `lz4-9000-random` | 577,844 | 0.441 | 0.418 | -5.45% | -6.53% to -4.62% |
| `lz4-small` | 947 | 8.944 | 8.985 | +0.50% | -0.24% to +1.72% |
| `none-100` | 63,433 | 12.977 | 12.578 | -3.13% | -3.50% to -2.65% |
| `snappy-100` | 24,265 | 6.503 | 6.567 | +1.08% | +0.88% to +1.28% |
| `zstd-100` | 17,153 | 3.272 | 3.218 | -1.40% | -2.03% to -0.72% |

## Checks

Formatting and strict Clippy passed with default and all features.
Each profile passed 79 record tests, including checksummed linked and
independent frames with exact and one-short byte limits. The Produce wire
fixture passed, as did three allocation checks.
[Protocol checks](https://github.com/mingley/partitionline/actions/runs/44632985049/job/135944211040) [Allocation checks](https://github.com/mingley/partitionline/actions/runs/44632985049/job/135944210557)

All ten cases passed the frozen x86_64 instruction budget with Rust 1.100.0
and Valgrind 3.26.0. LZ4 decode measured 904,024 instructions against
903,610 recorded (+0.05%).
[Instruction gate](https://github.com/mingley/partitionline/actions/runs/44632985049/job/135944210911)

## Inputs and results

- Source, build and runner identities. [Provenance](provenance.json)
- Timings and wire digests for all 108 comparisons. [Pairs](pairs.jsonl)
- Measured and committed instruction counts. [Counts](instructions.tsv)
- Timed fixtures and commands. [Rust bench](https://github.com/mingley/partitionline/blob/a8c47aa0dfa9c54e590f496c4af79b53e17712d7/benchmarks/codec/src/bin/lz4-decode-timing.rs) [Pairing script](https://github.com/mingley/partitionline/blob/a8c47aa0dfa9c54e590f496c4af79b53e17712d7/benchmarks/codec/lz4-decode-compare.py) [Workflow](https://github.com/mingley/partitionline/blob/a8c47aa0dfa9c54e590f496c4af79b53e17712d7/.github/workflows/lz4-slice-cursor-qualification.yml)
