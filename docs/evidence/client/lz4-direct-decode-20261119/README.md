# Direct LZ4 decoded-block copy

Rejected. The direct copy improved throughput on two large random fixtures
by about 6%. The 100-byte and 1 KiB text cases fell 3.54% and 5.44%. The LZ4
decode instruction count also exceeded the recorded 1% budget.

## Candidate

The synchronous decoder copies from the LZ4 frame decoder's output
buffer. The cooperative decoder limits each turn's copy to 8 KiB and
yields while the output has room. The existing checks enforce the
declared size and bound actual output bytes. The decoder still handles
frame and block checksums and determines where the frame ends.
[Source](https://github.com/mingley/partitionline/blob/175ed1659751c7803a84be36eb4ba4e548e249df/src/protocol/records.rs) ·
[Pinned decoder](https://github.com/PSeitz/lz4_flex/blob/1bffdcbbf906a234b913937cb2f57c0245915038/src/frame/decompress.rs)

## Paired synchronous decode

Twelve pairs alternated two locked release builds on one virtual CPU
of a hosted Linux x86_64 runner. The nine fixtures covered five LZ4
shapes and four codec controls. Each binary generated the input, verified
all records and checked that decode consumed the frame before timing.
All 108 pairs had identical input lengths and SHA-256 digests. The table
reports the median paired throughput ratio and its 96.14% sign interval.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44621276914/job/135903280754)

| Fixture | Baseline million records/s | Candidate million records/s | Change | Interval |
| --- | ---: | ---: | ---: | ---: |
| `gzip-100` | 5.634 | 5.693 | -0.82% | -2.53% to +2.66% |
| `lz4-100-text` | 10.405 | 10.052 | -3.54% | -4.53% to -3.03% |
| `lz4-1024-h2-text` | 1.712 | 1.605 | -5.44% | -7.56% to -4.30% |
| `lz4-64k-random` | 0.109 | 0.115 | +6.13% | +3.85% to +7.29% |
| `lz4-9000-random` | 0.948 | 1.015 | +5.93% | +2.23% to +10.05% |
| `lz4-small` | 9.837 | 10.171 | +2.76% | -0.47% to +4.49% |
| `none-100` | 18.192 | 18.434 | +1.17% | -4.82% to +4.40% |
| `snappy-100` | 8.480 | 8.459 | +0.40% | -0.58% to +10.46% |
| `zstd-100` | 4.572 | 4.613 | +0.74% | -0.32% to +1.08% |

## Checks

Rust 1.100.0 formatting and strict Clippy passed after applying a test-only
format correction. Default and all-feature builds each passed 79 record
tests, including linked and independent multi-block frames, actual byte
limits, frame termination and a cooperative yield. The committed Produce
wire fixture and the three allocation checks passed.
[Formatted tests](https://github.com/mingley/partitionline/actions/runs/44623236351/job/135910176224) ·
[First format attempt](https://github.com/mingley/partitionline/actions/runs/44621276914/job/135903280498) ·
[Allocations](https://github.com/mingley/partitionline/actions/runs/44621276914/job/135903280130)

The exact baseline `51a4d86` itself measured 920,104 LZ4 decode instructions
against the committed 903,610 (+1.83%). The first gate job failed on that
baseline before reaching the candidate. A separate run of the same gate
measured 1,003,191 for the formatted candidate (+11.02% against the recorded
baseline). The other nine cases stayed within the budget in both runs.
[Baseline gate](https://github.com/mingley/partitionline/actions/runs/44621276914/job/135903280638) ·
[Candidate gate](https://github.com/mingley/partitionline/actions/runs/44623236351/job/135910175679)

## Inputs and results

- Source identities, compiler, runner and release binary hashes. [Provenance](provenance.json)
- Per-arm work, times and wire digests. [Pairs](pairs.jsonl)
- Committed and measured instruction counts. [Callgrind counts](instructions.tsv)
- Timed fixtures, pairing and build commands. [Rust bench](https://github.com/mingley/partitionline/blob/175ed1659751c7803a84be36eb4ba4e548e249df/benchmarks/codec/src/bin/lz4-decode-timing.rs) [Pairing script](https://github.com/mingley/partitionline/blob/175ed1659751c7803a84be36eb4ba4e548e249df/benchmarks/codec/lz4-decode-compare.py) [Workflow](https://github.com/mingley/partitionline/blob/175ed1659751c7803a84be36eb4ba4e548e249df/.github/workflows/lz4-direct-decode-qualification.yml)
