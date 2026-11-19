# Direct two-byte varint writer

Rejected. The 100-byte uncompressed case gained 12.09% throughput and the
Snappy case gained 3.06%. Three other uncompressed cases lost 5.48% to 6.71%,
exceeding the 2% guardrail.

## Throughput

These timings cover Kafka magic-v2 record batch encoding with reusable
output and compression scratch on one hosted Linux x86_64 worker using stable
Rust 1.100.0. The test used separate locked release builds with Zstandard
enabled. Twelve pairs alternated execution order on one virtual CPU, and
each build warmed up before timing a fixed amount of work. Both builds
produced identical SHA-256 digests and decoded back to the source records
for all eight fixtures. The reported change is the median paired throughput
ratio, and the interval is the 96.14% sign interval for the median.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44610246146/job/135864592738)

| Fixture | Baseline million records/s | Candidate million records/s | Change | Interval |
| --- | ---: | ---: | ---: | ---: |
| `gzip-100` | 0.235 | 0.236 | +0.25% | +0.00% to +0.55% |
| `lz4-1024-h2` | 0.617 | 0.621 | +0.54% | -0.70% to +2.59% |
| `none-100` | 14.628 | 16.388 | +12.09% | +10.32% to +13.40% |
| `none-1024-h2` | 3.682 | 3.486 | -5.48% | -5.91% to -4.93% |
| `none-large` | 0.629 | 0.591 | -6.12% | -6.35% to -5.71% |
| `none-tiny` | 16.781 | 15.611 | -6.71% | -9.27% to -5.50% |
| `snappy-100` | 4.784 | 4.933 | +3.06% | +2.72% to +3.24% |
| `zstd-1024-h2` | 0.242 | 0.240 | -0.91% | -1.51% to +0.07% |

## Checks

Formatting, strict all-feature Clippy, 33 buffer and 78 record tests in
each feature profile, and the committed Produce wire fixture passed.
Three allocation census tests passed after gating the optional Zstandard
fixture in the benchmark harness. The production source was identical
across those runs.
[Protocol tests](https://github.com/mingley/partitionline/actions/runs/44610246146/job/135864594184)
[Allocation tests](https://github.com/mingley/partitionline/actions/runs/44610836343/job/135866678607)

Callgrind measured 8.43% fewer instructions in `iai_encode/f03` and 4.58%
fewer in `iai_request/v9`. All ten tracked cases either fell or matched.
[Instruction job](https://github.com/mingley/partitionline/actions/runs/44610836343/job/135866679014)

## Inputs and results

- Exact build identities, tool versions and binary digests. [Provenance](provenance.json)
- Timings, work counts and wire digests for each arm of all 96 comparisons. [Pairs](pairs.jsonl)
- Baseline and candidate instruction totals. [Callgrind counts](instructions.tsv)
- Timed fixture and pairing procedure. [Rust bench](https://github.com/mingley/partitionline/blob/2371eaba6b5aee01c97a98841303ff2ad1f79b97/benchmarks/codec/src/bin/record-varint-timing.rs) [Pairing script](https://github.com/mingley/partitionline/blob/2371eaba6b5aee01c97a98841303ff2ad1f79b97/benchmarks/codec/record-varint-compare.py) [Build steps](https://github.com/mingley/partitionline/blob/2371eaba6b5aee01c97a98841303ff2ad1f79b97/.github/workflows/qualification-record-varint-nov19.yml)
