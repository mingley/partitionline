# Direct writes for one to three Kafka varint groups

Rejected. Encoding uncompressed 100-byte records gained 32.47% throughput.
Tiny records fell 3.28%, beyond the 2% guardrail, and the LZ4 decode
instruction count rose 1.83%, beyond the 1% guardrail.

## Throughput

Both locked release builds used stable Rust 1.100.0 with Zstandard enabled
on one hosted Linux x86_64 worker. Twelve pairs alternated execution order
on one virtual CPU. Each build warmed up before timing the same work with
reusable output and compression scratch. All 108 pairs matched SHA-256
digests, with a full encode/decode equality check for each measured arm.
The table reports the median paired throughput ratio and the 96.14% sign
interval for that median. [Timing job](https://github.com/mingley/partitionline/actions/runs/44611335011/job/135868415358)

| Fixture | Baseline million records/s | Candidate million records/s | Change | Interval |
| --- | ---: | ---: | ---: | ---: |
| `gzip-100` | 0.279 | 0.279 | +0.19% | -0.19% to +0.25% |
| `lz4-1024-h2` | 0.562 | 0.565 | +0.46% | -0.18% to +0.93% |
| `none-100` | 19.628 | 26.099 | +32.47% | +32.14% to +33.50% |
| `none-1024-h2` | 6.078 | 6.486 | +5.87% | +4.88% to +7.87% |
| `none-1m` | 0.007 | 0.007 | -0.22% | -0.65% to +0.86% |
| `none-large` | 1.180 | 1.183 | +0.35% | -0.17% to +1.55% |
| `none-tiny` | 21.082 | 20.318 | -3.28% | -4.29% to -2.27% |
| `snappy-100` | 4.640 | 4.875 | +5.06% | +4.71% to +5.37% |
| `zstd-1024-h2` | 0.262 | 0.261 | -0.90% | -1.23% to -0.27% |

## Checks

Formatting and strict all-feature Clippy passed. Both feature profiles passed
33 buffer tests and 78 record tests. The committed Produce wire fixture and
all three allocation census tests passed.
[Protocol tests](https://github.com/mingley/partitionline/actions/runs/44611335011/job/135868415053) [Allocation tests](https://github.com/mingley/partitionline/actions/runs/44611335011/job/135868415370)

Callgrind measured 8.00% fewer instructions in `iai_encode/f03`, 4.50%
fewer in `iai_request/v9` and 1.83% more in `iai_decompress_lz4/text`.
The ten cases and exact counts are retained.
[Instruction job](https://github.com/mingley/partitionline/actions/runs/44611335011/job/135868415474)

## Inputs and results

- Source identities, tool versions and binary digests. [Provenance](provenance.json)
- Timings and wire digests for each arm of all 108 comparisons. [Pairs](pairs.jsonl)
- Baseline and candidate instruction counts. [Callgrind counts](instructions.tsv)
- Timed fixtures and procedure. [Rust bench](https://github.com/mingley/partitionline/blob/703d7faca4c9f1426596cb22e06dac8997854fad/benchmarks/codec/src/bin/record-varint-timing.rs) [Pairing script](https://github.com/mingley/partitionline/blob/703d7faca4c9f1426596cb22e06dac8997854fad/benchmarks/codec/record-varint-compare.py) [Build steps](https://github.com/mingley/partitionline/blob/703d7faca4c9f1426596cb22e06dac8997854fad/.github/workflows/qualification-record-varint-nov19.yml)
