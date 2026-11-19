# Inline single-byte Kafka varints

Rejected by the instruction-count gate. Moving wider writes out of line improved
median throughput by 16.60% for 100-byte uncompressed records and 6.72% for
tiny records. The LZ4 decode benchmark exceeded the 1% instruction budget.

## Throughput

Both locked release builds used stable Rust 1.100.0 with Zstandard enabled
on one hosted Linux x86_64 worker. Twelve pairs alternated execution order
on one virtual CPU using the same nine fixtures as the inline short-width
candidate. Both builds warmed up before timing equal work with reusable output
and compression scratch. All 108 pairs produced identical wire digests and
each build decoded the result back to the input records. The table reports
the median paired throughput ratio and its 96.14% sign interval.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44615403653/job/135882612816)

| Fixture | Baseline million records/s | Candidate million records/s | Change | Interval |
| --- | ---: | ---: | ---: | ---: |
| `gzip-100` | 0.236 | 0.237 | +0.28% | -0.02% to +0.38% |
| `lz4-1024-h2` | 0.615 | 0.623 | +1.09% | -0.00% to +2.56% |
| `none-100` | 14.449 | 16.853 | +16.60% | +15.24% to +17.03% |
| `none-1024-h2` | 3.679 | 3.788 | +2.92% | +2.32% to +3.25% |
| `none-1m` | 0.005 | 0.005 | +0.03% | -0.03% to +0.17% |
| `none-large` | 0.628 | 0.631 | +0.44% | +0.01% to +0.67% |
| `none-tiny` | 15.692 | 16.775 | +6.72% | +6.27% to +7.64% |
| `snappy-100` | 4.783 | 5.026 | +5.02% | +4.50% to +5.21% |
| `zstd-1024-h2` | 0.240 | 0.241 | +0.30% | -0.26% to +0.84% |

## Checks

Formatting and strict all-feature Clippy passed. Each feature profile
passed 33 buffer tests and 78 record tests. The committed Produce wire fixture
and all three allocation census tests passed.
[Protocol tests](https://github.com/mingley/partitionline/actions/runs/44615403653/job/135882613240) [Allocation tests](https://github.com/mingley/partitionline/actions/runs/44615403653/job/135882613191)

Callgrind measured 9.25% fewer instructions in `iai_encode/f03` and 6.45%
fewer in `iai_request/v9`. The LZ4 decode count went from 903,590 to 920,096
in the paired build, up 1.83%. The stored x86_64 baseline is 903,610,
which gives a 1.82% increase against the gate.
[Instruction job](https://github.com/mingley/partitionline/actions/runs/44615403653/job/135882613056)

## Inputs and results

- Source identities, tool versions and binary digests. [Provenance](provenance.json)
- Timings and wire digests for each arm of all 108 comparisons. [Pairs](pairs.jsonl)
- Baseline and candidate instruction counts. [Callgrind counts](instructions.tsv)
- Timed fixtures and procedure. [Rust bench](https://github.com/mingley/partitionline/blob/b47dc50e9793f3d4b9f5356a5e6a4c928ffc5b69/benchmarks/codec/src/bin/record-varint-timing.rs) [Pairing script](https://github.com/mingley/partitionline/blob/b47dc50e9793f3d4b9f5356a5e6a4c928ffc5b69/benchmarks/codec/record-varint-compare.py) [Build steps](https://github.com/mingley/partitionline/blob/b47dc50e9793f3d4b9f5356a5e6a4c928ffc5b69/.github/workflows/qualification-record-varint-nov19.yml)
