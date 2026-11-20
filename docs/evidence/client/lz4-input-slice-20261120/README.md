# Read LZ4 frame input from a bounded slice

The synchronous decoder used a slice reader with one copy per
`Read::read` and a direct copy for `read_exact`. Incomplete exact reads
used the standard slice reader. The frame decoder and bounded output
reader remained in the path. [Candidate](https://github.com/mingley/partitionline/commit/150751bc53ce32f28cdfcc23f00cf2088dd8ccf8)

The frozen gate measured 920,084 LZ4 decode instructions against
903,610, or 1.82% above baseline. The other nine cases were within
the 1% limit. The candidate stayed on its trial branch.
[Instruction gate](https://github.com/mingley/partitionline/actions/runs/44647341685/job/135993176950)

Twelve alternating pairs per fixture on one hosted Linux x86_64
runner checked identical work and wire digests. Rates are millions of
records per second. Changes and 96.14% sign intervals use paired
throughput ratios. [Timing job](https://github.com/mingley/partitionline/actions/runs/44647341685/job/135993177035)

| Fixture | Baseline | Trial | Change | Interval |
| --- | ---: | ---: | ---: | ---: |
| `gzip-100` | 4.765 | 4.875 | +2.24% | +1.37% to +2.82% |
| `lz4-100-text` | 7.081 | 7.130 | +0.63% | +0.26% to +0.93% |
| `lz4-1024-h2-text` | 1.010 | 1.026 | +0.83% | +0.37% to +1.94% |
| `lz4-64k-random` | 0.063 | 0.060 | −5.38% | −5.86% to −4.77% |
| `lz4-9000-random` | 0.439 | 0.415 | −5.26% | −5.83% to −4.89% |
| `lz4-small` | 8.842 | 8.796 | +0.03% | −1.23% to +0.74% |
| `none-100` | 12.929 | 12.654 | −2.31% | −2.60% to −1.88% |
| `snappy-100` | 6.481 | 6.595 | +1.62% | +0.48% to +1.90% |
| `zstd-100` | 3.281 | 3.239 | −1.41% | −1.70% to −0.91% |

Formatting, both strict Clippy profiles, record and Produce fixture tests,
and the allocation checks passed.
[Correctness](https://github.com/mingley/partitionline/actions/runs/44647341685/job/135993176600) ·
[Allocations](https://github.com/mingley/partitionline/actions/runs/44647341685/job/135993177011)

[Paired measurements](pairs.jsonl) · [Instruction counts](instructions.tsv) ·
[Source and build details](provenance.json)
