# Decode header-free records from shrinking slices

The trial consumed short fields from a remaining slice and formed key
and value slices only after it checked the full record. Other layouts
used the existing fallback. This targets parsing after decompression,
including the same path for plaintext records. [Candidate](https://github.com/mingley/partitionline/commit/4b927ae1b204d1e81ecd6708b278a7f01e759b88)

The frozen gate measured 923,924 LZ4 decode instructions against
903,610 (+2.25%). The uncompressed decode case measured 372,214
against 368,394 (+1.04%). Both exceeded the 1% limit. The candidate
stayed on its trial branch. [Instruction gate](https://github.com/mingley/partitionline/actions/runs/44646514535/job/135990360513)

Twelve alternating pairs per fixture on one hosted x86_64 runner
checked identical inputs, work and wire digests. The table reports
millions of records per second, paired median changes and 96.14% sign
intervals. [Timing job](https://github.com/mingley/partitionline/actions/runs/44646514535/job/135990360314)

| Fixture | Wire bytes | Baseline | Trial | Change | Interval |
| --- | ---: | ---: | ---: | ---: | ---: |
| `gzip-100` | 15,498 | 6.446 | 6.479 | +0.72% | −0.15% to +1.13% |
| `lz4-100-text` | 28,710 | 9.066 | 9.138 | +0.73% | −0.14% to +1.30% |
| `lz4-1024-h2-text` | 225,641 | 1.371 | 1.381 | +0.71% | +0.51% to +1.19% |
| `lz4-64k-random` | 2,098,228 | 0.076 | 0.076 | −0.01% | −0.09% to +0.95% |
| `lz4-9000-random` | 577,844 | 0.540 | 0.541 | +0.18% | −0.52% to +0.55% |
| `lz4-small` | 947 | 10.804 | 10.901 | +0.76% | +0.11% to +1.69% |
| `none-100` | 63,433 | 15.431 | 15.440 | +0.07% | −0.24% to +0.18% |
| `snappy-100` | 24,265 | 8.190 | 8.254 | +0.84% | +0.43% to +1.10% |
| `zstd-100` | 17,153 | 4.072 | 4.059 | −0.31% | −0.78% to +0.28% |

Formatting, strict Clippy, both 78-test record profiles, the Produce
fixture and allocation checks passed.
[Correctness checks](https://github.com/mingley/partitionline/actions/runs/44646514535/job/135990360445) ·
[Allocation checks](https://github.com/mingley/partitionline/actions/runs/44646514535/job/135990360028)

The [measurements](pairs.jsonl), [instruction counts](instructions.tsv)
and [source and build details](provenance.json) retain every case.
