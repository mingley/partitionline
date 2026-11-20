# Registry JSON owned fields

This candidate is held for regressions. The parser moved
successful response strings into their result and compacted spare
capacity for exact cache accounting. The string scanner matches
the baseline source byte for byte.

The 64 MiB response's parser median fell from 114.60 ms to
83.71 ms and peak RSS fell from 195.71 MiB to 131.76 MiB.
The 1 MiB and 8 MiB responses took 23.33% and 22.13% longer.
[Paired run](https://github.com/mingley/partitionline/actions/runs/44648404945/job/135996802001)

Stable Rust 1.100.0 and LLVM 23.1.5 compiled `e7c6e3cca0deb18e406ac4da4e4ae899834993e5`
and `85c5e91f1c182049cf64ae8b5311a9a7ac76f673`. Twelve alternating pairs ran
per fixture on one hosted Linux x86_64 logical CPU. The fixtures,
independent byte and field checks, and reporting procedure match the
combined trial. All 168 pairs verified. Peak RSS includes the test
process, input and verification.
[Method](../combined/README.md) · [Fixtures](../combined/fixtures.json)

Rustfmt, strict Clippy, the default, all-feature and no-default
test and doctest profiles, and the extracted crate passed. The pinned
Redpanda fixture passed both live profiles with its existing HTTPS,
Basic auth, cache and Avro checks.
[Correctness and package](https://github.com/mingley/partitionline/actions/runs/44648404945/job/135996801654) ·
[Live fixture](https://github.com/mingley/partitionline/actions/runs/44648404886)

| Case | Response bytes | Parse median (µs) | Paired time change [95% interval] | Peak RSS (MiB) | Paired RSS change |
| --- | ---: | ---: | ---: | ---: | ---: |
| `small-id` | 352 | 0.72 → 0.77 | +5.52% [+4.65%, +6.98%] | 4.00 → 4.00 | +0.39% |
| `small-subject` | 549 | 1.91 → 1.86 | -2.39% [-4.58%, -0.81%] | 3.91 → 3.96 | +1.46% |
| `escaped-unicode` | 440 | 1.05 → 1.09 | +4.44% [+3.84%, +5.47%] | 3.83 → 3.84 | +0.51% |
| `doc-64k` | 65536 | 56.35 → 55.20 | -2.01% [-2.07%, -1.96%] | 4.08 → 4.26 | +5.10% |
| `doc-1m` | 1048576 | 947.77 → 1170.34 | +23.33% [+23.14%, +23.83%] | 6.62 → 6.66 | +0.94% |
| `doc-8m` | 8388608 | 8042.16 → 9812.98 | +22.13% [+20.58%, +22.88%] | 27.62 → 27.63 | +0.03% |
| `doc-64m` | 67108864 | 114599.06 → 83709.13 | -27.01% [-27.19%, -26.74%] | 195.71 → 131.76 | -32.67% |
| `avro-512` | 38602 | 37.05 → 33.66 | -9.19% [-10.03%, -8.17%] | 3.94 → 4.10 | +4.80% |
| `avro-8192` | 614650 | 605.72 → 699.69 | +15.78% [+14.70%, +16.23%] | 5.24 → 4.96 | -5.34% |
| `metadata-63` | 2381 | 11.48 → 13.30 | +16.31% [+13.15%, +17.73%] | 3.87 → 3.85 | -0.00% |
| `metadata-256` | 8364 | 76.41 → 74.70 | -2.00% [-4.40%, -0.25%] | 3.79 → 3.97 | +4.60% |
| `metadata-4096` | 127404 | 1541.84 → 1575.87 | +2.28% [+1.37%, +2.56%] | 4.41 → 4.24 | -3.77% |
| `metadata-16384` | 508332 | 6638.51 → 6890.45 | +3.87% [+3.48%, +4.99%] | 6.13 → 6.13 | +0.38% |
| `references-256` | 20403 | 149.10 → 121.27 | -18.79% [-20.73%, -16.47%] | 4.36 → 4.24 | -1.98% |

[Raw pairs](pairs.jsonl) · [Summary](summary.json) · [Provenance](provenance.json)
