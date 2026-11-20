# Registry JSON string spans and owned fields

This candidate is held for regressions. Copying string spans and moving
returned fields cut median parser time for a 64 MiB response from
111.01 ms to 71.03 ms. Small ID lookups took 14.41% longer, the
8,192-field Avro response took 27.14% longer, and the 8 MiB response's
peak RSS rose 28.96%. [Paired run](https://github.com/mingley/partitionline/actions/runs/44647583240/job/135993998233)

The baseline was `e7c6e3cca0deb18e406ac4da4e4ae899834993e5`. The candidate at
`0eb4e64be03c0f3905e8d07e360909172e7323c4` preserved exact returned
string capacities for cache accounting. Its archive, strict lint,
112 default, 153 all-feature, and 20 no-default tests and doctests
passed. The existing pinned Redpanda HTTPS, Basic auth, cache and
Avro checks also passed.
[Correctness and package](https://github.com/mingley/partitionline/actions/runs/44647583240/job/135993998612) · [Live fixture](https://github.com/mingley/partitionline/actions/runs/44647583272)

## Paired measurements

Stable Rust 1.100.0 and LLVM 23.1.5 compiled both parser sources
with the same measurement module. Twelve alternating pairs ran
per fixture on one logical CPU of a hosted Linux x86_64 runner.
Python built complete Registry JSON responses and independently
decoded their expected fields. Both Rust arms checked each returned
schema byte, identifiers, type, ordered references, and string
capacities before and after timing. All 168 pairs passed those checks.

Each process measured the synchronous success-response parse call.
Peak RSS is GNU time's process high-water mark. It includes the
compiled test harness, input and validation. Durations include
scheduler delays. Median changes are paired ratios, with percentile
95% bootstrap intervals for time. Absolute columns show medians
across the twelve runs. Long documentation fields exercise selected
response sizes through the configured 64 MiB maximum. Metadata and
Avro field counts exercise different object and string shapes.

| Case | Response bytes | Parse median (µs) | Paired time change [95% interval] | Peak RSS (MiB) | Paired RSS change |
| --- | ---: | ---: | ---: | ---: | ---: |
| `small-id` | 352 | 0.71 → 0.82 | +14.41% [+13.73%, +15.47%] | 3.94 → 3.88 | -1.73% |
| `small-subject` | 549 | 1.89 → 1.79 | -5.24% [-6.67%, -4.44%] | 3.97 → 3.88 | -1.92% |
| `escaped-unicode` | 440 | 1.05 → 1.12 | +6.86% [+5.79%, +8.39%] | 3.75 → 3.77 | 0.00% |
| `doc-64k` | 65536 | 56.26 → 39.55 | -29.73% [-29.78%, -29.65%] | 4.06 → 3.90 | -3.86% |
| `doc-1m` | 1048576 | 951.31 → 954.50 | +0.42% [+0.18%, +0.52%] | 6.54 → 7.60 | +16.17% |
| `doc-8m` | 8388608 | 7989.31 → 8189.26 | +2.53% [+0.18%, +5.65%] | 27.57 → 35.55 | +28.96% |
| `doc-64m` | 67108864 | 111010.60 → 71026.33 | -36.13% [-36.86%, -35.03%] | 195.67 → 131.63 | -32.74% |
| `avro-512` | 38602 | 37.00 → 40.89 | +10.16% [+9.44%, +10.75%] | 3.88 → 4.05 | +3.61% |
| `avro-8192` | 614650 | 605.39 → 772.11 | +27.14% [+26.71%, +28.22%] | 5.21 → 4.92 | -5.86% |
| `metadata-63` | 2381 | 11.46 → 11.80 | +2.66% [+1.57%, +4.62%] | 3.79 → 3.78 | +1.37% |
| `metadata-256` | 8364 | 76.14 → 70.02 | -8.03% [-10.63%, -7.15%] | 3.75 → 3.88 | +3.60% |
| `metadata-4096` | 127404 | 1533.29 → 1496.13 | -2.52% [-3.38%, -1.80%] | 4.38 → 4.21 | -4.25% |
| `metadata-16384` | 508332 | 6602.28 → 6620.25 | +0.13% [-0.11%, +1.29%] | 6.08 → 6.10 | +0.10% |
| `references-256` | 20403 | 148.34 → 103.42 | -30.20% [-30.65%, -29.98%] | 4.36 → 4.12 | -6.02% |

The initial fixture run stopped at rustfmt and Cargo rejected a
baseline checkout nested inside the candidate workspace. The final
measurement copied both unchanged manifests and parser files into
separate build directories. The checkout, build and binary hashes
are in the provenance record.
[Initial run](https://github.com/mingley/partitionline/actions/runs/44646908113) · [Provenance](provenance.json)

[Fixtures](fixtures.json) · [Raw pairs](pairs.jsonl) ·
[Summary](summary.json) ·
[Measurement module](https://github.com/mingley/partitionline/blob/0eb4e64be03c0f3905e8d07e360909172e7323c4/benchmarks/registry-json/measure.rs) ·
[Timing driver](https://github.com/mingley/partitionline/blob/0eb4e64be03c0f3905e8d07e360909172e7323c4/benchmarks/registry-json/compare.py)
