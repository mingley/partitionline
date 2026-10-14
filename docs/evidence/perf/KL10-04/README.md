# KL10-04 in-client gzip deflate overhead

Analysis only. `src/protocol/records.rs` and `src/producer.rs` were read,
not edited. Suite HOLD remains. **No performance claim.**

## Question

Why did gateway-adoption.md section 2 (2026-09-30, miniz_oxide, Apple M4
Pro) spend about 18 µs/record inside `gzip_compress_into` when the same
backend compressing the same bytes in one call cost about 10 µs?

## What the source already does

`gzip_compress_into` (`records.rs`) clears a pre-sized `Vec`, wraps it in
`flate2::write::GzEncoder` at `Compression::default()` (level 6),
`write_all`s the **entire** uncompressed Kafka record section, and
`finish`es. The produce worker reuses one `CompressScratch` via
`write_record_batch_scratch`. It is not a per-record or 8 KiB feed.

`Producer` fires a batch when linger (100 ms) expires or estimated record
bytes reach `batch_bytes` (1 MiB), with max in flight 1. json1k + 16-byte
key + 8-byte `gw` header packs **973 records** at the byte cap (~1.03 MiB
section). Open-loop 25k usually hits linger first (~312 records per
partition per 100 ms).

## Measurements

Harness: `docs/evidence/perf/KL10-04/measure` (release, 40 timed iters,
median). Backends: default zlib-rs and `default-features = false`
miniz_oxide. Host: Linux x86_64 Xeon, rustc 1.99.0.

| Hypothesis | zlib-rs µs/rec | miniz µs/rec |
|---|---:|---:|
| `encode_record_batch` gzip | 28.19 | 46.98 |
| reused `CompressScratch` | 28.17 | 47.24 |
| encode only (no gzip) | 0.25 | 0.25 |
| `write_all` + `finish` | 28.01 | 47.06 |
| 8 KiB writes | 28.03 | 47.33 |
| per-record writes | 29.00 | 46.67 |
| encoder new + empty finish | 0.010 | 0.009 |

Encode-vs-one-call gap on zlib-rs: **0.18 µs/record** (0.6% of deflate,
0.4% of the KL10-02 gzip open-loop process CPU of ~48 µs/record). 8 KiB
vs `write_all` is noise. Encoder init is ~10 µs **per batch**, not per
record.

`scripts/perf-profile.sh --only rusage` ran (cpu/syscall/heap tools are
absent and fail closed). See `profile/`.

## Disposition

**Gap not reproducible.** No child card. Remaining gzip CPU is the
backend itself (KL10-05 already switched the default to zlib-rs).
Further produce CPU on `lb-gateway-produce-gzip` is outside this card.
