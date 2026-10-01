# KL10-16 lazy fetch-batch decode measurements

Parent: `3eb74f4d` (the `nb-fetch-gzip-overfetch` cell commit, measured as local commit `68c6924b`, which differs only in a benchmark contract test; consumer code
unchanged from `origin/main` `e9e41538`). Candidate: the KL10-16 change, which
frames and CRC-checks every fetched batch but decompresses and decodes it only
when it is applied within `buffer_memory`.

## Results

| Measurement | Parent | Candidate | Change |
|---|---:|---:|---:|
| `nb-fetch-gzip-overfetch` CPU per record | 13,321 ns | 9,410 ns | −29.5% (95% CI −30.6 to −27.1) |
| `nb-fetch-gzip-overfetch` records/s | 11,332 | 11,894 | +5.2% (CI +4.8 to +6.2) |
| `nb-fetch-gzip-overfetch` allocated bytes per record | 58,911 | 6,229 | −89% |
| `nb-fetch-gzip-overfetch` peak RSS | 237 MiB | 76 MiB | −68% |
| `nb-fetch-bulk` CPU per record (guardrail) | 1,661 ns | 1,532 ns | −5.2% (CI −18.0 to 0.0) |
| Real broker, real gzip: CPU per record | 15.11 µs | 4.49 µs | −70% |
| Real broker: records/s | 30,842 | 46,401 | +50% |
| Real broker: max RSS | 1,334 MB | 308 MB | −77% |

The null-broker cell serves gzip as stored frames, so its wasted decompression
is little more than a copy; it understates the real-gzip gain. Both setups still
fetch about 13× the consumed bytes, because batches past the budget are fetched
again; KL10-17 keeps them instead. librdkafka measured 2.35 µs per record on
the same real-broker setup.

Instructions (callgrind, aarch64 Linux): tracked decode and decompress benches
fall 0.1–1.6%; the largest increase is `iai_encode/f03` at +0.53% (code
layout; the encode path is unchanged). Allocation counts and bytes in the codec
allocation gates are unchanged.

## Method

- `nb-fetch-*`: seven interleaved repetitions per arm (parent then candidate,
  alternating order), `runtime --cell <id> --repetitions 1` per run, on an
  Apple M4 Pro (14 cores, macOS, rustc 1.98.1, release) at a 1-minute load
  average of 14–19. Paired deltas per repetition; the CI is a 10,000-sample
  bootstrap of the median.
- Real broker: a local mTLS Kafka on `127.0.0.1:9092`, topic `rsbench-k8` (8
  partitions, 1 KiB gzip records written by librdkafka), manual assignment,
  `max_bytes` 50 MiB, `max_partition_fetch_bytes` 8 MiB, default
  `buffer_memory` 32 MiB, up to 1M records or 30 s. The probe from the
  2026-10-01 compression-parity evaluation was rebuilt against each arm; three
  interleaved repetitions. Raw lines: `real-broker-consume.jsonl`.
- Instructions: `iai` and `json1k_iai` benches in a Debian 12 aarch64
  container (valgrind 3.19.0, iai-callgrind 0.14.2), both arms in one run.

## Limits

- Local and unsigned; not a Kafka throughput claim or peer comparison.
- x86_64 was not measured; CI runs the instruction and allocation gates there.
