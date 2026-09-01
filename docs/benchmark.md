# Benchmarks

Produce is acked records/second. Fetch is consumed records/second from a
topic this crate already filled. Per-run JSON and historical tables:
[benchmark-runs.md](benchmark-runs.md).

Two comparisons live here. Do not mix them.

| Label | What it is |
|---|---|
| **Lab A** | vs librdkafka **2.15.0** `rdkafka_performance` C. Apple M4 Pro, Docker Kafka 3.9.1. High watermark must equal records sent. |
| **this-VM, unsigned** | 2026-08-28 vs rust-rdkafka **0.39.0** (bundled librdkafka 2.12.1). Linux VM. Not Lab A, not C 2.15.0. |

## Locked knobs (both binaries)

| Knob | Produce / fetch throughput | Latency |
|---|---|---|
| Messages | 8,000,000 | 10,000 timed (+ 1,000 warmup) |
| Payload | 100 bytes | 100 bytes |
| `acks` | 1 (idempotent runs force all) | 1 |
| `linger.ms` | 5 | 0 |
| Topic | `plbench`, 6 partitions, RF 1 | `pllat`, 1 partition, RF 1 |
| Fresh topic | yes | yes |

`acks=0` is not this comparison. The C tool default linger is 1000 ms; it
must be overridden. Do not publish rec/s unless broker high watermark
equals records sent.

## Summary

Lab A produce medians (partitionline vs C 2.15.0):

| Run | partitionline | C 2.15.0 |
|---|---|---|
| uncompressed `acks=1` (2026-08-25) | 6.17M rec/s | 4.94M rec/s |
| uncompressed `acks=1` (2026-08-24) | 7.28M rec/s | 3.88M rec/s |
| lz4 | 6.81M rec/s | 6.05M rec/s |
| idempotent (`acks=all`) | 7.16M rec/s | 3.13M rec/s |
| TLS | 7.42M rec/s | 1.52M rec/s |
| SCRAM-SHA-256 | 6.81M rec/s | 3.98M rec/s |
| SCRAM-SHA-512 | 6.89M rec/s | 3.43M rec/s |
| OAUTHBEARER | 6.82M rec/s | 3.64M rec/s |

this-VM 2026-08-28, **unsigned**:

| | partitionline median | rdkafka 0.39.0 median |
|---|---|---|
| Fetch 8e6 × 100B | 5.28M rec/s | 0.90M rec/s |
| Produce-ack p50 / p99 | 62 µs / 95 µs | 58 µs / 90 µs |

The latency row is not a win. rust-rdkafka fetch `poll` returns one
record from an internal queue, so fetch-request latency was measured on
partitionline only (median p50 121 µs / p99 751 µs).

## Reproduce

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 KAFKA_TOPIC=plbench \
  cargo run --release --example bench_produce

COUNT=8000000 MAX_WAIT_MS=100 MAX_BYTES=16777216 MIN_BYTES=1 KAFKA_TOPIC=plbench \
  cargo run --release --example bench_fetch

COUNT=10000 WARMUP=1000 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=0 \
  MODE=both KAFKA_TOPIC=pllat \
  cargo run --release --example bench_latency
```

C produce bar (Lab A). Build `rdkafka_performance` from the v2.15.0 tag.
Do not use rust-rdkafka as the C bar.

```
rdkafka_performance -P -t plbench -s 100 -c 8000000 -b 127.0.0.1:9092 -a 1 -q \
  -X linger.ms=5 -X compression.codec=none \
  -X batch.num.messages=32768 -X batch.size=1000000 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true
```

TLS / SASL / lz4 / idempotent command lines are in
[benchmark-runs.md](benchmark-runs.md).
