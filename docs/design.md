# partitionline design

## Goal

Pure-Rust Kafka client. No C. Wire-compatible with Apache Kafka brokers.
First ship: produce path that can be compared to librdkafka 2.15.0 C on a
locked 60s warmup + 180s × 3 window.

## Non-goals (this commit)

Admin, transactions/idempotence, TLS, GSSAPI, KIP-848 share groups, Schema
Registry. `zstd`/`lz4-sys` are C and stay out of default features.

## Why not wrap existing crates

- `rdkafka` is librdkafka FFI. Violates No C.
- `kafrust` (0.3.x) is a broad pure-Rust client (~50–140k rec/s on GH runners)
  with feature coverage, not a librdkafka produce-throughput claim.
- `kafka-protocol` is a generated codec. Useful as a test oracle; owning
  RecordBatch encode is the produce hot path.

## Architecture

```
app --try_send--> per-connection worker --batch--> BrokerConn --TCP--> leader
                      |                               |
                 murmur2 / RR                    ApiVersions
                 metadata cache                  Metadata / Produce
```

`Producer` is a cloneable handle over sharded worker queues. Partition is
assigned on enqueue when metadata is cached. Each worker pipelines
Produce requests (`max_in_flight`). Record payload is copied once into the
RecordBatch buffer; CRC32-C is computed over attributes..end.

## Protocol pitfalls that are load-bearing

- Request `ClientId` is always a classic nullable string, including flexible
  header v2 (`flexibleVersions: none` on that field).
- ApiVersions **response** header is never flexible (KIP-482). Body v3 is.
- Produce throttle_time is *after* the topic array. Metadata throttle is first.
- RecordBatch magic 2 CRC is CRC32-C (Castagnoli) of bytes from attributes
  to end, stored as big-endian u32.
- Record key/value lengths are zigzag varints; compact protocol lengths are
  unsigned varints of `n+1` (0 = null).

## Versions

Negotiate the highest mutually supported version in:

- Produce 3–8 (classic records bytes; skip flexible v9+)
- Metadata 1–12 (skip 13 top-level error / 10–11 unimplemented topic ids)
- Fetch 4–11, group APIs for join/heartbeat/commit, SASL PLAIN, gzip via flate2 `rust_backend`

## Bench contract

Same linger (5ms), batch.num.messages (10000), batch.size (1MB), acks=1,
compression=none, payload size, and broker as the C line. Numbers in the
README are not a win tag.
