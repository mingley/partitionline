# Produce-path MVP

> Implemented in this branch. Task list is the shipped surface, not a backlog.

**Goal:** Working PLAINTEXT produce client with tests against a mock broker.

**Architecture:** Hand-rolled protocol + tokio producer actor + murmur2.

**Tech stack:** Rust 1.85+, tokio, bytes, crc32c.

## Global constraints

- No C in the library (no librdkafka, no libzstd, no lz4-sys).
- Do not restore a Faster-than-librdkafka README slogan.
- Async/tokio-native. No `spawn_blocking` on the produce path.

## Shipped

- [x] Protocol primitives (unsigned varint, zigzag, compact vs classic)
- [x] Request/response headers including ApiVersions KIP-482
- [x] RecordBatch magic 2 encode/decode + CRC32-C
- [x] ApiVersions v3, Metadata v4/v9/v12, Produce v3/v9
- [x] Producer linger/batch actor
- [x] Mock-broker integration test
- [x] `produce` and `bench_produce` examples
- [x] CI fmt/clippy/test
