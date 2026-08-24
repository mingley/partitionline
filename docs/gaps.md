# Gaps vs librdkafka

This is the tracker for **full client parity with librdkafka**. It is not a
promise that every row ships in the next commit. Status is the contract:

| Status | Meaning |
|---|---|
| **done** | Callers can use it in this crate today |
| **in progress** | Work has started in-tree |
| **not started** | Tracked, not implemented |
| **blocked on C** | Typical Kafka ecosystem implementation is a C library; default features will not take that dependency |

librdkafka is the C client. Matching it means matching **behavior a Kafka
application needs**, not cloning `rd_kafka_*` symbols.

## Inventory

| Capability | partitionline | librdkafka | Status |
|---|---|---|---|
| Produce (acks, linger, batches, offsets) | yes | yes | **done** |
| Fetch with manual assignment | yes | yes | **done** |
| Classic consumer groups (join / sync / heartbeat / commit) | yes | yes | **done** |
| gzip | yes (`flate2` rust backend) | yes | **done** |
| snappy | yes (`snap`, snappy-java framing on produce; raw snappy on fetch) | yes | **done** |
| SASL PLAIN | yes | yes | **done** |
| SASL SCRAM-SHA-256 | yes (RFC 5802/7677, PBKDF2-HMAC-SHA-256, no C SASL library) | yes | **done** |
| Java murmur2 partitioner | yes | optional (`murmur2`) | **done** |
| TLS / SSL | yes (`rustls` + `ring`, no OpenSSL; custom CA PEM or webpki-roots; optional mTLS) | yes (OpenSSL) | **done** |
| lz4 | yes (`lz4_flex` frame, independent 64KiB blocks, magic ≥ 1) | yes | **done** |
| zstd | no | yes (libzstd C) | **blocked on C** |
| SASL SCRAM-SHA-512 | no | yes | **not started** |
| SASL GSSAPI / Kerberos | no | yes (cyrus-sasl C) | **blocked on C** |
| SASL OAUTHBEARER / OIDC | no | yes | **not started** |
| Idempotent produce (`enable.idempotence`, PID/epoch/seq) | yes (`InitProducerId` v1, per-partition sequences, one TCP conn per partition, acks=all, max in-flight 5; `flush` fails on broker error) | yes | **done** |
| Transactions / EOS | no | yes | **not started** |
| Admin APIs (CreateTopics, DeleteTopics, ACLs, configs, …) | no | yes | **not started** |
| KIP-848 next-gen consumer groups | no | yes (newer releases) | **not started** |
| Fetch from follower / rack awareness | no | yes | **not started** |
| Share groups | no | yes | **not started** |
| Schema Registry | no | via extras | **not started** (out of scope) |

TLS produce vs C **was measured** on a dedicated `apache/kafka:3.9.1` SSL
listener (`localhost:9093`). SCRAM-SHA-256 produce vs C **was measured** on a
dedicated SASL_PLAINTEXT listener (`localhost:9095`, admin PLAINTEXT
`localhost:9096`). See `docs/benchmark.md`. Mock produce over SCRAM is
`sasl_scram_sha256_then_produce` in `tests/full_surface.rs`.

## Notes on the C-blocked rows

- **zstd**: Kafka-world zstd is almost always `libzstd` (`zstd-sys`). A default
  feature that links it would violate the no-C-codec rule. Pure-Rust zstd
  exists as research; it is not the Kafka ecosystem default.
- **GSSAPI**: librdkafka talks to Cyrus SASL. No plan to vendor that as a
  default feature.

## Next implementation order

1. Admin: CreateTopics / DeleteTopics / DescribeConfigs as a first slice.
2. SASL SCRAM-SHA-512 (same handshake, SHA-512 / HMAC-SHA-512).
3. Transactions and KIP-848 after the data-plane rows above.

## What “done” on this list does *not* mean

It does not mean a drop-in `rd_kafka_*` C API, rust-rdkafka types, or beating
C on fetch. Those are separate.
