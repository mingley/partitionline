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
| Produce (acks, linger, batches, offsets) | yes (to Metadata leader; retriable errors refresh and retry) | yes | **done** |
| Fetch with manual assignment | yes (to Metadata leader; retriable errors refresh and retry; Fetch sends Metadata `leader_epoch`) | yes | **done** |
| OffsetForLeaderEpoch / fetch fencing | yes (api 23; `FENCED_LEADER_EPOCH` / `UNKNOWN_LEADER_EPOCH` recover then fetch) | yes | **done** |
| ListOffsets, seek, `isolation.level` | yes (earliest/latest/timestamp; Fetch isolation 0 or 1) | yes | **done** |
| Classic consumer groups (join / sync / heartbeat / commit) | yes (range then sticky over all partitions; heartbeat loop; rebalance; LeaveGroup) | yes | **done** |
| gzip | yes (`flate2` rust backend) | yes | **done** |
| snappy | yes (`snap`, snappy-java framing on produce; raw snappy on fetch) | yes | **done** |
| SASL PLAIN | yes | yes | **done** |
| SASL SCRAM-SHA-256 | yes (RFC 5802/7677, PBKDF2-HMAC-SHA-256, no C SASL library) | yes | **done** |
| Java murmur2 partitioner | yes | optional (`murmur2`) | **done** |
| TLS / SSL | yes (`rustls` + `ring`, no OpenSSL; custom CA PEM or webpki-roots; optional mTLS) | yes (OpenSSL) | **done** |
| lz4 | yes (`lz4_flex` frame, independent 64KiB blocks, magic ≥ 1) | yes | **done** |
| zstd | no | yes (libzstd C) | **blocked on C** |
| SASL SCRAM-SHA-512 | yes (RFC 5802, PBKDF2-HMAC-SHA-512, no C SASL library) | yes | **done** |
| SASL GSSAPI / Kerberos | no | yes (cyrus-sasl C) | **blocked on C** |
| SASL OAUTHBEARER | yes (RFC 7628, unsecured JWT `alg=none`, matches librdkafka `enable.sasl.oauthbearer.unsecure.jwt`) | yes | **done** |
| SASL OIDC (token endpoint) | yes (RFC 6749 client_credentials `http://` or `https://` rustls POST, then OAUTHBEARER) | yes | **done** |
| Idempotent produce (`enable.idempotence`, PID/epoch/seq) | yes (`InitProducerId` v1, per-partition sequences, one TCP conn per partition, acks=all, max in-flight 5; `flush` fails on broker error) | yes | **done** |
| Transactions / EOS | yes (`transactional.id`, begin/commit/abort, AddPartitionsToTxn / AddOffsetsToTxn / EndTxn / TxnOffsetCommit) | yes | **done** |
| Admin: CreateTopics, DeleteTopics, DescribeConfigs | yes (classic CreateTopics v0–4, DeleteTopics v0–3, DescribeConfigs v0–1) | yes | **done** |
| Admin: IncrementalAlterConfigs, CreatePartitions, ACLs, OffsetDelete, AlterPartitionReassignments, ListPartitionReassignments, UpdateFeatures, AlterUserScramCredentials, DescribeUserScramCredentials, AlterClientQuotas, DescribeClientQuotas, AllocateProducerIds, DescribeTransactions, ListTransactions, UnregisterBroker | yes | yes | **done** |
| Admin: AlterConfigs, DeleteRecords, DescribeCluster | yes (legacy AlterConfigs 33, DeleteRecords 21, DescribeCluster 60) | yes | **done** |
| KIP-848 next-gen consumer groups | yes (`ConsumerGroup::join_consumer`, ConsumerGroupHeartbeat api 68; classic Join/Sync still work) | yes (newer releases) | **done** |
| Fetch from follower / rack awareness | yes (`ConsumerConfig.rack`; follow Fetch `preferred_read_replica`) | yes | **done** |
| Share groups | yes (`ShareGroup::join` / `poll` / `accept` / `release` / `leave`; ShareGroupHeartbeat 76, ShareFetch 78, ShareAcknowledge 79; ACCEPT/RELEASE; queue sharing) | yes | **done** |
| Schema Registry | no | via extras | **not started** (out of scope) |

TLS produce vs C **was measured** on a dedicated `apache/kafka:3.9.1` SSL
listener (`localhost:9093`). SCRAM-SHA-256 and SCRAM-SHA-512 produce vs C
**were measured** on SASL_PLAINTEXT `localhost:9095` (admin `localhost:9096`).
OAUTHBEARER produce vs C **was measured** on SASL_PLAINTEXT `localhost:9097`
(admin `localhost:9098`). Fetch vs C **was measured** on PLAINTEXT `localhost:9092`
(`examples/bench_fetch.rs` vs `rdkafka_performance -C -p 0..5`). See
`docs/benchmark.md`. Mock produce over OAUTH is `sasl_oauthbearer_then_produce`
in `tests/full_surface.rs`. Mock admin is `admin_create_then_produce_fetch`.

## Notes on the C-blocked rows

- **zstd**: Kafka-world zstd is almost always `libzstd` (`zstd-sys`). A default
  feature that links it would violate the no-C-codec rule. Pure-Rust zstd
  exists as research; it is not the Kafka ecosystem default.
- **GSSAPI**: librdkafka talks to Cyrus SASL. No plan to vendor that as a
  default feature.

## Next implementation order

1. Schema Registry stays out of crate scope. Latency vs C/Java is not claimed.

## What “done” on this list does *not* mean

It does not mean a drop-in `rd_kafka_*` C API or rust-rdkafka types. Fetch vs C
is measured in `docs/benchmark.md`; that is not an e2e latency claim.
