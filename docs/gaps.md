# Gaps

What this crate does not do. Implemented client APIs are in rustdoc and
[protocol.md](protocol.md).

This is **not** a drop-in for `rd_kafka_*` or rust-rdkafka types.

## vs librdkafka

| Capability | Status |
|---|---|
| zstd | **blocked** — Kafka-world zstd is almost always `libzstd`. A default feature that links it would violate the no-C-codec rule. |
| SASL GSSAPI / Kerberos | **blocked** — librdkafka talks to Cyrus SASL. No plan to vendor that as a default feature. |
| Schema Registry | **out of scope** — not a protocol client concern. |

Everything else on the usual client checklist is in-tree: produce, fetch,
classic / sticky / cooperative-sticky / KIP-848 groups, share groups,
gzip / snappy / lz4, TLS (`rustls`), SASL PLAIN / SCRAM / OAUTHBEARER /
OIDC, idempotence, transactions, and the Kafka 4.0 admin surface except
the rows below.

## Client APIs not spoken

These stay closed on purpose (controller / KRaft / not a 4.0 client
Admin method this crate maps):

| API | Key | Notes |
|---|---|---|
| ElectLeaders | 43 | Java `Admin.electLeaders` |
| DescribeLogDirs v5 | 35 | this crate speaks v1–v4 |
| DescribeQuorum | 55 | KRaft |
| Add / Remove / UpdateRaftVoter | 80–82 | Kafka 4.1 raft admin |
| Share-group state persister | 83–87 | broker-side, not the client ShareGroup API |

Broker-only keys (LeaderAndIsr, Vote, Envelope, FetchSnapshot, broker
registration, …) are named for header `Display` only.

Later protocol versions on APIs we already speak (Produce v13 topic IDs,
Fetch v18 HighWatermark, InitProducerId v6 2PC, OffsetFetch v10 topic
IDs, …) are not spoken. The crate tracks Kafka **4.0** client layouts
plus 4.1 share-offset keys 90–92.

## Tests and benches

See [STATUS.md](STATUS.md). Short version: CI is the in-tree mock broker.
Fetch and latency vs C 2.15.0 are not a signed Lab A result.
