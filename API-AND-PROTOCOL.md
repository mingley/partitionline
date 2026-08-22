# API and protocol (first slice)

Wire types come from [`kafka-protocol` 0.18.0](https://crates.io/crates/kafka-protocol).
We do **not** rewrite them.

```toml
kafka-protocol = { version = "0.18.0", default-features = false, features = ["client"] }
```

Default features pull C `lz4` / `zstd`. Those stay off. See PROTOCOL.md for
the named crate gaps (4.1.0 schema pin, #42, #104, #152/#161).

The public API is **idiomatic Rust** (builders, `async` methods, owned records).
It is not a C `conf` string clone. “Drop-in” means rust-rdkafka’s produce /
subscribe / poll-or-stream / commit / create-topic map 1:1 onto our types, not
that we copy `ClientConfig::set("linger.ms", "50")`.

## Wire (this slice)

| Piece | Notes |
|---|---|
| Size + Message | `int32` length prefix, then header + body. `frame.rs` only. |
| Request headers v0 / v1 / v2 | v2 is flexible (KIP-482 tagged fields). `kafka_protocol::messages::RequestHeader` + `ApiKey::request_header_version`. |
| ApiVersions | KIP-511: flexible **body**, header length must not change. We send v3. |
| Metadata | Cluster + topic/partition leaders. |
| FindCoordinator | Group coordinator lookup (classic groups). |

## Order of work

### 1. Producer (first)

- Record batch **magic v2** via `RecordBatchEncoder` (kafka-protocol).
- **Produce v9–v13** (flexible). Prefer the highest the broker advertises in that range.
- **InitProducerId** when `idempotent = true` (Lab A).
- **acks=1** and **acks=all**.
- **linger + batch** (Lab A: linger 50 ms).
- Partitioners: **hash** (Kafka murmur2 of the key) and **sticky** (null keys).

### 2. Consumer (second)

Classic consumer groups: **JoinGroup / SyncGroup / Heartbeat / LeaveGroup** +
**Fetch v12–v16**. Offset commit/fetch and ListOffsets ride with this slice.

**KIP-848** (consumer group heartbeat / server-side assignment) is a **named
gap**. Types exist in kafka-protocol; we do not silently pretend we speak it.

### 3. Admin (third)

CreateTopics, DeleteTopics, ListOffsets, DescribeConfigs,
IncrementalAlterConfigs, CreatePartitions. Informational for the bench bar.

## Flagged gaps (not silently skipped)

| Gap | Status |
|---|---|
| **PLAINTEXT only** until after the produce bench | TLS/SASL off. rustls later; not required OpenSSL. |
| TLS / SASL PLAIN / SCRAM | Types exist (`SaslHandshake`, `SaslAuthenticate`). Not wired. |
| OAUTH / GSSAPI | Out. |
| Transactions / EOS | InitProducerId is for idempotent produce, not a transactional producer. |
| KIP-714 telemetry | Out. |
| KIP-848 new consumer groups | Named gap. Classic groups only. |
| KIP-932 share groups | Out. |
| Record magic v0 / v1 | kafka-protocol rejects them. We do too. |
| gzip / snappy / zstd **encode** | Lab A is `compression=none`. lz4 encode is `lz4_flex` via the custom hook. gzip/snappy encode exist; zstd encode is incomplete (`ruzstd` is a decoder). |
| Streams keys 88–89 | Absent from kafka-protocol 0.18. We are not a Streams engine. |

## rust-rdkafka map (drop-in meaning)

| rust-rdkafka | partitionline |
|---|---|
| `FutureProducer::send(FutureRecord::to(topic).payload(..).key(..))` | `Producer::send(RecordTo { topic, key, value, .. })` |
| `StreamConsumer::subscribe` | `Consumer::subscribe` (classic group; later) |
| `stream()` / `recv()` | `poll` or `Stream` of records |
| `commit_message` / `commit` | `commit` |
| `AdminClient::create_topics` | `Admin::create_topics` |
