# partitionline

A pure-Rust Apache Kafka client and protocol implementation.

[![ci](https://github.com/mingley/partitionline/actions/workflows/ci.yml/badge.svg)](https://github.com/mingley/partitionline/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/partitionline.svg)](https://crates.io/crates/partitionline)
[![docs.rs](https://docs.rs/partitionline/badge.svg)](https://docs.rs/partitionline)

```toml
[dependencies]
partitionline = "0.1"
```

**Status:** partitionline 0.1.0 is on [crates.io](https://crates.io/crates/partitionline) (`partitionline = "0.1"`). Probe: `bash scripts/check-installable.sh`.

## Produce

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{ProduceRecord, Producer};

let producer = Producer::connect("127.0.0.1:9092").await?;
let md = producer
    .send(ProduceRecord::to("events").value(&b"hello"[..]))
    .await?;
println!("{}-{}@{}", md.topic, md.partition, md.offset);
producer.close().await?;
# Ok(())
# }
```

`send` waits for that record's offset. For many records, `send_all` queues
then waits; `try_send` plus `flush` is the throughput path.

## Fetch

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::Consumer;

let mut consumer = Consumer::connect("127.0.0.1:9092").await?;
consumer.assign("events", 0, 0).await?;
let recs = consumer.fetch().await?;
# let _ = recs;
# Ok(())
# }
```

`fetch` / group `poll` return `ConsumerRecords` (Java `count` / `partitions` /
`records` / `nextOffsets`). Share `poll` returns `ShareRecords`.
`assign_topic` assigns every partition. `seek` / `pause` / `resume` /
`wakeup` match the Java consumer. Fetch talks to every partition leader;
fenced partitions recover with OffsetForLeaderEpoch.

## Groups

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{ConsumerConfig, ConsumerGroup};

let mut group = ConsumerGroup::join_topics(
    ConsumerConfig::bootstrap(["127.0.0.1:9092"]),
    "workers",
    ["orders", "payments"],
)
.await?;
let recs = group.poll().await?;
group.commit_with_metadata(recs.next_offsets()).await?;
group.leave().await?;
# Ok(())
# }
```

Classic range, sticky, cooperative-sticky (KIP-429), KIP-848
(`join_consumer`), and KIP-932 share groups (`ShareGroup::join` / `poll` /
`accept` / `release` / `reject`). `auto_offset_reset` is used when the group
has no committed offset (`Earliest` by default, unlike Java's `latest`).

## Transactions

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{ProduceRecord, Producer, ProducerConfig};

let producer = Producer::new(
    ProducerConfig::bootstrap(["127.0.0.1:9092"]).transactional_id("orders-txn"),
)
.await?;
producer.init_transactions().await?;
producer.begin_transaction().await?;
producer
    .send(ProduceRecord::to("events").value(&b"hello"[..]))
    .await?;
producer.commit_transaction().await?;
producer.close().await?;
# Ok(())
# }
```

`transactional.id` implies idempotence. `send_offsets_to_transaction` takes
`TopicPartition`. `send_offsets_for_group` uses the group's
`ConsumerGroupMetadata` (see `examples/eos.rs`).

## Admin

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{Admin, NewTopic};

let mut admin = Admin::connect("127.0.0.1:9092").await?;
admin
    .create_topics(&[NewTopic::new("events", 1, 1)], 30_000, false)
    .await?;
let topics = admin.list_topics_with(false).await?;
# let _ = topics;
admin.close().await?;
# Ok(())
# }
```

CreateTopics, DeleteTopics, DescribeConfigs, IncrementalAlterConfigs, ACLs,
groups, transactions, log dirs, quotas, and the other Kafka 3.x / 4.x admin
APIs are on `Admin`. Method-level rustdoc names the matching Java
`Admin` / `*Options` calls.

## Configure

Builders, not a property bag of strings:

```rust,no_run
use std::time::Duration;
use partitionline::{Acks, Compression, IsolationLevel, ProducerConfig, Sasl};

let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
    .acks(Acks::All)
    .linger(Duration::from_millis(5))
    .compression(Compression::Lz4)
    .sasl(Sasl::scram_sha256("alice", "secret"));
let _iso = IsolationLevel::ReadCommitted;
```

TLS is `TlsConfig` (`rustls`, no OpenSSL). SASL PLAIN, SCRAM-SHA-256/512,
and OAUTHBEARER (including OIDC) are supported. Compression is gzip, snappy,
and lz4. Produce partitioning is murmur2 / round-robin; override it with
`ProducerConfig::partitioner`.

Defaults that differ from Java:

- `auto.offset.reset` is `Earliest` (Java `latest`)
- `allow.auto.create.topics` is `false` (Java consumer `true`)
- `delivery.timeout.ms` is 30s (Java 120s)
- `max.block.ms` is 30s (Java 60s)

`buffer.memory` (32 MiB) and `max.request.size` (1 MiB) match Java.
`retry.backoff.ms` / `reconnect.backoff.ms` / `connections.max.idle.ms` /
`metadata.max.age.ms` / `transaction.timeout.ms` match Java.

The crate rustdoc is the full Java-shaped API catalog (protocol helpers,
version ranges, and option names). Operator guide: [docs/guide.md](docs/guide.md).
Migrate from rust-rdkafka: [docs/migrate-from-rdkafka.md](docs/migrate-from-rdkafka.md).
Adoption / pilot checklist: [docs/ADOPTION.md](docs/ADOPTION.md).
Capability list vs librdkafka: [docs/gaps.md](docs/gaps.md).
Security: [docs/security.md](docs/security.md).
Release policy: [docs/RELEASE.md](docs/RELEASE.md).
Roadmap: [docs/CIVILIZATION.md](docs/CIVILIZATION.md).

**Not a drop-in for `rd_kafka_*`.** Still missing vs librdkafka: zstd and
Kerberos (C libraries), Schema Registry.

## Features vs C stack

| Capability | partitionline (default) | librdkafka |
|---|---|---|
| Produce / fetch / groups / EOS / admin / share | yes | yes |
| gzip / snappy / lz4 | yes (pure Rust) | yes (often C) |
| TLS | rustls (no OpenSSL) | OpenSSL |
| SASL PLAIN / SCRAM / OAUTHBEARER / OIDC | yes (pure Rust) | yes |
| zstd | no (see [docs/zstd-spike.md](docs/zstd-spike.md)) | yes (`libzstd`) |
| Kerberos / GSSAPI | no | yes (Cyrus) |
| Optional `tracing` spans | feature `tracing` | n/a |

## Examples

Broker on `127.0.0.1:9092` (Docker `apache/kafka:3.9.1` is enough):

```
cargo run --release --example roundtrip
```

Also: `produce`, `consume`, `group`, `txn`, `admin`, `sasl`, `oauth`, `tls`, `eos`,
`offsets`, `share`, `wakeup`, `pause`, `metrics`, `cooperative`, `intercept`,
`consume_intercept`.

Locked produce vs librdkafka 2.15.0 C (linger 5ms, 8e6×100B). Do not publish
rec/s unless broker high watermark equals records sent:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 KAFKA_TOPIC=plbench \
  cargo run --release --example bench_produce
```

Produce C bar (Lab A), fetch writeup (this-VM, unsigned), and latency
writeup (this-VM, unsigned): [docs/benchmark.md](docs/benchmark.md).
Suite HOLD: [docs/STATUS.md](docs/STATUS.md).

## Numbers

Locked produce vs librdkafka 2.15.0 C is Lab A (broker high watermark
equals records sent). Latency writeup is this-VM 2026-08-28, **unsigned**.

| Locked 8e6 × 100B produce (Lab A) | partitionline median | C 2.15.0 median |
|---|---|---|
| uncompressed, `acks=1` (2026-08-25) | 6.17M rec/s | 4.94M rec/s |
| uncompressed, `acks=1` (2026-08-24) | 7.28M rec/s | 3.88M rec/s |
| lz4 | 6.81M rec/s | 6.05M rec/s |
| idempotent (`acks=all`) | 7.16M rec/s | 3.13M rec/s |
| TLS / SSL | 7.42M rec/s | 1.52M rec/s |
| SASL SCRAM-SHA-256 | 6.81M rec/s | 3.98M rec/s |
| SASL SCRAM-SHA-512 | 6.89M rec/s | 3.43M rec/s |
| SASL OAUTHBEARER | 6.82M rec/s | 3.64M rec/s |

Fetch writeup **2026-08-28 this-VM** (Apache Kafka 3.9.1 KRaft; consumed
equals sent, HW 8e6). Comparison is rust-rdkafka **0.39.0**
`BaseConsumer::poll` (bundled librdkafka 2.12.1), not Lab A and not C
2.15.0. **Unsigned** until Kernel Integrity signs. Not a Suite HOLD lift.

| Fetch 8e6 × 100B (this-VM, unsigned) | partitionline median | rdkafka 0.39.0 median |
|---|---|---|
| consume, same log | 5.28M rec/s | 0.90M rec/s |

Latency writeup **2026-08-28 this-VM** (Apache Kafka 3.9.1 KRaft).
Sequential produce-ack, linger 0, 10k × 100 B, 1 partition, HW 11,000
(1k warmup + 10k timed). Comparison is rust-rdkafka **0.39.0**
`FutureProducer` (bundled librdkafka 2.12.1), not Lab A and not C 2.15.0.
**Unsigned** until Kernel Integrity signs. Not a Suite HOLD lift. Not a
"faster than librdkafka" claim: this-VM p50/p99 medians are not a win.

| Produce-ack 10k × 100B linger=0 (this-VM, unsigned) | partitionline median | rdkafka 0.39.0 median |
|---|---|---|
| p50 | 62 µs | 58 µs |
| p99 | 95 µs | 90 µs |

```
COUNT=10000 WARMUP=1000 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=0 \
  MODE=both KAFKA_TOPIC=pllat \
  cargo run --release --example bench_latency
```

## License

MIT OR Apache-2.0
