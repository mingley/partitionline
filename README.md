# partitionline

A Kafka client written in Rust. It does not call into C or librdkafka.

[![ci](https://github.com/mingley/partitionline/actions/workflows/ci.yml/badge.svg)](https://github.com/mingley/partitionline/actions/workflows/ci.yml)

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline" }
```

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

`fetch` / group `poll` return `ConsumerRecords` (Java `count` /
`partitions` / `records` / `nextOffsets`). Share `poll` returns `ShareRecords`.
`assign_topic` assigns every partition. `seek` / `seek_to` /
`seek_to_beginning` / `seek_to_end` / `seek_to_beginning_of` /
`seek_to_end_of` move the next fetch offset. `pause` /
`resume` skip partitions without dropping the assignment. `fetch` talks to every
partition leader at once when there is more than one. `partitions_for`
returns Metadata (leader, replicas, ISR, offline replicas, leader epoch) on both `Consumer` and
`Producer`. `beginning_offsets` / `end_offsets` take `TopicPartition`.
`list_offset` is ListOffsets for one partition.
`commit_offsets` takes `TopicPartition` (or anything that converts to one)
plus the next fetch offset. `assignment` is Java `assignment` (`positions`
is next fetch offset). `Admin::delete_records` / `describe_producers` /
`delete_offsets` take `TopicPartition`. `PartitionReassignment::assign`
takes `TopicPartition`. `send_offsets_to_transaction` takes
`TopicPartition`. `AclBinding::allow_topic`, `AclResourceType`,
`AclOperation`, and `AclPermission` cover CreateAcls / DescribeAcls /
DeleteAcls. `ConsumerConfig.isolation_level` is `IsolationLevel`.
`ConfigResourceType` / `ScramMechanism` type ListConfigResources and
user SCRAM.

## Groups

Classic range, sticky, cooperative-sticky (KIP-429), KIP-848 (`join_consumer`), and KIP-932 share groups
(`ShareGroup::join` / `join_topics` / `subscribe` / `unsubscribe`).
`join_topics` / `join_sticky_topics` / `join_cooperative_sticky_topics` /
`join_consumer_topics` subscribe to
several topics. Set `group.instance.id` with
`ConsumerConfig::group_instance_id` for static membership.
`ConsumerConfig::auto_offset_reset` is used when the group has no
committed offset (`Earliest` by default, unlike Java's `latest`).
`auto_commit(true)` commits after poll (off by default).
`Producer::metrics` / `Consumer::metrics` / `ShareGroup::metrics` are counter snapshots
plus latency min/mean/max and p50/p99 over the last 1024 samples (produce-ack / fetch round),
and per-topic rows on `ProducerMetrics::topics` / `ConsumerMetrics::topics` /
`ShareMetrics::topics`.
`client_instance_id` is Java `clientInstanceId` (KIP-714) on producer, consumer, group, share, and admin.
`max_poll_interval` is Kafka `max.poll.interval.ms` (default 5 minutes);
the heartbeat thread leaves the group if it is exceeded.
`Consumer::wakeup` / `WakeupHandle` interrupt fetch. Produce and fetch
interceptors are `ProducerConfig::interceptor` / `ConsumerConfig::interceptor`.
`TopicPartition` / `offsets_for_times` match Java `offsetsForTimes`
(`OffsetAndTimestamp::leader_epoch` is Java `getLeaderEpoch`).
`FetchedRecord::leader_epoch` is the record-batch partition leader epoch.
`Admin::create_partitions` takes `NewPartitions` (Java `increaseTo`).
`incremental_alter_configs` / `alter_configs` take `ConfigResource`.
`OffsetAndMetadata` / `commit_with_metadata` send leader epoch and a
metadata string. `commit_with_metadata(recs.next_offsets())` is Java
`commitSync(records.nextOffsets())`. `ProduceRecord::null_header` is a
null header value. `current_lag` is Java `currentLag`. `enforce_rebalance`
rejoins on the next poll. `subscription` is the topic list.
`subscribe` / `unsubscribe` change topics without dropping the handle.
`group_metadata` is Java `ConsumerGroupMetadata`. `list_topics` is cluster
Metadata. `assign_many` / `unassign` replace or drop a manual assignment.
`fetch_timeout` / `poll_timeout` are Java `poll(Duration)`.
`Consumer::close` drops fetch connections; group `close` is `leave`.
`ConsumerGroup::close_timeout` / `ShareGroup::close_timeout` cap `leave`
(Java `close(Duration)`).
`Admin::close` drops the admin connection. Interceptors have `close`;
consumer interceptors also see `on_commit`.
`Producer::init_transactions` is a no-op after connect when
`transactional.id` is set. `flush_timeout` caps `flush`.
`close_timeout` is Java `close(Duration)` (producer flush, group/share leave).

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

`ProducerConfig::allow_auto_create_topics` is Kafka `allow.auto.create.topics`.
`ProducerConfig::delivery_timeout` is Kafka `delivery.timeout.ms` (default 30s;
Java defaults to 120s). `ProducerConfig::max_block` is Kafka `max.block.ms`
(how long `send` waits for metadata; default 30s, Java 60s).
`ProducerConfig::retry_backoff` / `retry_backoff_max` are Kafka
`retry.backoff.ms` / `retry.backoff.max.ms` (exponential wait after a retriable
Produce; default 100ms / 1s). The same pair on `ConsumerConfig` covers retriable
Fetch (preferred-replica redirects do not wait).
`ProducerConfig::reconnect_backoff` / `reconnect_backoff_max` are Kafka
`reconnect.backoff.ms` / `reconnect.backoff.max.ms` (exponential wait after a
failed broker TCP connect; default 50ms / 1s, same as Java). The same pair is
on `ConsumerConfig` and `AdminConfig`.
`AdminConfig::retry_backoff` / `retry_backoff_max` are Kafka
`retry.backoff.ms` / `retry.backoff.max.ms` on admin RPCs (`NOT_CONTROLLER`,
coordinator moves, retriable IO; default 100ms / 1s).
`ProducerConfig::transaction_timeout` is Kafka `transaction.timeout.ms` on
InitProducerId (default 60s, same as Java).
`ProducerConfig::metadata_max_age` / `ConsumerConfig::metadata_max_age` are
Kafka `metadata.max.age.ms` (default 5 minutes; zero refreshes every lookup).
`connect_timeout` is on the producer, consumer, and admin builders.
TLS is `TlsConfig` on the same builders (`rustls`, no OpenSSL). Admin, gzip /
snappy / lz4, idempotent and transactional produce, fetch-from-follower, and
the Kafka 3.x / 4.x admin APIs are in the crate rustdoc. Produce partitioning
is murmur2 / round-robin; override it with `ProducerConfig::partitioner`.

**Not a drop-in for `rd_kafka_*`.** Still missing vs librdkafka: zstd and
Kerberos (C libraries), Schema Registry. Full list:
[docs/gaps.md](docs/gaps.md).

## Demo

Broker on `127.0.0.1:9092` (Docker `apache/kafka:3.9.1` is enough):

```
cargo run --release --example roundtrip
```

Also: `examples/produce.rs`, `examples/consume.rs`, `examples/group.rs`,
`examples/offsets.rs`, `examples/share.rs`, `examples/wakeup.rs`,
`examples/pause.rs`, `examples/metrics.rs`, `examples/cooperative.rs`.

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
