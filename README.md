# partitionline

An asynchronous Apache Kafka client with producer, consumer, group, transaction,
and Admin APIs. This repository also contains an experimental
[broker](https://github.com/mingley/partitionline/blob/dfe179d7066e2c1afecb88df5ba76d2920308c46/partitionline-broker/README.md) and a
[Schema Registry companion](https://github.com/mingley/partitionline/blob/a2d0af0a71a1364fefd3c2520dca4b7041d4a2dc/partitionline-schema/README.md).

[![ci](https://github.com/mingley/partitionline/actions/workflows/ci.yml/badge.svg)](https://github.com/mingley/partitionline/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/partitionline.svg)](https://crates.io/crates/partitionline)
[![docs.rs](https://docs.rs/partitionline/badge.svg)](https://docs.rs/partitionline)

```toml
[dependencies]
partitionline = "0.1"
```

Version 0.1.0 is available on [crates.io](https://crates.io/crates/partitionline).
The examples and features below describe the newer repository source; some are
not available in 0.1.0. For the published API, see the
[0.1.0 documentation](https://docs.rs/partitionline/0.1.0/partitionline/).
The published 0.1.0 overview still contains pre-publication installation
instructions and older benchmark figures. To try the current source, clone the
repository and point your application's
dependency at that checkout (adjust the relative path as needed):

```toml
[dependencies]
partitionline = { path = "../partitionline" }
```

The current source is **not yet qualified for production use**: client CI and
Kafka compatibility checks still fail. See the [current status](docs/STATUS.md)
for results and open work, and the [support matrix](docs/support.md) for tested
versions and platforms. Performance rankings have not been established.

Use the latest stable Rust to build the current source. Building the TLS
dependency Ring also needs a C toolchain. The examples assume a running Kafka
broker and existing topics.

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

`send` waits for that record's delivery result. A successful `send_all` returns
the results for multiple records; on error, other accepted records may still be
delivered. `try_send` queues without waiting; `flush` can return before some
retries finish or miss delivery timeouts. Use `send` for
an individual delivery result. With `acks=0`, completion means the request was
written locally; the broker provides no acknowledgement or offset.

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

`fetch` and group `poll` return `ConsumerRecords`, with records grouped by
partition and the next offsets to commit. Share `poll` returns `ShareRecords`.
`assign_topic` assigns every partition. Use `seek`, `pause`, `resume`, and
`wakeup` to control fetching. Fetch talks to every partition leader, and
leader-epoch recovery uses OffsetForLeaderEpoch.

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

`Admin` includes topic, configuration, ACL, group, transaction, log-directory,
and quota operations. Supported versions and remaining gaps are listed in
[protocol support](docs/gaps.md).

## Configure

Use typed configuration builders:

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

Use `TlsConfig` for TLS and optional client certificates. SASL supports PLAIN,
SCRAM-SHA-256/512, OAUTHBEARER, and OIDC token acquisition. Automatic
reauthentication on existing client connections is still in progress; see
[token and session lifecycle](docs/auth-refresh.md). Compression supports
gzip, snappy, and lz4. The default partitioner hashes keys with murmur2 and uses
round-robin for records without keys. Current source also has an opt-in
`StickyPartitioner`, selected with `ProducerConfig::partitioner`; its independent
qualification is still in progress.

Defaults that differ from Java:

- `auto.offset.reset` is `Earliest` (Java `latest`)
- `allow.auto.create.topics` is `false` (Java consumer `true`)
- `delivery.timeout.ms` is 30s (Java 120s)
- `max.block.ms` is 30s (Java 60s)

`buffer.memory` (32 MiB) and `max.request.size` (1 MiB) match Java.
`retry.backoff.ms` / `reconnect.backoff.ms` / `connections.max.idle.ms` /
`metadata.max.age.ms` / `transaction.timeout.ms` match Java.
Authoritative defaults reference: [operator guide](docs/guide.md#defaults-that-differ-from-java).

See the [operator guide](docs/guide.md) for recipes and troubleshooting,
[published API docs](https://docs.rs/partitionline) for methods and options, and the
[migration guide](docs/migrate-from-rdkafka.md) for porting from rust-rdkafka.

## Capabilities

| Area | Support |
|---|---|
| Client APIs | Produce, fetch, groups, transactions, Admin, share groups |
| Compression | gzip, snappy, lz4; optional zstd |
| Security | TLS, mTLS, SASL PLAIN, SCRAM, OAUTHBEARER, OIDC |
| Diagnostics | Metrics snapshots and optional `tracing` spans |
| Schema formats | Separate companion adapters for Protobuf, Avro, and JSON Schema |

Kerberos is not implemented. The companion requires an
application-selected serializer. See [protocol support](docs/gaps.md) for
version ranges and remaining gaps.

## Examples

With a broker on `127.0.0.1:9092` and an existing topic named `partitionline`:

```
cargo run --release --example roundtrip
```

Also: `produce`, `consume`, `group`, `txn`, `admin`, `sasl`, `oauth`, `tls`, `eos`,
`offsets`, `share`, `wakeup`, `pause`, `metrics`, `cooperative`, `intercept`,
`consume_intercept`.

## Performance

The repository includes produce, fetch, and latency benchmarks. For a local
produce run:

```sh
COUNT=100000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 KAFKA_TOPIC=plbench \
  cargo run --release --example bench_produce
```

Create a dedicated topic and verify its record history before comparing results.
This local sample uses seeded 100-byte payloads, 16-byte ID keys, and no
warmup. [Benchmark instructions and results](docs/benchmark.md) describe the
settings and limits of recorded runs. They do not establish a fastest-client
or fastest-broker claim; qualification remains in progress.

## Documentation

The [documentation index](docs/index.md) links to guides, reference material,
benchmarks, and implementation plans.

## License

MIT OR Apache-2.0
