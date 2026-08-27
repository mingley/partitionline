# partitionline

A Kafka client written in Rust. It does not call into C or librdkafka.

[![ci](https://github.com/mingley/partitionline/actions/workflows/ci.yml/badge.svg)](https://github.com/mingley/partitionline/actions/workflows/ci.yml)

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline" }
```

Produce, fetch, classic groups (range/sticky) and KIP-848
`ConsumerGroup::join_consumer`, share groups (KIP-932 `ShareGroup`),
ListOffsets/seek, gzip / snappy / lz4, SASL PLAIN / SCRAM-SHA-256 /
SCRAM-SHA-512 / OAUTHBEARER (unsecured JWT or OIDC `http://` and `https://`
token URLs), TLS (`rustls`, no OpenSSL), idempotent and transactional produce,
fetch-from-follower (`ConsumerConfig.rack`), OffsetForLeaderEpoch fencing,
admin (topics, partitions, configs, ACLs, DeleteRecords, DescribeCluster).
Talks Kafka 3.x / 4.x.

**Not a drop-in for `rd_kafka_*`.** Still missing vs librdkafka: zstd and
Kerberos (both blocked on C libraries in default features), Schema Registry.
Full list: [docs/gaps.md](docs/gaps.md).

Locked produce and fetch vs librdkafka 2.15.0 C on this machine (broker
high watermark / records consumed equal records sent). Latency was not
measured. Numbers: [docs/benchmark.md](docs/benchmark.md).

| Locked 8e6 × 100B | partitionline median | C 2.15.0 median |
|---|---|---|
| uncompressed, `acks=1` (2026-08-25) | 6.17M rec/s | 4.94M rec/s |
| uncompressed, `acks=1` (2026-08-24) | 7.28M rec/s | 3.88M rec/s |
| lz4 | 6.81M rec/s | 6.05M rec/s |
| idempotent (`acks=all`) | 7.16M rec/s | 3.13M rec/s |
| TLS / SSL | 7.42M rec/s | 1.52M rec/s |
| SASL SCRAM-SHA-256 | 6.81M rec/s | 3.98M rec/s |
| SASL SCRAM-SHA-512 | 6.89M rec/s | 3.43M rec/s |
| SASL OAUTHBEARER | 6.82M rec/s | 3.64M rec/s |
| fetch (consume, same 8e6×100B log) | 4.38M rec/s | 3.12M rec/s |

## Demo

Broker on `127.0.0.1:9092` (Docker `apache/kafka:3.9.1` is enough):

```
cargo run --release --example roundtrip
```

Locked produce vs librdkafka 2.15.0 C (linger 5ms, 8e6×100B). Do not publish
rec/s unless broker high watermark equals records sent:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 KAFKA_TOPIC=plbench \
  cargo run --release --example bench_produce
```

C bar and fetch bench: [docs/benchmark.md](docs/benchmark.md).

## Example

```rust,no_run
use partitionline::{
    Admin, Consumer, ConsumerConfig, NewTopic, ProduceRecord, Producer, ShareGroup,
};

# async fn example() -> partitionline::Result<()> {
let mut admin = Admin::connect("127.0.0.1:9092").await?;
admin
    .create_topics(&[NewTopic::new("topic", 1, 1)], 10_000, false)
    .await?;

let producer = Producer::connect("127.0.0.1:9092").await?;
let md = producer
    .send(ProduceRecord::to("topic").value(&b"hello"[..]))
    .await?;
println!("wrote {}-{}@{}", md.topic, md.partition, md.offset);
producer.close().await?;

let mut consumer = Consumer::connect("127.0.0.1:9092").await?;
consumer.assign("topic", md.partition, md.offset).await?;
let recs = consumer.fetch().await?;
# let _ = recs;

let mut share = ShareGroup::join(
    ConsumerConfig::bootstrap(["127.0.0.1:9092"]),
    "share-group",
    "topic",
)
.await?;
let acquired = share.poll().await?;
share.accept(&acquired).await?;
share.leave().await?;
# Ok(())
# }
```

More: `examples/produce.rs`, `examples/roundtrip.rs`.

## License

MIT OR Apache-2.0
