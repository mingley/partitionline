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

`assign_topic` assigns every partition. `seek` / `seek_to_beginning` /
`seek_to_end` move the next fetch offset. `fetch` talks to every
partition leader at once when there is more than one.

## Groups

Classic range, sticky, KIP-848 (`join_consumer`), and KIP-932 share groups.
`join_topics` / `join_sticky_topics` / `join_consumer_topics` subscribe to
several topics. Set `group.instance.id` with
`ConsumerConfig::group_instance_id` for static membership.

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
group.commit().await?;
group.leave().await?;
# let _ = recs;
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

TLS is `TlsConfig` on the same builders (`rustls`, no OpenSSL). Admin, gzip /
snappy / lz4, idempotent and transactional produce, fetch-from-follower, and
the Kafka 3.x / 4.x admin APIs are in the crate rustdoc.

**Not a drop-in for `rd_kafka_*`.** Still missing vs librdkafka: zstd and
Kerberos (C libraries), Schema Registry. Full list:
[docs/gaps.md](docs/gaps.md).

## Demo

Broker on `127.0.0.1:9092` (Docker `apache/kafka:3.9.1` is enough):

```
cargo run --release --example roundtrip
```

Also: `examples/produce.rs`, `examples/consume.rs`, `examples/group.rs`.

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
