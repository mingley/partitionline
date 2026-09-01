# partitionline

A Kafka client in pure Rust. No C, no librdkafka.

[![ci](https://github.com/mingley/partitionline/actions/workflows/ci.yml/badge.svg)](https://github.com/mingley/partitionline/actions/workflows/ci.yml)

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline" }
```

MSRV is **1.85**. License is MIT OR Apache-2.0.

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

`send` waits for that record's offset. `send_all` queues many then waits.
`try_send` plus `flush` is the throughput path.

More: [docs/producer.md](docs/producer.md)

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

`Consumer` is manual assignment. Groups and share groups are separate types.

More: [docs/consumer.md](docs/consumer.md) · [docs/groups.md](docs/groups.md)

## Configure

Builders, not a bag of strings:

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

TLS is `TlsConfig` (`rustls`, no OpenSSL). Defaults that differ from Java
are listed in [docs/config.md](docs/config.md).

## Examples

Broker on `127.0.0.1:9092` (Docker `apache/kafka:3.9.1` is enough). Topics
are not auto-created unless you set `allow_auto_create_topics`.

```
cargo run --release --example roundtrip
```

| Example | What it does |
|---|---|
| `produce` / `consume` / `roundtrip` | one-shot produce, fetch, both |
| `group` / `cooperative` / `share` | classic, cooperative-sticky, KIP-932 |
| `offsets` / `pause` / `wakeup` / `metrics` | seek, pause, interrupt, counters |
| `bench_produce` / `bench_fetch` / `bench_latency` | locked benches |

## Docs

| | |
|---|---|
| [docs/README.md](docs/README.md) | index |
| [docs/gaps.md](docs/gaps.md) | what this crate does not do |
| [docs/STATUS.md](docs/STATUS.md) | benches, e2e, closed APIs |
| [docs/benchmark.md](docs/benchmark.md) | numbers and how to reproduce |

This is **not** a drop-in for `rd_kafka_*` or rust-rdkafka types.

## Numbers

Locked produce vs librdkafka **2.15.0 C** is Lab A (broker high watermark
equals records sent). Fetch and latency vs rust-rdkafka **0.39.0** are
this-VM 2026-08-28 and **unsigned**. Details: [docs/benchmark.md](docs/benchmark.md).

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

| Fetch 8e6 × 100B (this-VM, unsigned) | partitionline median | rdkafka 0.39.0 median |
|---|---|---|
| consume, same log | 5.28M rec/s | 0.90M rec/s |

| Produce-ack 10k × 100B linger=0 (this-VM, unsigned) | partitionline median | rdkafka 0.39.0 median |
|---|---|---|
| p50 | 62 µs | 58 µs |
| p99 | 95 µs | 90 µs |

The latency row is not a win. Do not treat unsigned this-VM numbers as
Lab A vs C.
