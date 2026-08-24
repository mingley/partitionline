# partitionline

Pure-Rust Kafka client. No C. Drop-in protocol coverage.

[![ci](https://github.com/mingley/partitionline/actions/workflows/ci.yml/badge.svg)](https://github.com/mingley/partitionline/actions/workflows/ci.yml)

Shipped: produce (magic-2 RecordBatch), fetch with manual assignment,
consumer-group join/heartbeat/commit, gzip (`flate2` rust backend), SASL
PLAIN. Default features stay Rust-only (`bytes`, `tokio`, `crc32c`, `flate2`).
TLS, transactions, and admin are not in this tree.

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline" }
```

A local produce rec/s comparison vs librdkafka 2.15.0 C is a locked
methodology, not a CI gate. Re-run:

```
WARMUP_SECS=60 MEASURE_SECS=180 cargo run --release --example bench_produce
```

Do not retag Faster than librdkafka from this tree.

## Use

```rust,no_run
use partitionline::{Consumer, ProduceRecord, Producer};

# async fn example() -> partitionline::Result<()> {
let producer = Producer::connect("127.0.0.1:9092").await?;
let md = producer
    .send(ProduceRecord::to("topic").value(&b"hello"[..]))
    .await?;
println!("{}-{}@{}", md.topic, md.partition, md.offset);
producer.close().await?;

let mut consumer = Consumer::connect("127.0.0.1:9092").await?;
consumer.assign("topic", md.partition, md.offset).await?;
let recs = consumer.fetch().await?;
# let _ = recs;
# Ok(())
# }
```

## Protocol

Hand-rolled codec for the produce path, not a `librdkafka` FFI and not a
wrapper around `kafka-protocol`. Brokers: Kafka 3.x/4.x PLAINTEXT.

| API | client versions |
| --- | --- |
| ApiVersions (18) | send v3 (response header never flexible) |
| Metadata (3) | negotiated 1–12 |
| Produce (0) | negotiated 3–8 |
| Fetch (1) | v11 classic |
| Group (Join/Sync/Heartbeat/OffsetCommit/OffsetFetch/FindCoordinator) | classic versions |
| SASL Handshake / Authenticate | PLAIN |

## Layout

```
src/protocol/   wire types, RecordBatch, fetch, group, SASL
src/net.rs      length-prefixed TCP
src/producer.rs sharded linger workers, pipelined Produce
src/consumer.rs assigned Fetch
src/group.rs    consumer-group membership
src/partitioner.rs  Java-compatible murmur2
```

## License

MIT OR Apache-2.0
