# partitionline

A Kafka client written in Rust. It does not call into C or librdkafka.

[![ci](https://github.com/mingley/partitionline/actions/workflows/ci.yml/badge.svg)](https://github.com/mingley/partitionline/actions/workflows/ci.yml)

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline" }
```

Send and fetch records, join a consumer group, gzip, snappy, lz4, SASL PLAIN,
idempotent produce. Plain TCP to Kafka 3.x / 4.x.

- What is still missing vs librdkafka: [docs/gaps.md](docs/gaps.md)
- Produce speed vs the C client: [docs/benchmark.md](docs/benchmark.md)

## Example

```rust,no_run
use partitionline::{Consumer, ProduceRecord, Producer};

# async fn example() -> partitionline::Result<()> {
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
# Ok(())
# }
```

More: `examples/produce.rs`, `examples/roundtrip.rs`.

## License

MIT OR Apache-2.0
