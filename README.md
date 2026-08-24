# partitionline

A Kafka client written in Rust. It does not call into C or librdkafka.

[![ci](https://github.com/mingley/partitionline/actions/workflows/ci.yml/badge.svg)](https://github.com/mingley/partitionline/actions/workflows/ci.yml)

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline" }
```

**Not feature-complete vs librdkafka.** Today: produce, fetch, consumer groups,
gzip / snappy / lz4, SASL PLAIN, SASL SCRAM-SHA-256, TLS (`rustls`, no OpenSSL),
idempotent produce. Talks Kafka 3.x / 4.x. Missing: SCRAM-SHA-512, admin,
transactions, zstd, Kerberos. Full list: [docs/gaps.md](docs/gaps.md).

**Produce is faster than librdkafka 2.15.0 C** on this machine, including
idempotent produce, TLS, and SCRAM-SHA-256 (broker high watermark equals
records sent). Fetch was not measured. Numbers: [docs/benchmark.md](docs/benchmark.md).

| Locked produce, 8e6 × 100B, linger 5ms | partitionline median | C 2.15.0 median |
|---|---|---|
| uncompressed, `acks=1` | 7.28M rec/s | 3.88M rec/s |
| lz4 | 6.81M rec/s | 6.05M rec/s |
| idempotent (`acks=all`) | 7.16M rec/s | 3.13M rec/s |
| TLS / SSL | 7.42M rec/s | 1.52M rec/s |
| SASL SCRAM-SHA-256 | 6.81M rec/s | 3.98M rec/s |

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
