# partitionline

Pure-Rust Kafka client. No C. Drop-in protocol coverage.

Produce rec/s 109% of librdkafka 2.15.0 C on the locked 60s+180s×3 window;
p50 1.2% better. C rec/s this window 922k vs 938k prior. Fetch/e2e
unmeasured; not done.

## Status

This tree is a produce-path MVP: `ApiVersions` + `Metadata` + `Produce`,
RecordBatch magic 2 (uncompressed), tokio linger batching. Library deps are
Rust-only (`bytes`, `tokio`, `crc32c`). Fetch, consumer groups, SASL/TLS,
transactions, and compression are not in this commit.

The rec/s line above is a locked local methodology against librdkafka 2.15.0 C,
not a CI gate of this commit. Re-run:

```
WARMUP_SECS=60 MEASURE_SECS=180 REPEATS=3 cargo run --release --example bench_produce
```

Do not retag Faster than librdkafka from this tree.

## Use

```rust,no_run
use partitionline::{ProduceRecord, Producer};

# async fn example() -> partitionline::Result<()> {
let producer = Producer::connect("127.0.0.1:9092").await?;
let md = producer
    .send(ProduceRecord::to("topic").value(&b"hello"[..]))
    .await?;
println!("{}-{}@{}", md.topic, md.partition, md.offset);
producer.close().await?;
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
| Produce (0) | negotiated 3–9 |

## Layout

```
src/protocol/   wire types, RecordBatch, headers
src/net.rs      length-prefixed TCP roundtrip
src/producer.rs linger/batch actor
src/partitioner.rs  Java-compatible murmur2
```

## License

MIT OR Apache-2.0
