# partitionline

Pure-Rust Kafka client. No C. Drop-in protocol coverage.

Produce rec/s 109% of librdkafka 2.15.0 C on the locked 60s+180s×3 window;
p50 1.2% better. C rec/s this window 922k vs 938k prior. Fetch/e2e
unmeasured; not done.

## Status

Shipped: produce (magic-2 RecordBatch), fetch with manual assignment,
consumer-group join/heartbeat/commit, gzip (`flate2` rust backend), SASL
PLAIN. Library deps stay Rust-only (`bytes`, `tokio`, `crc32c`, `flate2`).
TLS, transactions, and admin are not in this commit.

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
