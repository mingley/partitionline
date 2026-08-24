# partitionline

A Kafka client written in Rust. It does not call into C or librdkafka.

[![ci](https://github.com/mingley/partitionline/actions/workflows/ci.yml/badge.svg)](https://github.com/mingley/partitionline/actions/workflows/ci.yml)

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline" }
```

## What works

- Send messages and get back a broker offset
- Read those messages (you pick the partition, or join a consumer group)
- gzip compression
- SASL PLAIN (username + password)
- Kafka 3.x / 4.x over plain TCP

## What does not

This is not a full replacement for librdkafka.

| | partitionline | librdkafka |
|---|---|---|
| Produce | yes | yes |
| Fetch / consume | yes | yes |
| Consumer groups | yes | yes |
| gzip | yes | yes |
| SASL PLAIN | yes | yes |
| TLS | no | yes |
| snappy / lz4 / zstd | no | yes |
| Transactions / exactly-once | no | yes |
| Admin APIs (create topic, ACLs, …) | no | yes |
| Kerberos / GSSAPI | no | yes |
| Schema Registry | no | via extras |

So: **not feature complete** versus the C client. Complete enough to produce, consume, and join a group on a plaintext Kafka cluster.

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

## Are we faster than the C client?

**Produce: yes, on this machine, on this test.** Fetch speed was not measured.

2026-08-24, Apple M4 Pro, local Docker `apache/kafka:3.9.1`, 6 partitions. Each side sent **8 million** 100-byte messages, `acks=1`, `linger.ms=5`, no compression. Fresh topic every run. No warmup. C binary is `rdkafka_performance` from librdkafka **2.15.0**.

| Run | partitionline | librdkafka 2.15.0 C |
|---|---|---|
| 1 | 7.61 million/s | 3.85 million/s |
| 2 | 7.28 million/s | 3.88 million/s |
| 3 | 6.78 million/s | 4.00 million/s |
| **median** | **7.28 million/s** | **3.88 million/s** |

That is about **1.9×** the C tool on this produce test. It is not a claim that every workload is faster, or that fetch/e2e is faster.

Re-run:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 \
  cargo run --release --example bench_produce
```

C side (same flags):

```
rdkafka_performance -P -t plbench -s 100 -c 8000000 -b 127.0.0.1:9092 -a 1 \
  -X linger.ms=5 -X compression.codec=none \
  -X batch.num.messages=32768 -X batch.size=1000000 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true
```

## License

MIT OR Apache-2.0
