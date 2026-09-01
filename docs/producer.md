# Producer

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

## Send paths

| Method | When to use |
|---|---|
| `send` | one record; waits for the offset |
| `send_all` | many records; queues then waits for every offset |
| `try_send` + `flush` | throughput. `try_send` Ok means queued, not acked |

`flush` waits for in-flight Produce responses and returns the first broker
error. `close` / `close_timeout` flush then drop connections.

Until topic metadata is cached, `try_send` returns `QueueFull` and `send` /
`send_all` wait (capped by `max.block.ms`).

## Partitioning

The partition is chosen **before** the record is queued: murmur2 if there is
a key, round-robin if not. Override with `ProducerConfig::partitioner`.

Each TCP worker owns `partition % connections`. Idempotent sequences for a
partition never share a socket with another worker.

## Compression

`gzip` (`flate2` rust backend), `snappy` (`snap`; snappy-java framing on
produce, raw snappy on fetch), `lz4` (`lz4_flex`). **zstd is not
implemented** (Kafka-world zstd is typically C `libzstd`).

## Idempotence and transactions

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{Producer, ProducerConfig};

let producer = Producer::new(
    ProducerConfig::bootstrap(["127.0.0.1:9092"])
        .idempotent(true),
).await?;
# let _ = producer;
# Ok(())
# }
```

Idempotence uses `InitProducerId`, per-partition sequences, `acks=all`, and
caps in-flight at 5. `flush` fails on a broker produce error.

Transactions: set `transactional.id`, then `init_transactions` (a no-op
after connect when the id is set), `begin_transaction`,
`commit_transaction` / `abort_transaction`.
`send_offsets_to_transaction` / `send_offsets_with_metadata` /
`send_offsets_for_group` take `TopicPartition`.

Produce is **v3–v12**. v13+ topic IDs are not spoken.

## Also on `Producer`

`partitions_for`, `metrics`, `client_instance_id`. Config:
[config.md](config.md). How the queue works: [design.md](design.md).
