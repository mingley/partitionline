# Operator guide

Runnable paths assume a broker on `KAFKA_BOOTSTRAP` (default
`127.0.0.1:9092`). Docker `apache/kafka:3.9.1` is enough for local smoke.

```bash
cargo run --release --example roundtrip
```

## Produce

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use std::time::Duration;
use partitionline::{ProduceRecord, Producer, ProducerConfig};

let producer = Producer::new(
    ProducerConfig::bootstrap(["127.0.0.1:9092"])
        .linger(Duration::from_millis(5)),
).await?;
let md = producer
    .send(ProduceRecord::to("events").value(&b"hello"[..]))
    .await?;
println!("{}-{}@{}", md.topic, md.partition, md.offset);
producer.close().await?;
# Ok(())
# }
```

- `send` — one offset future per record.
- `send_all` — queue many, wait for all.
- `try_send` + `flush` — throughput path (see `examples/bench_produce.rs`).

## Fetch (manual assignment)

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

`assign_topic` assigns every partition. `seek` / `pause` / `resume` / `wakeup`
match the Java consumer shapes.

## Consumer groups

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
group.commit_with_metadata(recs.next_offsets()).await?;
group.leave().await?;
# Ok(())
# }
```

Also: sticky / cooperative-sticky, KIP-848 (`join_consumer` /
`join_consumer_topics`), share groups (`ShareGroup::join` / `poll` /
`accept` / `release` / `reject`). Examples: `group`, `cooperative`,
`kip848`, `share`.

KIP-848 (Kafka **4.x** ConsumerGroupHeartbeat) join sends an **empty**
`TopicPartitions` array — brokers reject `null` with
`INVALID_REQUEST` (“must be empty when (re-)joining”). Live smoke:
`examples/kip848.rs` (`REQUIRE_KIP848=1` on 4.x broker smoke).

Share groups (KIP-932) need Kafka **4.1+** with finalized
`share.version=1` (`kafka-features.sh … upgrade --feature share.version=1`).
On 4.0/4.1 also set `group.share.enable=true` until that temporary flag is
removed upstream. Default `share.auto.offset.reset` is **latest** — produce
while the share member is already polling.

## Exactly-once (transactions)

Use `transactional_id`, `init_transactions`, and
`send_offsets_for_group` — see `examples/eos.rs`. Isolation on the consumer
side is `IsolationLevel::ReadCommitted`.

## Admin

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{Admin, NewTopic};

let mut admin = Admin::connect("127.0.0.1:9092").await?;
admin
    .create_topics(&[NewTopic::new("events", 1, 1)], 30_000, false)
    .await?;
admin.close().await?;
# Ok(())
# }
```

## TLS and SASL

```rust,no_run
use partitionline::{ProducerConfig, Sasl, TlsConfig};

let _ = ProducerConfig::bootstrap(["broker:9093"])
    .tls(TlsConfig::default())
    .sasl(Sasl::scram_sha256("alice", "secret"));
```

Examples: `tls`, `sasl`. OIDC is `OidcConfig` on the SASL OAUTHBEARER path.

## Defaults that differ from Java

| Knob | partitionline | Java |
|---|---|---|
| `auto.offset.reset` | Earliest | latest |
| `allow.auto.create.topics` | false | consumer true |
| `delivery.timeout.ms` | 30s | 120s |
| `max.block.ms` | 30s | 60s |

`buffer.memory` (32 MiB) and `max.request.size` (1 MiB) match Java.

## Metrics

`Producer::metrics`, `Consumer::metrics`, `ShareGroup::metrics`, and
`Admin::metrics` return counter snapshots plus latency min/mean/max and
p50/p99 over the last 1024 samples. Scrape on your process interval (for
example every 10–60s); these are process-local snapshots, not a push
protocol. See `examples/metrics.rs`. Optional `tracing` hooks are tracked
in `docs/CIVILIZATION.md` WP-4.2.

### Diagnosis cookbook (queue age)

| Symptom | Telemetry | Likely cause |
|---|---|---|
| Produce acks feel delayed while broker is fine | `Producer::metrics().queue_latency` rising; `bytes_buffered` high | Local linger/backlog — records aging in the client queue before the wire |
| Queue age stays near zero but ack p99 climbs | `queue_latency` flat; `ack_latency` / broker path rising | Broker or network path — not local linger |
| Buffer full / `QueueFull` | `bytes_buffered` at `buffer_memory`; queue_latency may spike | Offer rate above drain; apply backpressure |

`queue_latency` is accept→local-batch drain (linger/queue age). `ack_latency`
is accept→broker ack. Prefer these counters over logging payloads. Mock proof:
`tests/queue_age.rs`. KL-07 Partial — not two independent human diagnosis runs,
and not a Suite HOLD lift. Lag / throttle / reconnect recipes are separate slices.

## Recipes

### Backpressure

Use `try_send`; on `QueueFull` / memory pressure, `flush` or wait, then
retry. Do not unbounded-buffer in the application.

### Buffer ownership and overload (mock)

`ProducerConfig::buffer_memory` is a **key+value** reservation held from accept
until ack or fail — not encoded batch size, socket buffers, or process RSS.
`try_send` returns `QueueFull` when the cap is full; `send` waits up to
`max_block` then returns `Timeout`. `metrics().bytes_buffered` must stay
`≤ buffer_memory` under saturating load and return to `0` after `flush` /
`close`. This is a KL-02 mock honesty slice, not a 2×/24h RSS leadership claim.

Mock coverage: `tests/buffer_ownership.rs`.


### Produce cancellation and shutdown

Once `send` / `try_send` has accepted a record into `buffer_memory`, dropping the
caller future does **not** mean the record was never written. Delivery may still
reach the broker; treat the caller outcome as **ambiguous** until `flush` or
`close` settles it.

| Stage | Typical signals | Caller outcome |
|---|---|---|
| Before enqueue | `QueueFull`, `Timeout`, `RecordTooLarge`, never queued | **Failed** / not accepted |
| Buffered (queued, not yet on the wire) | `metrics().bytes_buffered > 0`; drop `send` future | **Ambiguous** — worker may still deliver |
| After send / before ack | in flight to broker; drop `send` future | **Ambiguous** |
| After broker ack | `Ok(RecordMetadata)` or successful `try_send`+`flush` | **Completed** |
| After `close` / `close_timeout` | further `send`/`try_send` on any clone | **Failed** with `Error::Closed` |

Prefer an explicit `close` (or `close_timeout`) over dropping the last `Producer`
handle: drop alone does not wait for in-flight produce outcomes. Mock coverage:
`tests/produce_cancel.rs`.



### Consumer leave/close and auto-commit

`ConsumerConfig::enable_auto_commit` defaults to **false**. When enabled, offsets
are stored only on the **poll-interval** path (after a successful fetch once the
interval elapses). `leave` / `close` / `unsubscribe` flush queued `commitAsync`
work but do **not** auto-commit positions — so polled-but-unprocessed records are
not silently marked done (KL-02). Prefer `commit` / `commit_with_metadata` (for
example `recs.next_offsets()`) before leave when you need stored offsets.

Mock coverage: `tests/consumer_close_commit.rs`.


### Rebalance

Prefer cooperative-sticky when partitions must move with less stop-the-world
pause (`examples/cooperative.rs`). Handle `on_rebalance` for revoke/assign.

### Exactly-once consume → produce

`examples/eos.rs`: read with `ReadCommitted`, produce inside a transaction,
`send_offsets_for_group`, `commit_transaction`.

## Tracing (optional feature)

Enable spans without changing default builds:

```toml
partitionline = { version = "0.1", features = ["tracing"] }
```

Spans cover `Producer::send` (topic field), `Consumer::fetch`,
`ConsumerGroup::poll`, cooperative rejoin, and transaction
init/begin/commit/abort. Pair with `tracing-subscriber` in the application.

## Integrity / benchmarks

Unsigned Lab A integrity (produce → broker high-watermark == acked → fetch →
consumed == seeded):

```bash
bash scripts/lab-a-integrity.sh          # small default COUNT
COUNT=50000 bash scripts/lab-a-fetch.sh  # fetch-focused
COUNT=8000000 PARTITIONS=6 RUNS=3 bash scripts/lab-a-produce.sh
```

Local smoke (small COUNT + relative latency gate):
`bash scripts/ci-integrity-smoke.sh`.

These refuse fake wins; they are **not** Suite HOLD lifts. See
[`STATUS.md`](STATUS.md) and [`benchmark.md`](benchmark.md).

## More

- Capability vs librdkafka: [`gaps.md`](gaps.md)
- Wire notes: [`design.md`](design.md)
- Migrate from rust-rdkafka: [`migrate-from-rdkafka.md`](migrate-from-rdkafka.md)
- Adoption / pilot checklist: [`ADOPTION.md`](ADOPTION.md)
- Security: [`security.md`](security.md)
- Pure-Rust zstd spike: [`zstd-spike.md`](zstd-spike.md)
