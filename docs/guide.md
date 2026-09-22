# Operator guide

**What this is:** tutorial (Produce through Admin, in order) plus task
recipes and troubleshooting. The [documentation map](index.md) lists the
one authoritative location for each kind of information.

**Conventions:** Rust behavior and ownership explanations below are
authoritative. Java client/method names appear only as porting
cross-references; they never substitute for the Rust semantics.

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
protocol. See `examples/metrics.rs`. Optional `tracing` spans are covered
in [Tracing](#tracing-optional-feature) below; [CIVILIZATION.md](CIVILIZATION.md)
is **history** (the historical foundation plan), not a feature tracker.

## Recipes

### Backpressure

Use `try_send`; on `QueueFull` / memory pressure, `flush` or wait, then
retry. Do not unbounded-buffer in the application.

### Buffer ownership and overload (mock)

`ProducerConfig::buffer_memory` is a **key + value + headers** reservation held
from accept until broker ack or terminal failure — not encoded batch size, socket
buffers, or process RSS. Both `metrics().bytes_buffered` and
`metrics().bytes_queued` count uncompressed record key, value, and header bytes
(header keys and values). Header-heavy, zero-value, shared-backing, and
compressed records cannot bypass the declared budget. When `bytes_buffered`
reaches `buffer_memory`, `try_send` returns `QueueFull` (even for zero-value
records) and `send` waits up to `max_block` then returns `Timeout`.
`metrics().bytes_buffered` stays `≤ buffer_memory` under saturating load and
returns to `0` after `flush` / `close`. A permit stays owned across queued,
in-flight, and retry states and is released exactly once.

Kafka's oversized-first-batch progress rule is preserved: any single record
whose serialized upper bound fits within `max_request_size` and `buffer_memory`
can acquire its permit when the buffer is empty (`bytes_buffered == 0`),
preventing producer stall.

Process RSS is **not** bounded by `bytes_buffered` or `buffer_memory`; process
memory includes allocator overhead, heap fragmentation, Tokio task allocations,
TLS contexts, socket buffers, batch headers, and wire framing. This is a KL-02
contract honesty slice, not a process RSS bound.

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
handle: drop alone does not wait for in-flight produce outcomes. Producer
shutdown is bounded: `close_timeout` enforces its deadline against stalled
brokers and retry queues, failing in-flight batches with `Error::Timeout`
(preserving ambiguous delivery after transmission) and immediately returning
`Error::Closed` to concurrent sends across all clones. Dropping the last
`Producer` handle aborts worker and background tasks without leaking buffer
permits or connections. Mock coverage: `tests/produce_cancel.rs`.



### Consumer leave/close and auto-commit

`ConsumerConfig::enable_auto_commit` defaults to **false**. When enabled, offsets
are stored only on the **poll-interval** path (committing previously delivered
records once the interval elapses; the batch about to be returned is never committed
before delivery). `leave` / `close` / `unsubscribe` flush queued `commitAsync`
work but do **not** auto-commit positions — so polled-but-unprocessed records are
not silently marked done (KL-02). Prefer `commit` / `commit_with_metadata` (for
example `recs.next_offsets()`) before leave when you need stored offsets.

Mock coverage: `tests/consumer_close_commit.rs`.


### Consumer fetch buffer budget and prefetch bounds

`ConsumerConfig::buffer_memory` (default 32 MiB, configurable via `.buffer_memory()`
or `.fetch_buffer_bytes()`) enforces an aggregate memory ceiling across pre-fetched
partition queues and brokers (KL02-08):

- **Bounded Prefetching:** When callers process slowly or set `max_poll_records`,
  excess fetched records stay in `self.pending`. If buffered records reach or
  exceed `buffer_memory`, subsequent fetch calls do not request or decode more
  bytes from brokers until application consumption drains the queue below the budget.
- **Across Partitions and Brokers:** The memory budget is aggregate across all
  assigned partitions and distinct broker connections, not merely a per-request
  cap. Once accepted record bytes reach the budget, later batches are not
  retained and their fetch cursors stay unadvanced. A single Fetch response is
  fully decoded before that check. Later leaders in the same round are not
  decoded after the budget is reached, but the round may already have requested
  every leader.
- **Oversized First Batch Progress:** In accordance with Kafka protocol rules
  (KIP-74), a valid first batch larger than the soft fetch limit (`max_partition_fetch_bytes`
  or `buffer_memory`) is accepted and delivered to guarantee forward progress,
  subject to the 64 MiB hard decode ceiling (`DEFAULT_MAX_RECORD_BATCH_DECODE_BYTES`).
- **Resource Contract & Ownership:**
  - `pause`: holds prefetched records in memory until `resume`; unconsumed records
    continue to count against `buffered_bytes`.
  - `resume`: unblocks draining of buffered records on the next poll.
  - `seek`: drops pending records for that partition, immediately releasing memory
    via `drop_pending_for` and resetting the delivered position to the seek offset.
  - `close` / `drop`: releases all buffered records and resets `buffered_bytes` to 0.
  - **No Undelivered Commits:** `position()` and `positions()` report the next
    undelivered record offset rather than the broker fetch cursor, ensuring auto-commit
    and explicit commits never commit unconsumed data.
- **RSS Non-Equivalence:** Process Resident Set Size (RSS) is **not** bounded by
  `buffer_memory` or `buffered_bytes`. Process memory includes allocator fragmentation,
  Tokio runtime worker tasks, TLS context buffers, TCP frame buffers, and client metadata.

Mock coverage: `tests/fetch_buffer_budget.rs`.


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

## Troubleshooting

Authoritative first stop for failures. Pair symptoms with
[metrics](#metrics) snapshots and the optional
[tracing](#tracing-optional-feature) spans; never log payloads or secrets
(see [security.md](security.md)).

| Symptom | Likely cause | Action |
|---|---|---|
| `try_send` returns `QueueFull` | `buffer_memory` budget held by unacked records | `flush` or wait, then retry; see [Backpressure](#backpressure) |
| `send` returns `Timeout` | metadata or buffer wait exceeded `max_block` | raise `max_block`, check broker reachability |
| `Error::Closed` on send | producer closed via `close` / `close_timeout` | recreate the client; see [cancellation](#produce-cancellation-and-shutdown) |
| `Error::RecordTooLarge` | record exceeds `max_request_size` | shrink the record or raise the limit |
| Stalled fetch / no records | paused partitions, wrong offset, fenced leader | check `pause` / `seek` state; fencing recovers via OffsetForLeaderEpoch |
| Share group gets nothing | share protocol prerequisites unmet | Kafka 4.1+ with finalized `share.version=1`; see [Consumer groups](#consumer-groups) |
| KIP-848 join rejected `INVALID_REQUEST` | non-empty `TopicPartitions` on (re-)join | join sends an empty array; see [Consumer groups](#consumer-groups) |
| Auth failures / rotation | expired credentials, broker-requested reauth | see [security.md](security.md) and [auth-refresh.md](auth-refresh.md) (reference) |
| Broker down / reconnect loop | network or broker outage | client retries with `reconnect.backoff`; supported brokers in [support.md](support.md) |

If the symptom persists, record the metric snapshot, broker version and
client version, then check [support.md](support.md) for what is covered.

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

Full navigation: the [documentation map](index.md) is the one
authoritative index of tutorial, recipe, reference and architecture
material.
