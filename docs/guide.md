# Operator guide

Examples, configuration, recipes, and troubleshooting for the client.
See the [documentation index](index.md) for API and protocol references.

## Read this first

| You want to | Go to |
|---|---|
| Send records | [Produce](#produce) |
| Read an assigned partition | [Fetch](#fetch-manual-assignment) |
| Join a group | [Consumer groups](#consumer-groups) |
| See defaults that differ from Java | [Defaults](#defaults-that-differ-from-java) |
| A call failed | [Troubleshooting](#troubleshooting) |
| Read a benchmark number | [Integrity and benchmarks](#integrity-and-benchmarks) and [benchmark.md](benchmark.md) |

The runtime and build-profile sections below are measurements from one
host. They are not setup steps. Suite HOLD remains.

## Choosing a Tokio runtime

Use `current_thread` when one application thread can drive the client and its
other work. Use `multi_thread` when application tasks need parallel scheduling,
and choose its worker count from measurements of that workload.

The [local runtime comparison](evidence/perf/runtime-flavors/README.md) tested
current-thread and 1, 2, 4 and 5 background workers on two client CPUs with Kafka
4.3.1. Median native bulk throughput was 245,443 records/s on current-thread and
214,721–233,172 records/s on the multithread configurations. Current-thread also
used less client CPU in that instrumented workload. Null-broker results varied
by operation; there was no consistent benefit from adding workers.

Open-loop latency does not establish a preferred worker count. Its low-load p99
varied between repetitions and reruns, two medium-load runs rejected records,
and every high-load run rejected records. Measure admission failures as well as
latency when choosing a configuration. These shared-host results do not establish
performance on other machines or the application's overall performance.

The client does not create an application runtime or change its defaults. The
benchmark adds a bounded cancellation barrier after close, outside timing, to
keep repetitions separate. Production shutdown completion remains tracked in
KL02-12.

## Performance builds

Library builds stay on Cargo's default release profile and the system
allocator. `partitionline` does not depend on jemalloc or mimalloc. Alternate
allocators exist only as features of the workspace-excluded `benchmarks/runtime`
crate.

The [build-profile study](evidence/perf/build-profiles/README.md) compared LTO,
codegen units, `target-cpu`, and PGO on this class of host. Throughput
intervals against the portable baseline included zero. PGO's CPU change was
specific to those fixtures. Do not treat thin LTO, fat LTO, native CPU, or PGO
as a portable throughput recipe.

Allocator choice was measured on `nb-fetch-bulk` with five interleaved rounds
of the system allocator (census wrapper), jemalloc 0.6.1, and mimalloc 0.1.52.
On this host mimalloc's paired records/s mean was +8.3% (95% CI +4.5% to
+12.1%) and CPU-ns/record was −16.6% (CI −32.8% to −0.4%). Peak RSS rose about
16%. Jemalloc's records/s interval included zero and its RSS rose about 12%.
These figures are host-specific. They are not a Kafka throughput claim, and
Suite HOLD remains active. Allocation counts are reported only for the census
build; the other allocators replace that wrapper.

```bash
cargo build --release --locked --manifest-path benchmarks/runtime/Cargo.toml --bin runtime --bin nb-serve
cargo build --release --locked --manifest-path benchmarks/runtime/Cargo.toml --bin runtime --features mimalloc
cargo build --release --locked --manifest-path benchmarks/runtime/Cargo.toml --bin runtime --features jemalloc
```

Enable only one of `jemalloc` or `mimalloc`. The client crate is unchanged
either way.

## Fresh package quickstart

This exercise produces one record, verifies application processing, commits
its next offset, and leaves the group. It uses a freshly extracted `.crate`
outside the checkout. The package built from this revision is **unpublished
HEAD**, even when its manifest still says `0.1.0`; it is not proof that a
crates.io release contains every HEAD feature. The recorded run used Apache
Kafka 4.1.0 and Rust 1.98.1. Use the latest stable Rust.

Prerequisites: Rust/Cargo, Bash, GNU `timeout`, Docker, a temporary directory,
and an unused loopback port 9092. Start these commands in the clean repository
root. They create their own broker, topic and group. Cleanup removes only this
exercise's container and temporary directory.

```bash
set -euo pipefail
quick_tmp=$(mktemp -d)
quick_broker="pl-quickstart-$(date +%s)-$$"
export KAFKA_BOOTSTRAP=127.0.0.1:9092
export KAFKA_TOPIC="$quick_broker" KAFKA_GROUP="$quick_broker"
timeout 180s docker pull apache/kafka:4.1.0
timeout 30s docker run -d --name "$quick_broker" \
  -p 127.0.0.1:9092:9092 apache/kafka:4.1.0
trap 'docker rm -f "$quick_broker" >/dev/null; rm -rf "$quick_tmp"' EXIT
ready_until=$((SECONDS + 60))
until timeout 5s docker exec "$quick_broker" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  if (( SECONDS >= ready_until )); then exit 1; fi
  sleep 1
done
timeout 30s docker exec "$quick_broker" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic "$KAFKA_TOPIC" \
  --partitions 1 --replication-factor 1

# The downstream dependency is the extracted package, never the checkout.
CARGO_TARGET_DIR="$quick_tmp/package-build" timeout 120s cargo package --locked --no-verify
package=$(ls "$quick_tmp"/package-build/package/partitionline-*.crate)
tar -xzf "$package" -C "$quick_tmp"
packed_dirs=("$quick_tmp"/partitionline-*)
packed=${packed_dirs[0]}
mkdir -p "$quick_tmp/client/src"
cat > "$quick_tmp/client/Cargo.toml" <<TOML
[package]
name = "partitionline-quickstart"
version = "0.0.0"
edition = "2021"
publish = false
[dependencies]
partitionline = { path = "$packed" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
TOML
```

Save this program as `$quick_tmp/client/src/main.rs` (the same program is
emitted by `pl_write_quickstart_consumer_main` in
`scripts/lib/adopter-consumer-main.sh` for automated downstream checks):

```rust,no_run
use partitionline::{
    Acks, ConsumerConfig, ConsumerGroup, Error, ProduceRecord, Producer, ProducerConfig,
};
use std::time::Duration;

async fn run() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic =
        std::env::var("KAFKA_TOPIC").map_err(|_| Error::protocol("KAFKA_TOPIC is required"))?;
    let group_id =
        std::env::var("KAFKA_GROUP").map_err(|_| Error::protocol("KAFKA_GROUP is required"))?;
    let payload = b"hello from packed partitionline";
    let producer = Producer::new(
        ProducerConfig::bootstrap([bootstrap.clone()])
            .acks(Acks::All)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(5))
            .delivery_timeout(Duration::from_secs(10)),
    )
    .await?;
    // send resolves after the broker ack. try_send would only admit to a queue.
    let md = producer
        .send(
            ProduceRecord::to(topic.clone())
                .partition(0)
                .value(&payload[..]),
        )
        .await?;
    println!("ack partition={} offset={}", md.partition, md.offset);
    producer.close().await?;
    let cfg = ConsumerConfig::bootstrap([bootstrap])
        .auto_commit(false)
        .max_wait_ms(100)
        .request_timeout(Duration::from_secs(5));
    // A fresh broker may still be loading its group coordinator. Retry the
    // join, never the already acknowledged produce, within the outer deadline.
    let join_until = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut group = loop {
        match ConsumerGroup::join_topics(cfg.clone(), group_id.clone(), [topic.clone()]).await {
            Ok(group) => break group,
            Err(err) if err.is_retriable() && tokio::time::Instant::now() < join_until => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(err) => return Err(err),
        }
    };
    let outcome = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            let records = group.poll_timeout(Duration::from_secs(2)).await?;
            if records.is_empty() {
                continue;
            }
            for record in &records {
                // Application processing succeeds before any offset is committed.
                if record.topic != topic
                    || record.partition != md.partition
                    || record.offset != md.offset
                    || record.value.as_deref() != Some(&payload[..])
                {
                    return Err(Error::protocol(
                        "unexpected record in isolated tutorial topic",
                    ));
                }
                println!(
                    "processed partition={} offset={} bytes={}",
                    record.partition,
                    record.offset,
                    payload.len()
                );
            }
            group
                .commit_with_metadata_timeout(records.next_offsets_async().await, Duration::from_secs(5))
                .await?;
            let committed = group.committed_timeout(Duration::from_secs(5)).await?;
            let expected_next = md.offset + 1;
            if !committed.iter().any(|(tp, offset)| {
                tp.topic == topic
                    && tp.partition == md.partition
                    && offset.offset() == expected_next
            }) {
                return Err(Error::protocol(
                    "committed position did not match processed record",
                ));
            }
            println!("committed next_offset={expected_next}");
            return Ok(());
        }
    }
    .await;
    // Explicitly leave on processing errors as well; auto commit is disabled.
    group.leave().await?;
    outcome?;
    println!("closed producer and left group");
    Ok(())
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), run())
        .await
        .map_err(|_| Error::Timeout)?
}
```

Build first so compilation time is separate from the 35-second process cap:

```bash
timeout 300s cargo build --manifest-path "$quick_tmp/client/Cargo.toml"
timeout 35s cargo run --locked --manifest-path "$quick_tmp/client/Cargo.toml"
# Leaving this shell runs the cleanup trap. To clean up immediately:
docker rm -f "$quick_broker"
rm -rf "$quick_tmp"
trap - EXIT
```

For this fresh, one-partition topic the output is:

```text
ack partition=0 offset=0
processed partition=0 offset=0 bytes=31
committed next_offset=1
closed producer and left group
```

`send().await` waits for an acknowledgement; `try_send()` returns after queue
admission and can return `QueueFull`. Handle backpressure by waiting for
completion or using bounded `send` admission rather than spinning. `flush()`
waits for queued work and reports delivery errors; `acks=0` cannot prove an
acknowledged offset. A timeout after sending can leave delivery ambiguous.

Polling advances delivered positions, not the group's committed recovery
position. This program disables auto commit and commits only after every
record in the returned batch has been processed successfully. Commit uses
**offset + 1** and preserves leader epochs through `next_offsets()`. If
processing fails, the program leaves without committing the batch; a later
member may receive it again. Group join retries transient coordinator errors
within a deadline without repeating the already acknowledged produce. Keep
processing idempotent when it changes
external state. Transactions are the separate recipe below when broker
records and consumed offsets need one atomic outcome.

Consumer-group coordinator discovery retries errors 14, 15, and 16 using
`retry_backoff` and `retry_backoff_max`. Connection setup, authentication,
requests, and retry waits share one `request_timeout` budget. Authorization
errors fail immediately. This applies to classic and KIP-848 discovery.

Producer initialization also retries errors 14, 15, and 16. Transaction
coordinator discovery, producer-ID allocation and their backoff share one
`request_timeout` budget. Authorization, invalid transaction state and fencing
errors stop initialization. These retries do not resend application records.

InitProducerId negotiates v0–v6. Ordinary producers keep both v6 two-phase
flags false and use the regular returned producer ID and epoch. Typed codecs
preserve the ongoing-transaction identity separately. A moved coordinator
uses its own negotiated version; prepared transaction lifecycle is separate.

The repository's smaller examples also use `KAFKA_BOOTSTRAP` and
`KAFKA_TOPIC`; `roundtrip` is a one-record smoke, while `consume` is the manual
assignment loop. Use this packed-package exercise when checking the actual
downstream surface and process/commit/shutdown ordering.

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

- `send` returns one offset future per record.
- `send_all` queues many records and waits for all of them. Records that still need topic metadata are split out and sent after that metadata arrives.
- `try_send` plus `flush` is the throughput path. See `examples/bench_produce.rs`.

A large produce pauses every 8,192 records or partitions so the caller's
timeout can run. Work already finished is kept. That includes queueing,
splitting unready records, assembling a batch that names many topics,
encoding it, and dropping acknowledgement slots from the previous batch.
The full list is in the
[resource contract](resource-contract.md#caller-deadlines).

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

`assign_topic` assigns every partition of one topic. `seek`, `pause`, `resume`, and `wakeup` follow the Java consumer shapes.

A partition that returns many one-record batches is applied 8,192 batches at a time. Batches already applied stay if the deadline fires.

When a fetch round has several leaders, `fetch` returns after the first
leader responds and about 10 ms have passed for the others. A slower
leader stays in flight; its records arrive on a later `fetch`. Seek,
assignment changes, and close drop that in-flight round. One leader is
unchanged: that fetch waits for its response.

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
group.commit_with_metadata(recs.next_offsets_async().await).await?;
group.leave().await?;
# Ok(())
# }
```

Classic range, sticky, and cooperative-sticky assignment are supported.
KIP-848 on Kafka 4.x uses `join_consumer` and `join_consumer_topics`.
Share groups use `ShareGroup::join`, `poll`, `accept`, `release`, and
`reject`. Examples: `group`, `cooperative`, `kip848`, `share`.

A KIP-848 join sends an empty `TopicPartitions` array. Brokers reject null
with `INVALID_REQUEST` ("must be empty when (re-)joining"). The live smoke
is `examples/kip848.rs` with `REQUIRE_KIP848=1` on the 4.x broker smoke.

Share groups need Kafka 4.1 or newer with finalized `share.version=1`
(`kafka-features.sh … upgrade --feature share.version=1`). On 4.0 and 4.1
also set `group.share.enable=true` until that temporary flag is removed
upstream. The default `share.auto.offset.reset` is latest, so produce while
the share member is already polling.

A long list is walked 8,192 items at a time so a timeout can fire between
slices. Finished slices are kept. The
[resource contract](resource-contract.md#caller-deadlines) names each walk,
including share-fetch topic indexes.

These results do not change with the size of the list:

- A repeated partition in an assignment is an error. An illegal topic name is an error.
- A negative delivered position is omitted.
- `next_offsets` is the last delivered offset plus one, with its leader epoch.
- A poll with no consume interceptor returns the fetched batch without copying each record.
- Seeking to a list-offset result finds each partition in the cursor index. An unknown partition is still an error.
- An offset-commit or offset-fetch response that names many topics is read 8,192 partitions at a time.
- A share-group topic id that belongs to two names is an error. An empty name is not used.

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

Create, delete, describe, and the other admin calls are methods on
`Admin`. A long batch is prepared 8,192 items at a time, so a timeout can
fire between slices. Finished slices are kept. The
[resource contract](resource-contract.md#caller-deadlines) names each walk.

A replica move may change the replication factor. The default allows that.
Pass `false` only when the controller speaks AlterPartitionReassignments
v1. On a v0 controller the call fails before it sends the reassignment.
Per-partition errors stay in the returned results.

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{Admin, AlterPartitionReassignmentsOptions, PartitionReassignment};
use std::time::Duration;

let mut admin = Admin::connect("127.0.0.1:9092").await?;
let results = admin
    .alter_partition_reassignments_with_options(
        &[PartitionReassignment::assign(("events", 0), [1, 2])],
        AlterPartitionReassignmentsOptions::default()
            .allow_replication_factor_change(false)
            .timeout(Duration::from_secs(10)),
    )
    .await?;
# let _ = results;
admin.close().await?;
# Ok(())
# }
```

`list_transactions_with_options` asks every discovered broker. The filter
can name a transaction id pattern, a state, a producer id, and a duration.
If any broker fails, the whole call fails. For a result per broker, use
`list_transactions_by_broker_with_options`. Each entry keeps that broker's
id and its own result.

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{Admin, ListTransactionsOptions};

let mut admin = Admin::connect("127.0.0.1:9092").await?;
let options = ListTransactionsOptions {
    transactional_id_pattern: Some("orders-.*".into()),
    ..Default::default()
};
let listings = admin.list_transactions_with_options(&options).await?;
# let _ = listings;
admin.close().await?;
# Ok(())
# }
```

Nonempty patterns require ListTransactions v2 on every selected broker. Null or
empty patterns mean no filter. The broker evaluates the regex and returns any
invalid-pattern error. Pattern text is limited to 256 KiB; filter arrays and
returned listings use the bounds documented on the Admin methods.

## TLS and SASL

```rust,no_run
use partitionline::{ProducerConfig, Sasl, TlsConfig};

let _ = ProducerConfig::bootstrap(["broker:9093"])
    .tls(TlsConfig::default())
    .sasl(Sasl::scram_sha256("alice", "secret"));
```

Examples: `tls`, `sasl`. OIDC is `OidcConfig` on the SASL OAUTHBEARER path.

## Older discovery versions

Metadata v0 and GROUP FindCoordinator v0 are spoken. Startup and discovery
share one timeout across the connection, version fallback, and retries. A
bootstrap list holds at most sixteen addresses.

| Call | What v0 does |
|---|---|
| Metadata v0 with an empty topic array | Returns every topic |
| Metadata v0 with named topics | Cannot turn automatic creation off. A producer or consumer that wants this version sets `allow_auto_topic_creation = true`. A named Admin query that must keep creation off returns `Unsupported`. Admin can still list every topic. |
| Metadata v0 fields | No controller id, topic UUID, rack, or internal-topic flag. A call that needs one of those refuses the missing identity. |
| GROUP FindCoordinator v0 | No coordinator-type field |
| Transaction coordinator discovery | Needs FindCoordinator v1 or newer |

## Defaults that differ from Java

| Knob | partitionline | Java |
|---|---|---|
| `auto.offset.reset` | Earliest | latest |
| `allow.auto.create.topics` | false | consumer true |
| `delivery.timeout.ms` | 30s | 120s |
| `max.block.ms` | 30s | 60s |
| `session.timeout.ms` | 10s | 45s |
| `heartbeat.interval.ms` | 150ms | 3s |

`buffer.memory` (32 MiB) and `max.request.size` (1 MiB) match Java.

## Metrics

`Producer::metrics`, `Consumer::metrics`, `ShareGroup::metrics`, and
`Admin::metrics` are process-local snapshots. Scrape them on your own
interval, for example every 10–60 seconds. They are not a push protocol.
`examples/metrics.rs` prints one Prometheus-style text sample. That example
is not a core exporter. Optional `tracing` spans are in
[Tracing](#tracing-optional-feature). [CIVILIZATION.md](CIVILIZATION.md) is
the historical foundation plan, not a feature tracker.

Each snapshot has counters plus latency min, mean, max, p50, and p99 over
the last 1,024 samples.

| Field | What it counts |
|---|---|
| `Producer::metrics().bytes_buffered` | Reserved visible produce bytes. |
| `Producer::retries_in_flight` | Produce retries that have not finished. |
| `ack_latency` | Completed queue-to-ack time. There is no separate in-flight age. |
| `Producer::metrics().throttle` | Positive throttle responses, the sum of requested milliseconds, the largest request, and ignored negative responses. |
| `Consumer::metrics().reconnects` | Failed leader connects. |
| `Consumer::metrics().throttle` | The same requested-time counters as the producer. |
| `ConsumerGroup::outcomes` | Rejoins after the first join, and OffsetCommit successes and failures. |
| `current_lag` | One ListOffsets call per partition you ask about. It is not a scraped label. |

`diagnose_produce`, `diagnose_fetch`, and `diagnose_group` return fixed
sentences. They do not include topic names, group ids, keys, values, or
credentials. Topic rows stay one per active topic.

Throttle counters have no broker or topic labels. Requested milliseconds are
not elapsed wait: intervals can overlap, and the producer may have nothing
queued.

## Produce versions

The producer negotiates Produce v3–v13.

| Peer | What is sent |
|---|---|
| Metadata v10 or newer, and a nonzero topic id | Produce v13. The request carries the topic id, not the name. |
| No Metadata v10 | Produce v12, by name |
| Older peers | Name-based Produce v3–v12 |
| Transactional, and initialization finalized `transaction.version` at 2 or higher and EndTxn v5 | Produce v12 or v13 |
| Transactional, otherwise | Produce v11. The client adds transaction partitions itself. |

A v13 response is matched to the UUID and name snapshot sent with that
request, so a concurrent metadata change cannot rename an acknowledgement.
`UNKNOWN_TOPIC_ID` drops the cached identity and refreshes metadata before
a retry inside the original delivery deadline. A missing or ambiguous
identity fails before the request is written. The transactional choice
stays fixed for the life of the producer. Recreate it after the cluster
transaction level changes.

## Broker throttle

| Request | What the client does with a positive quota |
|---|---|
| Produce v6–v13 | Mutes new Produce requests to that broker, on every connection. Other brokers keep sending. |
| Produce v3–v5 | Leaves the delay to the broker. The client does not add a second one. |
| Fetch v8–v18 | Mutes new Fetch requests to that broker. |
| Fetch v4–v7 | Leaves the delay to the broker. |
| Admin, groups, and heartbeats | Separate policies. A Produce mute does not apply to them. |

Responses already on the wire stay readable. The Produce wait uses the
original `delivery_timeout`, and a bounded close can interrupt it. Zero or
a negative value does not cancel an interval that is already running.
Overlapping intervals keep the latest expiry, including across a reconnect.

On Fetch, brokers that are not muted, and records already buffered, stay
available. Each interval starts when its response finishes, so a slow peer
cannot restart another broker's expired quota. Headers are read from every
completed response, including when the record buffer is too full to deliver
that body.

When every broker the poll needs is muted, the wait ends at the poll's
long-poll budget (`max_wait_ms`, or the one-shot `fetch_timeout`) and
returns an empty batch. `request_timeout` still bounds connection and Fetch
work, including the quota wait. `wakeup` and an outer caller deadline
interrupt the wait. Cancelling a one-shot poll leaves the next poll's
settings alone. Group `poll` uses these same Fetch calls. Admin and
heartbeat quotas are separate policies.

## Recipes

### Backpressure

Call `try_send`. On `QueueFull`, call `flush` or wait, then retry. Keep any
buffer in the application bounded.

### Buffer ownership

`ProducerConfig::buffer_memory` reserves the visible key, value, and header
bytes from the moment a record is accepted until the broker acks it or the
send fails for good. Header keys and header values both count. A header-heavy
record, a zero-value record, and a record that will be compressed all count
their uncompressed visible bytes.

`metrics().bytes_buffered` and `metrics().bytes_queued` use that same count.

| Counts toward the budget | Stays outside it |
|---|---|
| Uncompressed key, value, and header bytes of an accepted record | Encoded batch size |
| | Socket buffers and TLS |
| | Process RSS, allocator rounding, and the record object |
| | Bytes the caller still holds, records waiting to be admitted, and `send_all` inputs that have not been accepted |

Admission copies shared or custom-owned `Bytes` into a buffer sized to the
visible length. A uniquely owned allocation with spare capacity, or with a
hidden prefix, is compacted. An allocation that is already the exact size can
be reused. Header strings and the header vector release spare capacity too.

When `bytes_buffered` reaches `buffer_memory`, `try_send` returns `QueueFull`,
including for a zero-value record. `send` waits up to `max_block` and then
returns `Timeout`. Under a full load, `bytes_buffered` stays at or below
`buffer_memory`. After `flush` or `close` it returns to 0. One permit covers
the record while it is queued, in flight, or retrying, and that permit is
released once.

A single record can still enter an empty buffer when its serialized upper
bound fits in both `max_request_size` and `buffer_memory`. That is Kafka's
rule that the first oversized-looking batch must be able to make progress,
so an empty producer does not stall on one legal record.

The mock for this budget is `tests/buffer_ownership.rs`.


### Process only the returned batch, then commit

This one-batch program caps each poll at one record, pauses its current
assignment while processing, commits only that batch's `next_offsets()`, and
leaves within 20 seconds. Paste it into the quickstart downstream package's
`src/main.rs`; set `KAFKA_TOPIC` to an existing populated topic and
`KAFKA_GROUP` to your group. Printing is the processing step in this example.
Replace it with your application operation and return an error before commit
if processing fails.

```rust,no_run
use partitionline::{ConsumerConfig, ConsumerGroup, Error};
use std::time::Duration;

async fn run() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC")
        .unwrap_or_else(|_| "partitionline".into());
    let group_id = std::env::var("KAFKA_GROUP")
        .unwrap_or_else(|_| "partitionline-process-recipe".into());
    let config = ConsumerConfig::bootstrap([bootstrap])
        .auto_commit(false)
        .max_poll_records(1)
        .max_wait_ms(100)
        .connect_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(2));
    let mut group = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ConsumerGroup::join_topics(config.clone(), group_id.clone(), [topic.clone()]).await {
                Ok(group) => return Ok(group),
                Err(error) if matches!(error.broker_code(), Some(
                    partitionline::error::COORDINATOR_NOT_AVAILABLE
                    | partitionline::error::COORDINATOR_LOAD_IN_PROGRESS
                    | partitionline::error::NOT_COORDINATOR
                )) => tokio::time::sleep(Duration::from_millis(100)).await,
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout)??;
    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let batch = group.poll_timeout(Duration::from_secs(1)).await?;
            if batch.is_empty() {
                continue;
            }
            let paused = group.assignment_async().await;
            group.pause_async(paused.clone()).await;
            for record in &batch {
                println!("processed {}-{}@{}", record.topic, record.partition, record.offset);
            }
            let committed = group
                .commit_with_metadata_timeout(batch.next_offsets_async().await, Duration::from_secs(2))
                .await;
            group.resume_async(paused).await;
            committed?;
            println!("committed returned batch only");
            return Ok(());
        }
    })
    .await
    .map_err(|_| Error::Timeout)
    .and_then(std::convert::identity);
    let left = tokio::time::timeout(Duration::from_secs(3), group.leave())
        .await
        .map_err(|_| Error::Timeout)?;
    outcome?;
    left
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    tokio::time::timeout(Duration::from_secs(20), run())
        .await
        .map_err(|_| Error::Timeout)?
}
```

The commit stores the next offset of the records this poll returned, with
the leader epoch. Prefetched records past `max_poll_records` stay
uncommitted. `leave`, `close`, and `unsubscribe` do not auto-commit work
you have not finished.

A failed or cancelled commit can still have stored an offset. On restart,
read the committed offsets. When an effect can run twice, key it by
`(topic, partition, offset)`. Restart from the stored group offset. Commit
only the batch you processed.

Pause keeps buffered bytes and stops delivery for those partitions. It does
not block a rebalance, and it does not extend `max_poll_interval`. Bound the
processing, handle revoke and assign, and stop work whose partitions were
revoked. After a membership or fencing error, skip a commit from the old
member. A cooperative rejoin keeps pending records for partitions the member
still owns and drops the revoked ones. The new owner starts from the stored
group offsets. The pause in this example does not give the process exclusive
ownership for an outside operation of any length.

The regressions are
`consumer_close_commit::commit_after_capped_poll_must_not_commit_buffered_records`,
`commit_after_capped_poll_with_pause_and_resume`,
`rebalance_preserves_buffered_records_for_retained_partitions`,
`static_member_unsubscribe_then_commit_sends_no_offset_commit`, and
`kip848_fencing_rejoin_preserves_delivered_position`.

### Committed offsets and topic IDs

Group and Admin offset methods negotiate v10 when the coordinator supports
it. On v10 each topic is a UUID.

| Call | Identity |
|---|---|
| Named Admin offset call | Captures metadata before the request and reads the response with that same snapshot. |
| Group commit, including a queued async commit | Uses the UUID already attached to the assigned partition. |
| `OffsetClient` with topic ids | Requires v10. An older coordinator returns `Error::Unsupported` before the offset RPC. |
| `OffsetClient` with names | Uses v8 or v9. |

A topic recreated under the same name does not receive an old commit. The
client does not convert a name into a UUID, or a UUID into a name, on its
own.

Use `OffsetClient` when you already have topic ids or you need the raw
per-partition errors.

```rust,no_run
# async fn example(topic_id: [u8; 16]) -> partitionline::Result<()> {
use partitionline::{AdminConfig, OffsetClient, OffsetOptions};
use partitionline::protocol::group::{
    OffsetFetchRequestData, OffsetFetchGroupData, OffsetFetchTopicData,
    OffsetTopicIdentity,
};

let mut client = OffsetClient::new(AdminConfig::bootstrap(["127.0.0.1:9092"]))?;
let request = OffsetFetchRequestData {
    groups: vec![OffsetFetchGroupData {
        group_id: "workers".into(),
        topics: Some(vec![OffsetFetchTopicData {
            identity: OffsetTopicIdentity::Id(topic_id),
            partition_indexes: vec![0],
        }]),
        ..Default::default()
    }],
    require_stable: true,
};
let response = client.fetch(&request, &OffsetOptions {
    topic_ids: true,
    timeout: Some(std::time::Duration::from_secs(3)),
    ..Default::default()
}).await?;
println!("{:?}", response.groups);
# Ok(())
# }
```

`topics: None` fetches every committed topic. `Some(vec![])` selects none.
The raw response keeps UUIDs, nullable metadata, member and leader epochs,
and broker errors. A named projection rejects an unknown UUID. It does not
invent a topic name.

The size limits cover the request collections, the metadata lookup, the
decoded results, and retries. One deadline covers discovery, the connection,
and the offset RPC. An interrupted commit may already have reached the
coordinator. Fetch the stored offsets before you decide to process the
records again.

### Produce cancellation and shutdown

After `send` or `try_send` has accepted a record into `buffer_memory`,
dropping that future leaves delivery ambiguous. The worker may still write
the record. Keep the delivery result, or settle the queue with `flush`,
before treating the outcome as final. `close` stops workers. A successful
`close` does not replace the send or flush error you still need to keep.

| Stage | Typical signals | Caller outcome |
|---|---|---|
| Before enqueue | unpolled `send`; rejected `try_send` (`QueueFull`, `RecordTooLarge`); known admission failure | **Not accepted** |
| Buffered (queued, not yet on the wire) | `metrics().bytes_buffered > 0`; drop `send` future | **Ambiguous.** The worker may still deliver. |
| After send / before ack | in flight to the broker; drop `send` future | **Ambiguous** |
| After broker ack | `Ok(RecordMetadata)`, or accepted `try_send` plus a successful `flush`, with `acks=1` or `all` | **Acknowledged.** `acks=0` cannot prove an offset. |
| After `close` / `close_timeout` | further `send` or `try_send` on any clone | **Failed** with `Error::Closed` |

Call `close` or `close_timeout` instead of dropping the last `Producer`.
Drop alone does not wait for in-flight produce. `close_timeout` applies its
deadline to a stalled broker and to the retry queue. In-flight batches then
fail with `Error::Timeout`, and a record already transmitted stays
ambiguous. Concurrent sends on every clone get `Error::Closed` immediately.
Dropping the last handle aborts the worker and background tasks. Buffer
permits and connections are released. The mock is `tests/produce_cancel.rs`.

Run the recipe against an existing topic:

```bash
KAFKA_TOPIC=events cargo run --example produce_cancel
```

It polls one send for 20 ms, drops that wait, flushes queued work, and
closes inside a 15-second process deadline. An unpolled send has accepted
nothing. Once the send has been polled, an outer timeout can land before or
after admission. A delivery timeout after the bytes were transmitted stays
ambiguous. The recipe keeps the flush result separate from close and does
not send that record again. A later aggregate result cannot rebuild the
missing per-record receipt. A fresh send after that ambiguity can duplicate
the record, including when the original producer had idempotence on.

A rejected `try_send` has not admitted that record. Wait for other work to
settle inside a deadline, then retry admission. `send` already waits up to
`max_block` for admission and uses its delivery deadline after that. Keep
the application queue bounded.

```bash
cargo test --locked --test buffer_ownership
cargo test --locked --test produce_cancel
```

Named cases: `dropping_send_future_while_buffered_is_ambiguous_but_still_delivers`,
`stalled_broker_close_timeout_terminates_boundedly_and_completes_inflight`,
and `concurrent_sends_during_close_do_not_hang_and_observe_closed`.



### Consumer leave, close, and auto-commit

`enable_auto_commit` defaults to false. When it is on, offsets are stored
only on the poll-interval path: records that were already delivered, after
the interval elapses. The batch about to be returned is not committed
before the caller sees it.

`leave`, `close`, and `unsubscribe` flush queued `commit_async` work. They
do not store positions on their own. Records you polled and have not
processed stay uncommitted. Call `commit` or `commit_with_metadata` with
`recs.next_offsets()` before leave when those offsets must be stored.

The mock is `tests/consumer_close_commit.rs`.


### Consumer fetch buffer budget and prefetch bounds

`ConsumerConfig::buffer_memory` defaults to 32 MiB. Set it with
`buffer_memory` or `fetch_buffer_bytes`. The cap is one total for every
assigned partition and every broker connection. It is not a per-request cap.

When the application is slow, or `max_poll_records` returns only part of a
fetch, the rest stays in `pending`. Once buffered records reach
`buffer_memory`, later fetches neither request nor decode more broker bytes
until consumption drops the queue under the cap.

A batch that arrives after the cap is full stays framed and compressed in a
per-partition queue. It is decoded when consumption frees budget. Its fetch
cursor does not move until that decode. Every fetched batch is framed and
CRC-checked when the response arrives, and decompressed only when it is
applied. Retained framed bytes are capped by the larger of `buffer_memory`
and `max_bytes`. One batch that does not fit is still kept, so it is not
downloaded again. Decoded payload stays within `buffer_memory`.

Pause keeps that queue. Seek, unassign, and a leader change drop it together
with the decoded pending records.

A valid first batch larger than `max_partition_fetch_bytes` or
`buffer_memory` is still accepted (KIP-74), so the consumer can make
progress. The hard decode ceiling is 64 MiB
(`DEFAULT_MAX_RECORD_BATCH_DECODE_BYTES`).

| Call | Effect on the buffer |
|---|---|
| `pause` | Holds prefetched records and framed batches until `resume`. Unconsumed records still count toward `buffered_bytes`. |
| `resume` | The next poll can drain buffered records and retained batches. |
| `seek` | Drops pending records and framed batches for that partition and sets the delivered position to the seek offset. |
| `close` or drop | Releases every buffered record and sets `buffered_bytes` to 0. |

`position` and `positions` report the next undelivered offset, not the broker
fetch cursor. Auto-commit and explicit commit therefore store only records
the caller has been given.

Process RSS is a different number. Allocator fragmentation, Tokio tasks, TLS,
TCP frames, and client metadata sit outside `buffer_memory` and
`buffered_bytes`.

The mock for this budget is `tests/fetch_buffer_budget.rs`.


### Fetch version 18

Each broker connection negotiates Fetch up to version 18. KIP-1166 adds a
request partition HighWatermark tag; ordinary consumers omit its i64MAX
default. The added `encode_fetch_request_with_replica_high_watermarks` and
`decode_fetch_request_with_high_watermarks` helpers use a sidecar aligned with
topic and partition order, preserving `FetchPartition` and existing signatures.
They encode replica values, including unknown `-1`, without implementing follower
acknowledgment or quorum-commit behavior. Older versions omit this ignorable
field, and response layouts remain unchanged.

### Incremental Fetch sessions

The consumer keeps one Fetch session per active broker on v7–v18, sends changed
partitions and removes forgotten partitions, and retains full requests on v4–v6.
Session metadata is bounded by the active assignment. Each completed response
advances its broker epoch even when the record buffer budget discards the body.
A failed, abandoned or canceled request requires a full map before reusing the
session. Session-not-found and invalid-epoch responses retry with a full map
within the original request deadline; a topic-ID error also refreshes metadata.
The consumer checks the session identity and partition set before applying any
records. Reassigning a recreated topic discards buffered records from its old ID.
Finding, dropping, and copying session partitions runs 8,192 items at a
time, so a metadata refresh can still reach its deadline. The topic-id
index is rebuilt the same way. Prepare generations are cleared the same
way when they wrap. Finished slices are kept.
Buffered records, delivered positions and commits retain their existing contracts.

Pausing every partition, moving to another broker, or replacing the assignment
retires inactive known sessions on the next fetch. `unassign()` is synchronous;
retirement happens on the next assigned fetch or close. Manual and group close
send a best-effort terminal epoch for known sessions within one total remaining
budget. Terminal requests also respect broker quotas; a connection is dropped
when the mute exceeds the remaining budget. `close_timeout(Duration::ZERO)`
immediately drops manual connections.
Session retirement never commits offsets.

With 128 unchanged partitions, measured Fetch v17 request bodies shrink from
4,268 to 25 bytes, matching Apache Java 4.3.1 serialization; this is a request-byte
measurement. It does not establish a throughput or latency improvement. The
KL05-07 runner observes Fetch IDs, epochs, offsets and forgotten partitions over
32 partitions, pause/resume, an idle reconnect reset and terminal close, then
compares all 65 records with the digest-pinned Apache 4.1.2 Java console consumer.
The Apache 4.3.1 handler provides a separate executed recovery reference. These
bounded scenarios do not establish a long-running broker fault campaign.

### Rebalance

Prefer cooperative-sticky when partitions must move with less stop-the-world
pause (`examples/cooperative.rs`). Handle `on_rebalance` for revoke/assign.

### Exactly-once consume → produce

Run `KAFKA_TOPIC=events KAFKA_OUTPUT_TOPIC=events-out KAFKA_GROUP=recipe-eos
KAFKA_TRANSACTIONAL_ID=recipe-eos cargo run --example eos` (on one shell line)
with both topics present and at least one source record. The program uses
`ReadCommitted`, auto-commit off and a 64-record poll cap; it copies one
returned batch, sends `next_offsets()` with current group metadata, commits,
leaves and closes within 45 seconds.

Initialization and group join retry transient coordinator-startup errors
within their own deadlines, before staging any transaction data. The producer
internally retries sends; the recipe never resends staged data.
It may retry **the same transaction's commit** within a ten-second budget:
a lost commit response is ambiguous, and EndTxn is idempotent. It does not
switch an ambiguous commit to abort. A failure before commit attempts a
bounded abort; explicit abort-required errors need successful abort before
another transaction. Failed/ambiguous abort stops the instance. Fencing
(`PRODUCER_FENCED` or `INVALID_PRODUCER_ID_MAPPING`) is terminal: abort or
`init_transactions` cannot repair that instance. Classic identity recovery
after abort is authorized by the coordinator; transaction V2 supplies its
identity through EndTxn. Do not invent a local epoch increment.

After abort, the consumer's delivered position may already have advanced.
This one-batch program exits; its replacement joins from stored committed
group offsets, rather than continuing from that delivered position. Use one
stable transactional ID per logical worker and one active owner of that ID.
If commit remained ambiguous, resolve it through stored group offsets and
`ReadCommitted` output before deciding to reprocess. If no source record is
available, the example returns a bounded timeout rather than waiting forever.

The atomic boundary is Kafka output plus consumed Kafka offsets. Database
writes, HTTP calls, filesystem effects and log printing are outside it; use
a durable idempotency key/outbox or an application protocol for those effects.
This is a tested Kafka contract, not universal Java API or external
exactly-once equivalence. Regression evidence:
`client_api::txn_abort_required_codes_block_commit_and_recover_only_after_abort`,
`txn_abort_fatal_fencing_is_terminal_across_coordinator_versions`, the
`full_surface` transactional cases, and
`consumer_fetch_semantics::committed_transaction_after_abort_for_same_pid_must_be_visible`.

## zstd codec (optional feature)

Enable `zstd` to read and write compression-ID-4 record batches. Decoding checks the
caller's byte budget, a 64 MiB maximum output/window and at most 1,024 frames.
Encoding supports levels 1 through 19 (default 3), selected with
`protocol::records::ZstdLevel` and `ProducerConfig::zstd_level`. Dictionaries
return `Error::Unsupported`. See
[zstd-spike.md](zstd-spike.md) for the tested profile and limits.

```sh
cargo test --locked --features zstd --test zstd_decode
```

## gzip backend (default feature)

The default `zlib-rs` feature compresses and decompresses gzip with zlib-rs.
To limit `unsafe` in the gzip path to the small CRC-32 kernel in `crc32fast`,
disable default features; gzip then uses miniz_oxide, which forbids `unsafe`
and is slower:

```toml
partitionline = { version = "0.1", default-features = false }
```

Both backends speak the same wire format. See
[gzip-backend.md](gzip-backend.md) for measurements and the safety trade-off.

## Tracing (optional feature)

Enable spans without changing default builds:

```toml
partitionline = { version = "0.1", features = ["tracing"] }
```

Spans cover `Producer::send` (topic field), `Consumer::fetch`,
`ConsumerGroup::poll`, cooperative rejoin, and transaction
init/begin/commit/abort. Pair with `tracing-subscriber` in the application.

## Troubleshooting

Start here when a call fails. Pair the symptom with a
[metrics](#metrics) snapshot and, if you enabled it, a
[tracing](#tracing-optional-feature) span. Do not log payloads or secrets.
Credential rules are in [security.md](security.md).

| Symptom | Likely cause | What to do |
|---|---|---|
| `try_send` returns `QueueFull` | `buffer_memory` is held by records that are not yet acked | `flush`, or wait, then retry. See [Backpressure](#backpressure). |
| `send` returns `Timeout` | Metadata or buffer wait exceeded `max_block` | Raise `max_block`, and check that the broker is reachable. |
| `Error::Closed` on send | The producer was closed | Create a new producer. See [cancellation](#produce-cancellation-and-shutdown). |
| `Error::RecordTooLarge` | The record exceeds `max_request_size` | Shrink the record, or raise the limit. |
| Fetch returns no records | A partition is paused, the offset is wrong, or the leader was fenced | Check `pause` and `seek`. Fencing recovers with OffsetForLeaderEpoch. |
| A share group receives nothing | The cluster is not offering share groups | Use Kafka 4.1 or newer with finalized `share.version=1`. See [Consumer groups](#consumer-groups). |
| A KIP-848 join returns `INVALID_REQUEST` | The join carried topic partitions | This client sends an empty array. See [Consumer groups](#consumer-groups). |
| Produce or fetch fails after the SASL session lifetime | Client loops do not reauthenticate on a timer. The connection closes when the session expires. | An owned `OidcTokenManager` refreshes its token. The shared connect-time cache does not. See [token lifecycle](auth-refresh.md). GSSAPI is not available. |
| The client keeps reconnecting | The broker or the network is down | Retries use `reconnect.backoff`. Tested brokers are in [support.md](support.md). |

If the symptom persists, record the metric snapshot, broker version and
client version, then check [support.md](support.md) for what is covered.

## Streams heartbeats

`StreamsClient` sends caller-supplied Streams group heartbeats. The caller owns
the member ID, epochs, topology, tasks and assignment reconciliation. Set
`allow_unstable` explicitly: v0 alone does not identify the broker release’s
stability policy. Routine heartbeats reuse the coordinator connection.

```rust,no_run
# async fn example(request: partitionline::protocol::streams::StreamsGroupHeartbeatRequest) -> partitionline::Result<()> {
use partitionline::{StreamsClient, StreamsConfig};

let mut config = StreamsConfig::bootstrap(["127.0.0.1:9092"]);
config.allow_unstable = true;
let mut client = StreamsClient::new(config)?;
let response = client.heartbeat(&request).await?;
println!("member epoch: {}, error: {}", response.member_epoch, response.error_code);
client.close().await;
# Ok(())
# }
```

Join uses epoch 0. Updates use the epoch the broker returned. Leave uses
-1, or -2 for a static member that will rejoin. Terminal broker errors remain in
the typed response. `heartbeat_timeout` gives discovery, authentication, retries
and the RPC one total deadline. Cancellation discards its connections; a lost
reply can still follow an accepted heartbeat. `close` closes the cached socket
without sending a leave. This client does not run a Streams application.

## Streams group descriptions

`Admin::describe_streams_groups` returns groups in caller order, including
repeated IDs. Each entry contains either the complete typed description or a
discovery, connection or capability error. Broker response errors remain in
`description.error_code`; topology, tasks, member offsets and endpoints are
preserved. The operation uses GROUP coordinators and one total deadline.

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{Admin, AdminConfig, DescribeStreamsGroupsOptions};
let mut admin = Admin::new(AdminConfig::bootstrap(["127.0.0.1:9092"])).await?;
let options = DescribeStreamsGroupsOptions {
    allow_unstable: true,
    include_authorized_operations: true,
    ..Default::default()
};
for group in admin.describe_streams_groups(&["application"], &options).await? {
    match group.description {
        Ok(description) => println!("{}: {}", group.group_id, description.group_state),
        Err(error) => eprintln!("{}: {}", group.group_id, error),
    }
}
# Ok(())
# }
```

The explicit opt-in is required because v0 alone does not identify whether a
broker release treats the API as stable. This call observes groups without
joining them or running their tasks.

## Integrity and benchmarks

These scripts check that produced records, the broker high watermark, and a
later fetch agree with the seeded records. They are local or unsigned Lab A
checks. They do not lift Suite HOLD.

```bash
bash scripts/lab-a-integrity.sh          # small default COUNT
COUNT=50000 bash scripts/lab-a-fetch.sh  # fetch-focused
COUNT=8000000 PARTITIONS=6 RUNS=3 bash scripts/lab-a-produce.sh
```

`bash scripts/ci-integrity-smoke.sh` is a small local smoke with a relative
latency gate. How to read the numbers is in [benchmark.md](benchmark.md).
The qualification status is in [STATUS.md](STATUS.md).

## More

Full navigation: the [documentation map](index.md) is the one
authoritative index of tutorial, recipe, reference and architecture
material.
