# Migrate from rust-rdkafka / librdkafka (and Java)

Use this guide to port a service from rust-rdkafka, librdkafka, or the Java
client. partitionline uses typed builders, owned records, and async methods.
Porting requires changes to configuration and call sites.

## How to read this map

Every default below is pinned to the constructor that sets it. Four
categories stay separate throughout:

- **API shape** — what the equivalent call is.
- **Intentional default differences** — same knob, different default, on
  purpose.
- **Unsupported features** — no equivalent exists; each row names its
  [feature-registry](../tests/conformance/features.json) entry.
- **Verified behavioral equivalence** — what tests or measurements actually
  prove, and what they do not.

The tables name the Rust symbol and the file. They do not cite line numbers.
An older revision pinned those lines to `ca50ca1`, and the lines no longer
matched current source.

The default values were compared, field by field, between the packed
crates.io `0.1.0` crate and later source. The mapped defaults match, with
two additions only on current source: `ConsumerConfig::buffer_memory` (a
32 MiB fetch cap) and the hidden `ProducerConfig::pre_send_fault` test hook.
Adopters on published `0.1.0` get the same mapped defaults and the older
runtime. The examples below compile against that packed dependency.

## Dependency

```toml
# before
rdkafka = { version = "0.39", features = ["cmake-build"] }

# after (crates.io 0.1.0)
partitionline = "0.1"
```

The client uses Tokio for async I/O and rustls with Ring for TLS. Ring requires
a C compiler during the build. Current source enables the `zlib-rs` gzip backend
by default and provides optional `tracing` spans; the published 0.1.0 feature
set is an earlier baseline.

## API shape

### Concepts

| librdkafka / rust-rdkafka | Java | partitionline |
|---|---|---|
| `FutureProducer` / `BaseProducer` | `KafkaProducer` | `Producer` (`Producer::new(cfg)`, `src/producer.rs`) |
| `StreamConsumer` / `BaseConsumer` | `KafkaConsumer` (manual) | `Consumer` (`Consumer::new(cfg)`, `src/consumer.rs`) |
| group consumer | `KafkaConsumer` (subscribed) | `ConsumerGroup::join_*` (`src/group.rs`) |
| — (KIP-932) | `ShareConsumer` | `ShareGroup::join_*` (`src/share.rs`) |
| `AdminClient` | `Admin` | `Admin` (`Admin::new(cfg)`, `src/admin.rs`) |
| String property bag (`ClientConfig::set`) | `Properties` | Typed builders: `ProducerConfig`, `ConsumerConfig`, `AdminConfig` |
| `Message` / `BorrowedMessage` | `ConsumerRecord` / `ProducerRecord` | `FetchedRecord` / `ShareRecord` / `ProduceRecord` (owned; no borrow lifetimes) |
| Delivery callbacks / `DrQueue` polling | `Callback` / `Future<RecordMetadata>` | `send` future, or `try_send` + `flush` (see Callbacks) |
| `commit` / committed offsets | `commitSync` / `committed` | `commit_with_metadata`, `committed`, `OffsetAndMetadata` |
| `ClientContext` stats | `Metrics` | `Producer::metrics` / `Consumer::metrics` / `ShareGroup::metrics` / `Admin::metrics` |

### Task map

| Task | rust-rdkafka-ish | partitionline |
|---|---|---|
| Produce one | `send` + delivery future | `Producer::send` (`src/producer.rs`) |
| High throughput | poll delivery queue | `try_send` + `flush` (`src/producer.rs`) |
| Assign partitions | `assign` | `Consumer::assign` / `assign_topic` / `assign_partitions` (`src/consumer.rs`) |
| Subscribe group | `subscribe` | `ConsumerGroup::join_topics` (or sticky / cooperative / KIP-848 variants) |
| Pattern subscribe | `subscribe` with regex | `join_matching` / `join_sticky_matching` / `join_consumer_matching` (re-list on poll) |
| Poll records | `recv` / `poll` | `fetch` / `fetch_timeout` or `group.poll` / `poll_timeout` → `ConsumerRecords` |
| Commit | `commit_message` | `commit_with_metadata(recs.next_offsets())` |
| Async commit | `commit` (async) | `commit_async` / `commit_async_with` (queued `OffsetCommit` on poll / leave) |
| Transactions | init / begin / send_offsets / commit | same names on `Producer`; see `examples/eos.rs` |
| Create topic | admin create | `Admin::create_topics` |
| Wake a blocked poll | — | `Consumer::wakeup` / `WakeupHandle` (`src/consumer.rs`; interrupts in-flight Fetch) |
| Share-group ack | — | `ShareGroup::{accept, release, reject}` |

## Partitioning

| Knob | partitionline | Default (source) | Java | librdkafka |
|---|---|---|---|---|
| Keyed records | `DefaultPartitioner` (`src/partitioner.rs`) | murmur2, `partition_for_key` (`src/partitioner.rs`) | murmur2 (`DefaultPartitioner`) — same | `murmur2` partitioner — same |
| Unkeyed records | `DefaultPartitioner` | round-robin from 0 | sticky (`StickyPartitioner`, KIP-794) — **differs** | `consistent_random` default — **differs** |
| Custom | `Partitioner` trait + `ProducerConfig::partitioner` (`src/producer.rs`) | — | `partitioner.class` | `partitioner_cb` |
| Explicit pin | `ProduceRecord::partition` (`src/producer.rs`) | skips the partitioner | `ProducerRecord` partition ctor | `TopicPartition` |

```rust
use partitionline::{Partitioner, ProducerConfig};

// Keyed records hash with murmur2; unkeyed records round-robin.
let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"]);

// Custom partitioner (Java `partitioner.class`).
struct ModN;
impl Partitioner for ModN {
    fn partition(&self, _topic: &str, key: Option<&[u8]>, n: i32) -> i32 {
        key.map(|k| k.len() as i32).unwrap_or(0) % n.max(1)
    }
}
let _custom = ProducerConfig::bootstrap(["127.0.0.1:9092"]).partitioner(ModN);
```

Equivalence: `murmur2` matches `org.apache.kafka.common.utils.Utils.murmur2`
byte-for-byte, proven by unit vectors in `src/partitioner.rs`
(`murmur2(b"kafka") == -798_503_068`). Unkeyed placement is **not**
equivalent to either Java (sticky batching) or librdkafka, and the sticky
partitioner is an explicit gap (`producer.sticky_partitioner`, missing in the
[feature registry](../tests/conformance/features.json)).

## Acks, idempotence, transactions

| Knob | partitionline | Default (source) | Java | librdkafka |
|---|---|---|---|---|
| `acks` | `ProducerConfig::acks(Acks)` (`src/producer.rs`); stored as `i16` | `1` (`Acks::Leader`, `src/config.rs`, `src/producer.rs`) | `1` documented, forced to `all` when idempotence is on | `request.required.acks=-1` (all) — **differs** |
| `enable.idempotence` | `ProducerConfig::idempotent(bool)` (`src/producer.rs`) | `false` (`src/producer.rs`) | `true` (since 3.0) — **differs** | `false` — same |
| Idempotent effects | `Producer::new` (`src/producer.rs`) | forces `acks=all`, `max_in_flight ≤ 5`, one TCP conn per partition | same forcing | `enable.idempotence=true` forces `acks=all`, `max.in.flight ≤ 5` |
| `max.in.flight` | `ProducerConfig::max_in_flight` (`src/producer.rs`) | `16` (`src/producer.rs`) | `5` — **differs** | `1000000` — **differs** |
| `transactional.id` | `ProducerConfig::transactional_id` (`src/producer.rs`) | none; implies idempotence | `null`; implies idempotence — same | same shape |
| `transaction.timeout.ms` | `ProducerConfig::transaction_timeout` (`src/producer.rs`) | 60 s (`src/producer.rs`) | 60 s — same | `transaction.timeout.ms=60000` — same |

```rust
use std::time::Duration;
use partitionline::{Acks, ProducerConfig};

// Exactly-once producer: transactional id implies idempotence,
// forces acks=all and max_in_flight <= 5 in Producer::new.
let cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
    .transactional_id("orders-1")
    .transaction_timeout(Duration::from_secs(60));
let _ = cfg;

// Idempotent without transactions (Java enables this by default;
// partitionline and librdkafka do not).
let _idem = ProducerConfig::bootstrap(["127.0.0.1:9092"])
    .idempotent(true)
    .acks(Acks::All);
```

## Timeout budgets

| Knob | partitionline | Default (source) | Java | librdkafka |
|---|---|---|---|---|
| `request.timeout.ms` | `Producer/Consumer/AdminConfig::request_timeout` | 30 s (`src/producer.rs`) | 30 s — same | `socket.timeout.ms=60000` (different shape) |
| `delivery.timeout.ms` | `ProducerConfig::delivery_timeout` (`src/producer.rs`) | **30 s** (`src/producer.rs`) | 120 s — **differs (shorter)** | `message.timeout.ms=300000` — **differs (shorter)** |
| `max.block.ms` | `ProducerConfig::max_block` (`src/producer.rs`) | **30 s** (`src/producer.rs`) | 60 s — **differs (shorter)** | `message.timeout.ms` + queue-full errors (different shape) |
| `linger.ms` | `ProducerConfig::linger` (`src/producer.rs`) | 5 ms (`src/producer.rs`) | 0 ms — **differs** | `queue.buffering.max.ms=5` — same |
| Batching | `batch_records` / `batch_bytes` (`src/producer.rs`) | 32_768 records / 1_000_000 bytes (`src/producer.rs`) | `batch.size=16384` bytes — **differs** | `batch.size=1000000` — same bytes |
| `buffer.memory` | `ProducerConfig::buffer_memory` (`src/producer.rs`) | 32 MiB (`src/producer.rs`) | 33554432 — same | `queue.buffering.max.kbytes=1048576` (different shape) |
| `max.request.size` | `ProducerConfig::max_request_size` (`src/producer.rs`) | 1 MiB (`src/producer.rs`) | 1048576 — same | `message.max.bytes=1000000` — same |
| `retry.backoff.ms` | `retry_backoff` | 100 ms (`src/config.rs`) | 100 ms — same | `retry.backoff.ms=100` — same |
| `retry.backoff.max.ms` | `retry_backoff_max` | 1 s (`src/config.rs`) | 1000 ms — same | `retry.backoff.max.ms=1000` — same |
| `reconnect.backoff.ms` / max | `reconnect_backoff[_max]` | 50 ms / 1 s (`src/config.rs`) | 50 / 1000 ms — same | `reconnect.backoff.ms=100`, `reconnect.backoff.max.ms=10000` — **differs** |
| `connections.max.idle.ms` | `connections_max_idle` | 9 min (`src/config.rs`) | 540000 — same | `connections.max.idle.ms=540000` — same |
| `metadata.max.age.ms` | `metadata_max_age` | 5 min (`src/producer.rs`) | 300000 — same | `metadata.max.age.ms=300000` — same |
| `session.timeout.ms` | `ConsumerConfig::session_timeout` (`src/consumer.rs`) | **10 s** (`src/consumer.rs`) | 45000 — **differs (shorter)** | 45000 — **differs (shorter)** |
| `heartbeat.interval.ms` | `ConsumerConfig::heartbeat_interval` (`src/consumer.rs`) | **150 ms** (`src/consumer.rs`) | 3000 — **differs** | 3000 — **differs** |
| `max.poll.interval.ms` | `ConsumerConfig::max_poll_interval` (`src/consumer.rs`) | 300 s (`src/consumer.rs`) | 300000 — same | 300000 — same |
| `max.poll.records` | `ConsumerConfig::max_poll_records` (`src/consumer.rs`) | none (uncapped, `src/consumer.rs`) | 500 — **differs** | — |
| `fetch.max.wait.ms` | `ConsumerConfig::max_wait_ms` (`src/consumer.rs`) | 500 ms (`src/consumer.rs`) | 500 — same | `fetch.wait.max.ms=500` — same |
| `fetch.min.bytes` | `ConsumerConfig::min_bytes` (`src/consumer.rs`) | 1 (`src/consumer.rs`) | 1 — same | 1 — same |
| `fetch.max.bytes` | `ConsumerConfig::fetch_max_bytes` (`src/consumer.rs`) | **16 MiB** (`src/consumer.rs`) | 50 MiB — **differs** | `fetch.message.max.bytes=1048576` — **differs** |
| `max.partition.fetch.bytes` | `ConsumerConfig::max_partition_fetch_bytes` (`src/consumer.rs`) | **16 MiB** (`src/consumer.rs`) | 1 MiB — **differs** | — |

Porting rule: the shorter `delivery.timeout.ms` (30 s vs 120 s / 300 s),
`max.block.ms` (30 s vs 60 s), `session.timeout.ms` (10 s vs 45 s), and the
150 ms heartbeat change failure-detection timing. Raise them explicitly if
your service was tuned against Java or librdkafka timeouts. Broker
`throttle_time_ms` is decoded on the wire but the runtimes do not sleep on it
(`quotas.producer_throttle`, `quotas.consumer_throttle`, partial), so
throttled clusters behave more aggressively than Java here.

## Offsets and auto-commit

| Knob | partitionline | Default (source) | Java | librdkafka |
|---|---|---|---|---|
| `auto.offset.reset` | `ConsumerConfig::auto_offset_reset` (`src/consumer.rs`, `AutoOffsetReset`, `src/config.rs`) | **Earliest** (`src/consumer.rs`) | `latest` — **differs** | `largest` — **differs** |
| `enable.auto.commit` | `ConsumerConfig::auto_commit` (`src/consumer.rs`) | **false** (`src/consumer.rs`) | `true` — **differs** | `true` — **differs** |
| `auto.commit.interval.ms` | `ConsumerConfig::auto_commit_interval` (`src/consumer.rs`) | 5 s (`src/consumer.rs`) | 5000 — same | 5000 — same |
| `isolation.level` | `ConsumerConfig::isolation` (`src/consumer.rs`, `src/config.rs`) | `read_uncommitted` (`src/consumer.rs`) | same | same |
| `allow.auto.create.topics` | `allow_auto_topic_creation` | **false** (`src/consumer.rs`) | `true` — **differs** | — |
| Committed lookup | `ConsumerGroup::committed[_timeout]` (`src/group.rs`) | Java `committed(Duration)` shape | same shape | `committed` |

```rust
use std::time::Duration;
use partitionline::{AutoOffsetReset, ConsumerConfig, ConsumerGroup};

async fn join() -> partitionline::Result<()> {
    // Java-shaped group: explicit Latest reset + auto-commit on,
    // matching Java/librdkafka defaults (partitionline defaults differ).
    let cfg = ConsumerConfig::bootstrap(["127.0.0.1:9092"])
        .auto_offset_reset(AutoOffsetReset::Latest)
        .auto_commit(true)
        .auto_commit_interval(Duration::from_secs(5));
    let mut group = ConsumerGroup::join_topics(cfg, "orders", ["events"]).await?;
    let recs = group.poll().await?;
    // Explicit commit of the delivered batch (the portable shape).
    group.commit_with_metadata(recs.next_offsets()).await?;
    group.leave().await?;
    Ok(())
}
```

Auto-commit contract (KL03-07, `src/group.rs`, proven by
`tests/consumer_close_commit.rs`): when enabled, the interval commit runs
before fetch and stores the **previously delivered** positions — the batch
about to be returned is not committed until a subsequent poll, so a crash
after one capped poll cannot skip prefetched records on rejoin. Zero interval
commits after delivery on the same poll when records were returned.
Leave/close/unsubscribe **never** auto-commit. Repaired contracts are proven
by mock/fixture tests: `auto.offset.reset=None` returns out-of-range without
advancing (KL03-05); seeking within a whole batch filters earlier records
(KL03-04); ABORT ends its aborted interval so later committed transactions
under the same producer ID remain visible (KL03-03). These are targeted
correctness results, not universal Java or live compatibility qualification.

## Codecs

| Codec | partitionline | Java | librdkafka |
|---|---|---|---|
| none | default (`Compression::None`, `src/protocol/records.rs`) | default — same | `none` default — same |
| gzip | `Compression::Gzip` (`flate2`; zlib-rs by default) | same wire format | same wire format |
| snappy | `Compression::Snappy` (`snap`; snappy-java framing on produce, raw on fetch) | same wire format | same wire format |
| lz4 | `Compression::Lz4` (`lz4_flex` frame, independent 64 KiB blocks) | same wire format | same wire format |
| zstd | optional `zstd` feature; levels 1–19, default 3 | `zstd` | `zstd` |

```rust
use partitionline::{Compression, ProducerConfig};

let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
    .compression(Compression::Lz4);
```

Enable the optional `zstd` feature for zstd topics. The codec uses
`zstd-rs 0.1.0`; default dependencies remain unchanged. See
[zstd-spike.md](zstd-spike.md) for encoder levels and resource limits. Codec
limits describe decoded bytes rather than process RSS.

## Auth and TLS

| Mechanism | partitionline | Java / librdkafka |
|---|---|---|
| No auth | default (all `sasl_*` are `None`, TLS `None`) | same shape |
| PLAIN | `Sasl::plain(user, pass)` (`src/config.rs`) | same |
| SCRAM-SHA-256 | `Sasl::scram_sha256(user, pass)` (`src/config.rs`, RFC 5802/7677) | same |
| SCRAM-SHA-512 | `Sasl::scram_sha512(user, pass)` (`src/config.rs`) | same |
| OAUTHBEARER | `Sasl::oauthbearer(principal)` (`src/config.rs`, RFC 7628 unsecured JWT) | same (`enable.sasl.oauthbearer.unsecure.jwt`) |
| OIDC token endpoint | `Sasl::oidc(OidcConfig)` (`src/config.rs`, `src/protocol/oidc.rs`) | same shape |
| GSSAPI / Kerberos | **not implemented**. The proposal is a helper process with `libgssapi`, not Cyrus SASL. See [gssapi-boundary.md](gssapi-boundary.md). | supported |
| TLS | `TlsConfig` (`src/net.rs`, rustls; `ca_pem` / `server_name` / `client_identity` for mTLS; Mozilla roots by default) | `ssl.*` (JVM TLS / OpenSSL) |
| Token refresh | An owned `OidcTokenManager` refreshes before expiry. The shared connect-time cache does not, and client loops do not reauthenticate on a timer. See [auth-refresh.md](auth-refresh.md). The registry id `auth.sasl_oidc_refresh` is still missing. | supported |

```rust
use partitionline::{ProducerConfig, Sasl, TlsConfig};

// SASL_SSL with SCRAM-512 and a custom CA (rustls, no OpenSSL).
let _cfg = ProducerConfig::bootstrap(["kafka:9093"])
    .sasl(Sasl::scram_sha512("alice", "secret"))
    .tls(TlsConfig::default().ca_pem(std::vec::Vec::new()));
```

Setting `.sasl(...)` replaces any previously set mechanism (at most one).
Credential `Debug` output is redacted (`RedactedUserPass`, `security.md`).

## Callbacks and interceptors

rust-rdkafka routes delivery reports and events through `ClientContext`
callbacks and `DrQueue` polling. partitionline has no global callback queue;
completion surfaces through futures plus Java-shaped interceptors:

| rust-rdkafka | partitionline |
|---|---|
| Delivery callback / `FutureRecord` | `Producer::send` future → `RecordMetadata` or terminal `Error` |
| `DrQueue` polling | `try_send` (non-blocking, `Error::QueueFull`) + `flush` / `flush_timeout` |
| `ClientContext::stats` | `Producer::metrics` / `Consumer::metrics` / `ShareGroup::metrics` / `Admin::metrics` |
| Rebalance callback | `ConsumerConfig::on_rebalance(f)` (`src/consumer.rs`) + `ConsumerInterceptor::on_commit` |
| — | `ProducerInterceptor::{on_send, on_ack, on_error, close}` (`src/interceptor.rs`) |
| — | `ConsumerInterceptor::{on_consume, on_commit, close}` (`src/interceptor.rs`); filtering in `on_consume` does not rewind positions |

```rust
use partitionline::interceptor::ProducerInterceptor;
use partitionline::{Error, ProducerConfig, RecordMetadata};

struct Logging;
impl ProducerInterceptor for Logging {
    fn on_ack(&self, _md: &RecordMetadata) {}
    fn on_error(&self, _err: &Error) {}
}

let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"]).interceptor(Logging);
```

For runnable bounded process/commit, cancellation/shutdown and transaction
recipes, see the [operator guide](guide.md#recipes). Commit only fully
processed returned offsets, retain send/flush errors, and distinguish
ambiguous commit from abort-required or terminal fencing outcomes.

Cancellation differs from Java futures: dropping a `send` future does **not**
dequeue the record — once accepted into `buffer_memory`, delivery may
continue and the caller outcome stays ambiguous until `flush`/`close`
(`src/producer.rs`).

## Error classification

| Concern | partitionline | Porting note |
|---|---|---|
| Typed errors | `Error` (`src/error.rs`): `Io`, `Protocol`, `Broker{code,message}`, `UnknownTopic`, `NoLeader`, `Unsupported`, `Closed`, `Timeout`, `QueueFull`, `RecordTooLarge{size,max,config}`, `MaxPollInterval`, `Wakeup` | No `RDKafkaErrorCode` enum; broker codes ride on `Error::Broker` |
| Broker code | `Error::broker_code() -> Option<i16>` (`src/error.rs`) | Compare against `partitionline::error::*` constants |
| Retriable? | `Error::is_retriable()` (`src/error.rs`): not-leader, `LEADER_NOT_AVAILABLE`, not-enough-replicas, `REQUEST_TIMED_OUT`, coordinator moves, `NOT_CONTROLLER`, unknown-topic, share-session errors, plus all `Io`/`Timeout` | Internal retries already apply `retry.backoff.ms` exponential backoff; use this for your own retry loops |
| Oversize | `Error::RecordTooLarge` from `send`/`try_send` without waiting for `max_block` | Mirrors Java `ensureValidRecordSize` (`buffer.memory` then `max.request.size`) |
| Closed/wakeup | `Error::Closed`, `Error::Wakeup`, `Error::MaxPollInterval` | `MaxPollInterval` also triggers a heartbeat LeaveGroup |

## Intentional differences

1. **Feature gaps** — Kerberos is not implemented. See the
   [provider proposal](gssapi-boundary.md). zstd is an optional feature.
   Unkeyed records use round-robin; the sticky partitioner is not shipped.
   The current inventory is [protocol support](gaps.md).
2. **Defaults** — Earliest offset reset, auto-commit off, idempotence off,
   shorter delivery (30 s) / max-block (30 s) / session (10 s) timeouts,
   150 ms heartbeats, 5 ms linger, uncapped `max.poll.records`, 16 MiB fetch
   caps both levels, no auto topic creation. See the tables above for the
   exact Java/librdkafka comparison.
3. **Types over strings** — invalid configs fail at compile/type time where
   possible; there is no `ClientConfig::set("linger.ms", ...)` string bag.
4. **Not symbol-compatible** — rewrite call sites; do not expect
   `BorrowedMessage` lifetimes, `DrQueue`, or `rd_kafka_*` symbols.
5. **Auto-commit fencing** — interval commits cover previously delivered
   positions only; leave/close/unsubscribe never commit.

## What is not a drop-in

Two lists. The first has no equivalent to port. The second exists, and a
port has to keep the limit in the last column. Dispositions are the
[feature registry](../tests/conformance/features.json).

### No equivalent

| Feature | Registry entry | What to do instead |
|---|---|---|
| SASL GSSAPI / Kerberos | `auth.sasl_gssapi` (missing) | Pick SCRAM or OAUTHBEARER. The [provider proposal](gssapi-boundary.md) is waiting for approval. |
| Sticky unkeyed partitioner | `producer.sticky_partitioner` (missing) | Unkeyed records round-robin. KL05-10 is in progress. |
| Kafka Streams / Connect / C ABI | `streams.runtime`, `connect.framework`, `c_abi.librdkafka` (out of scope) | Rewrite the call sites. This crate is not a C library. |
| Broker-internal replication APIs | `broker_internal.*` (out of scope) | Not part of the client. |

### Present, with a limit

| Feature | Registry entry | Limit to keep |
|---|---|---|
| zstd compress and decompress | `codecs.zstd.encode`, `codecs.zstd.decode` (present, feature `zstd`) | Off by default. Levels are 1–19, default 3. The live matrix is a finite correctness check on Kafka 3.9.1, 4.1.0, 4.1.2, 4.2.1, and 4.3.1. See [zstd](zstd-spike.md). |
| Produce v13 | `producer.v13_wire` (present) | Uses Metadata topic IDs. Transactional Produce stays at v11 unless `transaction.version >= 2` is finalized and EndTxn v5 is supported. |
| Fetch v18 | `manual_consumer.v18_wire` (present) | Ordinary consumers omit the KIP-1166 high-watermark tag. |
| ListOffsets v11 | `manual_consumer.list_offsets_v11` (present) | Earliest-pending-upload is exposed. There is no live tiered-storage qualification. |
| Incremental Fetch | `manual_consumer.incremental_fetch_runtime` (present) | Sessions run on Fetch v7–v18. Older peers get a full fetch. Live qualification of reset histories is KL01-24. |
| OIDC refresh and SASL lifetime | `auth.sasl_oidc_refresh` (missing) | An owned `OidcTokenManager` refreshes before expiry. The shared connect-time cache does not. Producer, consumer, admin, and group loops do not reauthenticate on a timer. The pipeline closes when the SASL session expires. See [token lifecycle](auth-refresh.md). |
| Schema Registry | `schema_ecosystem.registry_client` (present in the companion) | Unpublished crate, read-only lookups. The application picks the serializer. See the [companion](../partitionline-schema/README.md). |
| ElectLeaders | `full_admin.elect_leaders` (present) | Per-partition results, controller route, one deadline. Unclean election needs v1 or later. No live unclean-election qualification. |
| DescribeQuorum | `full_admin.describe_quorum` (present) | Reads metadata partition 0 through broker bootstrap. Unknown timestamps stay `-1`. |
| Raft voters | `full_admin.add_raft_voter`, `full_admin.remove_raft_voter` (present) | Broker-forwarded. A v1 local append is not committed membership. No live membership-change qualification. |

## Verified behavioral equivalence

Proven, with the test that proves it:

- Keyed partitioning is byte-identical to Java murmur2
  (`src/partitioner.rs` unit vectors; `tests/client_api.rs` partition
  placement).
- Interval auto-commit never commits the batch about to be returned; crash
  after one capped poll cannot skip prefetched records
  (`tests/consumer_close_commit.rs`, KL03-07).
- Leave/close/unsubscribe never auto-commit; explicit async commit outcomes
  stay observable (`tests/consumer_close_commit.rs`, `tests/client_api.rs`
  `auto_commit`).
- Oversize records fail fast with `Error::RecordTooLarge` mirroring Java
  `ensureValidRecordSize` (`buffer.memory`, then `max.request.size`).

Explicitly **not** proven (do not assume parity):

- Codec corrupt-input budgets, `read_committed` aborted-marker filtering,
  `auto.offset.reset=None`, in-batch seek, and heartbeat/throttle scheduling
  are `partial` in the registry.
- TLS/SASL produce-vs-C and fetch/ack-latency-vs-rust-rdkafka writeups in
  `docs/benchmark.md` are recorded but **unsigned**; Suite HOLD remains
  (`docs/support.md`).

## Validation checklist

1. Point at a test cluster; run `cargo run --release --example roundtrip`.
2. Port produce path; confirm offsets and partitioning (murmur2 keyed,
   round-robin unkeyed — not Java-sticky).
3. Port consumer group; set `auto_offset_reset` and `auto_commit` explicitly
   (defaults are Earliest/off, unlike Java and librdkafka).
4. Raise `delivery_timeout` / `max_block` / `session_timeout` if your service
   was tuned to Java or librdkafka budgets.
5. Enable the `zstd` feature if the old client used zstd. GSSAPI has no
   replacement; use SCRAM or OAUTHBEARER.
6. Compare metrics via `Producer::metrics` / `Consumer::metrics` during soak.

## Further reading

- [`guide.md`](guide.md) — operator tour
- [`gaps.md`](gaps.md) — full capability inventory
- [`support.md`](support.md) — supported combinations for 0.1.x
- [`security.md`](security.md) — threat model
- [`../tests/conformance/features.json`](../tests/conformance/features.json) — feature registry (KL05-01)
