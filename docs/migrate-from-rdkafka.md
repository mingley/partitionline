# Migrate from rust-rdkafka / librdkafka (and Java)

partitionline is **not** a drop-in for `rd_kafka_*`, rust-rdkafka types, or
the Java client's string property bag. It is a pure-Rust client with
Java-shaped typed builders. Use this map when porting a service.

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

**Source pin:** this document was verified against source
`ca50ca1103e37b5159875b8b5562c7cc1087c335` (crate version `0.1.0` in
`Cargo.toml`). Line numbers below are relative to that revision.

**Current source versus the published crate:** crates.io `partitionline`
`0.1.0` predates this source (14 files under `src/` differ). The configuration
defaults mapped here were diffed field-by-field between the packed `0.1.0`
crate and current source: all mapped defaults are identical, with two
source-only additions — `ConsumerConfig::buffer_memory` (new 32 MiB fetch
buffer cap) and the hidden `ProducerConfig::pre_send_fault` test hook. Runtime
behavior (auto-commit fencing, batching, retry paths) has evolved since the
cut, so adopters on the published `0.1.0` get the same defaults but the older
runtime. The examples below compile against the packed `0.1.0` dependency.

## Dependency

```toml
# before
rdkafka = { version = "0.39", features = ["cmake-build"] }

# after (crates.io 0.1.0)
partitionline = "0.1"
```

No C toolchain, librdkafka, OpenSSL, or cmake-build feature required for the
default feature set (`default = []` in `Cargo.toml`; the only optional feature
is `tracing`).

## API shape

### Concepts

| librdkafka / rust-rdkafka | Java | partitionline |
|---|---|---|
| `FutureProducer` / `BaseProducer` | `KafkaProducer` | `Producer` (`Producer::new(cfg)`, `src/producer.rs:1113`) |
| `StreamConsumer` / `BaseConsumer` | `KafkaConsumer` (manual) | `Consumer` (`Consumer::new(cfg)`, `src/consumer.rs:1333`) |
| group consumer | `KafkaConsumer` (subscribed) | `ConsumerGroup::join_*` (`src/group.rs:525`) |
| — (KIP-932) | `ShareConsumer` | `ShareGroup::join_*` (`src/share.rs:559`) |
| `AdminClient` | `Admin` | `Admin` (`Admin::new(cfg)`, `src/admin.rs:3000`) |
| String property bag (`ClientConfig::set`) | `Properties` | Typed builders: `ProducerConfig`, `ConsumerConfig`, `AdminConfig` |
| `Message` / `BorrowedMessage` | `ConsumerRecord` / `ProducerRecord` | `FetchedRecord` / `ShareRecord` / `ProduceRecord` (owned; no borrow lifetimes) |
| Delivery callbacks / `DrQueue` polling | `Callback` / `Future<RecordMetadata>` | `send` future, or `try_send` + `flush` (see Callbacks) |
| `commit` / committed offsets | `commitSync` / `committed` | `commit_with_metadata`, `committed`, `OffsetAndMetadata` |
| `ClientContext` stats | `Metrics` | `Producer::metrics` / `Consumer::metrics` / `ShareGroup::metrics` / `Admin::metrics` |

### Task map

| Task | rust-rdkafka-ish | partitionline |
|---|---|---|
| Produce one | `send` + delivery future | `Producer::send` (`src/producer.rs:1434`) |
| High throughput | poll delivery queue | `try_send` + `flush` (`src/producer.rs:1579,2000`) |
| Assign partitions | `assign` | `Consumer::assign` / `assign_topic` / `assign_partitions` (`src/consumer.rs:1406`) |
| Subscribe group | `subscribe` | `ConsumerGroup::join_topics` (or sticky / cooperative / KIP-848 variants) |
| Pattern subscribe | `subscribe` with regex | `join_matching` / `join_sticky_matching` / `join_consumer_matching` (re-list on poll) |
| Poll records | `recv` / `poll` | `fetch` / `fetch_timeout` or `group.poll` / `poll_timeout` → `ConsumerRecords` |
| Commit | `commit_message` | `commit_with_metadata(recs.next_offsets())` |
| Async commit | `commit` (async) | `commit_async` / `commit_async_with` (queued `OffsetCommit` on poll / leave) |
| Transactions | init / begin / send_offsets / commit | same names on `Producer`; see `examples/eos.rs` |
| Create topic | admin create | `Admin::create_topics` |
| Wake a blocked poll | — | `Consumer::wakeup` / `WakeupHandle` (`src/consumer.rs:1320`; interrupts in-flight Fetch) |
| Share-group ack | — | `ShareGroup::{accept, release, reject}` |

## Partitioning

| Knob | partitionline | Default (source) | Java | librdkafka |
|---|---|---|---|---|
| Keyed records | `DefaultPartitioner` (`src/partitioner.rs:20`) | murmur2, `partition_for_key` (`src/partitioner.rs:137`) | murmur2 (`DefaultPartitioner`) — same | `murmur2` partitioner — same |
| Unkeyed records | `DefaultPartitioner` | round-robin from 0 | sticky (`StickyPartitioner`, KIP-794) — **differs** | `consistent_random` default — **differs** |
| Custom | `Partitioner` trait + `ProducerConfig::partitioner` (`src/producer.rs:383`) | — | `partitioner.class` | `partitioner_cb` |
| Explicit pin | `ProduceRecord::partition` (`src/producer.rs:595`) | skips the partitioner | `ProducerRecord` partition ctor | `TopicPartition` |

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
byte-for-byte, proven by unit vectors in `src/partitioner.rs:148`
(`murmur2(b"kafka") == -798_503_068`). Unkeyed placement is **not**
equivalent to either Java (sticky batching) or librdkafka, and the sticky
partitioner is an explicit gap (`producer.sticky_partitioner`, missing in the
[feature registry](../tests/conformance/features.json)).

## Acks, idempotence, transactions

| Knob | partitionline | Default (source) | Java | librdkafka |
|---|---|---|---|---|
| `acks` | `ProducerConfig::acks(Acks)` (`src/producer.rs:290`); stored as `i16` | `1` (`Acks::Leader`, `src/config.rs:112`, `src/producer.rs:220`) | `1` documented, forced to `all` when idempotence is on | `request.required.acks=-1` (all) — **differs** |
| `enable.idempotence` | `ProducerConfig::idempotent(bool)` (`src/producer.rs:361`) | `false` (`src/producer.rs:245`) | `true` (since 3.0) — **differs** | `false` — same |
| Idempotent effects | `Producer::new` (`src/producer.rs:1141`) | forces `acks=all`, `max_in_flight ≤ 5`, one TCP conn per partition | same forcing | `enable.idempotence=true` forces `acks=all`, `max.in.flight ≤ 5` |
| `max.in.flight` | `ProducerConfig::max_in_flight` (`src/producer.rs:356`) | `16` (`src/producer.rs:244`) | `5` — **differs** | `1000000` — **differs** |
| `transactional.id` | `ProducerConfig::transactional_id` (`src/producer.rs:370`) | none; implies idempotence | `null`; implies idempotence — same | same shape |
| `transaction.timeout.ms` | `ProducerConfig::transaction_timeout` (`src/producer.rs:377`) | 60 s (`src/producer.rs:247`) | 60 s — same | `transaction.timeout.ms=60000` — same |

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
| `request.timeout.ms` | `Producer/Consumer/AdminConfig::request_timeout` | 30 s (`src/producer.rs:226`) | 30 s — same | `socket.timeout.ms=60000` (different shape) |
| `delivery.timeout.ms` | `ProducerConfig::delivery_timeout` (`src/producer.rs:428`) | **30 s** (`src/producer.rs:227`) | 120 s — **differs (shorter)** | `message.timeout.ms=300000` — **differs (shorter)** |
| `max.block.ms` | `ProducerConfig::max_block` (`src/producer.rs:439`) | **30 s** (`src/producer.rs:228`) | 60 s — **differs (shorter)** | `message.timeout.ms` + queue-full errors (different shape) |
| `linger.ms` | `ProducerConfig::linger` (`src/producer.rs:297`) | 5 ms (`src/producer.rs:221`) | 0 ms — **differs** | `queue.buffering.max.ms=5` — same |
| Batching | `batch_records` / `batch_bytes` (`src/producer.rs:304,311`) | 32_768 records / 1_000_000 bytes (`src/producer.rs:222`) | `batch.size=16384` bytes — **differs** | `batch.size=1000000` — same bytes |
| `buffer.memory` | `ProducerConfig::buffer_memory` (`src/producer.rs:323`) | 32 MiB (`src/producer.rs:224`) | 33554432 — same | `queue.buffering.max.kbytes=1048576` (different shape) |
| `max.request.size` | `ProducerConfig::max_request_size` (`src/producer.rs:335`) | 1 MiB (`src/producer.rs:225`) | 1048576 — same | `message.max.bytes=1000000` — same |
| `retry.backoff.ms` | `retry_backoff` | 100 ms (`src/config.rs:16`) | 100 ms — same | `retry.backoff.ms=100` — same |
| `retry.backoff.max.ms` | `retry_backoff_max` | 1 s (`src/config.rs:18`) | 1000 ms — same | `retry.backoff.max.ms=1000` — same |
| `reconnect.backoff.ms` / max | `reconnect_backoff[_max]` | 50 ms / 1 s (`src/config.rs:20`) | 50 / 1000 ms — same | `reconnect.backoff.ms=100`, `reconnect.backoff.max.ms=10000` — **differs** |
| `connections.max.idle.ms` | `connections_max_idle` | 9 min (`src/config.rs:24`) | 540000 — same | `connections.max.idle.ms=540000` — same |
| `metadata.max.age.ms` | `metadata_max_age` | 5 min (`src/producer.rs:231`) | 300000 — same | `metadata.max.age.ms=300000` — same |
| `session.timeout.ms` | `ConsumerConfig::session_timeout` (`src/consumer.rs:396`) | **10 s** (`src/consumer.rs:260`) | 45000 — **differs (shorter)** | 45000 — **differs (shorter)** |
| `heartbeat.interval.ms` | `ConsumerConfig::heartbeat_interval` (`src/consumer.rs:404`) | **150 ms** (`src/consumer.rs:261`) | 3000 — **differs** | 3000 — **differs** |
| `max.poll.interval.ms` | `ConsumerConfig::max_poll_interval` (`src/consumer.rs:435`) | 300 s (`src/consumer.rs:265`) | 300000 — same | 300000 — same |
| `max.poll.records` | `ConsumerConfig::max_poll_records` (`src/consumer.rs:364`) | none (uncapped, `src/consumer.rs:258`) | 500 — **differs** | — |
| `fetch.max.wait.ms` | `ConsumerConfig::max_wait_ms` (`src/consumer.rs:296`) | 500 ms (`src/consumer.rs:250`) | 500 — same | `fetch.wait.max.ms=500` — same |
| `fetch.min.bytes` | `ConsumerConfig::min_bytes` (`src/consumer.rs:302`) | 1 (`src/consumer.rs:251`) | 1 — same | 1 — same |
| `fetch.max.bytes` | `ConsumerConfig::fetch_max_bytes` (`src/consumer.rs:322`) | **16 MiB** (`src/consumer.rs:252`) | 50 MiB — **differs** | `fetch.message.max.bytes=1048576` — **differs** |
| `max.partition.fetch.bytes` | `ConsumerConfig::max_partition_fetch_bytes` (`src/consumer.rs:329`) | **16 MiB** (`src/consumer.rs:253`) | 1 MiB — **differs** | — |

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
| `auto.offset.reset` | `ConsumerConfig::auto_offset_reset` (`src/consumer.rs:357`, `AutoOffsetReset`, `src/config.rs:435`) | **Earliest** (`src/consumer.rs:257`) | `latest` — **differs** | `largest` — **differs** |
| `enable.auto.commit` | `ConsumerConfig::auto_commit` (`src/consumer.rs:421`) | **false** (`src/consumer.rs:263`) | `true` — **differs** | `true` — **differs** |
| `auto.commit.interval.ms` | `ConsumerConfig::auto_commit_interval` (`src/consumer.rs:428`) | 5 s (`src/consumer.rs:264`) | 5000 — same | 5000 — same |
| `isolation.level` | `ConsumerConfig::isolation` (`src/consumer.rs:336`, `src/config.rs:156`) | `read_uncommitted` (`src/consumer.rs:254`) | same | same |
| `allow.auto.create.topics` | `allow_auto_topic_creation` | **false** (`src/consumer.rs:272`) | `true` — **differs** | — |
| Committed lookup | `ConsumerGroup::committed[_timeout]` (`src/group.rs:1075`) | Java `committed(Duration)` shape | same shape | `committed` |

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

Auto-commit contract (KL03-07, `src/group.rs:1173`, proven by
`tests/consumer_close_commit.rs`): when enabled, the interval commit runs
before fetch and stores the **previously delivered** positions — the batch
about to be returned is not committed until a subsequent poll, so a crash
after one capped poll cannot skip prefetched records on rejoin. Zero interval
commits after delivery on the same poll when records were returned.
Leave/close/unsubscribe **never** auto-commit. Known partials:
`auto.offset.reset=None` unconditionally resets to log start on
`OFFSET_OUT_OF_RANGE` (`manual_consumer.auto_offset_reset`), seeking inside
an existing batch redelivers earlier records (`manual_consumer.seek`), and
aborted control markers cause subsequent committed batches under the same PID
to be discarded under `read_committed`
(`transactions.read_committed_consumer`).

## Codecs

| Codec | partitionline | Java | librdkafka |
|---|---|---|---|
| none | default (`Compression::None`, `src/protocol/records.rs:183`) | default — same | `none` default — same |
| gzip | `Compression::Gzip` (`flate2` Rust backend) | same wire format | same wire format |
| snappy | `Compression::Snappy` (`snap`; snappy-java framing on produce, raw on fetch) | same wire format | same wire format |
| lz4 | `Compression::Lz4` (`lz4_flex` frame, independent 64 KiB blocks) | same wire format | same wire format |
| zstd | **not supported** (`codecs.zstd.decode/encode/wire_helper`, missing) | `zstd` | `zstd` |

```rust
use partitionline::{Compression, ProducerConfig};

let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
    .compression(Compression::Lz4);
```

zstd stays out because the Kafka ecosystem codec is `libzstd` C, and
`deny.toml:45` bans `zstd-sys` / `libzstd-sys` from the feature set (pure-Rust
zstd is tracked as research in `docs/zstd-spike.md`). Plan a compression
alternative before porting zstd topics. Decompression of the supported codecs
currently allocates without a decoded-byte budget (`codecs.gzip/snappy/lz4`,
partial).

## Auth and TLS

| Mechanism | partitionline | Java / librdkafka |
|---|---|---|
| No auth | default (all `sasl_*` are `None`, TLS `None`) | same shape |
| PLAIN | `Sasl::plain(user, pass)` (`src/config.rs:532`) | same |
| SCRAM-SHA-256 | `Sasl::scram_sha256(user, pass)` (`src/config.rs:540`, RFC 5802/7677, no C) | same |
| SCRAM-SHA-512 | `Sasl::scram_sha512(user, pass)` (`src/config.rs:548`) | same |
| OAUTHBEARER | `Sasl::oauthbearer(principal)` (`src/config.rs:556`, RFC 7628 unsecured JWT) | same (`enable.sasl.oauthbearer.unsecure.jwt`) |
| OIDC token endpoint | `Sasl::oidc(OidcConfig)` (`src/config.rs:564`, `src/protocol/oidc.rs:24`) | same shape |
| GSSAPI / Kerberos | **not supported** (`auth.sasl_gssapi`, missing; blocked on Cyrus SASL C) | supported |
| TLS | `TlsConfig` (`src/net.rs:232`, rustls; `ca_pem` / `server_name` / `client_identity` for mTLS; Mozilla roots by default) | `ssl.*` (JVM TLS / OpenSSL) |
| Token refresh | proactive refresh before expiry is **missing** (`auth.sasl_oidc_refresh`) | supported |

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
| Rebalance callback | `ConsumerConfig::on_rebalance(f)` (`src/consumer.rs:411`) + `ConsumerInterceptor::on_commit` |
| — | `ProducerInterceptor::{on_send, on_ack, on_error, close}` (`src/interceptor.rs:11`) |
| — | `ConsumerInterceptor::{on_consume, on_commit, close}` (`src/interceptor.rs:28`); filtering in `on_consume` does not rewind positions |

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

Cancellation differs from Java futures: dropping a `send` future does **not**
dequeue the record — once accepted into `buffer_memory`, delivery may
continue and the caller outcome stays ambiguous until `flush`/`close`
(`src/producer.rs:1428`).

## Error classification

| Concern | partitionline | Porting note |
|---|---|---|
| Typed errors | `Error` (`src/error.rs:10`): `Io`, `Protocol`, `Broker{code,message}`, `UnknownTopic`, `NoLeader`, `Unsupported`, `Closed`, `Timeout`, `QueueFull`, `RecordTooLarge{size,max,config}`, `MaxPollInterval`, `Wakeup` | No `RDKafkaErrorCode` enum; broker codes ride on `Error::Broker` |
| Broker code | `Error::broker_code() -> Option<i16>` (`src/error.rs:110`) | Compare against `partitionline::error::*` constants |
| Retriable? | `Error::is_retriable()` (`src/error.rs:119`): not-leader, `LEADER_NOT_AVAILABLE`, not-enough-replicas, `REQUEST_TIMED_OUT`, coordinator moves, `NOT_CONTROLLER`, unknown-topic, share-session errors, plus all `Io`/`Timeout` | Internal retries already apply `retry.backoff.ms` exponential backoff; use this for your own retry loops |
| Oversize | `Error::RecordTooLarge` from `send`/`try_send` without waiting for `max_block` | Mirrors Java `ensureValidRecordSize` (`buffer.memory` then `max.request.size`) |
| Closed/wakeup | `Error::Closed`, `Error::Wakeup`, `Error::MaxPollInterval` | `MaxPollInterval` also triggers a heartbeat LeaveGroup |

## Intentional differences

1. **No C** — zstd and Kerberos stay out of default features (`deny.toml`
   bans `zstd-sys`).
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

## Unsupported features

No equivalent exists today; do not port these call sites as-is:

| Feature | Registry entry | Note |
|---|---|---|
| zstd compress/decompress | `codecs.zstd.decode`, `codecs.zstd.encode`, `codecs.zstd.wire_helper` (missing) | Blocked on C (`deny.toml:45`); see `gaps.md` |
| SASL GSSAPI / Kerberos | `auth.sasl_gssapi` (missing) | Blocked on Cyrus SASL C |
| Proactive OIDC/token refresh | `auth.sasl_oidc_refresh` (missing) | Re-auth on `session_lifetime_ms` not implemented |
| Sticky unkeyed partitioner | `producer.sticky_partitioner` (missing) | Unkeyed records round-robin instead |
| Produce v13 (topic IDs) | `producer.v13_wire` (missing) | Negotiates v3–v12 |
| Fetch v18, ListOffsets v11 | `manual_consumer.v18_wire`, `manual_consumer.list_offsets_v11` (missing) | Fetch v4–v17, ListOffsets v1–v10 |
| Incremental fetch sessions | `manual_consumer.incremental_fetch_runtime` (missing) | Always sends `FetchMetadata::LEGACY` |
| Schema Registry client | `schema_ecosystem.registry_client/cache/avro/protobuf/json_schema` (missing) | Companion design only; `partitionline-schema` not published |
| ElectLeaders / DescribeQuorum / Raft voters | `full_admin.elect_leaders/describe_quorum/add_raft_voter/remove_raft_voter` (missing) | Not exposed on `Admin` |
| Kafka Streams / Connect / C ABI | `streams.runtime`, `connect.framework`, `c_abi.librdkafka` (out of scope) | Never planned for this crate |
| Broker-internal replication APIs | `broker_internal.*` (out of scope) | Not client surface |

## Verified behavioral equivalence

Proven, with the test that proves it:

- Keyed partitioning is byte-identical to Java murmur2
  (`src/partitioner.rs:148` unit vectors; `tests/client_api.rs` partition
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
5. If you used zstd or GSSAPI, plan compression/auth alternatives first.
6. Compare metrics via `Producer::metrics` / `Consumer::metrics` during soak.

## Further reading

- [`guide.md`](guide.md) — operator tour
- [`gaps.md`](gaps.md) — full capability inventory
- [`support.md`](support.md) — supported combinations for 0.1.x
- [`security.md`](security.md) — threat model
- [`../tests/conformance/features.json`](../tests/conformance/features.json) — feature registry (KL05-01)
