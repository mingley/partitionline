# API stability

While the crate is **0.x**, nothing is permanently frozen. This document
tells callers what is safe to build on versus what may churn.

## Stable (prefer these)

Breaking these is a `0.MINOR` bump and a CHANGELOG entry:

| Surface | Notes |
|---|---|
| `Producer`, `ProducerConfig`, `ProduceRecord`, `RecordMetadata` | Produce path |
| `Consumer`, `ConsumerConfig`, `ConsumerRecords`, `FetchedRecord` | Manual fetch |
| `ConsumerGroup`, `ConsumerGroupMetadata`, group join helpers | Classic / KIP-848 |
| `ShareGroup`, `ShareRecords`, acknowledge helpers | KIP-932 |
| `Admin`, `AdminConfig`, `NewTopic`, common admin options | Java-shaped admin |
| `Sasl`, `TlsConfig`, `OidcConfig`, `Acks`, `IsolationLevel`, `Compression` | Config enums |
| `Error`, `Result`, `ApiError` | Error surface |
| `TopicPartition`, `OffsetAndMetadata`, `OffsetAndTimestamp` | Shared types |
| `Partitioner` / `DefaultPartitioner` | Custom partitioning |
| `ProducerInterceptor` / `ConsumerInterceptor` | Interceptors |
| `ProducerMetrics` / `ConsumerMetrics` / `ShareMetrics` / `AdminMetrics` | Snapshots |
| `CLIENT_NAME`, `CLIENT_VERSION` | ApiVersions identity |

Method-level rustdoc that names a Java `Admin` / consumer / producer call is
part of the contract for those methods.

## Evolving

Expect additive churn; renaming or removing requires a CHANGELOG note:

| Surface | Notes |
|---|---|
| `partitionline::protocol` | Wire codecs; public for tests and advanced tools |
| Large `admin` type soup (every `*Request` / `*Response` re-export) | Prefer high-level `Admin` methods |
| New Kafka API versions and KIP helpers | Added as brokers ship them |
| Metrics field sets | Counters may grow; existing fields stay |

## Out of scope (this crate)

- Schema Registry (companion crate only; see `gaps.md`)
- Drop-in `rd_kafka_*` / rust-rdkafka types
- Native SASL backends in default features

Supported broker and platform combinations are listed in [`support.md`](support.md).
Current builds use the latest stable Rust. The 0.1.x support policy may change
before 1.0.

## Experimental

Nothing is marked `#[doc(hidden)]` experimental today. If an API is added for
a single deployment need before it hardens, mark it in rustdoc with
`Experimental:` and list it here in the same PR.

## Proposed owned delivery API (KL10-07; awaiting maintainer review)

This is an additive proposal, not an available API. The existing `send`,
`send_all`, and `try_send` signatures and behavior remain stable. KL10-08
must wait for recorded maintainer approval of this contract.

Proposed signatures (future implementation acceptance commands):

```text
Producer::try_send_with_delivery(&self, record: ProduceRecord) -> Result<Delivery>
Delivery: Future<Output = Result<RecordMetadata>> + Send + 'static
```

`Delivery` is an opaque owned future. It contains no borrow of `Producer`
or the record and can be moved into another task. Returning it admits the
record eagerly: validation, partition selection, byte reservation, and
enqueueing finish before the synchronous call returns. There is no hidden
wait for `max_block`, metadata, a connection, or buffer capacity.

Admission errors return synchronously. `QueueFull` means no admission:
buffer/queue capacity or a ready metadata/leader route was unavailable.
The latter may nudge background metadata/connection setup, as `try_send`
does today. Oversized records return `RecordTooLarge`; invalid usage returns
`Protocol`; closed and fatally fenced producers retain their existing
errors. A failed enqueue releases its byte reservation. Rejected calls do
not count as queued records or create a completion. The record is consumed
on both success and failure, matching `try_send`; callers that need a retry
copy retain a clone before calling, rather than assuming a returned record.

For consecutive successful calls from one caller to the same partition,
admission order is call order, regardless of when completions are polled.
There is no ordering promise between concurrent callers or across
partitions. Wire retries and broker ordering retain the existing
idempotence/in-flight guarantees; admission order alone cannot eliminate
non-idempotent retry reordering. Records still pass through the configured
partitioner and ordered `on_send` interceptors before admission. Interceptor
callbacks run synchronously and can themselves consume caller CPU time;
"no hidden wait" refers to client admission waits, not arbitrary callbacks.

Every accepted record has exactly one terminal completion: metadata for the
configured acknowledgment mode, or the actual terminal delivery error.
With `acks=0`, completion reports a successful write with offset `-1`,
never broker acknowledgment. A broker acknowledgment inside a transaction
does not establish a committed transaction. Request/delivery deadlines
continue to bound retries, and produce timeouts remain ambiguous delivery.

Dropping or never polling `Delivery` does not cancel the accepted record,
release its byte budget early, or suppress delivery/error interceptors.
Its reservation ends on the existing terminal ownership path. `flush`
includes accepted records even if their completions are dropped; observing
a completion does not remove the record from flush/error accounting.
`close` and `close_timeout` drain or terminate accepted work under their
existing contracts and resolve outstanding completions; dropping the last
producer still stops its workers. A shutdown error without an acknowledgment
does not prove the broker failed to append the record.

Idempotent sequences are assigned by the existing worker path. Transaction
admission uses the same guards as `try_send`; the new API cannot begin,
commit, abort, recover, or clear fencing implicitly. Transaction commit
continues to drain accepted work. Dropping its record completions neither
commits nor aborts the transaction. Existing borrowed `send()` remains
sufficient when the caller can poll bounded futures in its own task and
needs admission to wait for metadata/capacity. Its async body admits on
poll, so an unpolled future fixes neither admission time nor queue order
and cannot be moved into a `'static` task without retaining its producer.

KL10-08 acceptance must compile a `Send + 'static` trait assertion and a
spawned owned completion; pin call-order versus reverse-poll order; test
buffer-full/route-unavailable/closed rejection without admission; prove
dropped completions retain byte ownership until delivery; and exercise
flush, bounded close, interceptors, idempotence, transaction commit/abort,
and fatal fencing. Those are future checks, not executed evidence.

## Error categories (KL07-10)

Every public operation resolves to at most one `Error`, classified below.
The mapping is normative for 0.x callers: match the category, not the
`Display` text. `Display` strings are human diagnostics and may change on
any release.

| Category | Meaning | Caller action |
|---|---|---|
| Retryable | Transient broker or transport state; the same call may succeed later. `Error::is_retriable()` is exactly this set: `Io`, `Timeout`, and broker codes `NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE`, `NOT_ENOUGH_REPLICAS`, `NOT_ENOUGH_REPLICAS_AFTER_APPEND`, `REQUEST_TIMED_OUT`, `COORDINATOR_LOAD_IN_PROGRESS`, `COORDINATOR_NOT_AVAILABLE`, `NOT_COORDINATOR`, `NOT_CONTROLLER`, `UNKNOWN_TOPIC_OR_PARTITION`, `SHARE_SESSION_NOT_FOUND`, `INVALID_SHARE_SESSION_EPOCH`. | Retry with backoff, or let the client's internal loops retry (produce, fetch, admin and coordinator RPCs already do, bounded by the configured timeouts). |
| Abort-required | A failed transactional Produce poisons the open transaction: `UNKNOWN_PRODUCER_ID`, `INVALID_PRODUCER_EPOCH`, `TRANSACTION_ABORTABLE`, `INVALID_TXN_STATE` or `CONCURRENT_TRANSACTIONS`. Commit stays invalid until abort; recovery uses a broker-authorized epoch when supported. | Abort before starting the next transaction. Existing sends may still enter the poisoned open transaction, so stop sending until abort; they cannot make it committable. Producing outside a transaction is a usage (`Protocol`) error. |
| Fatal | The client or handshake cannot proceed: `Closed`, constructor/authentication failures, or terminal transactional Produce errors such as `PRODUCER_FENCED`, `INVALID_PRODUCER_ID_MAPPING` and `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`. | Fix configuration/credentials or ownership and construct a new client. A fenced producer cannot recover through abort or a locally invented epoch. |
| Unsupported | Broker or feature lacks a required capability: `Error::Unsupported` from ApiVersions negotiation (constructor fails), per-operation version gates, or a SASL mechanism the broker did not advertise. | Upgrade the broker, enable the API, or stop calling the operation. Blind retry cannot succeed. |
| Timeout | A configured deadline expired: `request_timeout` (one RPC), `delivery_timeout` (queue until ack), `max_block` (send admission), or join/assignment waits. `Timeout` never names which deadline; the caller knows which wait it issued. | Treat produce timeouts as ambiguous delivery (next row). Other timeouts are retryable once the stall clears. |
| Ambiguous-delivery | The record may or may not have been appended: any produce `Timeout` after retries, dropping a `send` future after the record entered `buffer_memory`, or `acks=0` (no ack by design). Non-idempotent retries may duplicate. | Reconcile out of band (offsets, idempotency keys) or enable idempotence so retries deduplicate. Idempotent produce re-inits the epoch on `UNKNOWN_PRODUCER_ID` and requeues rather than failing. |

Two control-flow outcomes are not failures: `Wakeup` (retry `fetch`/`poll`;
a wakeup was requested) and `MaxPollInterval` (the member already left the
group; rejoin instead of continuing the poll loop).

Caller-bug rejects fail the call but leave the client usable: `RecordTooLarge`
(fix the record or raise `max_request_size`/`buffer_memory`), `QueueFull`
(wait or raise `buffer_memory`), and usage `Protocol` errors (empty
`group.id`, missing subscription, negative seek offset, producing outside a
transaction). Metadata-state errors `UnknownTopic`/`NoLeader` surface from
metadata-query helpers when the cache has no entry; treat them as
retry-after-refresh. They are intentionally outside `is_retriable()`, which
covers only wire/transport outcomes.

Transactional classification follows the operation-specific Apache handler
tables pinned by [KL03-10](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/plan/evidence/KL03-10.json). A Produce fencing
error is terminal; abort-required errors clear through abort. Classic
coordinators recover using InitProducerId with the last authorized identity;
transaction V2 uses the EndTxn response identity without a second re-init.
Repeating `init_transactions()` only checks initialization; it neither clears
these states nor obtains a new identity.
The [KL07-14 public-API checks](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/plan/evidence/KL07-14.json) cover both paths.
These categories describe client state separately from `is_retriable()`;
a broker code's disposition can differ between Produce and EndTxn.
`Error::Closed` displays the client-neutral `client closed` on all paths.

## Configuration contract (KL07-10)

Builders (`ProducerConfig::bootstrap(..).acks(..)` and friends) are the
stable construction path. Raw `pub` fields stay writable, but construct via
`Default` plus overrides (`..Default::default()`): new fields may be added
on a `0.MINOR` with a CHANGELOG note, which breaks exhaustive struct
literals. Renaming, removing, or retyping a field is breaking under the
Stable rules above. Defaults below are frozen on the same terms; any change
ships as `0.MINOR` plus a CHANGELOG note.

### Rejected at construction or connect

| Combination | Outcome |
|---|---|
| More than one of `sasl_plain`, `sasl_scram`, `sasl_scram_sha512`, `sasl_oauthbearer`, `sasl_oauthbearer_oidc` set (raw fields; the `sasl(..)` builder replaces) | `Protocol` error: set only one |
| Empty bootstrap list, or every entry blank/unparseable | `Protocol` error: no bootstrap servers |
| Raw producer `acks` outside `{0, 1, -1}` | `Protocol` error before bootstrap validation or network I/O |
| Negative consumer `max_wait_ms`, `min_bytes`, `max_bytes`, or `max_partition_fetch_bytes` | `Protocol` naming the field before bootstrap validation or network I/O; zero remains valid |
| Empty `group.id` at group/share join | `Protocol` error naming `group.id` |
| Join or poll with no topics and no subscription/assignment | `Protocol` error: no topics / not subscribed |
| Broker lacks a required ApiVersions range (e.g. no usable Produce/Fetch/Metadata) | `Unsupported` naming the API |

### Normalized, not rejected

`transactional_id` set implies idempotence; idempotence forces `acks=-1`
and `max_in_flight <= 5` at construction. `retry_backoff_max` below
`retry_backoff` (and likewise for reconnect) is raised to the base, with no
jitter. `connections(..)` / `max_in_flight(..)` builders clamp to at least
1 and construction clamps connections again. `max_poll_interval` zero means
no limit (zero sends `i32::MAX` on join). `connections_max_idle` zero never
closes idle connections. `metadata_max_age` zero refreshes on every lookup.

### Passed through, not validated (caller/broker enforced)

`transaction_timeout` above the broker's `transaction.max.timeout.ms`
fails broker-side with `INVALID_TRANSACTION_TIMEOUT`. Bounded constructor
validation for raw acknowledgments and consumer fetch bounds is enforced
under KL07-12 and KL07-15.

### Defaults that differ from Java

| Field | This crate | Java |
|---|---|---|
| Producer `delivery_timeout` | 30 s | 120 s |
| Producer `max_block` | 30 s | 60 s |
| Producer `linger` | 5 ms | 0 |
| Producer `batch_records` / `batch_bytes` | 32,768 / 1,000,000 | n/a (`batch.size` 16 KiB bytes) |
| Producer `connections` / `max_in_flight` | 8 / 16 | n/a (crate-specific) |
| Consumer `auto_offset_reset` | `earliest` | `latest` |
| Consumer `enable_auto_commit` | `false` | `true` |
| Consumer `heartbeat_interval` | 150 ms | 3 s |
| Consumer `max_partition_fetch_bytes` | 16 MiB | 1 MiB |
| Consumer `max_bytes` (`fetch.max.bytes`) | 16 MiB | 50 MiB |
| Consumer `allow_auto_topic_creation` | `false` | `true` (consumer) |
| Retry waits | exponential, no jitter | up to 20% jitter |
| `connections_max_idle` zero | never closes | closes immediately |

Matching Java: `acks=1`, `buffer.memory` 32 MiB, `max.request.size` 1 MiB,
`request.timeout.ms` 30 s, `connect.timeout.ms` 10 s,
`retry.backoff.ms`/`max` 100 ms/1 s, `reconnect.backoff.ms`/`max` 50 ms/1 s,
`connections.max.idle.ms` 9 min, `metadata.max.age.ms` 5 min,
`session.timeout.ms` 10 s, `max.poll.interval.ms` 5 min,
`auto.commit.interval.ms` 5 s, `transaction.timeout.ms` 60 s,
`isolation.level` read-uncommitted, no compression, `client.id`
`partitionline`, and redacted SASL passwords in `Debug`.
