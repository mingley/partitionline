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
- Default features that link C (zstd, Kerberos / GSSAPI)

Supported broker/MSRV/OS combinations are listed in [`support.md`](support.md).
That matrix is operational honesty for 0.1.x, not a permanent 1.0 promise.

## Experimental

Nothing is marked `#[doc(hidden)]` experimental today. If an API is added for
a single deployment need before it hardens, mark it in rustdoc with
`Experimental:` and list it here in the same PR.

## Error categories (KL07-10)

Every public operation resolves to at most one `Error`, classified below.
The mapping is normative for 0.x callers: match the category, not the
`Display` text. `Display` strings are human diagnostics and may change on
any release.

| Category | Meaning | Caller action |
|---|---|---|
| Retryable | Transient broker or transport state; the same call may succeed later. `Error::is_retriable()` is exactly this set: `Io`, `Timeout`, and broker codes `NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE`, `NOT_ENOUGH_REPLICAS`, `NOT_ENOUGH_REPLICAS_AFTER_APPEND`, `REQUEST_TIMED_OUT`, `COORDINATOR_LOAD_IN_PROGRESS`, `COORDINATOR_NOT_AVAILABLE`, `NOT_COORDINATOR`, `NOT_CONTROLLER`, `UNKNOWN_TOPIC_OR_PARTITION`, `SHARE_SESSION_NOT_FOUND`, `INVALID_SHARE_SESSION_EPOCH`. | Retry with backoff, or let the client's internal loops retry (produce, fetch, admin and coordinator RPCs already do, bounded by the configured timeouts). |
| Abort-required | A transactional send failed in a way that poisons the open transaction: `UNKNOWN_PRODUCER_ID`, `INVALID_PRODUCER_ID_MAPPING` or `INVALID_PRODUCER_EPOCH` on transactional produce. The send is failed and an epoch bump is latched. | Call `abort_transaction()` before any further transactional send; `commit_transaction()` without an abort is not valid from this state. Producing outside a transaction is rejected with a usage (`Protocol`) error instead. |
| Fatal | The client or handshake cannot proceed as configured: `Closed` (client shut down), constructor failures (bootstrap, TLS, SASL handshake, ApiVersions), and authentication failures such as `SASL_AUTHENTICATION_FAILED`. | Fix configuration/credentials and construct a new client. Do not retry the same instance in a hot loop. |
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

Known classification gaps (bounded repair cards proposed in
[KL07-10 evidence](plan/evidence/KL07-10.json), not fixed here):
`PRODUCER_FENCED`, `TRANSACTION_ABORTABLE`, `INVALID_TXN_STATE` and
`CONCURRENT_TRANSACTIONS` currently fail the send like any other
non-retriable broker error instead of latching abort-required state;
`Error::Closed` displays as `producer closed` even on consumer/admin/net
paths.

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

A raw `acks` outside `{0, 1, -1}` is sent on the wire (broker answers
`INVALID_REQUIRED_ACKS`); negative consumer fetch bounds are likewise sent
raw. `transaction_timeout` above the broker's `transaction.max.timeout.ms`
fails broker-side with `INVALID_TRANSACTION_TIMEOUT`. Bounded constructor
validation for the client-side cases is proposed in the KL07-10 evidence.

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
