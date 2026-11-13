# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to the 0.x policy in [`docs/RELEASE.md`](docs/RELEASE.md).

## [Unreleased]

Unpublished repository work (`partitionline-broker` and `partitionline-schema`
remain `publish = false`; see [`docs/STATUS.md`](docs/STATUS.md) and
[`docs/RELEASE.md`](docs/RELEASE.md)).

## [0.2.0] - 2026-11-12

Major protocol, performance, safety, and API milestone (3,750+ commits since
`v0.1.0`). Earlier intermediate working notes are preserved in the
[2026-10-30 archive](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/archive/unreleased-notes-20261030.md).

### Breaking

- **Minimum Supported Rust Version (MSRV):** Raised `rust-version` from `1.85`
  to `1.99` (build and validate with latest stable Rust).
- **Default gzip backend (`zlib-rs`):** Enabled `zlib-rs` by default
  (`default = ["zlib-rs"]`) with `flate2/runtime_detection`. `partitionline`
  continues to forbid `unsafe` in its own source, while `zlib-rs` uses SIMD and
  internal `unsafe` for gzip streams. Disable default features
  (`default-features = false`) to retain the `miniz_oxide` fallback
  ([`docs/gzip-backend.md`](docs/gzip-backend.md)).
- **`ConsumerConfig` public fields:** Added `buffer_memory: usize` (default
  32 MiB; caps retained Fetch record bytes across partitions and brokers) and
  `fetch_metadata_max_bytes: usize` (default 128 MiB; caps decoded per-response
  Fetch metadata). Prefer `ConsumerConfig::bootstrap(..)` builders or construct
  struct literals with `..Default::default()`.
- **Compact record & metadata types:**
  - `FetchedRecord.topic` and `ShareRecord.topic` now use `TopicName` (inline
    storage up to 23 bytes, heap `String` above 23 bytes; `Deref<Target = str>`,
    `as_str()`, and `PartialEq` with `str` / `String` remain available).
  - `FetchedRecord.key` now uses `Option<RecordBytes>` (inline up to 24 bytes,
    shared buffer above 24 bytes; `Deref<Target = [u8]>`).
  - `RecordMetadata.topic` now uses `Arc<str>` instead of `String`.
- **Metrics struct fields:** Added `throttle: ThrottleStats` to
  `ProducerMetrics` and `ConsumerMetrics`, and `reconnects: u64` to
  `ConsumerMetrics`. Add `..Default::default()` to exhaustive struct literals.
- **Protocol, codec, and share types:**
  - `protocol::admin::DescribeLogDirsResult` adds `is_cordoned: bool` for
    DescribeLogDirs v5 (`DescribeLogDirsResult::new` defaults it to `false`;
    `with_cordoned` sets it; v1–v4 decode `false`).
  - `protocol::admin::ListTransactionsRequest` adds `state_filters`,
    `producer_id_filters`, `duration_ms`, and `transactional_id_pattern` (v2).
  - `AcknowledgeType` adds `Renew` for share-group acquisition lock renewal
    (KIP-1222).
  - Enabling optional `feature = "zstd"` adds `Compression::Zstd` and
    `ProducerConfig.zstd_level`.
- **Shutdown, pagination, and error display:**
  - `Producer::close` and `Producer::close_timeout` now return errors observed
    by their initial `flush` after attempting worker shutdown, rather than
    suppressing non-timeout flush errors. Keep checking per-record `send`
    results.
  - High-level `Admin::describe_topic_partitions` rejects detected non-adjacent
    cursor repeats with a non-retriable `Error::Protocol` (raw page requests
    remain available).
  - `Error::Closed` formats as `"client closed"` on all paths; match `Error`
    variants and [`Error` categories](docs/api-stability.md#error-categories-kl07-10)
    rather than `Display` strings.

### Performance

- **Lazy Fetch batch decompression & compressed retention:** Fetch responses
  frame and CRC-check record batches on arrival, deferring decompression and
  record decoding until a batch fits within `ConsumerConfig::buffer_memory` and
  retaining excess framed batches per partition instead of re-downloading them.
  On gzip overfetch benchmarks, lazy decompression reduced real-broker consume
  CPU/record by 70% (15.11 µs → 4.49 µs, +50% records/s) and peak RSS by 77%
  (1,334 MB → 308 MB), and cut null-broker `nb-fetch-gzip-overfetch` allocated
  bytes/record by 89% and peak RSS by 68% ([`docs/guide.md`](docs/guide.md)).
- **SIMD gzip (`zlib-rs` + runtime CRC detection):** Default `zlib-rs` with
  runtime PCLMULQDQ/AVX2/ARM-CRC detection increased gzip compression
  throughput by +29% to +75% and decompression throughput by +15% to +99% on
  JSON/text cells across arm64 and x86_64 while lowering callgrind instruction
  counts by 21–50% ([`docs/gzip-backend.md`](docs/gzip-backend.md)).
- **Reduced per-record allocations:** Inline `TopicName` (≤23 B) and
  `RecordBytes` keys (≤24 B), shared `Arc<str>` `RecordMetadata.topic`, bounded
  initial batch decode reservations, producer payload compaction on admission,
  and returning fetched batches without per-record cloning when no
  `ConsumerInterceptor` is configured.
- **Incremental Fetch sessions (v7–v18):** Consumers maintain one incremental
  Fetch session per active broker on Fetch v7–v18 (full-Fetch fallback on
  v4–v6), transmitting only changed and forgotten partitions on steady-state
  polls (shrinking a 128-partition unchanged Fetch v17 request from 4,268 B to
  25 B).
- **Cooperative yielding on large collections:** Protocol encoders/decoders,
  metadata indexes, and producer/consumer/group/share/transaction/admin batch
  walks yield every 8,192 elements so caller deadlines and sibling Tokio tasks
  stay responsive without altering wire bytes
  ([`docs/resource-contract.md`](docs/resource-contract.md#caller-deadlines)).

### Added

- **Optional pure-Rust `zstd` codec (`feature = "zstd"`):** Bounded Zstandard
  compression (levels 1–19, default 3 via `ZstdLevel` and
  `ProducerConfig::zstd_level`; Produce v7+ gate) and decompression (64 MiB
  window/output ceiling, 1,024-frame cap, dictionary rejection) via `zstd-rs`,
  verified across 150 bidirectional Rust/Java/librdkafka codec cells
  ([`docs/zstd-spike.md`](docs/zstd-spike.md)).
- **Kafka 4.x wire protocol support:**
  - **Produce v13:** Topic UUID encoding from Metadata v10+ with per-request
    identity snapshots, `UNKNOWN_TOPIC_ID` metadata refresh within the delivery
    deadline, v3–v12 name-based fallback, and v12/v13 transactional Produce
    when finalized `transaction.version >= 2` and EndTxn v5 are negotiated.
  - **Fetch v18 & leader hints:** Per-broker Fetch v4–v18 negotiation, KIP-951
    `CurrentLeader` and `NodeEndpoints` fast leader-cache updates on Produce
    v10+ and Fetch v12+/v16+, `DivergingEpoch` truncation seeks, and KIP-1166
    replica high-watermark sidecar codecs.
  - **Offsets & initialization:** ListOffsets v1–v11 including
    `OffsetSpec::earliest_pending_upload` (`-6`, KIP-1023), OffsetCommit and
    OffsetFetch v10 with topic UUIDs plus typed `OffsetClient` /
    `OffsetOptions`, InitProducerId v0–v6, Metadata v0–v13, and GROUP
    FindCoordinator v0–v6.
- **Groups, KIP-848, Share groups, and Streams:** KIP-848 signed-byte nullable
  assignment markers, unchanged-assignment heartbeats, and fencing recovery;
  KIP-932 / KIP-1222 share-group `ShareAcquireMode`, acquisition lock renewal
  (`AcknowledgeType::Renew`), and topic-ID validation; caller-driven
  `StreamsClient` heartbeats and `Admin::describe_streams_groups` (Streams v0,
  explicit `allow_unstable`).
- **Admin & partitioning additions:** `Admin::elect_leaders`,
  `describe_quorum`, `add_raft_voter` (v0–v1), `remove_raft_voter`,
  `alter_partition_reassignments_with_options` (v1 replication-factor guard),
  `list_transactions_with_options` and `list_transactions_by_broker_with_options`
  (v2 `transactional_id_pattern`), DescribeLogDirs v5 `is_cordoned`, and opt-in
  `StickyPartitioner` (`ProducerConfig::partitioner`).
- **Broker quota throttling:** Produce v6–v13 and Fetch v8–v18 honor broker
  `throttle_time_ms` mutes per broker within caller delivery, poll, and close
  deadlines while unmuted brokers and buffered records continue progressing,
  tracked in `ThrottleStats`.

### Changed

- **Transactional error & recovery contract:** Operation-specific Apache error
  tables classify retryable, abort-required (`TRANSACTION_ABORTABLE`,
  `CONCURRENT_TRANSACTIONS`, `INVALID_TXN_STATE`, etc.), and fatal fencing
  (`PRODUCER_FENCED`, `INVALID_PRODUCER_ID_MAPPING`) outcomes, recovering via
  coordinator-authorized `InitProducerId` on classic coordinators or `EndTxn` v5
  on transaction v2 ([`docs/api-stability.md`](docs/api-stability.md)).
- **Configuration validation:** Constructor/connect checks reject multiple
  simultaneous raw SASL mechanisms, out-of-domain raw producer `acks`, negative
  consumer fetch bounds, empty bootstrap lists, and empty `group.id` before
  network I/O.

### Fixed

- **Consumer group & offset correctness:** Interval auto-commit commits only
  previously delivered offsets (never the batch being returned, prefetched
  records beyond `max_poll_records`, or on `leave`/`close`/`unsubscribe`);
  in-batch seeks filter earlier records; `auto.offset.reset=None` surfaces
  out-of-range without advancing; `ReadCommitted` clears aborted intervals on
  `ABORT` so subsequent committed transactions under the same producer ID stay
  visible; topic recreation drops stale buffered records.
- **Connection & coordinator safety:** Replacement sockets negotiate their own
  `ApiVersions` ranges before SASL authentication; name-based offset requests
  re-encode across coordinator version changes and reject unrepresentable fields
  or UUID downgrades before transmission; producer worker retirement closes
  admission before the final drain and retains worker/retry counts until
  callbacks and payload cleanup finish.
- **Buffer & codec accounting:** Fixed reservation sizing for reused and
  partially occupied encode/producer/share buffers, incremental reservation
  growth above 8,192 elements, and Java-matching malformed record-count and
  negative header-key error ordering.

### Security

- Enforced 128 MiB default decoded-metadata ceilings on Fetch and
  ConsumerGroupHeartbeat decoders (with explicit override helpers) to bound
  amplification from valid frames.
- Redacted `Sasl`, `OidcConfig`, and `TlsConfig` secrets in `Debug` output,
  omitted OIDC HTTP error bodies and raw OAUTHBEARER error payloads from
  `Display`, skipped `&self` in `tracing` spans, and bounded OIDC transient
  retries.
- Raised `tokio`, `rustls`, `rustls-webpki`, `webpki-roots`, `ctutils`,
  `hashbrown`, and SCRAM crypto dependency minimums past published advisories.

## [0.1.0] - 2026-09-04

First crates.io release baseline (publish via `docs/RELEASE.md` / tag `v0.1.0`).

### Added

- Kafka client: produce, fetch, classic groups, cooperative-sticky,
  KIP-848 consumer groups, KIP-932 share groups, transactions / EOS, and
  Kafka 3.x/4.x admin APIs.
- TLS via rustls (no OpenSSL); SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER,
  OIDC client credentials.
- Compression: gzip, snappy, lz4 (zstd and Kerberos remain out of default
  features; see `docs/gaps.md` and `docs/zstd-spike.md`).
- Java-shaped builders and rustdoc naming matching Java client calls.
- Optional `tracing` feature (produce/fetch/poll/rejoin/txn spans).
- Examples: produce, consume, group, txn, eos, admin, tls, sasl, oauth, share,
  benches, interceptors, metrics (`FORMAT=prom`).
- Operator docs: guide, rdkafka migration, security, release policy,
  API stability, civilization plan.
- CI: mock tests (MSRV 1.85 + stable), clippy, audit, cargo-deny, `cargo package`,
  broker-smoke (Kafka 3.9.1 + 4.1.0), fuzz-smoke, latency-gate.
- libFuzzer targets under `fuzz/`; decode allocation DoS guards.
- TLS PEM parsing via `rustls-pki-types` (no archived `rustls-pemfile`).

### Fixed

- KIP-932 share groups on Kafka 4.1: join with a client Uuid member id, wait for
  a real heartbeat assignment (no zero topic id), decode null Assignment as
  INT8 `-1`, and use the group coordinator for membership. Share smoke needs
  `group.share.enable` and finalized `share.version=1`.

### Changed

- Lab A produce harness exits non-zero unless broker HW sum equals acked each run.
- Civilization/publish-ready gates verify a downstream crate can depend on the packed `.crate`.
- Release workflow accepts `workflow_dispatch` on an existing `v*` tag; rustdoc/`ci-docs` smoke is part of publish-ready.
- Fixed broken rustdoc `[Display]` intra-doc links (now `std::fmt::Display`).
- Cleared remaining unresolved rustdoc intra-doc links (module docs resolve in
  submodule scope; crate denies `rustdoc::broken_intra_doc_links`; `ci-docs`
  fails the gate if any reappear).
- CI: `dev/**` branch pushes run a single `branch-lite` job; full matrix on
  pull_request, `main`, and `workflow_dispatch` so scarce runners can finish.
- Lab A harness accepts `TOPIC=` as well as `KAFKA_TOPIC=`; STATUS notes a
  fresh unsigned latency-gate + HW==acked smoke sample (not a Suite HOLD lift).
- `scripts/check-installable.sh` probes crates.io for the Installable bar;
  ADOPTION notes Actions is stuck org-wide (`main` queued for hours).
- `ci-broker-smoke`: when Docker overlay fails but `$KAFKA_BOOTSTRAP` is already
  up, fall back to that broker automatically (same path as `SKIP_DOCKER=1`).
- Fuzz: Join/Sync/Heartbeat/OffsetCommit + ShareFetch decode targets; Lab A
  fetch integrity harness (`scripts/lab-a-fetch.sh`) requires consumed==seeded
  (unsigned; not a Suite HOLD lift).
- `examples/oauth` for OAUTHBEARER (unsecured JWT) and OIDC client-credentials;
  ADOPTION documents pinable `v0.1.0-rc.2` git install until crates.io lands.
- Broker smoke: Kafka CI matrix uses `apache/kafka:4.1.0`; Docker 4.x starts with
  share coordinator RF=1 and upgrades `share.version=1`; `REQUIRE_SHARE=1` fails
  the job if share cannot fetch records on 4.x. Civilization-check only counts
  broker smoke when the log contains `ci-broker-smoke: ok` (Docker soft-skips
  are not evidence).

### Security

- Reject array / tagged-field lengths that exceed remaining buffer bytes so
  malformed broker frames cannot force multi-gigabyte `Vec` allocations.

### Notes

- Not a drop-in for `rd_kafka_*` or rust-rdkafka types.
- Defaults that differ from Java: `auto.offset.reset=Earliest`,
  `allow.auto.create.topics=false`, shorter `delivery.timeout.ms` /
  `max.block.ms` (see README).
- Mock TLS fixtures use the `openssl` CLI (no `rcgen` / `time` in the graph).
