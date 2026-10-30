# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to the 0.x policy in [`docs/RELEASE.md`](docs/RELEASE.md).

## [Unreleased]

Source changes after the published 0.1.0 release. The next release is still
being qualified; see [status](docs/STATUS.md) and the [release guide](docs/RELEASE.md).
Earlier working notes and dated measurements are in the
[archive](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/archive/unreleased-notes-20261030.md).

### API and configuration changes

- Build with the latest stable Rust. Package manifests now require Rust 1.99.
  The compiler requirement and public API changes require at least a 0.2.0
  client release.
- `ConsumerConfig` adds `buffer_memory` (32 MiB by default) and
  `fetch_metadata_max_bytes` (128 MiB by default). Use their builders or add
  both fields to exhaustive struct literals. The first limits retained Fetch
  data; the second limits decoded Fetch metadata.
- `FetchedRecord.topic` and `ShareRecord.topic` use `TopicName`. Names up to
  23 bytes are stored inline; longer names use a `String`. `as_str()`, string
  dereferencing, and comparisons with `str` and `String` remain available.
  Share-acquisition keys use the same representation.
- `FetchedRecord.key` uses `Option<RecordBytes>`. Keys up to 24 bytes are
  stored inline; longer keys use a shared buffer.
- `ProducerMetrics` and `ConsumerMetrics` add `throttle`
  (`metrics::ThrottleStats`). Add the field to exhaustive struct literals or
  use `..Default::default()`.
- `DescribeLogDirsResult` adds `is_cordoned` for DescribeLogDirs v5. The
  existing five-argument constructor defaults it to false; `with_cordoned`
  sets it. Older wire versions omit it and decode false.
- `Error::Closed` displays as `client closed`. Match the error variant rather
  than its message.

### Protocol and runtime changes

- Produce v13 uses Metadata topic IDs and maps replies through the identity
  snapshot sent with each request. Stale IDs refresh within the original
  delivery deadline; v3–v12 fallback remains. Transactions stay at Produce v11
  unless initialization finalizes `transaction.version >= 2` and supports
  EndTxn v5.
- Consumers negotiate Fetch v18 per broker. Replica helpers support the
  KIP-1166 HighWatermark tag; ordinary consumer requests omit its default.
  This adds wire support, not follower-acknowledgement behavior.
- Incremental Fetch sessions run per broker on v7–v18, with full Fetch fallback
  on v4–v6. Requests track changed and forgotten partitions. Session recovery
  uses the original deadline, and topic recreation clears stale buffered data.
- Fetch frames and CRCs are checked on arrival. Record decompression waits
  until a batch can be applied within `buffer_memory`; batches that cannot be
  retained are fetched again later. Decompression errors can therefore surface
  when the batch is applied.
- Produce v6–v13 and Fetch v8–v18 honor broker throttles while other eligible
  connections can make progress. Quota waits use the original delivery, request,
  poll, or close budget.
- ListOffsets v11 adds `EARLIEST_PENDING_UPLOAD_TIMESTAMP` and
  `OffsetSpec::earliest_pending_upload`. Each leader negotiates its own range;
  unsupported selectors or isolation return `Error::Unsupported`. Discovery,
  connection, capability lookup and retries share the caller deadline, with
  one ApiVersions control request per attempt. Live tiered-storage behavior
  remains unqualified.
- Replacement sockets negotiate their own API ranges and authenticate before
  application requests. Name-based offset requests re-encode when coordinators
  change versions; unrepresentable fields and UUID downgrades fail before the
  request is sent. Discovery and retries share the original deadline.
- gzip uses the default `zlib-rs` feature. Disable default features to use
  miniz_oxide. Runtime CRC detection is enabled; compressed bytes can differ
  while the wire format and decode limits stay the same. See
  [gzip backends](docs/gzip-backend.md) for the measured tradeoffs.
- Consumer-group heartbeat decoders limit decoded metadata to 128 MiB by
  default. Explicit metadata-limit APIs accept another limit; very large
  responses may require one.
- Large protocol encoders yield every 8,192 topics, partitions, brokers, or
  results so other tasks and caller deadlines can run. This covers Metadata,
  Produce, Fetch, share, transaction, and offset encoders without changing
  their encoded bytes.
- The unpublished schema companion checks bounded Apache Avro Rust 0.22.0
  encoding against Java Avro 1.12.1 in both directions. It rejects truncated
  strings that the backend can otherwise resolve as null.

### Fixes and maintenance

- Close producer admission before retiring a worker. Cached handles and held
  channel permits cannot accept records after its final drain.
- Keep workers and retry records counted until callbacks and payload cleanup
  finish, including cancellation before a task's first poll.
- Fix incremental reservation growth above 8,192 elements. Decode grouped
  assignments directly from borrowed topic names, and size small Share batch
  lists from framing without changing validation errors.
- Raise dependency minimums past the recorded Tokio, Rustls, WebPKI, trust-root
  and RustCrypto advisories, including compatible versions retained by
  downstream lockfiles. Refresh the active product, benchmark and fuzz locks.
- Repair links to repository-only material in packaged documentation. Check
  wrapped link labels and changelog navigation as well as the README and guides.
- Correct reservation sizes for reused encoding, producer, and share buffers.
  Allocation checks now cover partially occupied buffers and warm reuse.
- Record decoding uses a bounded initial reservation for small batches while
  continuing to charge allocation peaks to the shared metadata limit. The
  existing 1,000-record allocation budget is restored without raising it.
- Restore Kafka Java's error ordering for malformed record counts and negative
  header-key lengths while retaining decoded-metadata limits.
- KIP-848 heartbeat responses use Apache's signed-byte nullable-assignment
  marker. Unchanged assignments and consumer close are handled, and responses
  missing the final tag count are rejected.
- Release readiness requires default and all-feature tests, exact-commit CI,
  package-consumer checks, and the interruption-recovery rehearsal before
  publication. Client 0.1.0 is already published; the broker and schema crates
  remain unpublished.
- Guides describe buffer accounting, cancellation and ambiguous delivery,
  protocol versions, and the limits of recorded benchmarks. Production and
  fastest-performance claims remain unqualified.

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
