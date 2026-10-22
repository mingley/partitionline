# Kafka client and broker plan

The goals are a production-ready Kafka client and broker, the protocol
behavior this repository commits to, and leading performance at the same
Kafka settings. [tasks.json](plan/tasks.json) is the queue: one card, its
dependencies, the files it starts from, and the check that closes it.
[TODO.md](../TODO.md) is the short launch order. It is not a second status
list. The [task workflow](plan/README.md) says how to pick a card.

The KL packages below are scope and closing gates. They are not one session
of work. The numbers in the scorecard are proposed targets. They are not
results this repository has signed. Michael Ingley coordinates scope and
signoff. A card has an owner only after someone claims it.

Suite HOLD stays until there is signed Lab A evidence. Unsigned samples do
not lift it. See [STATUS.md](STATUS.md).

Section 1 is the frozen 2026-09-21 audit. Its links stay on that snapshot.
Current card status is `tasks.json`.

## 1. Audit baseline (2026-09-21)

Baseline: [cb7e97d](https://github.com/mingley/partitionline/tree/cb7e97d3b92a8555aea34d59266a2990c206395f),
audited **2026-09-21**. Read the [source audit](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/audits/2026-09-21.md) for
frozen source locations, upstream pins, reproduced failures and coverage limits.
The exact-SHA [CI run 33948193731](https://github.com/mingley/partitionline/actions/runs/33948193731)
has 14 successful jobs, with actual broker identities checked. Existing CI
is useful and green; it does not exercise every required behavior.

Five deterministic consumer cases failed at that baseline: whole-batch seek filtering,
committed records after an earlier abort under the same PID, preserving
successful partitions across a partial retry, `auto.offset.reset=None`, and
committing after a capped poll. **KL03-01 through KL03-07** fixed these cases;
independent replay is tracked by **KL01-22**. **KL07-01** fixed the private OIDC
rustdoc link, and **KL08-01** fixed the release probe's non-CI fallback.

The 2026-09-04 `54020e2` formatting/nested-latency failure is historical.
Incoming fixes through `0146b98`, plus the 2026-09-05 broker identity,
timeout portability, cancellation, buffer-counter, auth and release slices,
are already present. Do not schedule their original implementation again.
Controlled-host qualification and stronger behavioral evidence remain open.

Crates.io `0.1.0` is published, with adopter pins and subsequent work on `main`.
**Suite HOLD remains** until signed Lab A evidence.
Unsigned Verifiable/latency samples do not lift it; see
[CIVILIZATION.md](CIVILIZATION.md) and [STATUS.md](STATUS.md).

Current upstream targets are also explicit: on the audit date Apache lists
4.1.2, 4.2.1 and 4.3.1 as supported releases. Existing 3.9.1/4.1.0 smoke
does not establish those cells. The audit pins each source revision; KL01-10
adds the new evidence rather than changing the support promise by prose alone.

| Area | Existing evidence and source | Qualification gap |
|---|---|---|
| Package and dependency boundary | [Cargo.toml](../Cargo.toml): published `partitionline` 0.1.0, latest stable Rust declaration, MIT OR Apache-2.0. Client code forbids unsafe; no librdkafka dependency. TLS uses `rustls`/`ring`, including native compilation via `cc`. | Published/installable does not mean production-qualified. Do not claim the full build has no C dependencies. |
| Producer, consumer and protocol | [producer](../src/producer.rs), [consumer](../src/consumer.rs), [protocol](https://github.com/mingley/partitionline/tree/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol), [mock tests](../tests/full_surface.rs): routing, negotiated versions, batching, retries, idempotence and transactions. | The five baseline consumer defects are fixed (KL03-01 through KL03-07). Independent replay is still KL01-22. Mock agreement is not an independent broker oracle. |
| Group and share APIs | [group](../src/group.rs), [share](../src/share.rs), [broker smoke](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/scripts/ci-broker-smoke.sh): classic/cooperative, KIP-848 and share-group paths. | Existing 3.9.1/4.1.0 CI smoke is not multi-broker chaos. Check executed cases and capability gates; an ignored live test is not covered by default `cargo test`. |
| Build and safety CI | [CI](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/.github/workflows/ci.yml) uses latest stable Rust, features, package, audit/deny, short fuzz, broker and auth lanes; the audited run is green. | Add strict rustdoc/doctests, independent case reports and exact required-lane release checks. Extend existing lanes instead of creating duplicates. |
| Codecs and ecosystem | [zstd spike](zstd-spike.md), [schema companion](schema-companion.md): gzip/snappy/LZ4 and optional zstd are implemented; `partitionline-schema` remains unpublished with bounded framing, lookup/cache and caller-selected adapters. | Complete the bidirectional live codec matrix and broader schema qualification. Keep default dependencies unchanged. |
| Performance | [benchmark.md](benchmark.md) separates locked Lab A produce results from unsigned this-VM fetch/latency results. Recorded produce latency was 62/95 us p50/p99 versus rust-rdkafka 58/90 us. | No universal speed claim. Preserve Suite HOLD and its signoff rules; do not combine different hosts/configurations into one victory. |
| Operations | [metrics](../src/metrics.rs), optional `tracing`, [security policy](security.md), [adoption checklist](ADOPTION.md) already exist. | Prove diagnostic usefulness, redaction, auth rotation, recovery and operator-driven rollback on a defined profile. |

## 2. Scope and promotion gates

| Profile | Required work | Not automatically required |
|---|---|---|
| Bounded producer/consumer deployment | KL-01/02, applicable leader-recovery cases in KL-03, transport/security in KL-06, diagnostics in KL-07, baseline measurements in KL-04 and release/adoption in KL-08. | zstd, Schema Registry, Kerberos, every Kafka API or a benchmark victory. |
| Transactional processing | Above plus KL-03 crash histories, fencing, aborted-record visibility and atomic output/offset proofs. | Exactly-once external side effects without application cooperation. |
| Dynamic group or share-group deployment | Above plus the corresponding KL-03 broker-version/churn/lock-recovery matrix. | Treating a classic-group success as evidence for KIP-848 or share semantics. |
| Core feature-complete client | Applicable correctness gates plus all major producer/consumer/group/transaction/common-admin features, all five Kafka codecs, incremental Fetch, quota behavior and supported current broker lines. | Calling implemented helpers or lower-version fallback proof of an unimplemented feature. |
| Enterprise/full-admin extensions | Corresponding KL-05/06 cards: opt-in GSSAPI lifecycle, secure rotation, client-facing quorum/election/log-dir operations and explicit platform support. | Broker-internal replication APIs, a C ABI, or native compression/SASL in defaults without approval. |
| Wider ecosystem support | Companion Registry lookup/cache and independent Avro/Protobuf/JSON Schema adapters in KL-05. | Schema Registry being an Apache Kafka wire requirement, Streams/Connect, or adding companion dependencies to the core runtime. |

Production qualification is profile-specific. Semver 1.0 additionally requires
a stable API, supported-version/MSRV policy and a maintenance commitment.
It does not require every optional feature or lifting a performance claim HOLD.
Keep [api-stability.md](api-stability.md) and the support matrix authoritative.

The complete-client goal cannot be closed by permanently relabeling missing
major features as demand-gated. A narrower deployment can qualify without
them, but its exclusions must be named. If a dependency-policy decision blocks
zstd or GSSAPI, the corresponding complete profile remains blocked.

There is no single portable Kafka certification runner or Apache performance
threshold in the inspected upstream evidence. KL01 maps pinned protocol JSON,
Java behavioral tests, Apache system scenarios and applicable librdkafka
regressions to explicit case dispositions. Performance gates below are
repository goals, not OSS standards. See the audit's
[standards boundary](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/audits/2026-09-21.md#standards-and-upstream-evidence-boundary).

## 3. Scorecard

Freeze workload, hardware, broker settings and thresholds before each
qualification exercise. Target changes need a recorded rationale; never adjust
them retrospectively merely to make a failing result green.

| Dimension | Proposed acceptance target | Evidence |
|---|---|---|
| Integrity | Zero unexplained missing/corrupt records, forbidden duplicates, order violations or exposure of aborted transactions. | Unique-ID/payload histories, outcome classification and independent consumers; not count equality alone. |
| Compatibility | Zero unexplained semantic differences across the advertised negotiated API/version matrix. | Pinned Java/broker oracle, golden wire fixtures and minimized differential failures; allow legitimate wire/layout differences. |
| Bounded resources | A declared byte/task/connection budget holds at 2x sustainable offered load. After warmup, final-hour median RSS within 10% of first steady-state hour over 24 hours. | Buffer ownership/counter evidence and RSS; allocator retention is not automatically a leak, and queue bounds alone are not a process bound. |
| Recovery | Initial target: resume eligible work within 5 seconds after a controlled broker restoration; respect earlier caller deadlines. | At least 1,000 seeded faults per supported profile. Group timers/transaction timeouts get separate predeclared budgets, not a universal 500 ms promise. |
| Performance leadership | At least 20% more successful records/s or 20% lower client CPU per successful record than the best relevant baseline, at equal durability/security and latency budget. | Multi-run x86_64/arm64 evidence; p99 no worse by more than 5% at matched load, all losing cells visible. |
| Ergonomics and operations | Two newcomers complete a producer/consumer guide in 15 minutes after prerequisites; enabled telemetry overhead initially budgeted at 2%. | External-consumer examples, feedback, profiles, redaction/cardinality tests and a diagnosis/rollback exercise. |

## 4. Ordered implementation packages

Each package may require several small PRs. Existing commands below are
building blocks, **not proof that new acceptance criteria already pass**.
New harnesses must be added and wired into CI before their package can close.
Use the corresponding `KL01-*` through `KL10-*` cards in
[tasks.json](plan/tasks.json). One session takes one ready card, not a
whole numbered package.

### KL-01: Recover the baseline and establish protocol oracles

**Priority:** P0. **Depends on:** none.

1. Keep the audited baseline and current case registry accurate. Name an owner
   for a single ready card, preserve already-landed recovery work, and map
   every required upstream case to evidence. Separate harness noise from a
   client regression; retain historical failures rather than overwriting them.
   **Partial (2026-09-05):** shared-runner vs local-native vs controlled-host
   budgets recorded in [https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/latency-ci-policy.json](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/latency-ci-policy.json);
   nested 1,344/750 µs miss is historical (`SKIP_LATENCY_GATE` on integrity).
   GHA 5000 µs and local 750 µs relative gate unchanged. Controlled-host
   reproduce remains open. Not Done.
2. Reconcile the requested versus actual broker identity and require explicit
   capability/result records. Address developer-host prerequisites: the broker
   script invokes GNU `timeout`; declare or provide a tested supported path
   rather than assuming it exists on macOS.
   **Partial (2026-09-05):** `scripts/lib/broker-identity.sh` stamps `actual=` on
   broker/auth smoke; `scripts/lib/pl-timeout.sh` prefers `timeout` then
   `gtimeout` (Homebrew coreutils) and fails closed with an install hint.
3. Compare Produce, Fetch, Metadata and ListOffsets first against pinned
   Kafka Java clients/brokers 3.9.1 and 4.1.0; expand by API coverage and user
   demand. Compare decoded semantics and required fields, not arbitrary byte
   ordering, client IDs or correlation IDs.
   **Partial (2026-09-05), clarified by the 2026-09-21 audit:**
   `tests/protocol_oracles.rs` and its matrix exercise self-encoded
   Produce/Fetch/Metadata/ListOffsets fixtures labeled 3.9.1/4.1.0.
   They are local consistency checks, not independent Java-generated
   fixtures. The ignored live path is optional via `REQUIRE_BROKER=1`.
   KL01-03 through KL01-08 are done: they added the independent Java
   fixtures and wired the reports into CI. Later evidence cards, including
   KL01-22 and KL01-24, are still pending.
4. Extend existing [fuzz targets](https://github.com/mingley/partitionline/tree/917d877d7b049f3da5af90bd2a5804b85080ed2b/fuzz) beyond short CI smoke, focusing on
   lengths, tagged fields, truncated batches, CRC, allocation and decompression
   bounds. Retain minimized failures and campaign/coverage metadata.
   **Partial (2026-09-05):** campaign metadata harness (`scripts/ci-fuzz-campaign.sh`,
   `fuzz/campaign/metadata.example.json`) is distinct from 15s CI smoke; minimized
   failures are retained under `fuzz/artifacts/minimized/`. Not a sustained-campaign close.

**Work surfaces:** [protocol](https://github.com/mingley/partitionline/tree/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol), [fuzz smoke](../tests/fuzz_decode_smoke.rs),
[broker smoke](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/scripts/ci-broker-smoke.sh), [integrity smoke](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/scripts/ci-integrity-smoke.sh),
[latency gate](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/scripts/ci-latency-gate.sh).
**Done when:** required CI is green without silent skips or weaker assertions;
each advertised matrix cell names the actual broker and cases; differential
fixtures and sustained campaigns have linked results and no unresolved failures.

### KL-02: Bound memory and define cancellation/shutdown outcomes

**Priority:** P0. **Depends on:** KL-01's supported contract.

10 of 12 KL02 cards are done. Still pending: KL02-10, a controlled
24-hour overload record, and KL02-12, producer shutdown completion under
the close deadline. The 2026-09-05 notes below are that date's partials.

1. Trace ownership from caller enqueue through worker/retry queues, batch
   encoding, sockets and fetch buffers. Account for queued **and in-flight**
   bytes, compressed/uncompressed storage, connections and spawned tasks.
2. Verify current queue/time-limit behavior before changing defaults. Define
   cancellation before enqueue, while buffered, after send, and after broker
   acknowledgment. A dropped future must not imply a record was never written.
   **Partial (2026-09-05):** guide cancellation table + `Producer::send`/`close` rustdoc;
   mock tests in `tests/produce_cancel.rs`; `close` sets a durable `closed` flag so clones
   cannot respawn workers. Remaining: overload soak, full byte/task ownership trace.
   **Partial (2026-09-05):** consumer `leave`/`close`/`unsubscribe` no longer
   auto-commit positions (`tests/consumer_close_commit.rs`); poll-interval
   auto-commit unchanged.
3. Exercise saturating senders, slow consumers, stalled brokers, shutdown and
   timeout races. Specify whether each operation drains, rejects or reports an
   unknown outcome. Never auto-commit unprocessed consumer offsets on close.

**Work surfaces:** [producer](../src/producer.rs), [consumer](../src/consumer.rs),
[config](../src/config.rs), [cluster](../src/cluster.rs), [network](../src/net.rs).
**Done when:** 2x-overload and cancellation histories obey the configured
resource model; no hung completions or leaked permits/tasks remain after drain;
all accepted work has a documented completed, failed or ambiguous outcome.

### KL-03: Prove HA, transactions and group semantics with crash histories

**Priority:** P0 for the corresponding production profile. **Depends on:** KL-01/02.

KL03-01 through KL03-07 are done. They fixed the five reproduced consumer
failures and poll-boundary auto-commit. Do not start them again. Independent
replay of those histories is KL01-22, which is still pending. The numbered
items below are the later qualification shape. They are not a claim that
none of that work has landed. Current status is `tasks.json`.

1. Extend smoke to a three-broker KRaft topology with controller quorum,
   replication factor 3 and explicit `min.insync.replicas`. Exercise leader and
   coordinator moves, lost responses, socket partitions, process pause/restart,
   rolling upgrades, stale metadata and topic/partition changes.
2. Record unique record IDs, payload hashes and per-key order before submission,
   after acknowledgments and in independent consumers. Classify failed and
   unknown outcomes; compare full histories, not just high-watermark deltas.
   Broker offsets include control records and are not a count of application
   records in transactional tests.
3. Test PID/sequence/epoch recovery, fencing, transaction commit/abort, coordinator
   failover and `read_committed` output plus committed input offsets against
   version-pinned Java behavior. Nontransactional Java recovery can increment
   a local epoch, with precise sequence/identity rules; transactional recovery
   is different. Qualify those states and retained in-flight batches rather
   than replacing them with a blanket rule for or against local epoch bumps.
4. Cover classic/cooperative/KIP-848 membership churn separately from share
   acquisition, lock expiry, release/reject and redelivery. Define which forms
   of duplicate delivery are expected for each API. Verify assignments, progress,
   offset safety and coordinator reconnection under TLS/SASL.

**Work surfaces:** [producer](../src/producer.rs), [consumer](../src/consumer.rs),
[group](../src/group.rs), [share](../src/share.rs), [transaction protocol](../src/protocol/txn.rs),
[live tests](../tests/kip848_live.rs), [broker smoke](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/scripts/ci-broker-smoke.sh).
**Done when:** seeded histories satisfy each profile's invariants and recovery
budget across supported broker versions, with no unexplained outcomes.
`acks=all` alone is not an exactly-once guarantee.

### KL-04: Establish reproducible leadership, then optimize measured limits

**Priority:** baseline early; optimization after correctness. **Depends on:** KL-01;
KL-02/03 before production-performance claims.

9 of 14 KL04 cards are done, including the frozen benchmark contract and
scheduled open-loop latency (KL04-06). Still pending: controlled x86_64
and arm64 campaigns (KL04-10, KL04-11), the comparison report (KL04-12),
losing-cell cards (KL04-13), and independent signoff (KL04-14). Suite HOLD
remains. The numbered steps below are the original order.

1. Freeze separate low-latency, bulk-throughput, fetch, transactional and
   mixed-load workloads. Compare pinned rust-rdkafka/librdkafka and Java, plus
   another Rust client when its semantics match. Preserve the existing Lab A
   contracts and compare separate producer, consumer and end-to-end results.
   **Partial (2026-09-05):** CI vs local vs controlled-host budgets recorded
   in [https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/latency-ci-policy.json](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/latency-ci-policy.json). Shared-runner smoke
   is not Lab A; unsigned samples must not lift Suite HOLD. Controlled-host
   qualification remains open. Not Done.
2. Match acks, idempotence, isolation, replication/ISR, compression, batch/linger,
   partition count/skew, keys, connections, in-flight bounds, TLS/SASL and payload
   distribution. Distinguish enqueue acceptance from broker acknowledgment and
   independent consumption; acknowledgment is not proof of a per-record fsync.
3. Extend [bench_latency](../examples/bench_latency.rs) with scheduled open-loop
   arrivals through saturation. Include intended-send delay and rejected/timed-out
   work to avoid coordinated omission; do not report only successful fast samples.
4. Collect p50/p95/p99/p99.9, sample counts, successful records/s, client/broker
   CPU, allocations, RSS and lag. Separate hosts/processes, test real-network
   RTT, randomize A/B order, and retain at least five long-enough repetitions per
   cell with uncertainty intervals on x86_64 and arm64.
5. Profile serialization, compression, batching, allocation, syscalls, routing
   and executor contention. Land one explained, reversible optimization per PR.
   Reject apparent wins from weaker durability, hidden errors or one bespoke schema.

**Work surfaces:** [benchmark examples](https://github.com/mingley/partitionline/tree/917d877d7b049f3da5af90bd2a5804b85080ed2b/examples), [latency gate](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/scripts/ci-latency-gate.sh),
[producer](../src/producer.rs), [network](../src/net.rs), [record codec](../src/protocol/records.rs).
**Done when:** an independent operator reproduces named scorecard wins from
raw artifacts and exact configs/revisions. Publish losses too. Performance
signoff follows [benchmark.md](benchmark.md)/Suite HOLD; no new plan bypasses it.

### KL-05: Expand codec and ecosystem coverage only when justified

**Priority:** P1 core-feature completion; P2 enterprise/full-admin/ecosystem.
**Depends on:** KL-01; KL-04 for performance claims.

1. Evaluate zstd decompression and compression separately for Kafka framing,
   error handling, memory, throughput, maintenance and licensing. `ruzstd` was
   a decompression candidate, not an assumed complete encode/decode solution.
   That evaluation landed. KL05-02 selected `zstd-rs` 0.1.0. `ruzstd` was not
   selected. KL05-03 through KL05-05 implemented and checked decode, encode,
   and the codec matrix. The feature stays optional. `zstd-sys` stays banned.
2. Require independent encoder/decoder interoperability, malformed-input tests
   and bounded decompression. The recorded backend decision is KL05-02.
   Bounded decode and the codec matrix are KL05-03 through KL05-05.
   The [deny policy](../deny.toml) bans `zstd-sys`. Optional native support
   still needs an explicit policy decision, not a quiet feature flag.
3. KL05-06 through KL05-09 and KL05-11 through KL05-15 are done. That is
   incremental Fetch, broker throttle, Produce v13, Fetch v18, ListOffsets
   v11, and the share-group wire and client path. KL05-10, the sticky
   unkeyed partitioner, is in progress. A codec field or version cap is
   not the corresponding high-level feature.
4. Use one API-specific card for full-admin extensions. Classify internal
   broker replication separately; do not clone every key in `ApiKeys`.
5. Keep Schema Registry in the [companion design](schema-companion.md).
   Implement lookup, bounded caching and each format adapter separately.
   GSSAPI's backend, exchange and live lifecycle are KL06-08 through KL06-10.
   These may be excluded from a narrow deployment profile, not from a claim
   that the corresponding complete profile is supported.

**Work surfaces:** [records](../src/protocol/records.rs), [Cargo.toml](../Cargo.toml),
[zstd spike](zstd-spike.md), [gaps](gaps.md).
**Done when:** the selected scope passes interoperability, safety and performance
checks. Dependencies and deferred features are documented.

### KL-06: Qualify authentication and transport recovery

**Priority:** P0 for supported secure deployments; extensions demand-gated.
**Depends on:** KL-01/02.

1. Audit existing SASL/OIDC refresh and connection behavior before adding new
   APIs. Test expiration, clock skew, token endpoint outages, reconnect,
   broker-requested reauthentication, TLS root/certificate rotation and mTLS.
2. Define bounded refresh/backoff and credential lifetime handling. Fail closed
   when authentication or TLS validation fails, without plaintext fallback.
   Distinguish a refresh-fetch failure while credentials remain valid from
   invalid credentials; do not force unnecessary reconnect storms.
3. Exercise recovery with producer, consumer, admin and group-coordinator paths.
   Verify redaction in Debug, errors, spans and metrics, including failure paths.
   **Partial (2026-09-05):** `Debug` redacts Sasl passwords, OIDC `client_secret`,
   and mTLS key PEMs (plus producer/consumer/admin config cascade); see
   [security.md](security.md) and `tests/credential_redact.rs`. OIDC/OAUTHBEARER `Error` bodies no longer embed IdP/broker payloads (2026-09-05).
   Metrics snapshots and tracing `skip(self)` checks added (2026-09-05).
   OIDC endpoint failures close authentication (503/timeout; 2026-09-05).
   Bounded transient OIDC retry (5xx/I/O/timeout; 2026-09-05).
   KL06-01 through KL06-05, KL06-07, and KL06-13 are done. An owned
   `OidcTokenManager` refreshes before expiry. The shared connect-time cache
   does not. Client loops do not reauthenticate on a timer. Still open: the
   live lifecycle matrix KL06-06, producer reauthentication KL06-11 (in
   progress), shared callers KL06-12, and GSSAPI KL06-08 through KL06-10.

**Work surfaces:** [network](../src/net.rs), [OAuth](../src/protocol/oauth.rs),
[OIDC](../src/protocol/oidc.rs), [auth smoke](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/scripts/ci-auth-smoke.sh),
[security policy](security.md).
**Done when:** short-lived-credential fixtures and real secure broker cases
cover multiple rotations/outages with documented outcomes, bounded recovery
and no secret exposure. Preserve existing audit/deny and security-reporting lanes.

### KL-07: Make adoption and diagnosis simpler than the alternatives

**Priority:** P0 for basic usability. **Depends on:** KL-01; KL-02/03 for recipes.

KL07-01 through KL07-08 and KL07-10 through KL07-15 are done. The strict
rustdoc failure is fixed, and CI runs rustdoc with doctests. The remaining
card in this lane is KL07-09, two independent newcomer exercises. It is
pending. Do not start KL07-01 again.

1. Compile the existing producer/consumer/transaction and
   [rust-rdkafka migration](migrate-from-rdkafka.md) examples as external package
   consumers. Explain enqueue versus acknowledgment, ownership, partitioning,
   offset commit, cancellation and exactly-once boundaries without Java API guesses.
2. Extend existing metrics and optional tracing only where diagnosis is missing:
   queue age/bytes, retries, broker throttle, reconnect, ack latency, lag,
   rebalance and transaction outcomes. Bound label cardinality; provide optional
   exporters as examples rather than mandatory core dependencies.
3. Have two independent users follow the guide and diagnose a throttled broker,
   stale leader and blocked consumer using telemetry rather than payload logging.

**Work surfaces:** [metrics](../src/metrics.rs), [interceptors](../src/interceptor.rs),
[guide](guide.md), [examples](https://github.com/mingley/partitionline/tree/917d877d7b049f3da5af90bd2a5804b85080ed2b/examples), [consumer fixture](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/scripts/ci-crate-consumer.sh).
**Done when:** examples run from a fresh package consumer, usability feedback
is resolved, and disabled/enabled instrumentation costs and redaction are measured.

### KL-08: Gate releases and promote through reversible adoption

**Priority:** P0 release safety; promotion after applicable packages.
**Depends on:** none for stopping release/CI divergence; applicable profile gates
for adoption and API stabilization. Optional KL-05 features do not gate everyone.

1. Select one serialized publication path across
   [release-plz](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/.github/workflows/release-plz.yml) and legacy workflows.
   Require green evidence for the exact release SHA, isolated package consumers,
   author/license/notice inventory and an idempotent partial-release recovery.
   Reconcile [release policy](RELEASE.md), metadata checks and stale handoff
   scripts with actual state. Scope "no C" metadata to the Kafka implementation,
   not the entire TLS dependency graph. Rehearse without publishing.
   **Partial (2026-09-05):** release-plz is PR-only (never auto-publish). The intended
   canonical path is tag → `release.yml` / `owner-cut-release` with
   `check-main-ci` + `ci-crate-consumer`. The 2026-09-21 audit found that
   `check-main-ci` can accept a non-CI workflow when CI is absent. KL08-01,
   KL08-02, and KL08-03 are done: the release check rejects a non-CI
   workflow, requires the release profile's CI evidence, and retires the
   obsolete publisher. KL08-04 through KL08-07 and KL08-14 through KL08-21
   are done as well. Still open: adopter records KL08-08 through KL08-11,
   shadowing and rollback KL08-12, and the 1.0 decision KL08-13.
   Rehearse with `DRY_RUN=1`. Do not cut a new version from this page.
   **Partial (2026-09-05):** `scripts/rehearse-partial-release.sh --self-test` proves idempotent
   recovery without publishing (0.1.0 stays; day1/handoff DRY_RUN, not another publish).
   `owner-publish` skips `cargo publish` when the version is already on crates.io;
   `release.yml` `actions: read` unblocks exact-SHA `gh run list`. KL-08 stays open.
2. Specify broker/API, OS/architecture, toolchain and feature support plus security
   response, upgrade and deprecation policies. Keep unsupported combinations
   explicit. Current development uses latest stable Rust only; old Rust 1.85
   runs are historical evidence.
   **Partial (2026-09-05):** [`support.md`](support.md) records the CI-backed
   matrix (Kafka 3.9.1/4.1.0, Rust 1.85, Linux/x86_64, default features)
   and explicit non-promises. That Rust 1.85 line is this 2026-09-05 note.
   Current source requires Rust 1.99. Linked from RELEASE/ADOPTION/api-stability. Does
   **not** close KL-08 (adopter 24h/7d records and promotion/rollback remain).
   Blank record format: [adopter-exercise.md](adopter-exercise.md) (UNFILLED — not evidence; 2026-09-05).
3. Qualify two independent adopter workloads with 24-hour then 7-day runs.
   With operator approval, shadow into isolated topics/read-only consumers
   before a 1%/10%/50% traffic rollout. Never duplicate real downstream side
   effects or assume a consumer group can arbitrarily assign a traffic percentage.
4. Compare IDs/payloads, not offsets across distinct shadow topics. Rehearse
   draining, transaction fencing/abort and resuming from correct committed
   offsets before rollback; do not auto-commit unprocessed records.
5. Stop immediately on integrity, credential or resource-bound violations.
   Initial rollback targets: unexpected-error rate > baseline by 0.1 percentage
   points or p99 > baseline by 10% for two 5-minute windows; freeze sample floors
   and any stricter service SLO beforehand. Keep rollback operator-controlled
   unless separately approved automation has been rehearsed.

**Done when:** maintainers/operators sign off on linked per-profile evidence,
no blocker remains for that profile, and upgrade/rollback is demonstrated.
A separate 1.0 decision includes API and maintenance commitments; no date or
version bump is implied by finishing a feature checklist.

### KL-09: Engineer and prove the fastest Kafka client

**Priority:** P0 claim-gate and evidence integrity; P1 measured optimization.
**Depends on:** KL-04 contract and result format; KL-03 for consumer
optimizations; KL-04 campaigns before any claim.

The [performance leadership program](plan/performance-leadership.md) holds
the strategy, hot-path map, and worker protocol. Status is
[tasks.json](plan/tasks.json). 51 of 74 KL09 cards are done. KL09-13,
ranking the hotspot hypotheses, is in progress. Suite HOLD remains. The
hot-path notes on that page name the pending cards. Do not read an older
blob of either file as the current queue.

1. Freeze the claim gate: peer set (librdkafka C, Java, franz-go,
   rust-rdkafka, Rust client peer), per-profile metrics and claim types. A
   global "fastest Kafka client" claim requires superiority in every
   required cell of all six profiles on both architectures, with no
   exceptions. Profile-scoped, X%-faster (CI lower bound) and efficiency
   claims are separate types. Add an exploratory null-broker
   client-ceiling tier that can never support a Kafka claim.
2. Build a fast local evidence loop: verified bench drivers, ratcheted
   allocation and instruction-count CI gates, a validating null broker, a
   profiler script and a pinned local baseline. Optimization no longer
   waits for the multi-host campaign critical path.
3. Land one measured hotspot per card across the producer, codecs,
   consumer (KL03-22 is done), transport and runtime. Measure first; a
   measured rejection with reverted code is a valid outcome. Only additive
   APIs; no unsafe code; no new runtime dependencies.
4. Join KL-04: add franz-go, measure every peer's client ceiling, re-profile
   and re-rank, then audit the claim per cell on x86_64 and arm64 with
   independent reproduction.

**Work surfaces:** [producer](../src/producer.rs), [consumer](../src/consumer.rs),
[record codec](../src/protocol/records.rs), [network](../src/net.rs),
[benchmarks](https://github.com/mingley/partitionline/tree/917d877d7b049f3da5af90bd2a5804b85080ed2b/benchmarks).
**Done when:** every KL09 card is accepted, has a measured rejection or
is explicitly blocked; the allocation and instruction gates run in CI;
and KL09-70 records a per-cell verdict from raw artifacts. Public wording
uses only claim types whose thresholds are met, and follows the Suite
HOLD signoff.

### KL-10: Win the gateway workload

**Priority:** P0 restore green `main` CI; P1 cells, gzip, delivery API and
gzip consume.
**Depends on:** KL-04 peers and result format; KL-09 worker protocol and
codec benchmarks; KL-05 for zstd.

The [gateway adoption plan](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/plan/gateway-adoption.md) holds the profile,
the dated evidence and the card map. Cards are KL10-01 through KL10-17 in
[tasks.json](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/plan/tasks.json).

1. Restore a green `main`: the verifiable contract tests must find the
   example binaries they run (KL10-01). This card is done.
2. Make the gateway profile measurable: about 1 KiB JSON-like records,
   gzip, mTLS, `acks=all`, max in flight 1 and per-record acknowledgment,
   against a same-settings librdkafka peer, plus 1 KiB codec microbenchmarks.
   The codec family (KL10-03) is done. The produce and fetch cells
   (KL10-02, KL10-14) are pending.
3. Close the measured gaps: gzip decode (KL09-34, then decode off the
   caller task), gzip feed overhead and backend policy, an owned
   per-record delivery API, consumer peak RSS, and zstd (KL05-02..05).
   KL09-34, KL05-02 through KL05-05, KL10-05, KL10-06, KL10-15, KL10-16,
   and KL10-17 are done. KL10-07 is in progress. KL10-04, KL10-08,
   KL10-09, and KL10-10 are pending.
4. Propose gzip and gateway cells for the benchmark contract, and publish a
   dated local per-cell verdict. KL10-11, KL10-12, and KL10-13 are pending.

**Work surfaces:** [record codec](../src/protocol/records.rs),
[producer](../src/producer.rs), [consumer](../src/consumer.rs),
[CI](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/.github/workflows/ci.yml), [benchmarks](https://github.com/mingley/partitionline/tree/917d877d7b049f3da5af90bd2a5804b85080ed2b/benchmarks).
**Done when:** `main` CI is green; on the gateway cells partitionline uses
no more CPU per record than librdkafka for gzip produce and gzip consume at
matched settings, with p99 no worse; per-record acknowledgment costs at
most 10% over `try_send`; and KL10-12 has published its dated verdict.

## 5. Original first deliveries

KL03-01 through KL03-07, KL07-01, and KL08-01 are done. The table is the
order those cards were meant to land. The next card is the one `tasks.json`
marks ready. Independent replay of the consumer histories is still KL01-22.

| Order | Delivery | Followed by |
|---|---|---|
| 1 | KL03-01: promote the small consumer wire fixture. | KL03-02: preserve successful records across partial retries. |
| 2 | KL03-03, then KL03-04: repair abort intervals and seek filtering, one change each. | KL03-05: honor the reset policy. |
| 3 | KL03-06, then KL03-07: separate delivered positions and fix auto-commit timing. | Idempotent, transaction, and group recovery proofs. |
| Alongside, other files | KL07-01: fix one rustdoc link. KL08-01: reject non-CI release evidence. | The strict documentation and release-gate follow-ups. |
| Alongside, specifications | KL01-01: conformance registry. KL02-01: resource contract. KL04-01: benchmark contract. KL05-01: feature matrix. | Independent fixtures, bounds, comparable peer drivers, and one-feature cards. |

Read one ready card and the evidence it names. Long evidence runs are
resumable jobs. The handoff rules are in the [task workflow](plan/README.md).

Existing proof entry points (from the repository root):

| Purpose | Command | Limit |
|---|---|---|
| Local fast lane | `bash scripts/ci-branch-lite.sh` | Not the complete CI matrix. |
| Unit/mock and optional feature behavior | `cargo test --all-targets`; `cargo test --all-targets --features tracing` | Ignored live suites do not run automatically. |
| Decode smoke | `cargo test --test fuzz_decode_smoke` | Not a sustained fuzz campaign. |
| Broker and auth smoke | `REQUIRE_BROKER=1 bash scripts/ci-broker-smoke.sh`; `REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh` | Starts local broker infrastructure; inspect prerequisites and actual broker versions. Not HA qualification. |
| Count and latency smoke | `bash scripts/ci-integrity-smoke.sh` | Count equality is not an exactly-once oracle. The historical nested latency failure is now separated from the integrity CI job. |
| Rust documentation | `bash scripts/ci-docs.sh` | Builds rustdoc and checks intra-doc warnings, not Markdown file links. |

Close a TODO only with a PR, exact source/tool/peer revisions, configuration,
commands, raw artifacts, observed result, exclusions and reviewer signoff.
Record owners and next actions for failures. Keep evidence dated and scoped;
neither a feature inventory nor a crates.io upload can substitute for it.
