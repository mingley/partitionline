# Partitionline Kafka Leadership & Production Roadmap

This roadmap establishes the executable, evidence-backed strategy to establish `partitionline` as the premier Apache Kafka client implementation: memory-safe, verifiable, resource-predictable, and enterprise-operable.

---

## 1. Audited Baseline & Integrity Anchors

All planning in this roadmap is anchored to the audited source state of this repository as of **September 4, 2026** (local):

**Honesty bar:** crates.io `0.1.0` is already Installable (day1 pins + parks on
`main`). **Suite HOLD remains** until signed Lab A evidence — unsigned tip
Verifiable / latency samples must not be marketed as a Suite HOLD lift. See
`docs/CIVILIZATION.md` + `docs/STATUS.md`.

- **Immutable Baseline SHA:** `54020e2f9e695b6d1c817cc80ec9e99e4e171df9` (`54020e2`).
- **Release Status & Metadata:** Package version `0.1.0` in `Cargo.toml`, published on crates.io (2026-09-05T01:32Z / Sept 4 local). Initial public release; publication proves packaging and installability, not enterprise production readiness.
- **Architectural & Native Dependency Identity:**
  - *Core Protocol & Runtime:* Pure memory-safe Rust with `#![forbid(unsafe_code)]`. No C Kafka client (`librdkafka`), no OpenSSL, no Cyrus SASL.
  - *Transport & Cryptography Separation:* TLS uses `rustls` with `ring`, which compiles C and assembly code via the `cc` build dependency. Pure-Rust client logic does not equate to zero C compilation in the Cargo dependency tree.
  - *Codec Reality:* `gzip` (`flate2`), `snappy` (`snap`), and `lz4` (`lz4_flex`) are implemented in pure Rust. `zstd` is currently **not implemented** per [gaps.md](gaps.md) and [zstd-spike.md](zstd-spike.md). Pure-Rust decompression (`ruzstd`) exists, but encoder compatibility is unverified at scale; adding zstd does not remove `ring`. Any future C wrapper (`zstd-sys`) must remain an explicit opt-in feature, never a default dependency.
  - *Schema Registry:* Explicitly companion-only (`partitionline-schema`, [schema-companion.md](schema-companion.md)) and excluded from the core client crate.
- **Benchmark Honesty & Suite HOLD:** Suite HOLD stands in [STATUS.md](STATUS.md) and [benchmark.md](benchmark.md). Lab A produce throughput is locked vs librdkafka 2.15.0 C; fetch and latency are this-VM 2026-08-28 unsigned samples. No universal "fastest" claims are made or permitted.
- **Audited Baseline: Interrupted Handoff & Scoped CI Stabilization Blockers:**
  The repository baseline represents an interrupted handoff where prior sessions stopped mid-flight due to context/token limits, leaving code and notes frozen mid-work. While the core protocol engine is functional (1,029 unit tests pass; data integrity verified in CI with 0 record loss), remote CI is blocked by scoped issues:
  - *Main CI Failure (Run 33938039612 on 54020e2):* GitHub Actions run completed in Failure with two scoped broken jobs:
    1. `fmt` (job 101229710789): `cargo fmt --all -- --check` fails strictly on import ordering and an extra blank line in `tests/fuzz_decode_smoke.rs` (landed during post-cut merge 9dc524a / run 33937605449).
    2. `integrity-smoke` (job 101229710713): Native Kafka 4.1.0 started, and an attempted Docker 3.9.1 port 9092 conflict fell back to native tools. Crucially, **the data integrity portion PASSED** (`acked 2000`, `hw_delta 2000`, `consumed 2000` with 0 record loss). The fatal failure was the follow-on `scripts/ci-latency-gate.sh` under `REQUIRE_INTEGRITY=1`: produce-ack `p99=1344µs` exceeded the `750µs` relative ceiling (`baseline 500µs` + 50% slack) with `p50=362µs` (`COUNT=2000`, `WARMUP=200`, `acks=1`, `linger=0`). Root cause remains unproven (transient CI virtualization load vs true client regression); do not claim data loss occurred and do not blindly loosen thresholds without controlled calibration.
    *(Note: `release-plz` succeeded independently on both 54020e2 and 9dc524a because release metadata checks do not evaluate test or formatting integrity).*
  - *Host Portability Blocker:* `scripts/ci-broker-smoke.sh:111` hard-codes GNU coreutils `timeout 45s`, failing on macOS (`timeout: command not found`) during group, eos, and kip848 smoke tests.
  - *Civilization Bar Failure:* `scripts/audit-civilization-bars.sh` fails on clean tree (`FAIL post-Installable handoff missing/unwired`).
  - *Metadata Verification Failure:* `scripts/check-crate-metadata.sh` fails due to description regex mismatch in the packaging checker.
  - *Unrun Live Suites:* `tests/kip848_live.rs` is `#[ignore]` in default `cargo test`; `tests/e2e.rs` is mock-only; multi-broker cluster chaos tests are completely unrun.

### Baseline Capability & Verification Status Matrix

| Subsystem / Surface | Implementation Status | Mock Suite Status | Live Broker CI Status | Measured Benchmark Status | Unverified Holes & Gaps |
|---|---|---|---|---|---|
| **Produce API** | Implemented (v3–v12, batches, acks, linger) | Verified (`tests/full_surface.rs`) | Smoke verified (Kafka 3.9.1 / 4.1.0) | Lab A median 6.17M–7.28M rec/s vs C | Multi-broker partition leader failover |
| **Fetch API** | Implemented (v4–v17, epoch recovery) | Verified (`tests/full_surface.rs`) | Smoke verified (Kafka 3.9.1 / 4.1.0) | this-VM unsigned 5.28M rec/s vs rdkafka 0.39 | 24-hour buffer leak and backpressure soak |
| **Transactions / EOS** | Implemented (`transactional.id`, commit/abort) | Verified (`tests/client_api.rs`) | Smoke verified (`examples/eos.rs`) | Unverified | Crash-history differential verification vs Java |
| **Classic Groups** | Implemented (range/sticky/cooperative) | Verified (`tests/full_surface.rs`) | Smoke verified (3.9.1 / 4.1.0 KRaft) | Unverified | Multi-consumer high-churn rebalance stability |
| **KIP-848 Next-Gen Groups** | Implemented (`ConsumerGroup::join_consumer`) | Verified (`tests/full_surface.rs`) | Smoke verified (Kafka 4.1.0 KRaft) | Unverified | Dynamic member revocation under network partition |
| **KIP-932 Share Groups** | Implemented (`ShareGroup` poll/ack/reject) | Verified (`tests/full_surface.rs`) | Smoke verified (4.1 `share.version=1`) | Unverified | Concurrent consumer lock expiry under broker restart |
| **Produce-Ack Latency** | Implemented (linger=0, fast-route `try_send`) | Verified | Gated in CI (`scripts/ci-latency-gate.sh`) | this-VM unsigned (p50 62µs, p99 95µs) | Open-loop latency avoiding coordinated omission |
| **Compression Codecs** | Implemented (none, gzip, snappy, lz4) | Verified | Smoke verified | lz4 measured Lab A | zstd not implemented (blocked on C / evaluation) |
| **Security & SASL** | Implemented (PLAIN, SCRAM-256/512, OAUTH, OIDC) | Verified (`tests/full_surface.rs`) | Smoke verified (`scripts/ci-auth-smoke.sh`) | Measured SASL_PLAINTEXT / SSL | Non-blocking token renewal before expiry |

---

## 2. Best-in-Class Scorecard & Target Thresholds

**Governing Law:** *Integrity gates strictly precede performance.* Any correctness regression, data loss, duplicate delivery under EOS, or protocol decoding mismatch immediately fails CI and halts release qualification.

All numerical targets below are explicitly marked **PROPOSED** engineering goals, not achieved guarantees:

| Dimension | Primary Metric | Baseline (Audited 2026-09-04) | PROPOSED Target Threshold | Verification Gate & Contract |
|---|---|---|---|---|
| **Data Integrity** | Uncommitted loss / duplicate delivery | 0 reported in mock/smoke | **0 data loss; 0 duplicates under EOS** | Differential crash harness vs Java oracle |
| **Protocol Fidelity** | Wire encoding divergence vs Java client | 0 known on tested paths | **0 divergence across negotiated APIs** | Differential wire comparator vs Apache Kafka 3.9/4.1 |
| **Epoch Fencing** | Undetected zombie sequence/epoch | Mock verified | **100% fencing detection across re-elections** | Fault-injection crash matrix with leader rebalance |
| **Open-Loop Tail Latency** | Produce-ack p99 (linger=0, 50k rec/s) | ~95 µs (this-VM, closed-loop, unsigned) | **p99 ≤ 100 µs; p99.9 ≤ 250 µs** (PROPOSED) | Open-loop Poisson-driven bench (no coordinated omission) |
| **Throughput & Efficiency** | Sustained produce (linger=5ms, 100B, acks=1) | 6.17M rec/s (Lab A median vs C) | **≥ 7.0M rec/s sustained @ < 1.0 CPU core** (PROPOSED) | Multi-run locked Lab A harness on x86_64 & aarch64 |
| **Memory Predictability** | Steady-state RSS under backpressure | Unverified long soak | **Bounded RSS ≤ 64 MiB; 0 buffer leaks** (PROPOSED) | 24-hour continuous saturated sender soak |
| **Operational Recovery** | Rebalance partition handoff pause | Smoke progress verified | **Partition handoff ≤ 500 ms** (PROPOSED) | Dynamic 3-node KRaft rolling restart test |
| **Diagnostics Overhead** | Tracing span & metric scrape impact | Untimed feature flag | **< 2% throughput penalty with spans enabled** (PROPOSED) | Comparative bench with `--features tracing` |

---

## 3. Product Scopes, Boundaries, & Decision Gates

To prevent unbounded scope creep, feature bloat, and regression risks, development is partitioned into three distinct tiers:

```
[ Tier 1: Core Production Hardening ] ---> [ Tier 2: Expanded Enterprise (Demand-Gated) ] ---> [ Tier 3: v1.0.0 SemVer Freeze ]
- Correctness oracles vs Java driver        - Pure-Rust zstd (decompressor first)              - Public API freeze ([api-stability.md](api-stability.md))
- Bounded memory & backpressure             - KIP-848 / KIP-932 multi-node chaos               - Guaranteed MSRV (Rust 1.85+)
- Graceful cancellation & flush             - SASL OAUTH/OIDC background token renewal          - Multi-day continuous soak sign-off
- 3-broker KRaft leader failover            - Decoupled OpenTelemetry tracing spans             - Signed Lab A benchmarks (lift HOLD)
```

### Exclusions, Tradeoffs, & Non-Goals

1. **No 1:1 C Symbol Cloning:** `partitionline` provides an idiomatic, asynchronous Rust API shaped like the Java driver (`Producer`, `Consumer`, `Admin`). Replicating internal `rd_kafka_*` handle structures, legacy string-based config bags, or obsolete v0/v1 protocol quirks is explicitly out of scope.
2. **No C Dependencies in Default Features:** Kerberos / GSSAPI requires Cyrus SASL (C) and remains excluded from default features.
3. **Decoupled Schema Registry:** Schema Registry support stays strictly in the companion crate `partitionline-schema` ([schema-companion.md](schema-companion.md)) to avoid dragging HTTP client, Avro/Protobuf encoders, and schema caching dependencies into the core client.
4. **Demand-Gated Codec Expansion:** Pure-Rust `zstd` decompression (`ruzstd`) will be evaluated first. If an enterprise user requires `zstd` compression before pure-Rust encoders mature, it must be offered as an opt-in, non-default feature linking `zstd-sys`, with explicit documentation of native build requirements.

---

## 4. Priority Execution Packets

Engineering work is organized into stable execution packets (`KL-01` through `KL-08`). The first three packets represent immediate pull requests:

```
KL-01 (Differential Oracle) ----> KL-02 (Memory & Cancellation) ----> KL-03 (KRaft Multi-Broker Chaos)
     |                                                                     |
     v                                                                     v
KL-04 (Open-Loop Latency)        KL-05 (zstd Codec Evaluation)         KL-06 (SASL Token Refresh)
     \                                     |                               /
      ------------------------------------>+------------------------------
                                           |
                                           v
                             KL-07 (Decoupled Observability)
                                           |
                                           v
                             KL-08 (Migration & Canary Playbook)
```

### Immediate PR 1: KL-01 — CI Stabilization, Portability Recovery, & Differential Oracle Harness
- **DRI / Maintainer:** Michael Ingley (Coordinating Maintainer).
- **Dependencies:** None.
- **Target Files:** `tests/fuzz_decode_smoke.rs`, `scripts/ci-integrity-smoke.sh`, `scripts/ci-latency-gate.sh`, `scripts/ci-broker-smoke.sh`, `scripts/check-crate-metadata.sh`, `tests/oracle.rs`, `src/protocol/`.
- **Deliverables (Stabilization & Recovery First):**
  1. **Restore Main CI Format:** Fix import ordering and extra blank line in `tests/fuzz_decode_smoke.rs` to unblock GHA job `101229710789` (broken since `9dc524a`).
  2. **Calibrate & Stabilize Latency Gate:** First reproduce and calibrate `scripts/ci-latency-gate.sh` on a controlled, quiet host vs shared CI runners to isolate the `p99=1344µs` latency spike (differentiating runner noise from genuine code regressions before adjusting thresholds), ensuring `REQUIRE_INTEGRITY=1` reliably passes in `ci-integrity-smoke.sh` (resolving GHA job `101229710713`).
  3. **Harness Portability Recovery:** Replace the hard-coded GNU `timeout` dependency in `scripts/ci-broker-smoke.sh:111` with a portable POSIX/Perl/Python fallback to fix macOS smoke test failures (`timeout: command not found`).
  4. **Packaging Check Recovery:** Fix description regex matching in `scripts/check-crate-metadata.sh` to unblock preflight verification.
  5. **Differential Protocol Oracle:** Build the initial differential wire comparator harness verifying Produce, Fetch, ListOffsets, and Metadata against version-pinned Apache Kafka Java client (`3.9.1`, `4.1.0`) and librdkafka 2.15.0.
- **Pass/Fail Acceptance Evidence:** 100% green GitHub Actions matrix on `main` (`fmt`, `clippy`, `test`, `integrity-smoke`); `scripts/ci-broker-smoke.sh` runs cleanly on macOS; calibrated latency baseline on controlled hardware; 0 serialization discrepancies vs Java oracle.
- **Runnable Command:** `cargo fmt --all -- --check && cargo test --test fuzz_decode_smoke && bash scripts/ci-branch-lite.sh`

### Immediate PR 2: KL-02 — Bounded Memory Backpressure & Graceful Shutdown Invariants
- **Role:** Systems & Runtime Engineer (Unassigned).
- **Dependencies:** `KL-01`.
- **Target Files:** `src/producer/mod.rs`, `src/consumer/mod.rs`, `src/connection/pool.rs`, `scripts/ci-latency-gate.sh`.
- **Deliverables:**
  1. Enforce strict `buffer.memory` limits under producer saturation with deterministic async backpressure (no unbounded task queues or hidden buffer allocations).
  2. Implement cancellation tokens and deterministic `close(timeout)` flushes that guarantee all acknowledged batches reach the broker or return descriptive errors without hung futures.
  3. **Flaky Latency Recovery:** Harden `scripts/ci-latency-gate.sh` and `ci-integrity-smoke.sh` against transient load spikes so that local test runs do not exit 2 (`PARTIAL`) under agent load.
- **Pass/Fail Acceptance Evidence:** Steady-state RSS strictly bounded under continuous 2× overload; 0 hanging tasks on immediate consumer cancellation; deterministic clean exit in latency gate under concurrent load.
- **Runnable Command:** `cargo test --lib && bash scripts/ci-integrity-smoke.sh`

### Immediate PR 3: KL-03 — Multi-Broker KRaft Chaos & Partition Leader Failover
- **Role:** Infrastructure & Chaos Engineer (Unassigned).
- **Dependencies:** `KL-02`.
- **Target Files:** `scripts/ci-broker-smoke.sh`, `tests/chaos.rs`, `tests/kip848_live.rs`, `scripts/audit-civilization-bars.sh`.
- **Deliverables:**
  1. Establish a 3-broker, 3-controller KRaft cluster harness in CI supporting controlled kill, SIGSTOP, and network delay injection.
  2. Test partition leader failover during saturated `acks=all` produce loops, verifying that producer sequence numbers and epoch fencing prevent both duplicate records and data gaps.
  3. **Live Suite Activation:** Un-ignore `tests/kip848_live.rs` during live broker runs; resolve post-Installable handoff failure in `scripts/audit-civilization-bars.sh` (pass=42/42).
- **Pass/Fail Acceptance Evidence:** Exactly-once record accounting (HW sum == acked records == consumed records) through rolling broker restarts; 100% passing civilization bar audit.
- **Runnable Command:** `REQUIRE_BROKER=1 bash scripts/ci-broker-smoke.sh`

### Subsequent Execution Packets

- **KL-04: Open-Loop Benchmark Suite & Reproducible Tail Latency (PR 4, Perf Engineer - Unassigned):**
  - Implement open-loop Poisson rate generator in `examples/bench_latency.rs` to eradicate coordinated omission.
  - Measure p50, p90, p95, p99, p99.9 latency under bounded offered load (10k, 50k, 100k rec/s).
  - Output raw JSON artifacts ([benchmark.md](benchmark.md)) across x86_64 and aarch64. Does not lift Suite HOLD without signed Lab A audit.
  - *Verification:* `bash scripts/ci-latency-gate.sh`
- **KL-05: Pure-Rust Codec Evaluation & Framing Verification (PR 5, Protocol Engineer - Unassigned):**
  - Benchmark pure-Rust `ruzstd` decompression against reference C `libzstd` on Kafka record batches.
  - Test encoder compatibility across Kafka 3.9 and 4.1 brokers. Define opt-in feature gating if native build is required.
  - *Verification:* `cargo test --lib --all-features`
- **KL-06: Non-Blocking SASL Token Refresh & Transport Resilience (PR 6, Security Engineer - Unassigned):**
  - Add proactive background token refresh for SASL OAUTHBEARER and OIDC before token expiration.
  - Enforce fail-closed connection reset on authentication refresh failure.
  - *Verification:* `REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh`
- **KL-07: Decoupled Observability Adapters (PR 7, Observability Engineer - Unassigned):**
  - Expand `tracing` spans across connection handshakes, request batching, and rebalance transitions.
  - Provide Prometheus / OpenTelemetry adapter examples without adding heavyweight HTTP/gRPC exporter dependencies to core.
  - *Verification:* `cargo test --features tracing && bash scripts/ci-docs.sh`
- **KL-08: Enterprise Migration Recipes & Canary Rollout Playbook (PR 8, DX Maintainer - Unassigned):**
  - Author complete migration guides from `rust-rdkafka` and Java drivers ([migrate-from-rdkafka.md](migrate-from-rdkafka.md)).
  - Publish reversible dual-write shadow testing and 24h/7d canary recipes with error-budget rollback triggers.
  - *Verification:* `bash scripts/ci-docs.sh && cargo check --examples`

---

## 5. Correctness & Verification Architecture

True leadership requires unassailable proof of correctness under failure:

### A. Differential Wire Oracles & Protocol Compatibility
- **Pinned Oracle Verification:** Continuously compare serialized RPC requests and decoded responses against reference outputs from version-pinned Apache Kafka Java client (`3.9.1`, `4.1.0`) and `librdkafka` `2.15.0`.
- **Flexible Frame Validation:** Exhaustively validate compact array encodings, nullable strings, varints, and tagged field handling across all supported API version ranges.
- **Error Code Mapping:** Verify that all Kafka error codes (e.g. `CORRUPT_MESSAGE`, `UNKNOWN_TOPIC_OR_PARTITION`, `NOT_ENOUGH_REPLICAS`) are mapped to typed `ApiError` variants with deterministic retry semantics.

### B. Sequence, Epoch, Fencing, & Transaction Crash Invariants
- **Monotonic Sequencing:** Maintain strict monotonic sequence numbers per `(ProducerId, Partition)` without gaps, even under network disconnects and broker failover.
- **Idempotence State Machine:** Handle `UNKNOWN_PRODUCER_ID` by bumping the producer epoch locally and retrying; terminate with non-retriable errors on `PRODUCER_FENCED` or `INVALID_PRODUCER_EPOCH`.
- **Transaction State Transitions:** Verify two-phase coordinator commit/abort flows (`AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn`) under injected broker crashes, ensuring no orphaned transaction markers.
- **Read-Committed Isolation:** Verify that consumers configured with `IsolationLevel::ReadCommitted` filter aborted transaction batches and control markers without stalling offset progression.

### C. Cancellation, Timeouts, & Graceful Shutdown
- **Tokio Cancellation Safety:** Dropping an active `send()`, `fetch()`, or `poll()` future must never leak buffer memory, orphan background tasks, or corrupt connection state.
- **Deterministic Timeout Enforcement:** Respect `delivery.timeout.ms` (30s) and `max.block.ms` (30s) as strict deadlines; expired batches are aborted with `Error::DeliveryTimeout` without stalling subsequent traffic.
- **Graceful Shutdown Protocol:** `Producer::close(timeout)` and `Consumer::close(timeout)` flush queued batches, commit offsets, send `LeaveGroup`, and terminate network connections within the allocated deadline.

### D. Memory Bounding & Backpressure
- **Strict Buffer Ceilings:** Enforce `buffer.memory` (32 MiB) as a hard upper bound on queued bytes. When saturated, `try_send` immediately returns `Error::BufferExhausted`, while `send` applies asynchronous backpressure up to `max.block.ms`.
- **Zero-Allocation Buffer Recycling:** Reuse pooled memory buffers across batch encode and fetch decode paths, eliminating heap churn on steady-state high-throughput workloads.
- **Leak Detection:** Enforce zero RSS growth and verify complete buffer reclamation across 24-hour continuous stress loops under sustained backpressure.

### E. Rebalance Stability & Next-Gen Consumer Protocols
- **Cooperative-Sticky Rebalance (KIP-429):** Execute two-phase revocations, allowing consumers to continue processing retained partitions while reassigned partitions migrate.
- **KIP-848 Next-Gen Groups:** Validate `ConsumerGroupHeartbeat` (API 68) state reconciliations, dynamic assignment changes, and rack-aware consumption against real Kafka 4.x KRaft brokers.
- **KIP-932 Share Groups:** Verify concurrent record acquisition, explicit per-record acknowledgment (`ACCEPT`, `RELEASE`, `REJECT`), and automated lock expiration on consumer node failure.

### F. Transport Resilience & Non-Blocking Auth Refresh
- **Proactive SASL Token Renewal:** Refresh SASL OAUTHBEARER and OIDC tokens in the background at 80% of token TTL, preventing connection churn on active pipelines.
- **Fail-Closed Security:** Terminate connections immediately if TLS certificates or SASL credentials fail validation; prevent plaintext fallback.

---

## 6. Empirical Benchmark Methodology

To ensure performance measurements reflect genuine production behavior rather than synthetic test artifacts:

### A. Eliminating Coordinated Omission (Open-Loop Testing)
- **Open-Loop Load Generation:** Drive offered load using an open-loop arrival schedule (Poisson or uniform independent rate generator) in `examples/bench_latency.rs`.
- **Unskewed Tail Measurement:** Decouple request scheduling from client response reception to ensure that server stalls are fully captured in p95, p99, and p99.9 latency distributions rather than artificially hidden.

### B. Apples-to-Apples Semantic Parity
- **Configuration Parity:** Lock exact parameters across `partitionline` and competitor clients:
  - Durability: `acks=1` or `acks=all` (idempotent).
  - Batching: Identical `batch.size` (1,000,000 bytes) and `linger.ms` (5 ms or 0 ms).
  - Codecs: Identical compression codecs and block sizes (e.g. LZ4 64 KiB blocks).
  - In-Flight Bounds: Maximum 5 in-flight requests per connection.

### C. Component Overhead Disaggregation
Measure and publish discrete latency breakdowns across the request lifecycle:
1. **Client Enqueue:** Duration from `Producer::send` to placement into the batch accumulator.
2. **Codec Overhead:** CPU time spent in compression (pure Rust LZ4/Snappy vs C).
3. **Transport Overhead:** Time spent in TLS encryption (`rustls`/`ring` vs OpenSSL) and socket I/O.
4. **Broker Persistence:** High-watermark commit time on the broker disk.

### D. Multi-Architecture Reproducibility & Regression Budgets
- **Hardware Coverage:** Execute standardized benchmark runs on both `x86_64` (Linux bare metal) and `aarch64` (Apple Silicon and AWS Graviton).
- **Regression Budget:** Automated CI checks fail if produce-ack p99 latency regresses by > 5% or throughput regresses by > 2% relative to the baseline.
- **Public Artifacts:** Publish complete, raw JSON latency histograms alongside benchmark writeups. Maintain Suite HOLD until signed Lab A protocol audit criteria are satisfied.

---

## 7. Operational Usability & Canary Rollout Playbook

Safe enterprise adoption requires clear operator ergonomics, robust security hygiene, and reversible deployment paths:

### A. Newcomer & Migration Ergonomics
- **Idiomatic Async API:** Provide direct, clean migration recipes from `rust-rdkafka` (`FutureProducer` -> `Producer`, `StreamConsumer` -> `ConsumerGroup`) and Java drivers ([migrate-from-rdkafka.md](migrate-from-rdkafka.md)).
- **Standard Enterprise Recipes:** Publish ready-to-run examples for dead-letter queues (DLQ), transactional outbox patterns (`examples/eos.rs`), and dynamic rebalance listeners.

### B. Decoupled Observability Architecture
- **Zero-Dependency Core Telemetry:** Expose structured client metrics via `Producer::metrics`, `Consumer::metrics`, and `Admin::metrics` without forcing HTTP/gRPC exporter dependencies into the core crate.
- **Pluggable Tracing Spans:** Provide standardized `tracing` spans across connection handshakes, batch dispatch, and consumer rebalances via the optional `tracing` feature flag.

### C. Security Governance & Bounded Credential Redaction
- **Credential Redaction:** Guarantee that passwords, SCRAM secrets, SASL tokens, and private keys are redacted from `std::fmt::Debug`, log events, and error messages.
- **Supply-Chain Integrity:** Maintain `#![forbid(unsafe_code)]`; enforce `cargo-deny` policies prohibiting unapproved C wrappers and tracking all dependency advisories.

### D. Reversible Phased Canary Rollout Playbook
1. **Phase 1: Shadow Validation (24 Hours):**
   - Dual-produce application traffic to a shadow topic using `partitionline` alongside existing clients.
   - Verify 100% payload and offset delivery match; compare latency histograms without consuming downstream.
2. **Phase 2: Canary Consumer Deployment (7 Days):**
   - Route 1% of production consumer group partitions to `partitionline`.
   - Monitor consumer lag, fetch error rates, and memory stability continuously.
3. **Automated Rollback Triggers:**
   - Any unhandled error rate > 0.01% of total requests.
   - Any produce-ack p99 latency exceeding 2× baseline for > 5 consecutive minutes.
   - Any detected partition rebalance stall exceeding 3 seconds.
   - Immediate automatic rollback to legacy client with zero operator manual intervention required.
