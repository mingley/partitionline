# TODO: Kafka Leadership & Production Hardening

Executable task tracker for bringing `partitionline` to tier-1 enterprise production readiness and v1.0.0. Aligned with [docs/ROADMAP.md](docs/ROADMAP.md).

**Baseline Reality (Interrupted Handoff):** Prior workers stopped mid-flight due to context/token limits, leaving code and notes frozen mid-work. The core protocol engine is functional (1029 unit tests pass; data integrity verified in CI [acked=hw_delta=consumed=2000]), but CI is blocked by scoped issues: `fmt` (job `101229710789`, import ordering in `tests/fuzz_decode_smoke.rs`) and follow-on `ci-latency-gate` (job `101229710713`, p99=1344µs vs 750µs limit; prior run `33937605449` failed identically, while `release-plz` passed independently). PRs 1–3 are stabilization- and recovery-first.

Installable (`0.1.0` on crates.io) is met. **Suite HOLD remains** (Lab A unsigned).
Do not treat unsigned Verifiable samples as a Suite HOLD lift.

---

## Immediate Priorities (Ranked PRs 1–3: Stabilization & Core Invariants)

- [ ] **KL-01: CI Stabilization, Portability Recovery, & Differential Oracle Harness**
  - **DRI / Maintainer:** Michael Ingley (Coordinating Maintainer) | **Rank:** PR 1 | **Effort:** Indicative Medium
  - **Dependencies:** None
  - **Target Files:** `tests/fuzz_decode_smoke.rs`, `scripts/ci-latency-gate.sh`, `scripts/ci-integrity-smoke.sh`, `scripts/ci-broker-smoke.sh`, `scripts/check-crate-metadata.sh`, `tests/oracle.rs`, `src/protocol/`
  - **Deliverable:**
    1. **Restore Main CI Format:** Fix import ordering and extra blank line in `tests/fuzz_decode_smoke.rs` (unblock GHA job `101229710789`).
    2. **Calibrate Latency Gate:** First reproduce on controlled quiet host to isolate p99=1344µs runner load contention vs true client regression before adjusting thresholds; stabilize `ci-integrity-smoke.sh` under `REQUIRE_INTEGRITY=1` (resolving GHA job `101229710713`).
    3. **Harness Portability Recovery:** Replace hard-coded GNU `timeout` in `scripts/ci-broker-smoke.sh:111` with a portable fallback for macOS developer environments (`timeout: command not found`).
    4. **Packaging Check Recovery:** Fix description regex in `scripts/check-crate-metadata.sh` to unblock preflight checks.
    5. **Differential Wire Oracle:** Wire comparator verifying Produce, Fetch, ListOffsets, and Metadata against version-pinned Apache Kafka Java client (`3.9.1`, `4.1.0`) and librdkafka 2.15.0.
  - **Acceptance:** Green CI on `main` (`fmt`, `clippy`, `test`, `integrity-smoke`); clean local macOS smoke runs; calibrated latency baseline on controlled hardware; 0 wire discrepancies vs Java oracle.
  - **Verification:** `cargo fmt --all -- --check && cargo test --test fuzz_decode_smoke && bash scripts/ci-branch-lite.sh`

- [ ] **KL-02: Bounded Memory Backpressure & Graceful Shutdown Invariants**
  - **Role:** Systems & Runtime Engineer (Unassigned) | **Rank:** PR 2 | **Effort:** Indicative Medium
  - **Dependencies:** `KL-01`
  - **Target Files:** `src/producer/mod.rs`, `src/consumer/mod.rs`, `src/connection/pool.rs`, `scripts/ci-latency-gate.sh`
  - **Deliverable:**
    1. Enforce strict `buffer.memory` (32 MiB default) ceilings under saturation; `try_send` returns `BufferExhausted`, while `send` applies async backpressure up to `max.block.ms`.
    2. Implement Tokio cancellation tokens and deterministic `close(timeout)` that flushes in-flight batches without dropping records or hanging futures.
    3. **Flaky Latency Recovery:** Harden `scripts/ci-latency-gate.sh` and `ci-integrity-smoke.sh` against transient load soft-misses (`PARTIAL` / exit 2) under concurrent agent load.
  - **Acceptance:** Steady-state RSS strictly bounded under continuous 2× overload; 0 hanging tasks on consumer cancellation; deterministic clean latency gate.
  - **Verification:** `cargo test --lib && bash scripts/ci-integrity-smoke.sh`

- [ ] **KL-03: Multi-Broker KRaft Chaos & Partition Leader Failover**
  - **Role:** Infrastructure & Chaos Engineer (Unassigned) | **Rank:** PR 3 | **Effort:** Indicative Large
  - **Dependencies:** `KL-02`
  - **Target Files:** `scripts/ci-broker-smoke.sh`, `tests/chaos.rs`, `tests/kip848_live.rs`, `scripts/audit-civilization-bars.sh`
  - **Deliverable:**
    1. Multi-broker (3-node KRaft) chaos test suite injecting rolling broker restart, leader re-election, and network delay.
    2. Verify that sequence numbers and epoch fencing prevent both duplicate records and data loss under saturated `acks=all` produce loops.
    3. **Live Suite Activation:** Un-ignore `tests/kip848_live.rs` during live broker runs; resolve civilization bar post-Installable handoff failure (`audit-civilization-bars.sh`).
  - **Acceptance:** Exactly-once record accounting (HW sum == acked records == consumed records) through rolling broker restarts; 100% passing civilization bar audit.
  - **Verification:** `REQUIRE_BROKER=1 bash scripts/ci-broker-smoke.sh`

---

## Intermediate Priorities (Packets KL-04 – KL-06)

- [ ] **KL-04: Open-Loop Latency Benchmarks & Reproducible Artifacts**
  - **Role:** Performance Engineer (Unassigned) | **Rank:** PR 4 | **Effort:** Indicative Medium
  - **Dependencies:** `KL-02`
  - **Target Files:** `examples/bench_latency.rs`, `docs/benchmark.md`, `scripts/ci-latency-gate.sh`
  - **Deliverable:** Implement open-loop Poisson rate generator in `examples/bench_latency.rs` to eradicate coordinated omission; measure p50, p95, p99, p99.9 produce-ack latency; publish raw JSON artifacts on x86_64 and aarch64.
  - **Acceptance:** Reproducible latency distributions with p99 ≤ 100 µs (PROPOSED) under 50k rec/s load; no bypass of Suite HOLD.
  - **Verification:** `bash scripts/ci-latency-gate.sh`

- [ ] **KL-05: Pure-Rust Codec Evaluation & Framing Verification**
  - **Role:** Protocol & Compression Engineer (Unassigned) | **Rank:** PR 5 | **Effort:** Indicative Medium
  - **Dependencies:** `KL-01`
  - **Target Files:** `docs/zstd-spike.md`, `src/record/compression.rs`, `Cargo.toml`
  - **Deliverable:** Benchmark pure-Rust `ruzstd` decompression against reference C `libzstd`; evaluate encoder maturity for Kafka frames; define opt-in feature gating if C wrapper is required (no default C dependencies).
  - **Acceptance:** Interoperable decode of zstd batches from Kafka 3.9/4.1; zero regression in existing gzip/snappy/lz4 suites.
  - **Verification:** `cargo test --lib --all-features`

- [ ] **KL-06: Non-Blocking SASL Token Refresh & Transport Resilience**
  - **Role:** Security & Network Engineer (Unassigned) | **Rank:** PR 6 | **Effort:** Indicative Small
  - **Dependencies:** `KL-02`
  - **Target Files:** `src/connection/sasl.rs`, `src/connection/pool.rs`, `examples/oauth.rs`
  - **Deliverable:** Background refresh for SASL OAUTHBEARER and OIDC tokens prior to expiration (at 80% TTL); fail-closed connection reset on auth refresh failure.
  - **Acceptance:** Seamless continuous produce/fetch across 2-hour token rotation cycles with 0 re-auth disconnect errors.
  - **Verification:** `REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh`

---

## Enterprise Usability & v1.0 Readiness (Packets KL-07 – KL-08)

- [ ] **KL-07: Decoupled Observability Adapters (Tracing & Metrics)**
  - **Role:** Observability Engineer (Unassigned) | **Rank:** PR 7 | **Effort:** Indicative Small
  - **Dependencies:** `KL-02`
  - **Target Files:** `src/metrics.rs`, `docs/guide.md`, `Cargo.toml`
  - **Deliverable:** Expand structured `tracing` spans on connection, batching, and rebalances; provide Prometheus / OpenTelemetry adapter examples without adding server dependencies to core crate.
  - **Acceptance:** Full span context propagation under `--features tracing` with < 2% throughput penalty.
  - **Verification:** `cargo test --features tracing && bash scripts/ci-docs.sh`

- [ ] **KL-08: Migration Recipes & Phased Canary Rollout Playbook**
  - **Role:** Developer Experience & Release Maintainer (Unassigned) | **Rank:** PR 8 | **Effort:** Indicative Small
  - **Dependencies:** `KL-03`, `KL-07`
  - **Target Files:** `docs/migrate-from-rdkafka.md`, `docs/ADOPTION.md`, `examples/`
  - **Deliverable:** Complete migration guide from `rust-rdkafka`; dual-write shadow testing harness; 24h shadow / 7d 1% canary rollout recipes with automated error-budget rollback triggers.
  - **Acceptance:** Tested copy-paste migration examples; clean doc links and validation.
  - **Verification:** `bash scripts/ci-docs.sh && cargo check --examples`

---

## Verification Contract (Existing Runnable Commands)

```bash
bash scripts/ci-branch-lite.sh               # Local mirror of fast Actions tip gate
cargo test --all-targets                    # Unit, integration, and mock suites
cargo test --test fuzz_decode_smoke         # Adversarial wire decode fuzzing smoke
bash scripts/ci-broker-smoke.sh              # Real Apache Kafka KRaft broker smoke (3.9.1 / 4.1.0)
REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh # SASL_SSL PLAIN/SCRAM/OAUTH/OIDC + mTLS gate
bash scripts/ci-integrity-smoke.sh           # Lab A HW==acked + consumed==seeded + latency gate
bash scripts/ci-docs.sh                      # Documentation build and intra-doc link validation
```
