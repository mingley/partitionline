# Production Readiness Roadmap

This document outlines the clear, phased ramp to bring `partitionline` from its initial `0.1.0` crates.io release to production-hardened **v1.0.0 General Availability**.

---

## Production Readiness Criteria

`partitionline` currently implements the complete Kafka client protocol surface in pure Rust (Producers, Consumers, Consumer Groups, Share Groups, Transactions/EOS, and Admin APIs) without C or `librdkafka` dependencies.

**Honesty bar:** crates.io `0.1.0` is already Installable (day1 pins + parks on
`main`). **Suite HOLD remains** until signed Lab A evidence — unsigned tip
Verifiable / latency samples must not be marketed as a Suite HOLD lift. See
`docs/CIVILIZATION.md` + `docs/STATUS.md`.

To achieve enterprise production readiness (`v1.0.0`), the following gates must be satisfied:

### 1. Compression Matrix Completion
- [ ] **Zstandard (zstd) — demand-gated**: Keep zstd **out of defaults** (`docs/zstd-spike.md`). Do **not** treat “integrate ruzstd by default” as P0 — pure-Rust Kafka interop is not proven. If survey [#85](https://github.com/mingley/partitionline/issues/85) demands it, ship an **opt-in** feature that documents any C link (`zstd-sys`), never enable by default; revisit pure-Rust when encode+decode roundtrips pass Apache Kafka 3.9/4.x in CI.

### 2. High-Availability & Chaos Testing
- [ ] **Multi-Node KRaft Integration Matrix**: Expand CI test suites to continuously validate against real multi-broker Apache Kafka clusters (3.8.x, 3.9.x, and 4.0.x KRaft).
- [ ] **Broker Failover & Partition Rebalance Soak**: Automated testing of broker restart, leader election, and network partitioning during active high-throughput produce and consume loops.
- [ ] **Memory & Buffer Leak Verification**: 24-hour continuous load testing verifying bounded memory usage, buffer recycling, and zero memory leaks under gigabyte-scale streams.

### 3. Observability & Enterprise Operations
- [x] **Metrics + tracing (WP-4 baseline)**: Guide documents snapshots; optional `tracing` feature; Prometheus text example shipped. Further OpenTelemetry exporters remain optional polish.
- [ ] **OpenTelemetry export (optional)**: Built-in OTLP/Prometheus exporters beyond the text example — only if adopters ask.

### 4. Ecosystem Integration
- [ ] **Schema Registry Support**: Companion `partitionline-schema` (separate crate; see `docs/schema-companion.md`) after survey demand — core `0.1.0` is already on crates.io.
- [ ] **Formal API Freeze & Stability Bar**: Guarantee MSRV (Rust 1.85+) and semver stability across all public structs and traits.

---

## Implementation Phases

| Phase | Milestone | Focus |
|---|---|---|
| **Phase 1: Chaos tooling & docs polish** | `v0.2.0` | Multi-node KRaft CI, failover/rebalance soaks, docs polish; zstd only if #85 demands (opt-in, non-default). |
| **Phase 2: Chaos & Soak Hardening** | `v0.3.0` – `v0.9.0` | 24-hour soak tests, broker failover validation, buffer memory stress tests, and OpenTelemetry metrics. |
| **Phase 3: Production GA (1.0.0)** | `v1.0.0` | API stability freeze, certified performance baseline vs librdkafka, and enterprise production sign-off. |
