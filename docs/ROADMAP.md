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
- [ ] **Pure-Rust Zstandard (zstd)**: Integrate a pure-Rust Zstandard decompression/compression backend (e.g., via `ruzstd` or pure-Rust bindings) to complete the codec matrix (Gzip, Snappy, LZ4, Zstandard) with zero native C dependencies.

### 2. High-Availability & Chaos Testing
- [ ] **Multi-Node KRaft Integration Matrix**: Expand CI test suites to continuously validate against real multi-broker Apache Kafka clusters (3.8.x, 3.9.x, and 4.0.x KRaft).
- [ ] **Broker Failover & Partition Rebalance Soak**: Automated testing of broker restart, leader election, and network partitioning during active high-throughput produce and consume loops.
- [ ] **Memory & Buffer Leak Verification**: 24-hour continuous load testing verifying bounded memory usage, buffer recycling, and zero memory leaks under gigabyte-scale streams.

### 3. Observability & Enterprise Operations
- [ ] **OpenTelemetry & Prometheus Export**: Built-in metrics exporters for client telemetry (records/sec, bytes/sec, produce latency percentiles, consumer group lag, rebalance duration).
- [ ] **Tracing Spans**: Standardized tracing instrumentation across connection handshakes, request batching, and rebalance transitions.

### 4. Ecosystem Integration
- [ ] **Schema Registry Support**: Provide a companion crate or integration guide for Confluent / Karapace Schema Registry (Avro, JSON Schema, Protobuf serialization).
- [ ] **Formal API Freeze & Stability Bar**: Guarantee MSRV (Rust 1.85+) and semver stability across all public structs and traits.

---

## Implementation Phases

| Phase | Milestone | Focus |
|---|---|---|
| **Phase 1: Codec Completion & Tooling** | `v0.2.0` | Pure-Rust zstd support, automated multi-node KRaft CI workflows, and crate documentation polish. |
| **Phase 2: Chaos & Soak Hardening** | `v0.3.0` – `v0.9.0` | 24-hour soak tests, broker failover validation, buffer memory stress tests, and OpenTelemetry metrics. |
| **Phase 3: Production GA (1.0.0)** | `v1.0.0` | API stability freeze, certified performance baseline vs librdkafka, and enterprise production sign-off. |
