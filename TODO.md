# TODO: Production Readiness Ramp

Actionable task tracker for taking `partitionline` to production readiness (v1.0.0).

Installable (`0.1.0` on crates.io) is met. **Suite HOLD remains** (Lab A unsigned).
Do not treat unsigned Verifiable samples as a Suite HOLD lift.

---

## High Priority (P0: Codec & Cluster Integration)

- [ ] **Pure-Rust Zstandard**: Integrate pure-Rust `zstd` support (e.g. `ruzstd` for decompression) so Zstandard-compressed topics work out of the box with zero C dependencies.
- [ ] **Multi-Node CI Testing**: Add multi-node KRaft broker test matrix (Kafka 3.8, 3.9, 4.0) to GitHub Actions CI.
- [ ] **Dynamic Partition Rebalance Stress Tests**: Write dedicated stress tests verifying partition revocation/assignment under cooperative-sticky rebalances during continuous consume.
- [ ] **Broker Failover Verification**: Test leader failover during active batch produce with idempotence (`acks=all`).

---

## Medium Priority (P1: Observability & Resilience)

- [ ] **OpenTelemetry Metrics Export**: Provide an optional feature to export client metrics (`p50`, `p99`, fetch latency, consumer lag) to OpenTelemetry / Prometheus.
- [ ] **Tracing Spans**: Add structured tracing spans to connection pools and batch dispatch loops.
- [ ] **Soak & Memory Audit**: Run long-duration soak test to verify zero buffer leaks under sustained backpressure.
- [ ] **Schema Registry Companion**: Publish documentation or companion crate for Schema Registry serialization (Avro, Protobuf, JSON).

---

## Low Priority (P2: Ergonomics & Extensions)

- [ ] **Dead Letter Queue (DLQ) Helper**: Add consumer error-handling helpers for routing malformed records to DLQ topics.
- [ ] **Transactional Outbox Pattern Example**: Create an end-to-end example demonstrating transactional produce alongside database state.
- [ ] **Micro-Benchmark Suite**: Automate continuous performance regression tracking in CI.
