# STATUS

Suite HOLD stands. This file records holes. It does not lift them.

| Hole | Status |
|---|---|
| Fetch writeup | **Recorded** 2026-08-28 on this-VM (Apache Kafka 3.9.1 KRaft + rust-rdkafka 0.39.0). **Unsigned** until Kernel Integrity signs. Not Lab A. Not a signed vs-C win. |
| Latency | **Recorded** 2026-08-28 on this-VM (Apache Kafka 3.9.1 KRaft + rust-rdkafka 0.39.0 produce-ack). **Unsigned** until Kernel Integrity signs. Not Lab A. Not a signed vs-C win. Not a Suite HOLD lift. CI relative gate (`scripts/ci-latency-gate.sh` vs `docs/latency-baseline.json`) rechecked 2026-09-04 on native Kafka 4.1 (produce-ack p99 ≈ 119µs vs baseline 500µs ceiling; earlier same-day sample ≈ 379µs) — still **unsigned**, still not a Suite HOLD lift. Lab A smoke same day: 50k produce with HW sum == acked (integrity only, not a Suite HOLD lift). |
| e2e | First mock-broker protocol-client e2e landed in #80 (`tests/e2e.rs`). Mock only. |

Named gaps stay closed: ElectLeaders (43), DescribeLogDirs v5, DescribeQuorum (55), Add/Remove/UpdateRaftVoter (80–82).

This slice is latency writeup only. It is not a Suite HOLD lift. It is not a win.
Numbers and reproduce steps: [benchmark.md](benchmark.md).
