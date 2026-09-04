# STATUS

Suite HOLD stands. This file records holes. It does not lift them.

| Hole | Status |
|---|---|
| Fetch writeup | **Recorded** 2026-08-28 on this-VM (Apache Kafka 3.9.1 KRaft + rust-rdkafka 0.39.0). **Unsigned** until Kernel Integrity signs. Not Lab A. Not a signed vs-C win. |
| Latency | **Recorded** 2026-08-28 on this-VM (Apache Kafka 3.9.1 KRaft + rust-rdkafka 0.39.0 produce-ack). **Unsigned** until Kernel Integrity signs. Not Lab A. Not a signed vs-C win. Not a Suite HOLD lift. CI relative gate (`scripts/ci-latency-gate.sh` vs `docs/latency-baseline.json`) rechecked 2026-09-04 on native Kafka 4.1 (produce-ack p99 ≈ 69µs vs baseline 500µs ceiling; earlier same-day samples ≈ 119µs / 379µs) — still **unsigned**, still not a Suite HOLD lift. Lab A produce smoke same day: HW sum == acked. Combined Lab A integrity (`scripts/lab-a-integrity.sh` / `ci-integrity-smoke.sh`): HW==acked and consumed==seeded (COUNT=2000 recheck same day) — unsigned only, not a Suite HOLD lift. |
| e2e | First mock-broker protocol-client e2e landed in #80 (`tests/e2e.rs`). Mock only. |

Named gaps stay closed: ElectLeaders (43), DescribeLogDirs v5, DescribeQuorum (55), Add/Remove/UpdateRaftVoter (80–82).

This file tracks holes and unsigned samples. It does not lift Suite HOLD.
Integrity harnesses (`scripts/lab-a-integrity.sh`, `lab-a-produce.sh`,
`lab-a-fetch.sh`) and the relative latency gate are **unsigned** evidence only.
Numbers and reproduce steps: [benchmark.md](benchmark.md).
