# STATUS

Suite HOLD stands. This file records holes. It does not lift them.

| Hole | Status |
|---|---|
| Fetch writeup | **Recorded** 2026-08-28 on this-VM (Apache Kafka 3.9.1 KRaft + rust-rdkafka 0.39.0). **Unsigned** until Kernel Integrity signs. Not Lab A. Not a signed vs-C win. |
| Latency | **Recorded** 2026-08-28 on this-VM (Apache Kafka 3.9.1 KRaft + rust-rdkafka 0.39.0 produce-ack). **Unsigned** until Kernel Integrity signs. Not Lab A. Not a signed vs-C win. Not a Suite HOLD lift. CI relative gate (`scripts/ci-latency-gate.sh` vs `docs/latency-baseline.json`) rechecked 2026-09-04 on native Kafka 4.1 (produce-ack p99 ≈ 71µs / 86µs / later ≈ 75µs / 77µs vs baseline 500µs ceiling; earlier same-day ≈ 69µs / 119µs / 379µs; later same-day native rechecks ≈ 663–865µs / 742µs under agent load — still under the relative slack ceiling, still **unsigned**, still not a Suite HOLD lift). Lab A produce smoke same day: HW sum == acked. Combined Lab A integrity (`scripts/lab-a-integrity.sh` / `ci-integrity-smoke.sh`): HW==acked and consumed==seeded (COUNT=2000 recheck same day, including later same-day native recheck) — unsigned only, not a Suite HOLD lift. Latency gate now fails loudly on a down broker (no silent `set -e`/`pipefail` swallow); integrity “soft” latency miss no longer `exit 1` unless `REQUIRE_INTEGRITY=1`. **Rechecked 2026-09-04 (later same day, tip `86412be`):** native Kafka 4.1 broker-smoke (kip848+share) green; `REQUIRE_AUTH=1` auth-smoke full matrix green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green; latency gate quiet samples p99≈126–203µs (pass vs 750µs relative limit) after an under-agent-load miss at ≈896–1108µs — still **unsigned**, still not a Suite HOLD lift. **Rechecked 2026-09-04 (later same day, tip `07861d6`):** native Kafka 4.1 broker-smoke (kip848+share) green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green; latency gate quiet samples p99≈711µs (pass vs 750µs relative limit) after an under-agent-load miss at ≈867µs — still **unsigned**, still not a Suite HOLD lift. **Native auth recheck (2026-09-04, tip `929ef57`):** `REQUIRE_AUTH=1` auth-smoke SASL_SSL PLAIN+SCRAM-256/512+OAUTHBEARER+OIDC+mTLS fail-closed green — unsigned; not a Suite HOLD lift. |
| e2e | First mock-broker protocol-client e2e landed in #80 (`tests/e2e.rs`). Mock only. |

Named gaps stay closed: ElectLeaders (43), DescribeLogDirs v5, DescribeQuorum (55), Add/Remove/UpdateRaftVoter (80–82).

This file tracks holes and unsigned samples. It does not lift Suite HOLD.
Integrity harnesses (`scripts/lab-a-integrity.sh`, `lab-a-produce.sh`,
`lab-a-fetch.sh`) and the relative latency gate are **unsigned** evidence only.
Numbers and reproduce steps: [benchmark.md](benchmark.md).
