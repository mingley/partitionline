# Controlled-host latency qualification exercise (KL-04)

**UNFILLED — not evidence.** This template Does **not** close KL-04 and does
**not** lift Suite HOLD. Blank fields mean no controlled-host produce-ack /
fetch qualification has been recorded here. Do not treat shared-runner
`latency-gate` greens, local-native samples, or the existence of this file as
Lab A or a Suite HOLD lift.

KL-04 still needs controlled-host qualification against frozen workloads and
baselines (see [ROADMAP.md](ROADMAP.md) KL-04 and
[latency-ci-policy.json](latency-ci-policy.json)). Shared-runner budgets are
catastrophic smoke only; local-native relative gates are still **unsigned**.

Copy this file (or open a tracking issue) per profile/cell. Fill every field
before claiming a record. Keep Suite HOLD and unsigned Lab A language unchanged.

## Record header

| Field | Value |
|---|---|
| Profile / cell id (low-latency / bulk / fetch / txn / mixed) | _UNFILLED_ |
| partitionline version / git SHA | _UNFILLED_ |
| Broker version(s) + topology | _UNFILLED_ |
| Host OS / arch / CPU isolation notes | _UNFILLED_ |
| Network RTT class (same-host / LAN / WAN) | _UNFILLED_ |
| Comparator client(s) (rust-rdkafka / Java / other) | _UNFILLED_ |
| Operator sign-off name | _UNFILLED_ |
| Lab A / Kernel Integrity sign-off (`yes` / `no` / `unsigned`) | _UNFILLED_ |

## Workload freeze

| Field | Value |
|---|---|
| `acks` / idempotence / isolation | _UNFILLED_ |
| Replication / ISR / compression | _UNFILLED_ |
| Batch / linger / partition count / keys | _UNFILLED_ |
| Connections / in-flight / TLS/SASL | _UNFILLED_ |
| Payload distribution | _UNFILLED_ |
| Open-loop schedule (arrival rate through saturation) | _UNFILLED_ |
| Rejected / timed-out / ambiguous outcomes retained? (`yes` / `no`) | _UNFILLED_ |

## Measured results

| Field | Value |
|---|---|
| Sample count / repetitions (≥5?) | _UNFILLED_ |
| Produce-ack p50 / p95 / p99 / p99.9 (µs) | _UNFILLED_ |
| Fetch / e2e p50 / p99 when in scope (µs) | _UNFILLED_ |
| Successful records/s | _UNFILLED_ |
| Client CPU / allocations / RSS notes | _UNFILLED_ |
| Comparator p50 / p99 at equal durability | _UNFILLED_ |
| Uncertainty interval / A-B order notes | _UNFILLED_ |
| Artifact path(s) | _UNFILLED_ |
| Outcome (`pass` / `fail` / `aborted` / `unsigned`) | _UNFILLED_ |

## Budget honesty

| Field | Value |
|---|---|
| Shared-runner ceiling consulted (`latency-ci-policy.json`)? | _UNFILLED_ |
| Local-native relative gate consulted? | _UNFILLED_ |
| Controlled-host target declared **before** the run? (`yes` / `no`) | _UNFILLED_ |
| Ceiling raised to greenwash a miss? (`yes` / `no`) | _UNFILLED_ |

## Explicit non-claims

- Shared-runner / GHA latency-gate pass is **not** controlled-host qualification.
- Local-native relative gate pass is **unsigned** and is **not** Lab A.
- Filling this template without Kernel Integrity / Lab A sign-off does **not**
  lift Suite HOLD.
