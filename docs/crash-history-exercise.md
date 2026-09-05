# Crash / HA history exercise (KL-03)

**UNFILLED — not evidence.** This template Does **not** close KL-03 and does
**not** lift Suite HOLD. Blank fields mean no three-broker crash/HA history
has been recorded here. Do not treat the existence of this file, mock
`tests/crash_history.rs`, or tip Verifiable greens as a signed HA proof.

KL-03 requires seeded unique-ID histories across a **three-broker KRaft**
topology (RF=3, explicit `min.insync.replicas`), leader/coordinator moves,
txn fencing/abort, and independent consumers — see [ROADMAP.md](ROADMAP.md)
KL-03 and [STATUS.md](STATUS.md). Mock abort/commit classification is a
**Partial** honesty slice only.

Copy this file (or open a tracking issue) per attempted history. Fill every
field before claiming a KL-03 profile close.

## Package header

| Field | Value |
|---|---|
| Tip git SHA under review | _UNFILLED_ |
| partitionline version | _UNFILLED_ |
| Topology (3-broker KRaft? RF? `min.insync.replicas`?) | _UNFILLED_ |
| Broker version(s) | _UNFILLED_ |
| Profile (produce / txn / classic-group / coop / KIP-848 / share) | _UNFILLED_ |
| Operator / submitter | _UNFILLED_ |
| Sign-off (if any; unsigned otherwise) | _UNFILLED_ |

## History discipline

| Field | Value |
|---|---|
| Unique record IDs / payload hashes recorded pre-send? (`yes` / `no`) | _UNFILLED_ |
| Outcomes classified (`acked` / `failed` / `unknown` / `aborted-hidden` / `committed-visible`)? | _UNFILLED_ |
| Independent consumer compared full history (not HW deltas alone)? (`yes` / `no`) | _UNFILLED_ |
| Control records excluded from application counts in txn tests? (`yes` / `no`) | _UNFILLED_ |

## Faults exercised

| Fault | Exercised? | Artifact / notes |
|---|---|---|
| Leader move / lost Produce response | _UNFILLED_ | _UNFILLED_ |
| Coordinator move / reconnect under TLS/SASL | _UNFILLED_ | _UNFILLED_ |
| Process pause/restart (producer and/or consumer) | _UNFILLED_ | _UNFILLED_ |
| Transaction commit vs abort visibility (`read_committed`) | _UNFILLED_ | _UNFILLED_ |
| PID/epoch fencing (no invented local epoch retry) | _UNFILLED_ | _UNFILLED_ |
| Group churn (classic / cooperative / KIP-848) or share lock/redelivery | _UNFILLED_ | _UNFILLED_ |

## Outcome

| Field | Value |
|---|---|
| Invariants held? (`yes` / `no` / `partial`) | _UNFILLED_ |
| Unexplained missing/duplicate/order/abort-leak events | _UNFILLED_ |
| Artifact path(s) | _UNFILLED_ |
| Claim (`pass` / `fail` / `aborted` / `unsigned-mock-only`) | _UNFILLED_ |

## Honesty

- Mock `tests/crash_history.rs` proves unique-ID commit/abort classification on
  a single mock broker — **not** a three-broker crash history.
- Suite HOLD / unsigned Lab A stay open regardless of this checklist.
