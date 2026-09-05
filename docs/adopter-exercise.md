# Adopter exercise record (24h / 7d)

**UNFILLED — not evidence.** This template Does **not** close KL-08 and does
**not** lift Suite HOLD. Blank fields mean no adopter run has been recorded
here. Do not treat the existence of this file as a passed 24h or 7d exercise.

KL-08 requires **two independent** adopter workloads, each with a **24-hour**
then **7-day** run, plus traffic-shadow promotion and operator-approved
rollback proof (see [ROADMAP.md](ROADMAP.md) KL-08 and [support.md](support.md)).

Copy this file (or open a tracking issue) per adopter. Fill every field before
claiming a record. Keep Suite HOLD and unsigned Lab A language unchanged.

## Record header

| Field | Value |
|---|---|
| Adopter id (org / service) | _UNFILLED_ |
| Independence note (why this is not a duplicate of the other record) | _UNFILLED_ |
| partitionline version / git SHA | _UNFILLED_ |
| Broker version(s) | _UNFILLED_ |
| Profile (produce / consume / group / txn / auth) | _UNFILLED_ |
| Environment (lab / staging / prod-shadow) | _UNFILLED_ |
| Operator sign-off name | _UNFILLED_ |

## 24-hour exercise

| Field | Value |
|---|---|
| Start (UTC) | _UNFILLED_ |
| End (UTC) | _UNFILLED_ |
| Workload summary (rate, topics, keys, payload size) | _UNFILLED_ |
| Integrity notes (HW vs acked, consumed vs seeded, error rate) | _UNFILLED_ |
| Latency notes (p50/p99 produce-ack or fetch; not a Suite HOLD claim) | _UNFILLED_ |
| Incidents / reconnects / auth failures | _UNFILLED_ |
| Outcome (`pass` / `fail` / `aborted`) | _UNFILLED_ |

## 7-day exercise

| Field | Value |
|---|---|
| Start (UTC) | _UNFILLED_ |
| End (UTC) | _UNFILLED_ |
| Continuity from 24h record (link or id) | _UNFILLED_ |
| Workload summary | _UNFILLED_ |
| Integrity notes | _UNFILLED_ |
| Latency notes (unsigned unless Lab A signed) | _UNFILLED_ |
| Incidents / reconnects / auth / storage growth | _UNFILLED_ |
| Outcome (`pass` / `fail` / `aborted`) | _UNFILLED_ |

## Promotion and rollback (optional until traffic shadow)

| Field | Value |
|---|---|
| Shadow traffic fraction | _UNFILLED_ |
| Compare method (payload / id / offset) | _UNFILLED_ |
| Rollback trigger (error-rate delta, SLO breach) | _UNFILLED_ |
| Rollback rehearsal performed? (`yes` / `no`) | _UNFILLED_ |
| Operator approval for promotion | _UNFILLED_ |

## Explicit non-claims

- Filling or merging this template alone does **not** close KL-08.
- Unsigned latency or integrity samples here do **not** lift Suite HOLD.
- Topic names, tokens, and PEMs must not be pasted into this record; link to
  private operator notes if needed ([security.md](security.md)).
