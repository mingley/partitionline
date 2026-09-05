# Promotion / rollback exercise record (traffic shadow)

**UNFILLED — not evidence.** This template Does **not** close KL-08 and does
**not** lift Suite HOLD. Blank fields mean no traffic-shadow promotion or
operator-approved rollback has been recorded here. Do not treat the existence
of this file as a passed promotion or rollback proof.

KL-08 still needs **two independent** adopter 24h/7d records
([adopter-exercise.md](adopter-exercise.md)) **plus** shadow promotion and
operator-approved rollback under production SLOs (see [ROADMAP.md](ROADMAP.md)
KL-08 and [support.md](support.md)).

Copy this file (or open a tracking issue) per profile cut. Fill every field
before claiming a record. Keep Suite HOLD and unsigned Lab A language unchanged.

## Record header

| Field | Value |
|---|---|
| Profile / service id | _UNFILLED_ |
| Linked adopter-exercise record id(s) | _UNFILLED_ |
| partitionline version / git SHA | _UNFILLED_ |
| Broker version(s) | _UNFILLED_ |
| Baseline client (if comparing) | _UNFILLED_ |
| Environment (staging / prod-shadow) | _UNFILLED_ |
| Operator sign-off name | _UNFILLED_ |

## Traffic-shadow plan

| Field | Value |
|---|---|
| Shadow isolation mode (dedicated topics / read-only consumers) | _UNFILLED_ |
| Side-effect guard (no duplicate real downstream writes) | _UNFILLED_ |
| Compare method (IDs / payloads — not offsets across distinct topics) | _UNFILLED_ |
| Rollout steps rehearsed (`1%` / `10%` / `50%` or N/A with reason) | _UNFILLED_ |
| Why a consumer group cannot arbitrarily assign traffic % (if applicable) | _UNFILLED_ |
| Start (UTC) | _UNFILLED_ |
| End (UTC) | _UNFILLED_ |
| Integrity compare summary | _UNFILLED_ |
| Error-rate baseline vs shadow | _UNFILLED_ |
| Latency notes (unsigned unless Lab A signed) | _UNFILLED_ |
| Outcome (`pass` / `fail` / `aborted`) | _UNFILLED_ |

## Rollback rehearsal

| Field | Value |
|---|---|
| Drain / fencing / abort steps rehearsed | _UNFILLED_ |
| Resume-from committed offsets verified? (`yes` / `no`) | _UNFILLED_ |
| Auto-commit of unprocessed records avoided? (`yes` / `no`) | _UNFILLED_ |
| Rollback trigger thresholds (error-rate Δ / p99 Δ / SLO) | _UNFILLED_ |
| Sample floor / window used (e.g. two 5-minute windows) | _UNFILLED_ |
| Operator-controlled (not silent auto-rollback)? (`yes` / `no`) | _UNFILLED_ |
| Rollback drill performed? (`yes` / `no`) | _UNFILLED_ |
| Drill start / end (UTC) | _UNFILLED_ |
| Drill outcome (`pass` / `fail` / `aborted`) | _UNFILLED_ |
| Operator approval to promote after drill | _UNFILLED_ |

## Explicit non-claims

- Filling or merging this template alone does **not** close KL-08.
- A rehearsal without production SLO sign-off does **not** prove promotion.
- Unsigned latency or integrity samples here do **not** lift Suite HOLD.
- Topic names, tokens, and PEMs must not be pasted into this record; link to
  private operator notes if needed ([security.md](security.md)).
