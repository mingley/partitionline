# Lab A signoff record (Suite HOLD)

**UNFILLED — not evidence.** This template Does **not** lift Suite HOLD and
Does **not** mark Lab A signed. Blank fields mean Kernel Integrity has not
signed a Lab A package for this tip. Do not treat this file, unsigned
`scripts/lab-a-*.sh` runs, or tip Verifiable greens as a Suite HOLD lift.

Suite HOLD stays until a **signed** Lab A package exists (see
[STATUS.md](STATUS.md), [benchmark.md](benchmark.md),
[CIVILIZATION.md](CIVILIZATION.md) WP-5). Unsigned integrity/latency samples
remain labeled unsigned.

Copy this file (or open a tracking issue) per attempted signoff. Fill every
field before claiming Lab A is signed.

## Package header

| Field | Value |
|---|---|
| Tip git SHA under review | _UNFILLED_ |
| partitionline version | _UNFILLED_ |
| Lab A host / image identity | _UNFILLED_ |
| Broker version(s) + topology | _UNFILLED_ |
| Comparison client (e.g. librdkafka version) | _UNFILLED_ |
| Operator / submitter | _UNFILLED_ |
| Kernel Integrity signer | _UNFILLED_ |
| Sign date (UTC) | _UNFILLED_ |
| Signature / attestation id | _UNFILLED_ |

## Produce (HW sum == acked)

| Field | Value |
|---|---|
| Harness (`scripts/lab-a-produce.sh` or equivalent) | _UNFILLED_ |
| COUNT / PARTITIONS / RUNS | _UNFILLED_ |
| Each run HW_delta == acked? (`yes` / `no`) | _UNFILLED_ |
| Median produce result summary | _UNFILLED_ |
| Artifact path(s) | _UNFILLED_ |
| Outcome (`pass` / `fail` / `aborted`) | _UNFILLED_ |

## Fetch / consume (consumed == seeded)

| Field | Value |
|---|---|
| Harness (`scripts/lab-a-fetch.sh` or equivalent) | _UNFILLED_ |
| Seeded vs consumed | _UNFILLED_ |
| Artifact path(s) | _UNFILLED_ |
| Outcome (`pass` / `fail` / `aborted`) | _UNFILLED_ |

## Combined integrity

| Field | Value |
|---|---|
| Harness (`scripts/lab-a-integrity.sh`) | _UNFILLED_ |
| COUNT | _UNFILLED_ |
| Produce HW==acked and fetch consumed==seeded | _UNFILLED_ |
| Latency notes (unsigned unless this package signs them) | _UNFILLED_ |
| Artifact path(s) | _UNFILLED_ |
| Outcome (`pass` / `fail` / `aborted`) | _UNFILLED_ |

## Explicit non-claims

- An UNFILLED or partially filled copy of this template Does **not** lift
  Suite HOLD.
- Tip Verifiable / CI greens are **not** Lab A signoff.
- Trusted Publishing UI completion is unrelated to Lab A signoff.
- Do not paste secrets, PEMs, or raw host dumps here; link private attestation
  stores ([security.md](security.md)).
