# RSS / process-budget exercise record (2× load · 24h)

**UNFILLED — not evidence.** This template Does **not** close KL-02 and does
**not** lift Suite HOLD. Blank fields mean no 2×-overload or 24-hour RSS run
has been recorded here. Do not treat this file as a passed process-budget proof.

KL-02 still needs a declared byte/task/connection budget that holds at **2×**
sustainable offered load, plus a **24-hour** steady-state RSS check (final-hour
median within **10%** of the first steady-state hour) after warmup — see
[ROADMAP.md](ROADMAP.md) KL-02 and the bounded-resources gate. Mock
`buffer_memory` ownership (`tests/buffer_ownership.rs`) is a separate honesty
slice and is **not** a substitute for this record.

Copy this file (or open a tracking issue) per profile. Fill every field before
claiming a record. Keep Suite HOLD and unsigned Lab A language unchanged.

## Record header

| Field | Value |
|---|---|
| Profile / service id | _UNFILLED_ |
| partitionline version / git SHA | _UNFILLED_ |
| Broker version(s) | _UNFILLED_ |
| Host OS / arch / cgroup limits | _UNFILLED_ |
| Declared budgets (bytes / tasks / connections) | _UNFILLED_ |
| Measurement method (RSS source, sample interval) | _UNFILLED_ |
| Operator sign-off name | _UNFILLED_ |

## 2× overload budget hold

| Field | Value |
|---|---|
| Sustainable offered load (baseline) | _UNFILLED_ |
| 2× offered load definition | _UNFILLED_ |
| Duration at 2× | _UNFILLED_ |
| `bytes_buffered` / queue vs `buffer_memory` | _UNFILLED_ |
| Task / connection counts vs budget | _UNFILLED_ |
| Reject / timeout / ambiguous outcomes observed | _UNFILLED_ |
| Integrity notes (no silent accept beyond budget) | _UNFILLED_ |
| Outcome (`pass` / `fail` / `aborted`) | _UNFILLED_ |

## 24-hour RSS steady-state

| Field | Value |
|---|---|
| Warmup end (UTC) | _UNFILLED_ |
| First steady-state hour window (UTC) | _UNFILLED_ |
| First-hour median RSS | _UNFILLED_ |
| Final-hour window (UTC) | _UNFILLED_ |
| Final-hour median RSS | _UNFILLED_ |
| Δ% (final vs first; pass if ≤ 10%) | _UNFILLED_ |
| Allocator / cache notes (retention ≠ automatic leak) | _UNFILLED_ |
| Outcome (`pass` / `fail` / `aborted`) | _UNFILLED_ |

## Explicit non-claims

- Filling or merging this template alone does **not** close KL-02.
- Mock `buffer_memory` tests do **not** satisfy this record.
- Unsigned RSS samples here do **not** lift Suite HOLD.
- Host metrics dumps with secrets must not be pasted; link private notes if
  needed ([security.md](security.md)).
