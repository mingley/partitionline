# Execution queue

The [Kafka leadership plan](docs/ROADMAP.md) defines scope, dependencies and
acceptance criteria. Michael Ingley coordinates; implementers and independent
reviewers are unassigned until claimed. All packages remain open.

## Resume the interrupted handoff

Installable (`0.1.0` on crates.io) is met. **Suite HOLD remains** (Lab A unsigned).
Do not treat unsigned Verifiable samples as a Suite HOLD lift.

The baseline is frozen work, not a claim that everything is broken or complete.
At `54020e2`, [CI run 33938039612](https://github.com/mingley/partitionline/actions/runs/33938039612)
failed formatting and the follow-on latency gate (p99 1,344 us versus a 750 us
ceiling). The count checks passed: acked, high-watermark delta and consumed were
all 2,000. Matching counts do not prove exactly-once delivery. No client-code
repairs or production qualification are claimed by these documentation changes.

Incoming `main` through `0146b98` is preserved: formatting was repaired and the
integrity CI job now skips its duplicate nested latency gate while retaining
the dedicated latency job. These source fixes have landed; controlled-host
performance qualification and exact-HEAD evidence remain open.

- [x] Preserve the incoming formatting/CI-policy fixes and Installable/Suite HOLD distinction.

## First three PRs

- [ ] **KL-01 recovery slice:** Reconcile status/handoff notes and incoming recovery fixes with committed code, assign unfinished work, confirm current required lanes, and make actual broker identity and platform prerequisites explicit.
- [ ] **KL-01/KL-04 latency slice:** Reproduce the failed gate on controlled hardware, distinguish noise from regression, and reconcile the separate CI smoke/performance budgets without blindly weakening thresholds.
- [ ] **KL-08 release slice:** Select one serialized publisher gated on exact-SHA CI and package-consumer evidence; reconcile metadata and handoff checks and rehearse partial-release recovery without publishing.

## Qualification and leadership packages

| Done | Package | Dependencies | Evidence required to close |
|---|---|---|---|
| [ ] | [KL-01: Baseline and protocol oracles](docs/ROADMAP.md#kl-01-recover-the-baseline-and-establish-protocol-oracles) | None | Green required lanes, actual broker/case matrix, semantic differential fixtures and sustained fuzz results. |
| [ ] | [KL-02: Resources and cancellation](docs/ROADMAP.md#kl-02-bound-memory-and-define-cancellationshutdown-outcomes) | KL-01 contract | Queued/in-flight byte model, overload soak and explicit completed/failed/ambiguous outcomes without hung tasks. |
| [ ] | [KL-03: HA and transactional/group histories](docs/ROADMAP.md#kl-03-prove-ha-transactions-and-group-semantics-with-crash-histories) | KL-01/02 | Three-broker faults, unique-ID/payload/order histories, fencing/read-committed proofs, and distinct group/share recovery matrices. |
| [ ] | [KL-04: Measurements and optimization](docs/ROADMAP.md#kl-04-establish-reproducible-leadership-then-optimize-measured-limits) | KL-01; KL-02/03 for production claims | Equal-semantics open-loop comparisons, tail latency/CPU/memory, repeated multi-host artifacts and independent reproduction. |
| [ ] | [KL-05: Demand-led codec/ecosystem scope](docs/ROADMAP.md#kl-05-expand-codec-and-ecosystem-coverage-only-when-justified) | KL-01; KL-04 for performance | Separate zstd decoder/encoder evaluation, framing/safety evidence and a dependency-policy decision. Schema Registry stays a companion design. |
| [ ] | [KL-06: Auth and transport recovery](docs/ROADMAP.md#kl-06-qualify-authentication-and-transport-recovery) | KL-01/02 | Existing-behavior audit, expiry/rotation/outage tests, bounded recovery, TLS verification and credential redaction. |
| [ ] | [KL-07: Usability and diagnostics](docs/ROADMAP.md#kl-07-make-adoption-and-diagnosis-simpler-than-the-alternatives) | KL-01; KL-02/03 for recipes | Fresh external consumers, two newcomer exercises, bounded metrics and measured tracing overhead. |
| [ ] | [KL-08: Release and adoption](docs/ROADMAP.md#kl-08-gate-releases-and-promote-through-reversible-adoption) | None for release safety; applicable profile gates for adoption | Exact-SHA publish gates, support policy, two adopter records, 24-hour/7-day exercises and operator-approved rollback proof. |

Optional codecs and ecosystem additions are not universal production blockers.
A 1.0 decision concerns API/support stability; it does not require a universal
performance win. Preserve Suite HOLD and all existing benchmark signoff rules.

## Closing an item

Link the implementing PR and record owner/reviewer, source and peer/tool versions,
commands, configuration, raw artifacts, observed results and remaining limits.
Partial PRs do not close the parent package. New harnesses must exist and run;
the roadmap's [existing commands](docs/ROADMAP.md#5-first-prs-and-proof-discipline)
are only starting points.

Review proposed scorecard thresholds before each exercise. Do not silently move
the bar, infer missing evidence from old notes, or hide failed matrix cells.
