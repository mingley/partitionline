# Next-session queue

**Default next task: `KL03-09` -- qualify nontransactional producer epoch recovery.**

Use [one task per session](docs/plan/README.md). The canonical
[task registry](docs/plan/tasks.json) contains dependencies, starting files,
one deliverable, acceptance criteria, focused checks, owner and evidence for
each card. Claim **one ready ID**, complete it, record the result, and stop.
Do not take an entire KL package as one assignment.

The [2026-09-21 source audit](docs/audits/2026-09-21.md) is frozen at `cb7e97d`.
Exact-source CI is green, but five additional consumer probes fail.
The probes are retained for reuse; the planning change does not repair the
client. Installable is met. **Suite HOLD remains.**

## First implementation chain

Each row is a separate session/PR-sized deliverable. The registry is the
authority for current readiness and completion, not this printed order.

| Order | Task | Observable result |
|---|---|---|
| 1 | `KL03-01` | A reusable bounded wire fixture; no mock pre-filtering that hides client bugs |
| 2 | `KL03-02` | A successful partition's records survive a neighboring partition retry |
| 3 | `KL03-03` | A committed transaction after an abort under the same PID remains visible |
| 4 | `KL03-04` | Seeking inside a whole fetched batch does not return earlier offsets |
| 5 | `KL03-05` | Out-of-range handling obeys Earliest/Latest/None |
| 6 | `KL03-06` | A capped poll followed by `commit()` cannot skip undelivered buffered records |
| 7 | `KL03-07` | Interval auto-commit does not commit the batch about to be returned |

Do not edit `src/consumer.rs` concurrently across these cards.

## Independent initial pickups

These have no task dependencies at the initial plan snapshot. Check ownership
and the ready-task query before starting; they are alternatives for separate
sessions, not a bundle to finish together.

| Task | Small deliverable |
|---|---|
| `KL07-01` | Fix the one private-link strict-rustdoc failure |
| `KL08-01` | Make the release check reject a green non-CI workflow when CI is missing |
| `KL01-01` | Create the versioned conformance case/provenance registry |
| `KL02-01` | Define byte ownership and completed/failed/ambiguous outcome budgets |
| `KL04-01` | Freeze equal-semantics benchmark scenarios, measurements and claim rules |
| `KL05-01` | Define the finite major-feature/profile completion matrix |
| `KL03-12` | Use the broker-provided KIP-848 heartbeat interval |
| `KL03-13` | Use the broker-provided share heartbeat interval |

## After the first repairs

Follow the dependency graph, not a new open-ended audit:

| Lane | Next outcomes |
|---|---|
| `KL01-*` | Independent Java fixtures, fail-closed case reports, upstream adapters, current broker cells and sustained fuzz evidence |
| `KL02-*` | Retained/decoded-byte bounds, terminal ownership, deadlines and a controlled overload record |
| `KL03-*` | Lost-ack/epoch/fencing behavior, group/share ownership and separate three-broker crash histories |
| `KL04-*` | Pinned peer drivers, open-loop load, ID/payload verification, raw artifacts, x86_64/arm64 runs and independent reproduction |
| `KL05-*` | zstd decode/encode, incremental Fetch, quota handling, sticky partitioning, current API deltas, full-admin and companion-format cards |
| `KL06-*` | OIDC expiry/refresh, SASL reauth, TLS rotation, redaction and explicitly approved opt-in GSSAPI |
| `KL07-*` | Strict docs, actual snippet checks, tutorial/recipes, migration, bounded diagnostics and newcomer exercises |
| `KL08-*` | Exact required-CI gates, one publisher, package/platform checks, separate adopter runs and operator-controlled rollback |

All new cards begin **pending/unassigned**. Future validation commands are
acceptance criteria, not completed evidence. A harness, example result,
unsigned benchmark or published crate does not close a package/profile.
No release, deployment, paid host, external message or destructive operation
is authorized merely by a task card.

<details>
<summary>Historical 2026-09-05 handoff (superseded as an execution queue)</summary>

The following records preserve already-landed slices and the old handoff.
Do not select its broad packages or first-PR ordering as current tasks.

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
  - [x] Actual broker identity stamp (`requested=` vs `actual=`) + portable `pl_timeout`/`gtimeout` path (2026-09-05).
  - [x] Protocol oracles (Produce/Fetch/Metadata/ListOffsets vs 3.9.1 and 4.1.0 fixture/semantic tests; live broker optional) (2026-09-05). Remaining: controlled latency reproduce, sustained campaign results.
  - [x] Fuzz campaign metadata partial-landed 2026-09-05 (`kind=campaign` distinct from 15s CI smoke; minimized artifacts retained).
- [ ] **KL-01/KL-04 latency slice:** Reproduce the failed gate on controlled hardware, distinguish noise from regression, and reconcile the separate CI smoke/performance budgets without blindly weakening thresholds.
  - [x] Record shared-runner vs local-native vs controlled-host budgets (`docs/latency-ci-policy.json`; `ci-latency-gate.sh --self-test` + bars). Nested 1,344/750 µs miss is historical (`SKIP_LATENCY_GATE` on integrity). GHA 5000 µs and local 750 µs relative gate unchanged. Remaining: controlled-host reproduce. Parent KL-01/KL-04 packages stay open.
- [ ] **KL-08 release slice:** Select one serialized publisher gated on exact-SHA CI and package-consumer evidence; reconcile metadata and handoff checks and rehearse partial-release recovery without publishing.
  - [x] Serialized path: release-plz PR-only; owner-cut-release/release.yml exact-SHA CI + crate-consumer; soft-skip if version already on crates.io (2026-09-05). Remaining: support policy, adopter 24h/7d, traffic rollout.
  - [x] Partial-release recovery rehearsal (`scripts/rehearse-partial-release.sh --self-test`; no publish / no re-cut 0.1.0) (2026-09-05). KL-08 package stays open.
  - [x] Support matrix doc (`docs/support.md`; CI brokers 3.9.1/4.1.0, MSRV 1.85, explicit non-promises) (2026-09-05). Adopter 24h/7d + promotion still open.
  - [x] owner-publish skips cargo publish when crates.io already has the version; release.yml `actions: read` for exact-SHA `gh run list`; rehearsal fail-closes if skip ifs are removed (2026-09-05).

## Qualification and leadership packages

| Done | Package | Dependencies | Evidence required to close |
|---|---|---|---|
| [ ] | [KL-01: Baseline and protocol oracles](docs/ROADMAP.md#kl-01-recover-the-baseline-and-establish-protocol-oracles) | None | Green required lanes, actual broker/case matrix, semantic differential fixtures and sustained fuzz results. |
| [ ] | [KL-02: Resources and cancellation](docs/ROADMAP.md#kl-02-bound-memory-and-define-cancellationshutdown-outcomes) | KL-01 contract | Queued/in-flight byte model, overload soak and explicit completed/failed/ambiguous outcomes without hung tasks. |
| | ↳ partial: produce cancel table + mock tests + durable `close`→`Closed` (2026-09-05) | | Overload soak / full ownership trace still open. |
| | ↳ partial: consumer leave/close/unsubscribe do not auto-commit positions (2026-09-05) | | Poll-interval auto-commit + explicit commit* unchanged. |
| | ↳ partial: buffer ownership + mock overload soak (`bytes_buffered` ≤ `buffer_memory`) (2026-09-05) | | Full ownership trace + 2×/24h RSS soak still open. |
| [ ] | [KL-03: HA and transactional/group histories](docs/ROADMAP.md#kl-03-prove-ha-transactions-and-group-semantics-with-crash-histories) | KL-01/02 | Three-broker faults, unique-ID/payload/order histories, fencing/read-committed proofs, and distinct group/share recovery matrices. |
| [ ] | [KL-04: Measurements and optimization](docs/ROADMAP.md#kl-04-establish-reproducible-leadership-then-optimize-measured-limits) | KL-01; KL-02/03 for production claims | Equal-semantics open-loop comparisons, tail latency/CPU/memory, repeated multi-host artifacts and independent reproduction. |
| [ ] | [KL-05: Demand-led codec/ecosystem scope](docs/ROADMAP.md#kl-05-expand-codec-and-ecosystem-coverage-only-when-justified) | KL-01; KL-04 for performance | Separate zstd decoder/encoder evaluation, framing/safety evidence and a dependency-policy decision. Schema Registry stays a companion design. |
| [ ] | [KL-06: Auth and transport recovery](docs/ROADMAP.md#kl-06-qualify-authentication-and-transport-recovery) | KL-01/02 | Existing-behavior audit, expiry/rotation/outage tests, bounded recovery, TLS verification and credential redaction. |
| | ↳ partial: credential `Debug` redaction for Sasl/Oidc/Tls + configs (2026-09-05) | | Rotation/outage recovery still open (error/span/metrics partials landed). |
| | ↳ partial: OIDC/OAUTHBEARER `Error` omits IdP/broker response bodies (2026-09-05) | | Rotation/outage recovery still open (span/metrics partial landed). |
| | ↳ partial: metrics snapshots + tracing `skip(self)` span honesty (2026-09-05) | | Rotation/outage recovery still open. |
| | ↳ partial: OIDC IdP outage fail-closed audit + 503/timeout tests (2026-09-05) | | Mid-connection refresh/rotation soak still open. |
| | ↳ partial: OIDC bounded transient retry (5xx/I/O/timeout; no 4xx retry) (2026-09-05) | | Mid-connection refresh/rotation soak still open. |
| [ ] | [KL-07: Usability and diagnostics](docs/ROADMAP.md#kl-07-make-adoption-and-diagnosis-simpler-than-the-alternatives) | KL-01; KL-02/03 for recipes | Fresh external consumers, two newcomer exercises, bounded metrics and measured tracing overhead. |
| [ ] | [KL-08: Release and adoption](docs/ROADMAP.md#kl-08-gate-releases-and-promote-through-reversible-adoption) | None for release safety; applicable profile gates for adoption | Exact-SHA publish gates, support policy, two adopter records, 24-hour/7-day exercises and operator-approved rollback proof. |
| | ↳ partial: support matrix doc (`docs/support.md`) for CI-backed brokers/MSRV/features (2026-09-05) | | Adopter 24h/7d records + promotion/rollback still open. |
| | ↳ partial: adopter 24h/7d exercise template (`docs/adopter-exercise.md`, UNFILLED) (2026-09-05) | | Filled independent records + promotion/rollback still open. |

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

</details>
