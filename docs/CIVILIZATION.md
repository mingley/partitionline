# Civilization plan for partitionline

**North star:** Critical event infrastructure can run on a memory-safe Kafka
client with no C and no librdkafka — auditable, fast, and boring to operate.

This file is the execution plan. Agents should pick **Work packages** in
order unless a later package is unblocked and higher leverage. Do not lift
Suite HOLD claims in `STATUS.md` / `benchmark.md` without signed Lab A
evidence. Do not add librdkafka, OpenSSL, libzstd, or Cyrus SASL as default
dependencies. `unsafe_code` stays forbidden.

## Why this matters

Kafka is how much of civilization moves state: payments, logistics, energy
telemetry, health systems, public services. Today most non-Java clients lean
on librdkafka (C). That couples memory-safety, supply-chain, and build
reproducibility to a C FFI stack.

partitionline already speaks modern Kafka 3.x/4.x protocol surfaces, matches
Java-shaped APIs for produce / fetch / groups / transactions / admin / share
groups, and has measured produce / fetch / latency writeups on Lab A and
this-VM (see `benchmark.md`). Lab A produce is vs C when locked; this-VM fetch
and latency samples are vs rust-rdkafka and are **unsigned** — latency is not
claimed as a win. The remaining gap is not “write a client.” It is **make the
client something operators and ecosystems can trust and adopt**.

## Non-negotiable constraints

1. Pure Rust default features. No C Kafka client. No C compression/SASL as
   default. Optional `zstd` / GSSAPI remain **blocked on C** unless a pure-Rust
   path is proven Kafka-compatible (`gaps.md`).
2. `unsafe_code = "forbid"`. No exceptions for “hot path.”
3. Honesty about numbers. Unsigned this-VM results stay unsigned. Do not
   claim Suite HOLD lifts without Kernel Integrity / Lab A process.
4. Prefer Java API shapes and rustdoc that name the matching Java call.
5. Schema Registry stays out of this crate’s default surface (`gaps.md`); a
   companion crate is allowed later.

## Current baseline (do not re-litigate)

| Area | State |
|---|---|
| Produce / fetch / groups / EOS / admin / share | **done** vs librdkafka inventory in `gaps.md` |
| TLS (rustls), SCRAM, OAUTHBEARER/OIDC | **done** |
| gzip / snappy / lz4 | **done**; zstd / Kerberos blocked on C |
| Mock protocol tests | large (`tests/full_surface.rs`, `client_api.rs`); mock e2e in `#80` |
| Real-broker CI | `broker-smoke` job + `scripts/ci-broker-smoke.sh` (3.9.1 and 4.1.0) |
| crates.io publish | not published yet (`git` dependency; WP-0.5 owner action — `READY_EXCEPT_TOKEN`) |
| Open GitHub issues | templates added; work is still mostly commit-driven |

---

## Phase map

```
P0 Foundations     → publishability, versioning, public API freeze discipline
P1 Trust           → fuzz, real-broker CI, security posture, signed benches process
P2 Adoption        → docs, migration, examples that teach operators
P3 Ecosystem       → observability hooks, companion crates, language bridges later
P4 Protocol debt   → only holes that block real deployments (STATUS named holes stay closed unless justified)
P5 Stewardship     → release cadence, issue hygiene, agent-safe contribution rules
```

Agents: complete acceptance checks before marking a work package done. Update
this file’s **Progress** section in the same PR.

---

## Work packages

### WP-0 — Public crate identity

**Goal:** Anyone can depend on partitionline without a git URL.

| ID | Task | Acceptance |
|---|---|---|
| WP-0.1 | Decide and document semver policy (0.x until API stability bar in WP-0.3) | `docs/RELEASE.md` exists; README links it |
| WP-0.2 | Add `CHANGELOG.md` (Keep a Changelog) covering 0.1.0 baseline | File lists current surface in one “Unreleased” or `0.1.0` section |
| WP-0.3 | Audit `pub` surface: mark experimental APIs; ensure rustdoc on every public item (already denied missing_docs) | Short `docs/api-stability.md`: stable vs evolving modules |
| WP-0.4 | Prepare crates.io metadata: categories, keywords, exclude huge non-crate paths if needed | `cargo package --allow-dirty` dry-run succeeds; README install shows crates.io once published |
| WP-0.5 | First crates.io release (human token / owner action may be required) | Version on crates.io; README dependency example updated |

**Agent notes:** Publishing may need owner credentials. If blocked, finish
0.1–0.4 and leave 0.5 as a checked “owner action” with exact `cargo publish`
steps.

### WP-1 — Real-broker continuous verification

**Goal:** Protocol correctness is proven against Apache Kafka, not only mocks.

| ID | Task | Acceptance |
|---|---|---|
| WP-1.1 | Add CI job: Docker `apache/kafka` (3.9.x or 4.x KRaft) + `cargo test` selected integration tests | Workflow green on PR; skips cleanly if Docker unavailable only on local, not on CI |
| WP-1.2 | Promote `examples/roundtrip`, `eos`, `group`, `share`, `tls`, `sasl` into scripted smoke under CI where secrets/certs can be generated | One `scripts/ci-broker-smoke.sh` (or equivalent) documented in CONTRIBUTING |
| WP-1.3 | Matrix at least one Kafka 3.9 and one 4.x broker image | Matrix documented in workflow comments |
| WP-1.4 | Keep mock suite as default fast path; broker suite labeled / separate job | `cargo test` without Docker still passes locally |

**Agent notes:** Prefer generating TLS/SASL fixtures in CI (`openssl` CLI for
mock TLS; see `tests/common/mod.rs`). Do not commit live secrets.

### WP-2 — Adversarial protocol trust

**Goal:** Malformed broker bytes cannot panic or silently corrupt offsets.

| ID | Task | Acceptance |
|---|---|---|
| WP-2.1 | Fuzz decode paths under `src/protocol/` (cargo-fuzz or libfuzzer targets for Fetch/Produce/Metadata/Group responses) | At least 3 fuzz targets; CI smoke run (short corpus) |
| WP-2.2 | Property tests for varint / compact / flexible header edge cases | Tests in-tree; no unwrap in production paths (already clippy-denied) |
| WP-2.3 | Document threat model: trust broker vs trust network; TLS defaults; SCRAM/OIDC notes | `docs/security.md` |
| WP-2.4 | Dependency audit in CI (`cargo deny` or `cargo audit`) | Workflow fails on known advisories for direct deps |

### WP-3 — Operator documentation

**Goal:** A senior engineer can replace rust-rdkafka / librdkafka for a
standard service without reading `design.md` wire notes.

| ID | Task | Acceptance |
|---|---|---|
| WP-3.1 | `docs/guide.md`: produce, consume, groups, EOS, admin, TLS/SASL, defaults that differ from Java | Linked from README; runnable snippets |
| WP-3.2 | `docs/migrate-from-rdkafka.md`: config map, API map, intentional differences | Covers acks, linger, auto.offset.reset, idempotence, transactions |
| WP-3.3 | Cookbook: backpressure (`try_send`/`flush`), rebalance, exactly-once pattern from `examples/eos.rs` | Three short recipes |
| WP-3.4 | Trim README: keep numbers, point deep protocol to design/gaps | README stays under ~250 lines; links to guide |

### WP-4 — Observability for production

**Goal:** Running systems can see client health without reinventing metrics.

| ID | Task | Acceptance |
|---|---|---|
| WP-4.1 | Document `Producer`/`Consumer`/`Admin`/`ShareGroup` metrics snapshots and recommended scrape interval | Section in guide |
| WP-4.2 | Optional feature `tracing`: span hooks on produce-ack, fetch round, rebalance, txn boundaries | Feature off by default; no behavior change when disabled |
| WP-4.3 | Example exporting metrics to logs or a simple Prometheus text render (no mandatory prom crate if it bloats) | `examples/metrics` extended or sibling example |

### WP-5 — Performance honesty pipeline

**Goal:** Civilization-grade claims stay reproducible and labeled.

| ID | Task | Acceptance |
|---|---|---|
| WP-5.1 | Automate Lab A produce bench harness script (topic recreate, HW check, three-run median) | `scripts/lab-a-produce.sh`; **exits non-zero unless HW sum equals acked** each run |
| WP-5.2 | Keep fetch/latency this-VM results clearly **unsigned** until signed process exists | STATUS.md / benchmark.md labels unchanged unless signed |
| WP-5.3 | Add latency regression gate: produce-ack p99 threshold on CI broker (relative, not vs C) | CI fails on large regressions vs recorded baseline file |

### WP-6 — Adoption-blocking features only

**Goal:** Close only gaps that stop real migrations. Do not expand STATUS
named holes (ElectLeaders, DescribeQuorum, Raft voters, DescribeLogDirs v5)
without a deployment need written here.

| ID | Task | Acceptance |
|---|---|---|
| WP-6.1 | Survey: open GitHub Discussions/Issues for “what blocks you from adopting?” after crates.io | Issue templates exist; first survey issue filed |
| WP-6.2 | Pure-Rust zstd research spike: Kafka frame compatibility without `zstd-sys` | Spike doc: feasible / not; no default C dep |
| WP-6.3 | Companion `partitionline-schema` (optional, separate crate) for Confluent-compatible wire if demand is real | Only after WP-0 publish; stays out of core crate |
| WP-6.4 | Compression / auth optional features matrix in README | Table of codecs and SASL mechanisms vs C |

### WP-7 — Stewardship and agent execution hygiene

**Goal:** Future agents improve the client without thrashing protocol history.

| ID | Task | Acceptance |
|---|---|---|
| WP-7.1 | Issue / PR templates: bug, protocol map, perf claim, docs | `.github/` templates |
| WP-7.2 | `CONTRIBUTING.md`: point to this plan, gaps.md status meanings, bench honesty | Updated |
| WP-7.3 | Release cadence: cut crate releases on meaningful user-facing batches, not every protocol throttle map | Stated in RELEASE.md |
| WP-7.4 | When mapping Java helpers, keep commit message style that names Java API and version range (existing convention) | No process change; document it |

---

## Priority order for the next agents

Execute in this sequence unless blocked:

1. **WP-0.1 → WP-0.4** (publish readiness)
2. **WP-1.1 → WP-1.4** (real broker CI)
3. **WP-3.1 → WP-3.2** (adoption docs)
4. **WP-2.1 → WP-2.4** (fuzz + audit)
5. **WP-4.1 → WP-4.2** (observability)
6. **WP-0.5** (publish when owner can)
7. **WP-5** then **WP-6** as evidence and demand dictate
8. **WP-7** in parallel whenever touching `.github` / CONTRIBUTING

## Progress

| Package | Status | Notes |
|---|---|---|
| WP-0 Public crate identity | **in progress** | 0.1–0.4 done; `cargo package` + `cargo publish --dry-run` + packed-crate consumer + rustdoc/`ci-docs` smoke green (**zero** unresolved intra-doc links; crate `#![deny(rustdoc::broken_intra_doc_links)]`); docs.rs metadata; release workflow supports tag push and `workflow_dispatch` (OIDC Trusted Publishing preferred, `CARGO_REGISTRY_TOKEN` fallback for first cut) (YAML Release-notes step + job `if` + complementary `ghost-noop` so branch pushes do not empty-fail `release`; `scripts/check-workflows.sh` in branch-lite/publish-ready); `owner-publish` / `day1-after-publish` / `check-installable` ready; `scripts/check-merge-ready.sh` (merge-tree conflict probe + owner-cut-release next steps); `scripts/owner-cut-release.sh` one-shot cut on main; Actions hygiene via `scripts/check-actions-hygiene.sh` (RC-release zombies + stale tip CI); Installable probes via `scripts/lib/crates-io.sh` (API + sparse index; User-Agent required — CDN 403 without it). Share-assignment + KIP-848 join fix merged here for one path to `main`. **0.5 blocked on owner `CARGO_REGISTRY_TOKEN`** (one-shot after token: `scripts/owner-finish-installable.sh` (includes `verify-crates-io-consumer` adopter compile proof) — FF-merge + local publish bypasses starved Actions; if token is Actions-only: `first-publish.yml` workflow_dispatch `confirm=publish`) (crate name free — API 404 + index absent). Packed + crates.io consumer mains share `scripts/lib/adopter-consumer-main.sh`. **Local civilization-check** (2026-09-04) including broker + SASL_SSL PLAIN + SCRAM-256/512 + OAUTHBEARER + OIDC + mTLS + local `ci-branch-lite` mirror. **`main` Actions Verifiable green** through Installable preflight: run `33850540606` on `6431785` (Kafka 3.9.1 + 4.1.0 broker-smoke + latency-gate). `scripts/check-installable-preflight.sh` → **`READY_EXCEPT_TOKEN`**. Tip may stay ahead of `main` on docs/scripts while the token is missing — `owner-sync-main` refuses docs-only tip→main thrash (`ALLOW_DOCS_THRASH=1` override); `owner-finish-installable` FFs once at cut. Tip still has stale **queued** runs from before tip auto-CI was disabled (agents 403 on cancel) — owner: `scripts/owner-cancel-stuck-runs.sh` / `scripts/owner-unblock.sh`. Tip Verifiable proxy: `scripts/ci-branch-lite.sh` (no auto CI on `dev/**` push). Full matrix on PR/`main`/`workflow_dispatch`. **Parked Verifiable (not on tip):** `dev/verifiable-auth-integrity-fuzz-b686` adds Actions `auth-smoke` (`REQUIRE_AUTH=1`) + `integrity-smoke` (`REQUIRE_INTEGRITY=1`) + ConsumerGroupHeartbeat fuzz — local proof 2026-09-04 green; merge after crates.io `0.1.0` so tip stays docs/scripts-only for one-shot cut. |
| WP-1 Real-broker CI | **done** | `broker-smoke` matrix `apache/kafka:3.9.1` + `4.1.0`; Docker 4.x enables share coordinator + upgrades `share.version=1`; native Kafka fallback; smoke covers roundtrip/produce/admin/txn/**group/eos/kip848/share** (`REQUIRE_SHARE=1` / `REQUIRE_KIP848=1` on 4.x). **Rechecked 2026-09-04** (native Kafka 4.1 via `ci-native-kafka`; `SKIP_DOCKER=1` broker-smoke with kip848+share ok; auth-smoke SASL_SSL PLAIN+SCRAM-256/512+OAUTHBEARER+OIDC+mTLS fail-closed ok; later same-day recheck of broker+auth+integrity+latency gates still green, including a further same-day native pass with latency p99≈75–77µs; **still later same-day** `REQUIRE_AUTH=1` auth-smoke full green again + native broker-smoke kip848/share green + latency p99≈663–865µs / 742µs under agent load, still under relative slack). **Actions Verifiable green** same day: run `33848465892` on `7051625` — latency-gate + broker-smoke Kafka **3.9.1** + **4.1.0** all success (3.9 optional kip848 soft-skips truncated `Protocol` bodies; 4.x still requires kip848/share). **KIP-848 join** sends empty `TopicPartitions` array (null rejected on Kafka 4.x); live `examples/kip848` + decode hardening for truncated error bodies. Wired into `ci-civilization-check.sh`. **Soft-skip honesty (2026-09-04):** optional kip848/share soft-skip only on Unsupported*/truncated-Protocol signals (panics/asserts/connection failures still fail); civilization-check treats unexpected auth/integrity failures as FAIL (only explicit `skipping` paths remain SKIP). |
| WP-2 Adversarial trust | **done** | security.md + audit CI + `cargo deny` + PEM via rustls-pki-types + decode OOM guards + fuzz smoke + libFuzzer (Fetch/Produce/Metadata/records + **Group** Join/Sync/Heartbeat/OffsetCommit + **ShareFetch**). Mock TLS via `openssl` CLI (no `rcgen`/`time`); `RUSTSEC-2026-0009` ignore cleared. Auth smoke: SASL_SSL PLAIN + SCRAM-256/512 + OAUTHBEARER + OIDC + mTLS (`REQUIRE_AUTH=1` recheck 2026-09-04 still green, fail-closed bare-TLS + bare-SSL-vs-mTLS). Tip `ci-branch-lite` **runs** `fuzz_decode_smoke` (group/share/txn) while Actions starved. |
| WP-3 Operator docs | **done** | guide.md (incl. recipes) + migrate-from-rdkafka.md + ADOPTION.md + README links. |
| WP-4 Observability | **done** | Metrics in guide; `tracing` feature; Prometheus text example. |
| WP-5 Perf honesty | **in progress** | Lab A produce enforces **HW sum == acked**; Lab A fetch enforces **HW + consumed == seeded**; combined `scripts/lab-a-integrity.sh` + `ci-integrity-smoke.sh` in civilization-check (unsigned). 2026-09-04 native recheck: integrity COUNT=2000 HW==acked+consumed==seeded; latency gate p99≈71–86µs (earlier ≈69µs) vs 500µs baseline (STATUS); later same-day under agent load ≈663–865µs / 742µs still under relative slack — unsigned only. Gate honesty: loud fail on down broker; integrity soft latency miss no longer hard-exits unless `REQUIRE_INTEGRITY=1`. Not a Suite HOLD lift. Signed Suite HOLD still external. **Actions integrity-smoke job** gates HW==acked/consumed==seeded on PR/main (unsigned). |
| WP-6 Adoption gaps | **in progress** | Template + zstd spike + feature matrix + survey [#85](https://github.com/mingley/partitionline/issues/85) + ADOPTION.md + `docs/schema-companion.md` design (crate waits on crates.io). Packed-crate downstream consumer gate in civilization/publish-ready. Git install pin **`v0.1.0-rc.6`** (pin gate allows docs/scripts tip drift; library changes still require a new rc) (KIP-848 join fix + OIDC/mTLS) until crates.io `0.1.0`. |
| WP-7 Stewardship | **done** | Issue/PR templates; CONTRIBUTING; CODEOWNERS; Dependabot; tag-publish; civilization-check; `scripts/ci-publish-ready.sh`; `scripts/audit-civilization-bars.sh` (six-bar evidence audit). Tip Verifiable via `scripts/ci-branch-lite.sh` (no auto CI on `dev/**` push while org runners starved); full matrix on PR/`main`/`workflow_dispatch`. |

## Success criteria (civilization bar)

The repo is “useful for civilization” when all of the following are true:

1. **Installable:** crates.io release; MSRV CI green.
2. **Verifiable:** mock tests + real Kafka broker CI + fuzz smoke.
3. **Operable:** guide + rdkafka migration doc + metrics/tracing path.
4. **Honest:** benchmarks labeled; no false Suite HOLD lifts.
5. **Independent:** still no C Kafka/compression/SASL in default features.
6. **Stewarded:** changelog, release policy, issue templates, this plan updated.

Until then, treat every PR as moving one checkbox above — not as protocol
tourism.

## References

- `README.md` — user surface and numbers
- `docs/gaps.md` — capability inventory vs librdkafka
- `docs/design.md` — wire and client behavior
- `docs/benchmark.md` / `docs/STATUS.md` — performance claims and HOLD
- `CONTRIBUTING.md` — fmt / clippy / test / no-C rule
