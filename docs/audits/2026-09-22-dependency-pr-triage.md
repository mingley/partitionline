# Dependency and release PR triage — 2026-09-22

KL08-06 analysis. Source SHA `ca50ca1103e37b5159875b8b5562c7cc1087c335`
(main tip; release-plz PR #98 is based on it). Repo MSRV: 1.85
(`rust-version` in `Cargo.toml`).

No merges, closes, comments, or publishes were made from this triage.
A pending release PR is not qualification evidence (see KL08-02 gate).

## Current PR states (re-read 2026-09-22, read-only)

| PR | Subject | State | CI | Disposition |
|---|---|---|---|---|
| #98 | release-plz: release v0.2.0 | OPEN, MERGEABLE, base=tip | no checks on branch | HOLD — do not merge from triage; no card |
| #99 | base64 0.22.1 → 0.23.1 | OPEN, MERGEABLE, base `cb7e97d` (stale) | all 14 jobs pass (run 34454097039) | ACCEPT — rebase + merge (KL08-15) |
| #100 | getrandom 0.2.17 → 0.4.3 | OPEN, MERGEABLE, base `cb7e97d` (stale) | red: E0425, `getrandom::getrandom` removed | ACCEPT WITH MIGRATION (KL08-16) |
| #101 | setup-java 4 → 5 | CLOSED (unmerged, superseded by #105) | — | no action |
| #102 | tokio-rustls 0.26.4 → 0.26.5 | OPEN, MERGEABLE, base `cb7e97d` (stale) | all 14 jobs pass (run 34454110109) | ACCEPT — rebase + merge (KL08-17) |
| #103 | snap 1.1.1 → 1.1.2 | OPEN, MERGEABLE, base `cb7e97d` (stale) | all 14 jobs pass (run 34454116671) | ACCEPT — rebase + merge (KL08-18) |
| #104 | rustls 0.23.43 → 0.23.44 | OPEN, MERGEABLE, base `cb7e97d` (stale) | audit+deny red: RUSTSEC-2026-0285 | SUPERSEDE — bump to ≥0.23.45 instead (KL08-19) |
| #105 | setup-java v4 → v6 | OPEN, base `a06a790` (stale) | mixed: broker-smoke green; audit/clippy/deny/features/fmt red from stale base | ACCEPT WITH REBASE (KL08-20) |

## Per-PR impact notes

### #98 — release v0.2.0 (HOLD)

- Proposes `0.1.0 → 0.2.0` with semver-breaking changes flagged by
  release-plz (`ConsumerConfig.buffer_memory`,
  `ProducerConfig.pre_send_fault` fields).
- `main` is currently red on `audit`/`deny` (RUSTSEC-2026-0285, rustls
  0.23.43 in lockfile); cutting a release on a known TLS advisory
  violates release-safety.
- Per KL08-02, a release needs complete release-profile CI evidence at
  the exact release SHA; this PR branch reports no checks.
- release-plz refreshes the PR itself (already rebased to tip today).
  No follow-up card: release execution belongs to KL08-07 / owner cut
  path after the dependency cards land.

### #99 — base64 0.22.1 → 0.23.1 (ACCEPT, KL08-15)

- MSRV: base64 0.23 declares rust 1.71.0 — below repo MSRV 1.85, no raise.
- API: repo uses only the `Engine` API (`STANDARD`, `URL_SAFE_NO_PAD`)
  in `scram.rs`, `oauth.rs`, `oidc.rs`, `api.rs`, `admin.rs`; 0.23 is
  source-compatible on that surface (full CI green on the PR).
- License: unchanged (`MIT OR Apache-2.0`); `deny` passed on the PR.
- Advisory: none for base64 in the PR's `audit` run.
- Note for reviewer: 0.23 adds a default-on `simd-unsafe` feature
  (unsafe SIMD inside the dependency; first-party `unsafe_code`
  forbidden lint is unaffected). Accept as-is; do not redesign
  features in the merge card.

### #100 — getrandom 0.2.17 → 0.4.3 (ACCEPT WITH MIGRATION, KL08-16)

- MSRV: getrandom 0.4.3 declares rust 1.85 — exactly the repo MSRV.
  No `rust-version` raise, but it pins the MSRV floor; the 1.85 cell
  must pass.
- API: breaking. `getrandom::getrandom` was removed (E0425 in CI log);
  4 call sites on current HEAD must migrate to the 0.4 API:
  `src/group.rs:2689`, `src/admin.rs:765`, `src/protocol/scram.rs:334`,
  `src/protocol/oidc.rs:1372`.
- License: unchanged (`MIT OR Apache-2.0`). New transitive dep
  `r-efi 6.0.0` enters the lockfile; `deny` must re-verify licenses.
- Lockfile keeps a second copy: `ring` still pulls getrandom 0.2.17.
  Acceptable duplication; do not force ring off 0.2 in this card.
- Advisory: none for getrandom; PR's `audit`/`deny` passed.

### #102 — tokio-rustls 0.26.4 → 0.26.5 (ACCEPT, KL08-17)

- Lock-only patch bump; full CI green on the PR.
- MSRV 1.71, license unchanged (`MIT OR Apache-2.0`), no advisory.

### #103 — snap 1.1.1 → 1.1.2 (ACCEPT, KL08-18)

- Lock-only patch bump (`snap = "1"` range already covers it); full CI
  green on the PR.
- License unchanged (`BSD-3-Clause`), no advisory.

### #104 — rustls 0.23.43 → 0.23.44 (SUPERSEDE, KL08-19)

- 0.23.44 does **not** fix RUSTSEC-2026-0285 (TLS 1.3 handshake
  messages accepted across encryption-level boundaries,
  GHSA-2mjx-qc3c-rqvc, CVSS 3.1 AV:N/C:L). Patched in ≥0.23.45;
  0.23.45 is published on crates.io. Both `audit` and `deny` fail on
  the PR for this advisory.
- P0: the same advisory is open on `main` (lockfile 0.23.43), so
  `main`'s `audit`/`deny` gates are red until this lands.
- rustls 0.23.45 declares rust 1.71 and keeps the same license
  (`Apache-2.0 OR ISC OR MIT`): no MSRV/license impact.
- Do not merge #104 as-is; the card bumps to ≥0.23.45 with fresh
  `audit`+`deny` proof.

### #105 — setup-java v4 → v6 (ACCEPT WITH REBASE, KL08-20)

- Workflow-only change (2 lines in `ci.yml`: broker-smoke and
  fixture-verification jobs, temurin 21).
- The v6 bump itself works: `broker-smoke` (both images) and
  `conformance-fixtures` passed on the PR's latest run (35666165128).
- Red jobs are stale-base artifacts (base `a06a790`, Sep 17), not the
  bump: `audit`/`deny` fail on the pre-existing rustls advisory,
  `fmt` on old `tests/protocol_oracles.rs` drift, `clippy`/`features`
  on lints fixed on current `main`.
- Rebase onto current `main` and re-run CI; no Java config change needed.

## Proposed follow-up cards

Six bounded cards (full JSON in `docs/plan/evidence/KL08-06.json`
under `proposed_cards`, for the parent to insert into
`docs/plan/tasks.json`):

- KL08-15 (P1, implementation): land base64 0.23.1.
- KL08-16 (P1, implementation): migrate to getrandom 0.4.3 (4 call sites).
- KL08-17 (P1, implementation): land tokio-rustls 0.26.5.
- KL08-18 (P1, implementation): land snap 1.1.2.
- KL08-19 (P0, implementation): land rustls ≥0.23.45 (RUSTSEC-2026-0285).
- KL08-20 (P1, ci): land setup-java v6 after rebase.

Each card names one dependency/version and targeted validation; no
combined upgrade. Suggested landing order: KL08-19 first (un-breaks
`audit`/`deny` on `main`), then KL08-15/16/17/18/20 in any order.
