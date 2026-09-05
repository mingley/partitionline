# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to the 0.x policy in [`docs/RELEASE.md`](docs/RELEASE.md).

## [Unreleased]

## [0.1.1](https://github.com/mingley/partitionline/compare/v0.1.0...v0.1.1) - 2026-09-05

### Added

- *(kl-06)* bounded OIDC retry for transient IdP failures

### Fixed

- *(kl-06)* omit IdP/broker bodies from OIDC and OAUTHBEARER errors

### Other

- stamp tip SHA for KL-06 OIDC bounded transient retry.
- stamp tip SHA for KL-06 OIDC outage fail-closed honesty.
- *(kl-06)* OIDC IdP outage fail-closed honesty (503/timeout)
- stamp tip SHA for KL-08 adopter exercise template.
- *(kl-08)* add UNFILLED adopter 24h/7d exercise template
- stamp tip SHA for KL-06 metrics/span redaction honesty.
- *(kl-06)* metrics snapshot and tracing skip(self) redaction honesty
- stamp tip SHA for KL-06 auth Error body hygiene.
- rustfmt buffer_ownership and avoid u64-to-usize casts.
- merge main into tip: KL-08 skip-gate + support matrix
- stamp tip SHA for KL-08 support matrix honesty.
- document CI-backed support matrix
- keep linger buffer occupied for max_block Timeout case
- stamp tip SHA for KL-02 buffer ownership mock soak.
- mock buffer ownership under saturating try_send
- stamp tip SHA for KL-06 credential Debug redaction.
- redact credentials in Sasl/Oidc/Tls Debug
- stamp tip SHA for KL-02 consumer close-commit honesty.
- leave/close/unsubscribe must not auto-commit positions
- rustfmt producer close flag and produce_cancel tests.
- stamp tip SHA for KL-08 partial-release rehearsal.
- rehearse partial-release recovery without publishing
- stamp tip SHA for KL-01 protocol oracles.
- Produce/Fetch/Metadata/ListOffsets semantic oracles vs 3.9.1 and 4.1.0
- stamp tip SHA for KL-01 fuzz campaign metadata.
- distinguish fuzz campaign metadata from 15s CI smoke
- stamp tip SHA for KL-01/KL-04 latency CI policy.
- record shared-runner vs controlled latency budgets; keep Suite HOLD
- stamp tip SHA for KL-02 produce cancel contract.
- produce cancel outcomes + durable close
- stamp tip SHA for KL-08 release serialize.
- serialize crates.io publish behind exact-SHA CI
- stamp tip SHA for KL-01 broker identity.
- stamp actual broker identity + portable timeout
- stamp tip SHA for schema companion scaffold.
- Harden schema companion scaffold unit-test gate
- Add partitionline-schema wire scaffold + finish registry proof
- stamp tip SHA for registry adopter consumer gate.
- Require registry adopter consumer once Installable is met.
- stamp tip SHA for maintain timer recheck.
- post-Installable maintain timer recheck.
- stamp tip SHA for handoff tip-ahead-of-parks honesty.
- tip ahead of parks is PARTIAL, not hard FAIL.
- stamp tip SHA for main CI green evidence.
- main CI green after #86 / unblock honesty.
- stamp tip SHA after main merge + #86 honesty.
- close #86 path; unblock/plan honesty.
- stamp tip SHA for crates.io description drift honesty.
- Warn when crates.io description lags Cargo.toml identity.
- stamp tip SHA for owner-status token honesty.
- Post-Installable owner-status: missing token is OK, not BLOCKED.
- stamp tip SHA for tip↔main unify.
- tip↔main unify after CI green land.
- Merge main CI green onto civilization tip.
- stamp tip SHA for post-Installable token MISSING honesty.
- Post-Installable honesty: token MISSING means future cuts, not Installable block.
- stamp tip SHA for main CI fmt/integrity fix.
- Fix main CI: rustfmt cgheartbeat import; integrity skips nested latency.
- stamp tip SHA for main roadmap absorb.
- Honesty-label main roadmap/TODO: Installable met, Suite HOLD still unsigned.
- add production readiness roadmap and task tracker
- Sync tip STATUS and bars honesty onto main.
- handoff DRY_RUN OK when parks already on main.
- Installable 0.1.0 on crates.io; post-cut parks on main.
- Stamp STATUS tip SHA for post-Installable bars honesty.
- Post-Installable bars: four-file pin + token honesty.
- flip adopter docs to crates.io 0.1 after publish.
- Stamp STATUS tip SHA for cut DRY_RUN / ALREADY_INSTALLABLE honesty.
- Fail-close cut DRY_RUN and ALREADY_INSTALLABLE four-file honesty.
- Stamp STATUS tip SHA for post-Installable PARTIAL exit 2.
- Fail-close post-Installable PARTIAL instead of soft-green exit 0.
- Stamp STATUS tip SHA for day1 four-file honesty.
- Align day1 commit/copy honesty to four-file crates.io surface.
- Require crates.io-shaped guide+migrate in handoff day1 gate.
- Flip guide and migrate to crates.io on day1, not only README.
- Stamp STATUS tip SHA for adopter-pin docs honesty.
- Keep adopter docs on the interim git pin until Installable.
- Fail-close handoff DRY_RUN when Installable but docs stay git-shaped.
- Stop greenwashing tip PARTIAL as ok; split pre/post-Installable copy.
- Fail-close handoff DRY_RUN parks/day1 PARTIAL with exit 2.
- Stamp STATUS with docs parks-off-main alignment on tip.
- Document expected parks-off-main in CIVILIZATION/RELEASE; gate in bars.
- Stamp STATUS tip SHA for READY_EXCEPT_TOKEN parks note.
- Note expected parks-off-main on READY_EXCEPT_TOKEN; stamp tip Verifiable.
- Frame parks-off-main as expected before Installable.
- Share parks-on-main probe; fast owner-status without token.
- Rehearse handoff DRY_RUN in tip Verifiable proxies.
- Capture handoff DRY_RUN rc in cut-path (no set -e abort).
- Probe parks-on-main in handoff DRY_RUN; surface re-entry.
- Fail-close handoff OK while post-cut parks remain off main.
- Refuse first-publish re-dispatch when already Installable.
- Chain day1 into post-Installable handoff for Actions-alternate honesty.
- Fail-close bare cut when Actions secret sync fails after Installable.
- Fail-close finish when Actions secret sync fails after Installable.
- Capture day1 PARTIAL in tip proxies; fail-close stale parks.
- Fail-close not-yet-Installable DRY_RUN and gate token asks on preflight.
- Prepare registry token before READY_EXCEPT_TOKEN; surface PARTIAL re-entry.
- Fail-close cut-path handoff: single chain, DRY_RUN rehearses, PARTIAL exits.
- Refuse integrity ok and handoff OK after soft/partial failures.
- Stamp quiet soft-latency STATUS live recheck at tip 436cb4c.
- Recover tip Verifiable quietly after soft latency misses.
- Refuse tip Verifiable ok after soft latency misses.
- Align owner checklists with cut-release PUBLISH_LOCAL auto-default.
- Default cut-release to local publish when token is in-env.
- Rehearse finish DRY_RUN through post-Installable handoff.
- Chain finish live cut through post-Installable handoff.
- Route finish already-Installable re-entry through handoff.
- Point Actions-alternate and day1 paths at post-Installable handoff.
- Add post-Installable handoff for TP and parks re-entry.
- Preserve day1 README/ADOPTION across parks land.
- Note git-tag adopter honesty wiring in STATUS.
- Prove the documented git-tag install pin still compiles.
- Gate open Dependabot bumps to post-cut parks coverage.
- Wire parks-refresh cut guards into Installable preflight.
- Stamp preflight honesty self-tests recheck at f9f3e00
- Run honesty self-tests from Installable preflight
- Stamp finish-path honesty self-tests recheck at 54b5bc6
- Run honesty self-tests from owner-finish-installable
- Stamp tip Verifiable PARTIAL --self-test recheck at 25d4456
- Make tip Verifiable PARTIAL honesty executable
- Stamp tip Verifiable PARTIAL fail-closed recheck at e1fecbe
- Fail tip Verifiable PARTIAL closed (exit 2)
- Stamp tip Verifiable soft-skip honesty recheck at f581359
- Harden tip Verifiable soft-skip honesty
- Harden Verifiable bar for tip live-broker chain
- Add tip live-broker Verifiable chain for branch-lite/cut-path
- Print owner-request-registry-token when finish lacks token
- Surface owner-request-registry-token from Installable preflight
- Add one-screen owner registry-token request helper
- Record unsigned native Verifiable recheck on tip 67072a5
- Gate whitespace TOKEN normalize in registry self-test
- Treat whitespace-only CARGO_REGISTRY_TOKEN as unset
- Fall back to native Kafka in Lab A prepare_broker
- Document SKIP_DOCKER ensure-broker in CHANGELOG
- Ensure broker across Verifiable gate chains
- Auto-start native Kafka when Docker overlay fails
- Detect misnamed registry token env and load TOKEN_FILE
- Stamp CIVILIZATION live tip after Secrets deep-link.
- Deep-link Cursor env Secrets URL to the Secrets tab.
- Stamp CIVILIZATION live tip after Dependabot label hygiene.
- Probe missing Dependabot dependencies label in Actions hygiene.
- Stamp CIVILIZATION live tip to absorb tip b4deece.
- Stamp CIVILIZATION live tip after absorbing main publish-new.
- Require tip first-publish.yml to match main for tip-delta.

### Changed

- Scripts: tip Verifiable quiet soft-latency recovery — after `latency gate failed (soft)`, `ci-tip-verifiable-broker` sleeps and re-runs integrity (`TIP_VERIFIABLE_QUIET_RETRIES` default 1, `TIP_VERIFIABLE_QUIET_SLEEP_SECS` default 8; `0` disables). Only a clean recheck may restore `ok`; still-soft stays PARTIAL/exit 2. `--self-test` + bars gate quiet retry (no greenwash).
- Scripts: tip Verifiable soft-latency honesty — soft latency miss is PARTIAL even when integrity-smoke prints `ok`; handoff `--self-test` gates day1 README/ADOPTION preserve on `LAND_PARKS=1`.
- Scripts/docs: cut-release `PUBLISH_LOCAL` auto-defaults to 1 when `CARGO_REGISTRY_TOKEN` is in-env; CIVILIZATION agent priority leads with WP-0.5 token cut.
- Scripts: `check-installable-preflight` runs honesty self-tests (`check-registry-token --self-test` + `ci-tip-verifiable-broker --self-test`) before `READY_EXCEPT_TOKEN`, so the one-screen Installable gate cannot skip PARTIAL/token-normalize units; bars require preflight wiring.
- Scripts: `owner-finish-installable` runs honesty self-tests before the token gate (`check-registry-token --self-test` + `ci-tip-verifiable-broker --self-test`) so the cut path cannot skip PARTIAL/token-normalize units; bars require finish wiring.
- Scripts: tip Verifiable soft-skip honesty is now executable — `ci-tip-verifiable-broker --self-test` proves finalize `ok`/PARTIAL exit 2/soft PARTIAL exit 0; wired into `ci-branch-lite` / `check-cut-path`; bars run the self-test (not grep-only).
- Scripts: tip Verifiable `PARTIAL` now exits **2** by default (was 0) so `set -e` tip proxies (`ci-branch-lite` / `check-cut-path`) cannot greenwash mid-chain soft-skips; `TIP_VERIFIABLE_SOFT=1` keeps PARTIAL exit 0 for constrained sandboxes. Bars audit requires `exit 2`.
- Scripts: tip Verifiable soft-skip honesty — `ci-tip-verifiable-broker` prints `ok` only when broker+auth+integrity all pass; mid-chain soft-skips print `PARTIAL` (not evidence). Capable envs (Java/openssl/keytool/python3 + Kafka) auto-set `REQUIRE_BROKER=1` / `REQUIRE_AUTH=1` unless `TIP_VERIFIABLE_SOFT=1`. Bars audit gates the pattern.
- Scripts: Verifiable bar now requires tip live-broker scripts (`ci-tip-verifiable-broker`, integrity/latency, `ensure-broker`) wired into `ci-branch-lite` / `check-cut-path`; `ci-civilization-check` no longer stops the shared native broker after broker-smoke (avoids integrity Connection refused / soft-miss greenwash). Opt-in old stop: `STOP_NATIVE_AFTER_BROKER=1`.
- Scripts: tip Verifiable (`ci-branch-lite` / `check-cut-path`) now runs `ci-tip-verifiable-broker` (ensure-broker → broker-smoke kip848+share → auth → integrity/latency) with soft-skip honesty when tooling/broker is absent — tip Verifiable is no longer fmt/clippy/lib-only while Installable waits.
- Scripts: `owner-finish-installable` missing-token path prints `owner-request-registry-token` inline so the publish-new Secrets ask is unavoidable.
- Scripts: `check-installable-preflight` surfaces `owner-request-registry-token` on READY_EXCEPT_TOKEN so the one-screen publish-new ask is the default next step.
- Scripts/docs: `owner-request-registry-token.sh` one-screen Installable token ask (Secrets deep link + publish-new scope + finish path); wired from `owner-status` / `owner-unblock`. CIVILIZATION post-cut parks tip ancestor stamp → `bb2506c`.
- Docs: same-day native Verifiable recheck on tip `67072a5` (broker kip848+share, auth matrix, integrity COUNT=2000, latency quiet p99≈150µs after under-load ≈380µs, fuzz decode smoke) recorded unsigned; not a Suite HOLD lift.
- Scripts: `check-registry-token --self-test` covers whitespace-only TOKEN / TOKEN_FILE normalize units (tip Verifiable + cut-path) before the fake-token network probe.
- Scripts: `check-registry-token` / `owner-finish-installable` treat whitespace-only `CARGO_REGISTRY_TOKEN` as unset (and trim leading/trailing space) via `pl_normalize_cargo_registry_token` / `pl_prepare_cargo_registry_token`, so a blank Secrets paste cannot look "set" while Installable stays blocked.
- Scripts: `lab-a-common` `prepare_broker` falls back to `ensure-broker` (native Kafka) when Docker overlay fails in nested Cloud Agent VMs, so direct Lab A integrity/produce/fetch no longer soft-skip without a broker.
- Scripts: `ci-broker-smoke` `SKIP_DOCKER=1` path now uses `ensure-broker` to start native Kafka when 9092 is down (agent Verifiable re-entry after auth/integrity) instead of hard-failing.
- Scripts: shared `scripts/lib/ensure-broker.sh` starts native Kafka when 9092 is down; `ci-latency-gate` uses it before benching, and `ci-integrity-smoke` no longer stops a shared native broker on EXIT (so agent Verifiable chains integrity → latency without Connection refused).
- Scripts: `ci-broker-smoke` auto-starts native Kafka (`ci-native-kafka.sh`) when Docker overlay mounts fail in nested Cloud Agent VMs, so local Verifiable does not soft-skip the broker gate.
- Docs: same-day native Verifiable recheck on tip `3a1b00a` (broker kip848+share, auth matrix, integrity COUNT=2000, latency quiet p99≈147–161µs after under-load miss ≈1007µs, fuzz decode smoke) recorded unsigned; not a Suite HOLD lift.
- Scripts/docs: `check-registry-token` / `owner-finish-installable` load `CARGO_REGISTRY_TOKEN_FILE` into the current shell and WARN on common misnamed env vars (`CARGO_TOKEN`, `CRATES_IO_TOKEN`, …) so Secrets UI typos cannot silently block Installable; shared helper `scripts/lib/cargo-registry-token.sh`.
- Scripts/docs: Cursor env Secrets deep link defaults to `.../secrets` so login redirect lands on the Secrets tab for `CARGO_REGISTRY_TOKEN` injection.
- Scripts: `check-actions-hygiene` probes for the GitHub `dependencies` label Dependabot expects and prints the owner `gh label create` one-shot (agents 403); surfaces in `owner-unblock` / cut-path hygiene.
- Tip: merge `main` `first-publish.yml` **publish-new** honesty; `check-post-cut-parks-stack` now requires tip↔main workflow match (not tip-soft-only) so tip-delta stays docs/scripts-only after the main Actions alternate landed.
- Scripts/docs: shared `scripts/lib/cursor-env-secrets-url.sh` prints the Cloud Agent Environments → Secrets deep link (overridable via `PARTITIONLINE_CURSOR_ENV_SECRETS_URL`); wired into `owner-finish-installable` / `owner-unblock` / RELEASE so token injection is one click away.
- Scripts: `ci-branch-lite` + `check-cut-path` run `DRY_RUN=1` `owner-enable-trusted-publishing` so tip Verifiable and cut-path rehearse the post-Installable OIDC UI checklist (crate may be absent); Installable preflight READY_EXCEPT_TOKEN messaging spells publish-new + Cursor/Actions secret paths.
- Scripts: `check-cut-path` runs `DRY_RUN=1` `day1-after-publish` (README flip rehearsal) and `check-actions-hygiene` (stale queue surface); tip Verifiable (`ci-branch-lite`) also surfaces Actions hygiene before Installable preflight.
- Scripts: `ci-branch-lite` + `check-cut-path` run `DRY_RUN=1` `owner-dispatch-first-publish` so tip Verifiable and cut-path prove `first-publish.yml` stays workflow_dispatch-visible on main (Actions-secret alternate) before the token cut.
- Docs: same-day native Verifiable recheck on tip (broker kip848+share, auth matrix, integrity COUNT=2000, latency quiet p99≈220µs after under-load miss ≈837µs, fuzz decode smoke) recorded unsigned; not a Suite HOLD lift.
- Scripts/docs: owner-finish / owner-unblock / RELEASE spell out Cursor Environments → Secrets for `CARGO_REGISTRY_TOKEN`; tip Verifiable + cut-path gate `check-merge-ready`.
- Scripts: `ci-branch-lite` + `check-cut-path` run `check-crate-metadata`; tip Verifiable also runs `check-installable-preflight` so READY_EXCEPT_TOKEN stays tip-gated before the token cut.
- Scripts: `ci-branch-lite` runs `cargo publish --dry-run` + `check-trusted-publishing-ready` so tip Verifiable proves upload shape and OIDC release.yml shape before the token cut.
- Scripts: `ci-branch-lite` + `check-cut-path` run `PRE_PUBLISH=1` `audit-civilization-bars` so five civilization bars are tip-gated before the token cut (Installable credentials may remain BLOCKED).
- Scripts: `ci-branch-lite` + `check-cut-path` run `ci-crate-consumer` so the packed `.crate` tarball is proven dependable (Installable packaging) before the token cut, not only via publish-ready.
- Scripts: `ci-branch-lite` + `check-cut-path` run `ci-deny` so the Independent bar (no C Kafka/OpenSSL/zstd defaults) is tip-gated, not only in publish-ready.
- Scripts: `ci-branch-lite` + `check-cut-path` run `ci-msrv` so Installable MSRV is exercised (compile+lib tests on declared rust-version), not only declared in Cargo.toml.
- Scripts: `check-post-cut-parks-stack` requires tip `first-publish.yml` to match `main` while Installable is unmet (main may already document publish-new) + park publish-new — so tip-delta stays docs/scripts-only.
- Scripts: `ci-branch-lite` + `check-cut-path` run `MODE=path` `verify-crates-io-consumer` so the day1 adopter compile proof is rehearsed before crates.io `0.1.0` exists.
- Scripts: `ci-branch-lite` rehearses `refresh-post-cut-parks` DRY_RUN (tip→Verifiable→SCRAM→lz4→checkout) before the parks stack gate.
- Scripts: `check-cut-path` rehearses `refresh-post-cut-parks` DRY_RUN (tip→Verifiable→SCRAM→lz4→checkout) before parks stack + publish dry-run.
- Docs: same-day native Verifiable recheck on tip (broker kip848+share, auth matrix, integrity COUNT=2000, latency quiet p99≈207µs after under-load miss ≈1049µs, fuzz decode smoke) recorded unsigned; not a Suite HOLD lift.
- Scripts: `refresh-post-cut-parks.sh` refreshes parks in land order (tip→Verifiable→SCRAM→lz4→checkout) so parallel tip merges cannot fork the tip⊆… chain; wired into owner-status / owner-unblock.
- Scripts: `check-post-cut-parks-stack` also gates park chain (Verifiable⊆SCRAM⊆lz4⊆checkout) so parallel tip refreshes cannot fork CHANGELOG histories that only conflict at stacked land time.
- Scripts/docs: rebuild lz4 + actions/checkout post-cut parks stacked on SCRAM (SCRAM⊆lz4⊆checkout) so tip→Verifiable→SCRAM→lz4→checkout DRY_RUN stays merge-clean after tip-is-ancestor gate; tip stays docs/scripts-only.
- Scripts: `check-post-cut-parks-stack` gates tip-is-ancestor of every post-cut park (catches parks lagging tip after docs/scripts commits) before stacked DRY_RUN merge + `cargo test --lib`.
- CI: bump `actions/checkout` to v7 on parked `dev/actions-checkout-bump-b686` (covers Dependabot #92; land after Installable).
- Dependencies: `lz4_flex` 0.11→0.14. Parked off tip until after Installable (lockfile breaks docs-only tip-delta).
- Dependencies: flate2 1.1.9→1.1.10 (gzip write header/footer infinite-loop fix; miniz_oxide 0.8→0.9). Parked off tip until after Installable (lockfile breaks docs-only tip-delta).
- Dependencies: SCRAM stack hmac 0.12→0.13, pbkdf2 0.12→0.13, sha2 0.10→0.11 (KeyInit import). Parked off tip until after Installable; auth-smoke green.
- Scripts/docs: `owner-enable-trusted-publishing` post-Installable helper verifies release.yml OIDC shape and prints exact crates.io Trusted Publishing UI steps; wired into day1 / owner-status / RELEASE.
- Scripts: `check-cut-path` runs `cargo publish --dry-run` so Installable rehearsal proves package upload shape before `CARGO_REGISTRY_TOKEN` arrives.
- Scripts/docs: park `actions/checkout` v7 on `dev/actions-checkout-bump-b686` and append it to post-cut land order (after Verifiable + SCRAM + lz4); tip stays docs/scripts-only until Installable.
- Scripts/docs: park `lz4_flex` 0.11→0.14 on `dev/lz4-flex-bump-b686` and wire it into post-cut parks land order (after Verifiable + SCRAM); tip stays docs/scripts-only until Installable.
- Docs/scripts: same-day native Verifiable recheck on tip (broker kip848+share, auth matrix, integrity COUNT=2000, latency quiet p99≈126–203µs after under-load miss) recorded unsigned; owner-status + publish-ready surface post-cut parks stack and Trusted Publishing shape.
- Scripts: `owner-land-post-cut-parks.sh` lands Verifiable + flate2 + SCRAM crypto parks after Installable; `owner-finish-installable` chains it by default.
- Scripts: READY_EXCEPT_TOKEN / owner-status / owner-unblock note that `owner-finish-installable` chains parked Verifiable merge by default.
- Scripts: `owner-finish-installable` defaults to chaining `owner-merge-parked-verifiable` after Installable (`MERGE_PARKED_VERIFIABLE=0` skips).
- Scripts: `owner-merge-parked-verifiable.sh` lands post-Installable Actions auth+integrity + ConsumerGroupHeartbeat fuzz from parked branch.
- Docs/scripts: post-cut Verifiable handoff for parked `dev/verifiable-auth-integrity-fuzz-b686` (Actions auth+integrity jobs + ConsumerGroupHeartbeat fuzz); tip stays docs/scripts-only until Installable.
- Docs: same-day native Verifiable recheck (broker kip848+share, auth matrix,
  integrity COUNT=2000, latency gate p99≈71–86µs) recorded in STATUS /
  ADOPTION / CIVILIZATION — unsigned; not a Suite HOLD lift.

### Fixed

- Scripts: post-cut parks DRY_RUN cleanup uses globals under `set -u` so conflict exits do not abort worktree teardown.
- Docs: CIVILIZATION live tip SHA + [#86](https://github.com/mingley/partitionline/issues/86) cut path aligned to publish-new + `owner-finish-installable` (issue body no longer steers to publish-update-only / owner-cut-release).
- Scripts: `owner-status` / `audit-civilization-bars` / `check-merge-ready` probe crates.io publish-new auth (not token presence alone) so publish-update-only or garbage tokens cannot greenwash Installable readiness; `owner-unblock` surfaces both post-cut parks and the token probe.
- Docs: CIVILIZATION tip SHA + post-cut parks list (Verifiable + SCRAM/flate2) aligned with live cut path and structured registry-token probe.
- Scripts: `check-registry-token` probes crates.io publish-new auth via PUT `/api/v1/crates/new` with a structured empty-tarball body (not `/api/v1/me`, which 403s publish-only tokens; not an empty body, which 400s before auth and false-accepts); `--self-test` proves fake tokens fail; wired into preflight, cut-path, finish, and `ci-branch-lite`.
- Docs/scripts: first-cut token guidance requires crates.io `publish-new` (not only `publish-update`) so a wrong-scoped token cannot silently block Installable.
- Scripts/docs: `check-trusted-publishing-ready` validates release.yml OIDC shape; ADOPTION warns not to merge Dependabot flate2/SCRAM bumps onto tip before Installable (covered by post-cut SCRAM park); Verifiable park refreshed onto tip.
- Scripts: finish DRY_RUN hard-fails dirty post-cut parks stacks (no `|| true` greenwash); `check-cut-path` + owner-unblock surface tip→Verifiable→SCRAM stack gate.
- Scripts: post-cut parks DRY_RUN uses a disposable worktree (no `checkout -f` on tip) so tip WIP is not discarded; tip Verifiable proxy hard-fails on dirty tip→Verifiable→SCRAM stacks (`check-post-cut-parks-stack`).
- Scripts: tip Verifiable proxy (`ci-branch-lite` / `ci-publish-ready`) hard-fails when parked post-cut branches no longer stack-clean onto tip (`check-post-cut-parks-stack`); lander DRY_RUN prefers local tip when ahead of origin and honors `REQUIRE_PARKS=1`.
- Scripts: `owner-land-post-cut-parks` DRY_RUN now performs real stacked merges (per-park merge-tree vs bare target hid tip→Verifiable→SCRAM CHANGELOG conflicts); SCRAM/flate2 park rebased onto tip for clean post-cut land.
- Soft-skip honesty: optional kip848/share broker soft-skips only on Unsupported*/truncated-Protocol signals; civilization-check fails unexpected auth/integrity errors instead of SKIP-greenwashing.
- CI broker-smoke: when `REQUIRE_KIP848=0` / `REQUIRE_SHARE=0` (Kafka 3.9 matrix), soft-skip kip848/share on truncated `Protocol` decode errors instead of hard-failing — KIP-848/share remain required on 4.x. Optional kip848 also stops retrying immediately on `Protocol(need N bytes)` so 3.9 matrix cells do not wait out six timeouts.
- CI: stop injecting partial `KAFKA_*` env on `apache/kafka:4.x` Docker (was breaking KRaft format with missing `process.roles`); enable share via post-ready `share.version=1` upgrade only.
- CI latency gate: honor `LATENCY_LIMIT_US` absolute ceiling; Actions sets `5000` for GHA+Docker noise (local relative baseline unchanged).

- `release.yml`: add complementary `ghost-noop` job so branch-push evaluations
  that skip `publish` stay green (avoids empty-job / all-skipped red X on tip).
- `scripts/owner-cut-release.sh`: `DRY_RUN=1` allowed on non-`main` (civilization
  tip rehearsal); still refuses real cuts off `main` without
  `ALLOW_NON_MAIN_PUBLISH=1`.

### Added

- Verifiable: Actions `auth-smoke` + `integrity-smoke` jobs (REQUIRE_AUTH/REQUIRE_INTEGRITY); ConsumerGroupHeartbeat (KIP-848) decode fuzz target + decode-smoke coverage; auth-smoke self-bootstraps Kafka binaries when missing.
- `scripts/check-installable-preflight.sh`: one-shot pre-publish probe that
  exits `0` with `READY_EXCEPT_TOKEN` when merge-ready + metadata + main CI
  are green and only `CARGO_REGISTRY_TOKEN` / crates.io cut remains (exit `3`
  if main CI is still running; exit `2` if already Installable).
- `scripts/check-main-ci.sh`: probe whether `origin/main` HEAD has terminal
  green CI (exit 0/1/2). Wired into `owner-finish-installable` step 2b —
  refuses Installable cut on red main unless `ALLOW_RED_MAIN=1`; real cuts
  default `REQUIRE_MAIN_CI=1` (inconclusive also refuses); `DRY_RUN=1` keeps
  soft warn unless `REQUIRE_MAIN_CI` is set explicitly.
- `.github/workflows/first-publish.yml`: workflow_dispatch first crates.io
  cut when `CARGO_REGISTRY_TOKEN` is an Actions secret (confirm=publish);
  documented in ADOPTION / RELEASE / owner-finish-installable as the
  Actions-only alternate to the in-env finish script.
- `scripts/owner-dispatch-first-publish.sh`: owner helper to
  `gh workflow run first-publish.yml` after tip is on `main` (dispatch is
  only listed from the default branch).
- `scripts/owner-sync-main.sh`: explicit tip→main FF with `CONFIRM=1` (avoids cancel-in-progress thrash on main CI while Installable waits). Refuses when main HEAD CI is still running unless `ALLOW_BUSY_MAIN=1`. Also refuses docs/scripts-only tip→main while crates.io cut is still absent unless `ALLOW_DOCS_THRASH=1` (leave tip ahead; `owner-finish-installable` FF's once at cut).

- `owner-finish-installable.sh`: `DRY_RUN=1` rehearses merge/cut without a
  token; `check-merge-ready` next steps detect when HEAD already matches
  `main` (skip FF; point at first-publish dispatch).
- `scripts/lib/adopter-consumer-main.sh`: shared operator-surface consumer
  used by `ci-crate-consumer` and `verify-crates-io-consumer` so packed-crate
  and crates.io proofs cannot drift.
- `scripts/verify-crates-io-consumer.sh`: prove an adopter can compile against
  partitionline — `MODE=registry` (default) after crates.io publish; `MODE=path`
  pre-publish rehearsal wired into `ci-publish-ready` so day1 cannot fail on
  API drift. Registry mode wired into `day1-after-publish` +
  `owner-finish-installable`.
- `scripts/owner-finish-installable.sh`: one-shot Installable finish once
  `CARGO_REGISTRY_TOKEN` is in-env — FF-merge civilization → `main`, local
  `cargo publish` (bypasses starved Actions), day1, prove Installable; after
  success, best-effort `gh secret set CARGO_REGISTRY_TOKEN` for later tags.
  `owner-status` / `ci-publish-ready` / bars audit point at this path first.
- `scripts/lib/crates-io.sh`: shared Installable probe (crates.io API + sparse
  index fallback; required User-Agent — CDN returns empty 403 without one).
  Wired into `check-installable`, `check-merge-ready`, and `owner-status`.
- `scripts/check-merge-ready.sh`: `git merge-tree --write-tree` conflict probe
  vs `origin/main` (flush-left `changed in both` fallback; avoids doc/script
  false positives); owner next steps prefer `owner-cut-release.sh`.
- `scripts/check-actions-hygiene.sh`: classifies stale queued Actions (RC-tag
  release zombies vs tip CI leftovers); wired into `owner-status` /
  `owner-unblock`. `owner-cancel-stuck-runs` labels the same classes.
- `scripts/ci-crate-consumer.sh`: packed-crate downstream check now links the
  operator surface (Producer/Consumer/ConsumerGroup/ShareGroup/Admin +
  SASL/TLS config), not only Producer.
- `scripts/ci-civilization-check.sh`: Installable section uses shared crates.io
  probe, day1 README `DRY_RUN` preflight, merge-ready gate, and actions hygiene;
  ends with `PRE_PUBLISH=1` civilization-bars audit.
- `scripts/audit-civilization-bars.sh`: evidence audit for the six CIVILIZATION
  success bars (PASS/PARTIAL/BLOCKED/FAIL); wired into `owner-status`.
  `PRE_PUBLISH=1` treats Installable BLOCKED as expected so publish-ready can
  gate on the other five bars before the first crates.io cut.
- `scripts/check-crate-metadata.sh`: crates.io package-shape gate (version,
  description identity, keywords/categories, license files, include allowlist,
  packed-crate omits scripts/fuzz); wired into merge-ready and publish-ready.
- `scripts/owner-cut-release.sh`: best-effort Actions `CARGO_REGISTRY_TOKEN`
  preflight (`REQUIRE_ACTIONS_SECRET=1` to hard-fail); runs
  `audit-civilization-bars` after a successful cut.

- `examples/kip848`: KIP-848 next-gen consumer group join (`ConsumerGroup::join_consumer_topics`).
  Broker smoke runs it on Kafka 4.x (`REQUIRE_KIP848=1` default). Join now sends an
  **empty** `TopicPartitions` array (null was rejected: "must be empty when (re-)joining").
  ConsumerGroupHeartbeat decode accepts truncated error bodies that omit trailing
  tagged fields (Kafka 4.x `INVALID_REQUEST` path).

- Auth smoke covers **OIDC client_credentials** (local `scripts/oidc-token-stub.py`)
  and **mTLS** (SSL listener with `ssl.client.auth=required`; `examples/tls`
  accepts `TLS_CLIENT_CERT_PEM` / `TLS_CLIENT_KEY_PEM`). X.509 v3 client certs
  required for rustls. Wired into `scripts/ci-auth-smoke.sh` / civilization-check.

- `scripts/ci-auth-smoke.sh`: native Kafka SASL_SSL + SCRAM-SHA-256/512 +
  OAUTHBEARER (unsecured JWT) smoke (private CA, isolated ports,
  `examples/sasl` / `examples/oauth` with `TLS_CA_PEM`); TLS-only produce must
  fail closed. Wired into `scripts/ci-civilization-check.sh`.
- `examples/sasl` and `examples/oauth` accept optional `TLS_CA_PEM` /
  `TLS_SERVER_NAME` for the production SASL_SSL path.
- `scripts/owner-status.sh` prints Installable/Verifiable blocker status
  (token, crates.io, Actions tip/main); wired into civilization-check and
  publish-ready as an informational footer.
- CI concurrency cancels superseded runs on `main` too, so a stuck queued
  `main` job cannot permanently block the next push after merge.
- `scripts/owner-cancel-stuck-runs.sh` cancels queued Actions runs older than
  15 minutes (`DRY_RUN=1` supported); wired from `owner-status` / ADOPTION for
  the org-wide runner starvation case (agents get 403 — owner must run it).
- `scripts/owner-unblock.sh` one-shot owner checklist: status probe, dry-run
  cancel targets, merge → tag `v0.1.0` → day1 path after token + runners.
- `scripts/lab-a-common.sh` shared broker helpers; `scripts/lab-a-integrity.sh`
  combined produce→HW→fetch integrity; `scripts/ci-integrity-smoke.sh` small-
  count local/CI smoke (+ unsigned latency) wired into civilization-check.
  `lab-a-fetch.sh` now also requires HW delta == acked.
- `tests/fuzz_decode_smoke.rs` also hammers group / share / txn response
  decoders (not only fetch/produce/metadata). `scripts/ci-branch-lite.sh` and
  civilization-check now **run** that smoke (tip Verifiable no longer
  `--lib`-only); optional short libFuzzer smoke when nightly+g++ are present.
- CI: `dev/**` tip pushes no longer auto-queue (org runner starvation /
  perpetual tip re-queue); full matrix on PR/`main`/`workflow_dispatch` only.
- Git install pin `v0.1.0-rc.6` (README / ADOPTION / migrate guide) for
  adopters before crates.io; does not trigger `release.yml` (final `vX.Y.Z`
  only). Advances `v0.1.0-rc.5` with `owner-publish` → `day1-after-publish`
  chaining and a refreshed native Kafka 4.1 broker smoke (kip848/share).
- `scripts/check-adopter-pin.sh` (wired into branch-lite / publish-ready /
  owner-status) so git pins cannot silently lag tip.
- `scripts/post-publish-readme.sh` `DRY_RUN=1` preflight (wired into
  `ci-publish-ready`) so day-1 README flip cannot silently break before
  crates.io exists. `owner-unblock` points at release issue #86 and the
  `v0.1.0-rc.6` interim git pin.

### Changed

- `scripts/owner-cut-release.sh` one-shot owner cut on clean `main` (tag → Actions publish → crates.io wait → day1; `PUBLISH_LOCAL=1` / `DRY_RUN=1`). Wired into `owner-unblock` and RELEASE.md.
- `ci-publish-ready` and `owner-status` also run `check-merge-ready` so the
  merge→tag path is visible in the same probes as Installable/Verifiable.
- Tip re-verified auth smoke (SASL_SSL PLAIN+SCRAM+OAUTHBEARER+OIDC + mTLS fail-closed) on 2026-09-04.
- Tip re-verified native Kafka 4.1 broker smoke (`ci-native-kafka` + kip848/share required) on 2026-09-04.
- `scripts/check-merge-ready.sh` (wired into `owner-unblock`, documented in
  RELEASE.md) gates civilization → main → `vX.Y.Z` without requiring a token:
  final version + CHANGELOG section, release.yml OIDC/secret auth, workflow
  YAML, adopter pin, and tip-vs-`main` ancestry (`FULL=1` adds branch-lite).
- `release.yml` prefers crates.io **Trusted Publishing** (OIDC via
  `rust-lang/crates-io-auth-action`, `id-token: write`) and falls back to
  `CARGO_REGISTRY_TOKEN` for the first publish. RELEASE.md / owner-unblock /
  day1 document the post-0.1.0 migration off long-lived Actions secrets.
- Tip Verifiable gates (`ci-branch-lite` / `ci-publish-ready`) run
  `scripts/check-workflows.sh` so invalid workflow YAML (the flush-left
  `run: |` class that empty-failed `release` on branch pushes) cannot regress
  unnoticed. `owner-status` prefers Actions runs for the exact HEAD SHA.
- `release.yml` hardens the crates.io publish trigger: fix invalid YAML in the
  GitHub Release notes step (flush-left multiline string caused empty-job
  `release` failures on branch pushes), input-safe concurrency / checkout
  expressions, job-level skip unless final tag or `workflow_dispatch`, and an
  explicit `X.Y.Z` tag shape check. Still publishes on `v0.1.0`.
- `scripts/post-publish-readme.sh` also inserts crates.io + docs.rs badges on
  day-1 README flip (`DRY_RUN=1` asserts badges). `release.yml` creates a
  GitHub Release after a successful crates.io publish.
- `scripts/check-adopter-pin.sh` allows docs/scripts tip drift without a new rc;
  library/`Cargo.toml` changes since the pin still fail the gate.

- `scripts/owner-publish.sh` runs `day1-after-publish` by default after a
  successful manual `cargo publish` (`RUN_DAY1_AFTER_PUBLISH=0` to skip).
- Migration guide git pin → `v0.1.0-rc.6`; guide documents KIP-848 empty
  `TopicPartitions` join + `examples/kip848`. Cargo.toml lists remaining
  examples (`share`, `cooperative`, `metrics`, `offsets`, `pause`, `wakeup`)
  so the packaged crate surface matches README.
- ADOPTION owner unblock: `main` Actions recovered; remaining cancel targets
  are stale tip/tag queues (not an eternal `main` queue).

- Mock TLS fixtures use the `openssl` CLI instead of `rcgen`, dropping
  `time` from the dependency graph and clearing the `RUSTSEC-2026-0009`
  ignore in `deny.toml` / `.cargo/audit.toml` without raising MSRV.
- Auth smoke covers PLAIN, SCRAM-SHA-256/512, and OAUTHBEARER over SASL_SSL;
  CI `test` jobs assert `openssl version` before `cargo test`.
- ADOPTION / CIVILIZATION record local civilization-check evidence
  (broker + SASL_SSL PLAIN + SCRAM-256/512 + OAUTHBEARER) while GitHub Actions
  remains queued.

### Fixed

- Mock TLS fixture reads cert/key PEMs via `File` + `Read` instead of
  `std::fs::read`, restoring `clippy::disallowed_methods` / publish-ready.
- `release.yml` only runs on final `vX.Y.Z` tags (not `v0.1.0-rc.*`), so git
  RC install pins do not queue crates.io publish jobs or burn Actions capacity.

## [0.1.0] - 2026-09-04

First crates.io release baseline (publish via `docs/RELEASE.md` / tag `v0.1.0`).

### Added

- Pure-Rust Kafka client: produce, fetch, classic groups, cooperative-sticky,
  KIP-848 consumer groups, KIP-932 share groups, transactions / EOS, and
  Kafka 3.x/4.x admin APIs.
- TLS via rustls (no OpenSSL); SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER,
  OIDC client credentials.
- Compression: gzip, snappy, lz4 (zstd and Kerberos remain out of default
  features; see `docs/gaps.md` and `docs/zstd-spike.md`).
- Java-shaped builders and rustdoc naming matching Java client calls.
- Optional `tracing` feature (produce/fetch/poll/rejoin/txn spans).
- Examples: produce, consume, group, txn, eos, admin, tls, sasl, oauth, share,
  benches, interceptors, metrics (`FORMAT=prom`).
- Operator docs: guide, rdkafka migration, security, release policy,
  API stability, civilization plan.
- CI: mock tests (MSRV 1.85 + stable), clippy, audit, cargo-deny, `cargo package`,
  broker-smoke (Kafka 3.9.1 + 4.1.0), fuzz-smoke, latency-gate.
- libFuzzer targets under `fuzz/`; decode allocation DoS guards.
- TLS PEM parsing via `rustls-pki-types` (no archived `rustls-pemfile`).

### Fixed

- KIP-932 share groups on Kafka 4.1: join with a client Uuid member id, wait for
  a real heartbeat assignment (no zero topic id), decode null Assignment as
  INT8 `-1`, and use the group coordinator for membership. Share smoke needs
  `group.share.enable` and finalized `share.version=1`.

### Changed

- Lab A produce harness exits non-zero unless broker HW sum equals acked each run.
- Civilization/publish-ready gates verify a downstream crate can depend on the packed `.crate`.
- Release workflow accepts `workflow_dispatch` on an existing `v*` tag; rustdoc/`ci-docs` smoke is part of publish-ready.
- Fixed broken rustdoc `[Display]` intra-doc links (now `std::fmt::Display`).
- Cleared remaining unresolved rustdoc intra-doc links (module docs resolve in
  submodule scope; crate denies `rustdoc::broken_intra_doc_links`; `ci-docs`
  fails the gate if any reappear).
- CI: `dev/**` branch pushes run a single `branch-lite` job; full matrix on
  pull_request, `main`, and `workflow_dispatch` so scarce runners can finish.
- Lab A harness accepts `TOPIC=` as well as `KAFKA_TOPIC=`; STATUS notes a
  fresh unsigned latency-gate + HW==acked smoke sample (not a Suite HOLD lift).
- `scripts/check-installable.sh` probes crates.io for the Installable bar;
  ADOPTION notes Actions is stuck org-wide (`main` queued for hours).
- `ci-broker-smoke`: when Docker overlay fails but `$KAFKA_BOOTSTRAP` is already
  up, fall back to that broker automatically (same path as `SKIP_DOCKER=1`).
- Fuzz: Join/Sync/Heartbeat/OffsetCommit + ShareFetch decode targets; Lab A
  fetch integrity harness (`scripts/lab-a-fetch.sh`) requires consumed==seeded
  (unsigned; not a Suite HOLD lift).
- `examples/oauth` for OAUTHBEARER (unsecured JWT) and OIDC client-credentials;
  ADOPTION documents pinable `v0.1.0-rc.2` git install until crates.io lands.
- Broker smoke: Kafka CI matrix uses `apache/kafka:4.1.0`; Docker 4.x starts with
  share coordinator RF=1 and upgrades `share.version=1`; `REQUIRE_SHARE=1` fails
  the job if share cannot fetch records on 4.x. Civilization-check only counts
  broker smoke when the log contains `ci-broker-smoke: ok` (Docker soft-skips
  are not evidence).

### Security

- Reject array / tagged-field lengths that exceed remaining buffer bytes so
  malformed broker frames cannot force multi-gigabyte `Vec` allocations.

### Notes

- Not a drop-in for `rd_kafka_*` or rust-rdkafka types.
- Defaults that differ from Java: `auto.offset.reset=Earliest`,
  `allow.auto.create.topics=false`, shorter `delivery.timeout.ms` /
  `max.block.ms` (see README).
- Mock TLS fixtures use the `openssl` CLI (no `rcgen` / `time` in the graph).
