# STATUS

Suite HOLD stands. This file records holes. It does not lift them.

| Hole | Status |
|---|---|
| Fetch writeup | **Recorded** 2026-08-28 on this-VM (Apache Kafka 3.9.1 KRaft + rust-rdkafka 0.39.0). **Unsigned** until Kernel Integrity signs. Not Lab A. Not a signed vs-C win. |
| Latency | **Recorded** 2026-08-28 on this-VM (Apache Kafka 3.9.1 KRaft + rust-rdkafka 0.39.0 produce-ack). **Unsigned** until Kernel Integrity signs. Not Lab A. Not a signed vs-C win. Not a Suite HOLD lift. CI relative gate (`scripts/ci-latency-gate.sh` vs `docs/latency-baseline.json`) rechecked 2026-09-04 on native Kafka 4.1 (produce-ack p99 ≈ 71µs / 86µs / later ≈ 75µs / 77µs vs baseline 500µs ceiling; earlier same-day ≈ 69µs / 119µs / 379µs; later same-day native rechecks ≈ 663–865µs / 742µs under agent load — still under the relative slack ceiling, still **unsigned**, still not a Suite HOLD lift). Lab A produce smoke same day: HW sum == acked. Combined Lab A integrity (`scripts/lab-a-integrity.sh` / `ci-integrity-smoke.sh`): HW==acked and consumed==seeded (COUNT=2000 recheck same day, including later same-day native recheck) — unsigned only, not a Suite HOLD lift. Latency gate now fails loudly on a down broker (no silent `set -e`/`pipefail` swallow); integrity “soft” latency miss no longer `exit 1` unless `REQUIRE_INTEGRITY=1`. **Rechecked 2026-09-04 (later same day, tip  (docs/pin honesty; Verifiable on  tree)`dc30c21`):** native Kafka 4.1 broker-smoke (kip848+share) green; `REQUIRE_AUTH=1` auth-smoke full matrix green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green; latency gate quiet samples p99≈126–203µs (pass vs 750µs relative limit) after an under-agent-load miss at ≈896–1108µs — still **unsigned**, still not a Suite HOLD lift. **Rechecked 2026-09-04 (tip `97d69d6`):** native Kafka 4.1 broker-smoke (kip848+share) green; `REQUIRE_AUTH=1` auth-smoke full matrix green; Lab A integrity COUNT=2000 green; latency quiet p99≈207µs (pass vs 750µs) after under-agent-load miss ≈1049µs; fuzz decode smoke green — still **unsigned**, still not a Suite HOLD lift. **Rechecked 2026-09-04 (later same day, tip `07861d6`):** native Kafka 4.1 broker-smoke (kip848+share) green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green; latency gate quiet samples p99≈711µs (pass vs 750µs relative limit) after an under-agent-load miss at ≈867µs — still **unsigned**, still not a Suite HOLD lift. **Native auth recheck (2026-09-04, tip `929ef57`):** `REQUIRE_AUTH=1` auth-smoke SASL_SSL PLAIN+SCRAM-256/512+OAUTHBEARER+OIDC+mTLS fail-closed green — unsigned; not a Suite HOLD lift. |
| e2e | First mock-broker protocol-client e2e landed in #80 (`tests/e2e.rs`). Mock only. |

**Native Verifiable recheck (2026-09-04, tip `194291a`):** native Kafka 4.1 broker-smoke (kip848+share) green; `REQUIRE_AUTH=1` auth-smoke SASL_SSL PLAIN+SCRAM-256/512+OAUTHBEARER+OIDC+mTLS fail-closed green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green; latency gate quiet p99≈220µs (pass vs 750µs) after under-agent-load miss ≈837µs; fuzz decode smoke green — still **unsigned**, still not a Suite HOLD lift. Named gaps stay closed: ElectLeaders (43), DescribeLogDirs v5, DescribeQuorum (55), Add/Remove/UpdateRaftVoter (80–82).

**Tip live-broker Verifiable (2026-09-04, tip `ca030ad` + `ci-tip-verifiable-broker`):** native Kafka 4.1 broker-smoke (kip848+share) green; auth-smoke SASL_SSL PLAIN+SCRAM+OAUTHBEARER+OIDC+mTLS fail-closed green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green (latency gate included) — still **unsigned**, still not a Suite HOLD lift. Wired into tip `ci-branch-lite` / `check-cut-path` so tip Verifiable is not fmt/clippy/lib-only while Installable waits.

**Native Verifiable recheck (2026-09-04, tip `194291a`):** native Kafka 4.1 broker-smoke (kip848+share) green; `REQUIRE_AUTH=1` auth-smoke SASL_SSL PLAIN+SCRAM-256/512+OAUTHBEARER+OIDC+mTLS fail-closed green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green; latency quiet p99≈150µs (pass vs 750µs) after under-load sample ≈380µs; fuzz decode smoke green — still **unsigned**, still not a Suite HOLD lift.

**Native Verifiable recheck (2026-09-04, tip `194291a`):** native Kafka 4.1 broker-smoke (kip848+share) green; `REQUIRE_AUTH=1` auth-smoke SASL_SSL PLAIN+SCRAM-256/512+OAUTHBEARER+OIDC+mTLS fail-closed green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green; latency quiet p99≈147–161µs (pass vs 750µs) after under-agent-load miss ≈1007µs; fuzz decode smoke green — still **unsigned**, still not a Suite HOLD lift.

**Tip live-broker Verifiable + soft-skip honesty (2026-09-04, tip `f581359` + `ci-tip-verifiable-broker` PARTIAL/auto-REQUIRE):** native Kafka 4.1 broker-smoke (kip848+share) green; auth-smoke SASL_SSL PLAIN+SCRAM+OAUTHBEARER+OIDC+mTLS fail-closed green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green; latency quiet p99≈96µs (pass vs 750µs) — still **unsigned**, still not a Suite HOLD lift. Tip Verifiable now refuses to print `ok` after mid-chain soft-skips (`PARTIAL` only); capable envs auto-`REQUIRE_BROKER`/`REQUIRE_AUTH`.

**Tip Verifiable PARTIAL fail-closed (2026-09-04, tip `e1fecbe` + `ci-tip-verifiable-broker` exit 2):** mid-chain soft-skips now exit 2 by default so tip proxies cannot treat PARTIAL as green (`TIP_VERIFIABLE_SOFT=1` opt-in exit 0). Capable-env recheck: native Kafka 4.1 broker-smoke (kip848+share) green; auth matrix green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green; latency quiet p99≈98µs (pass vs 750µs) — still **unsigned**, still not a Suite HOLD lift.

**Tip Verifiable PARTIAL --self-test (2026-09-04, tip `194291a`):** `ci-tip-verifiable-broker --self-test` proves finalize ok/exit0, PARTIAL/exit2, soft PARTIAL/exit0; wired into tip proxies + bars. Live recheck: integrity COUNT=2000, latency quiet p99≈86µs — still **unsigned**, still not a Suite HOLD lift.

**Finish-path honesty self-tests (2026-09-04, tip `194291a`):** `owner-finish-installable` step 0a runs registry-token + tip Verifiable `--self-test` before Installable short-circuit / token gate; bars gate the finish wiring — still **unsigned** tip Verifiable evidence only, still not a Suite HOLD lift.

**Preflight honesty self-tests (2026-09-04, tip `194291a`):** `check-installable-preflight` runs registry-token + tip Verifiable `--self-test` before `READY_EXCEPT_TOKEN`; bars gate preflight wiring alongside finish/cut-path — still not a Suite HOLD lift.

**Parks-refresh cut guards in preflight (2026-09-04):** `check-installable-preflight` also runs `check-parks-refresh-cut-guards` (finish restores main after parks auto-refresh; publish-ready restores caller). Bars require the guard wired into finish + cut-path + preflight — token-day footgun closed without lifting Suite HOLD.

**Dependabot ↔ parks coverage (2026-09-04):** `check-dependabot-parks-coverage` maps open Dependabot cargo/Actions bumps (#87–#92) to post-cut parks and fails on unmapped PRs; wired into cut-path, bars, and actions hygiene so tip stays docs/scripts-only while Installable waits — still not a Suite HOLD lift.

**Git-tag adopter consumer (2026-09-04):** `MODE=git bash scripts/verify-crates-io-consumer.sh` cargo-checks the documented README/ADOPTION git pin (`v0.1.0-rc.6`) so pre-crates.io adopters are not lied to; wired into cut-path + preflight + finish honesty + Operable bars — still not a Suite HOLD lift.

**Day1 docs preserve across parks (2026-09-04):** `scripts/lib/preserve-day1-docs.sh` backs up README/ADOPTION/guide/migrate before parks land and restores from filesystem if stash pop fails; wired into finish + cut-path + preflight honesty — token-day footgun closed without lifting Suite HOLD.

**Post-Installable handoff (2026-09-04):** `scripts/owner-post-installable-handoff.sh` re-enters Installable + adopter pin + registry consumer + full bars + Trusted Publishing + optional parks land after any cut path (finish, Actions first-publish, or soft-failed TP/parks). `DRY_RUN=1` rehearsed in cut-path + bars (`HANDOFF_FROM_BARS=1` avoids bars↔handoff recursion). Day1 / first-publish dispatch / status / unblock / cut-release / Trusted Publishing helpers all point owners at the handoff; finish's already-Installable short-circuit, live cut, **and** DRY_RUN cut rehearsal all chain into the handoff (`LAND_PARKS` from post-cut parks knobs) so Actions-alternate and in-env cuts cannot drift — still not a Suite HOLD lift.

**Cut-release PUBLISH_LOCAL auto-default (2026-09-04):** bare `owner-cut-release` (stepwise docs) now defaults to `PUBLISH_LOCAL=1` when `CARGO_REGISTRY_TOKEN` is in-env and `PUBLISH_LOCAL` is unset — closing the token-day footgun where an in-env token was ignored and the cut waited on starved Actions. Explicit `PUBLISH_LOCAL=0` still forces tag → `release.yml`. Gated by `--self-test` in cut-path + bars — still not a Suite HOLD lift.

**Cut-release owner-helper comments (2026-09-04):** `owner-unblock` / `owner-status` / merge-ready / publish-ready no longer steer bare `owner-cut-release` as tag→Actions; comments match the in-env auto `PUBLISH_LOCAL=1` default. Gated in cut-path + bars — still not a Suite HOLD lift.

**Tip Verifiable soft-latency honesty (2026-09-04):** `ci-tip-verifiable-broker` treats integrity `latency gate failed (soft)` / `ci-integrity-smoke: PARTIAL` as `PARTIAL` (exit 2). The integrity leaf itself no longer prints final `ok` after soft latency (prints `PARTIAL`, exit 2; `REQUIRE_INTEGRITY=1` hard-fails). Civilization-check ski's soft-misses. Tip quiet-retry + `--self-test` + bars gate the chain — still **unsigned**, still not a Suite HOLD lift.

**Tip Verifiable quiet soft-latency recovery (2026-09-04):** after integrity `latency gate failed (soft)`, `ci-tip-verifiable-broker` now quiet-rechecks integrity by default (`TIP_VERIFIABLE_QUIET_RETRIES=1`, sleep 8s) and only prints `ok` when the recheck is clean — still-soft stays `PARTIAL`/exit 2 (no greenwash). `--self-test` + bars cover quiet retry. **Unsigned live recheck (tip `194291a`):** native Kafka 4.1 broker-smoke (kip848+share) green; auth matrix green; Lab A integrity COUNT=2000 HW==acked and consumed==seeded green; latency quiet p99≈61µs (pass vs 750µs) without needing quiet retry — still **unsigned**, still not a Suite HOLD lift.

**Integrity leaf + handoff fail-closed (2026-09-04):** `ci-integrity-smoke` soft-latency → `PARTIAL`/exit 2 (no final `ok`). `owner-post-installable-handoff` exits `PARTIAL`/2 when `LAND_PARKS=1` parks land or Trusted Publishing helper fails (never final `OK`). `owner-cut-release` chains the same handoff after day1; finish/cut surface handoff PARTIAL without claiming a finished post-cut land. Bars + `--self-test` gate — still not a Suite HOLD lift.

**Cut-path single-handoff + DRY_RUN honesty (2026-09-04):** finish calls cut with `SKIP_HANDOFF=1`, syncs Actions secrets, then runs exactly one handoff (no double parks land). Bare cut-release still chains handoff; its `DRY_RUN` now rehearses handoff before `DRY_RUN complete` (self-test + bars gate). Full bars refuse final OK when PARTIAL notes remain (exit 2); `PRE_PUBLISH` still allows structural PARTIAL notes while Installable waits. Trusted Publishing helper prints `INFO` (UI still owner), not final OK. Still not a Suite HOLD lift.

**Preflight TOKEN prepare + PARTIAL re-entry (2026-09-04):** `check-installable-preflight` runs `pl_prepare_cargo_registry_token` before any `READY_EXCEPT_TOKEN` claim (whitespace→unset, `TOKEN_FILE` load, misname WARN) with `--self-test` gated in bars. Finish already-Installable `DRY_RUN` captures parks rehearsal rc and can exit `PARTIAL`/2. `owner-status` surfaces misname WARNs + PARTIAL bar lines and branches next-steps on `ALREADY_INSTALLABLE`; `owner-unblock` documents SKIP_HANDOFF + PARTIAL re-entry (do not re-cut). Still not a Suite HOLD lift.

**Not-yet-Installable DRY_RUN PARTIAL + gated token ask (2026-09-04):** finish not-yet-Installable `DRY_RUN` captures cut/parks/handoff rcs and exits `PARTIAL`/2 on soft-fail (no final OK). `day1-after-publish` absent+`DRY_RUN` exits `PARTIAL`/2; `check-cut-path` captures day1/finish PARTIAL instead of greenwashing final OK. `owner-request-registry-token` runs preflight first and refuses a READY_EXCEPT_TOKEN claim when parks/merge-ready/prepare honesty fail. Still not a Suite HOLD lift.

**Day1 PARTIAL tip-proxy + parks fail-closed (2026-09-04):** `ci-branch-lite` and `ci-publish-ready` now capture `day1-after-publish` DRY_RUN `PARTIAL`/exit 2 (same as cut-path) so tip Verifiable and token-day publish-ready cannot abort on the expected absent crate. Preflight refuses `READY_EXCEPT_TOKEN` (exit 1) when parks are stale without a token; token ask refuses the rehearsed claim on that path. Bars gate `day1_rc` in branch-lite + publish-ready. Still not a Suite HOLD lift.

**Finish Actions-secret PARTIAL (2026-09-04):** after a successful crates.io cut, `owner-finish-installable` tracks Actions `gh secret set` and exits `PARTIAL`/2 when sync fails (agents often 403) — never final OK with an unsynced Actions secret. Owner re-entry printed; bars gate `secret_rc` + PARTIAL string. Still not a Suite HOLD lift.

**Bare cut Actions-secret PARTIAL (2026-09-04):** bare `owner-cut-release` (stepwise path, `SKIP_HANDOFF=0`) now syncs Actions `CARGO_REGISTRY_TOKEN` after Installable and exits `PARTIAL`/2 on sync failure — same fail-closed honesty as finish. `SKIP_HANDOFF=1` still leaves sync to finish. Self-test + bars + status/unblock re-entry gated. Still not a Suite HOLD lift.

**Handoff day1 chain (2026-09-04):** `owner-post-installable-handoff` runs `day1-after-publish` after Installable prove (Actions-alternate / handoff-only re-entry) and exits `PARTIAL`/2 if adopter docs remain git-shaped — never final OK with pre-crates.io install pins. DRY_RUN captures day1 PARTIAL. Self-test + bars gate. Still not a Suite HOLD lift.

**First-publish already-Installable PARTIAL (2026-09-04):** `owner-dispatch-first-publish` refuses re-dispatch when crates.io already has the version (`PARTIAL`/exit 2 → handoff re-entry). Prevents soft-green no-op against `first-publish.yml`'s already-exists skip. Tip proxies capture `dispatch_rc`; self-test + bars gate. Still not a Suite HOLD lift.

**Handoff parks-on-main PARTIAL (2026-09-04):** bare `owner-post-installable-handoff` (`LAND_PARKS=0`) no longer final-OKs while post-cut parks are off `origin/main`. Probes each park is an ancestor of main; exits `PARTIAL`/2 unless landed or `ALLOW_PARKS_PENDING=1`. `LAND_PARKS=1` now uses `REQUIRE_PARKS=1`. Self-test + bars gate. Still not a Suite HOLD lift.

**Handoff DRY_RUN parks-on-main honesty (2026-09-04; amended 2026-09-05):** `DRY_RUN=1` handoff probes parks-on-main (not only stack). Already-Installable *and* pre-token parks pending both exit `PARTIAL`/2 (day1-aligned fail-closed; see 2026-09-05 entry). Status/unblock print parks-not-on-main re-entry. Self-test + bars gate. Still not a Suite HOLD lift.

**Cut-path handoff_rc capture (2026-09-04):** `check-cut-path` captures handoff DRY_RUN rc (like day1/dispatch) so already-Installable parks-off-main PARTIAL/2 cannot `set -e` abort the rehearsal. Aggregates into OK-with-PARTIAL. Finish comment no longer claims handoff DRY_RUN always exits 0. Bars gate `handoff_rc`. Still not a Suite HOLD lift.

**Tip-proxy handoff DRY_RUN (2026-09-04):** `ci-branch-lite` and `ci-publish-ready` now rehearse `owner-post-installable-handoff` DRY_RUN (with `HANDOFF_FROM_BARS=1`) and capture `handoff_rc` like cut-path — tip Verifiable / token-day publish-ready cannot ignore parks-on-main PARTIAL. Bars gate. Still not a Suite HOLD lift.

**Shared parks-on-main probe + fast owner-status (2026-09-04):** `scripts/check-parks-on-main.sh` is the shared ancestor-of-main probe (handoff DRY_RUN + live). Preflight `ALREADY_INSTALLABLE` surfaces `PARTIAL` when parks remain off main (Installable ≠ post-cut complete). `owner-status` live-probes parks-on-main and skips branch-lite/full bars when `CARGO_REGISTRY_TOKEN` is unset unless `OWNER_STATUS_FULL=1` (token-ask path stays usable). Self-test + bars gate. Still not a Suite HOLD lift.

**Pre-Installable parks-off-main framing (2026-09-04):** `owner-status` no longer labels parks-off-main as PARTIAL before crates.io exists (that looked like a cut blocker). Pre-token shows `pending (expected pre-Installable)`; post-Installable keeps PARTIAL + handoff re-entry. Token ask + unblock note the same. Bars gate. Still not a Suite HOLD lift.

**Preflight READY_EXCEPT_TOKEN parks expected + tip Verifiable recheck (2026-09-05, tip `dc30c21` / tip HEAD after docs `e0c8c6b`):** `check-installable-preflight` on `READY_EXCEPT_TOKEN` now prints that parks stay off main until after crates.io `0.1.0` (**expected pre-Installable**; tip⊆parks stack is the pre-cut gate). Bars grep the string. **Unsigned tip live-broker Verifiable** on tip `11af93e` tree: `ci-tip-verifiable-broker` → broker-smoke **ok**, auth-smoke **ok**, integrity-smoke **ok**, final **ok**; integrity leaf COUNT=2000 HW==acked consumed==seeded; quiet latency p99≈91µs (pass vs 750µs) — still **unsigned**, still not a Suite HOLD lift. **Docs alignment (`e0c8c6b`):** `docs/CIVILIZATION.md` + `docs/RELEASE.md` state the same expected parks-off-main framing; bars grep both. Crates.io `partitionline` **404**; `CARGO_REGISTRY_TOKEN` unset — Installable still blocked.

**Handoff DRY_RUN parks/day1 PARTIAL exit 2 (2026-09-05):** `owner-post-installable-handoff` pre-token DRY_RUN now exits **PARTIAL/2** (parks-off-main + day1 aggregate) instead of soft-green exit 0 after a PARTIAL note. Bars require `handoff` DRY_RUN rc=2 + `DRY_RUN complete with PARTIAL`. `ci-publish-ready` captures `dispatch_rc` like branch-lite/cut-path. `docs/ADOPTION.md` + preflight side paths state **expected pre-Installable** parks-off-main. Still not a Suite HOLD lift. Still blocked on `CARGO_REGISTRY_TOKEN` / crates.io 0.1.0.


**Owner-status / tip-proxy PARTIAL honesty (2026-09-05):** `owner-status` no longer labels branch-lite exit 0 + `ok with PARTIAL` as plain `ok`. Tip proxies (`ci-branch-lite`, `check-cut-path`) split pre-token vs already-Installable PARTIAL copy (token-blocked vs post-cut re-entry). Stale “pre-token handoff exit 0” comments/STATUS line amended. Trusted-publishing-ready final line is INFO when crate absent (not bare OK). Bars gate. Still not a Suite HOLD lift. Still blocked on `CARGO_REGISTRY_TOKEN` / crates.io 0.1.0.

**Handoff DRY_RUN git-shaped docs fail-close (2026-09-05):** already-Installable `DRY_RUN` now exits `PARTIAL`/2 when README stays git-shaped (Actions-alternate footgun), not only on live path. Self-test + bars gate. `ci-publish-ready` splits pre-token vs already-Installable PARTIAL copy. Still not a Suite HOLD lift. Still blocked on `CARGO_REGISTRY_TOKEN` / crates.io 0.1.0.

**Tip Verifiable recheck + adopter-pin docs honesty (2026-09-05, tip `65af9cd`):** unsigned tip live-broker Verifiable on tip `65af9cd` tree: broker/auth/integrity **ok**, COUNT=2000 HW==acked consumed==seeded, quiet latency p99≈75µs (pass vs 750µs) — still **unsigned**, not a Suite HOLD lift. `docs/guide.md` + `docs/migrate-from-rdkafka.md` now lead with the interim git rc pin (not live crates.io `0.1`); `check-adopter-pin` requires guide/migrate tag parity and refuses crates.io-leading stanzas while README is git-shaped; bars gate. Still blocked on `CARGO_REGISTRY_TOKEN` / crates.io 0.1.0.

**Day1 guide+migrate crates.io flip (2026-09-05):** `day1-after-publish` / `ci-publish-ready` now rehearse `post-publish-guide` + `post-publish-migrate` (not only README/ADOPTION). `preserve-day1-docs` covers those paths across parks land. Post-Installable `check-adopter-pin` refuses live git pins in guide/migrate once README is crates.io-shaped. Bars gate DRY_RUN flips. Still not a Suite HOLD lift. Still blocked on `CARGO_REGISTRY_TOKEN` / crates.io 0.1.0.

**Handoff git-shaped gate covers guide+migrate (2026-09-05):** `owner-post-installable-handoff` PARTIAL (live + already-Installable DRY_RUN) now requires crates.io-shaped README **and** ADOPTION/guide/migrate via `pl_adopter_docs_crates_io_shaped` — not README-only soft-green. Self-test + bars gate. Still not a Suite HOLD lift. Still blocked on `CARGO_REGISTRY_TOKEN` / crates.io 0.1.0.

**Day1 four-file commit/copy honesty (2026-09-05, tip `a991e68`):** finish/cut/publish/unblock/publish-ready/civilization-check + ADOPTION/RELEASE/CIVILIZATION now tell the owner to commit **README + ADOPTION + guide + migrate** after day1 (not README-only). `preserve-day1-docs` stash-pop recovery resets the full four-file surface. Civilization-check rehearses all four `post-publish-*` DRY_RUNs. Does **not** publish `0.1.0` or lift Suite HOLD.

**Post-Installable PARTIAL exit 2 (2026-09-05, tip `5530b78`):** tip proxies (`check-cut-path`, `ci-branch-lite`, `ci-publish-ready`) and `owner-request-registry-token` no longer soft-green (`exit 0`) when Installable is already met but parks/day1/handoff stay PARTIAL — they exit **PARTIAL/2**. Pre-token rehearsal PARTIAL still exits 0. Day1 DRY_RUN / unblock / STATUS preserve copy name the four-file surface. Bars gate. Does **not** publish `0.1.0` or lift Suite HOLD. Still blocked on `CARGO_REGISTRY_TOKEN`.

**Cut DRY_RUN / ALREADY_INSTALLABLE four-file honesty (2026-09-05 01:29 UTC, tip `9ea1013`):** `owner-cut-release` no longer claims crates.io Installable on pre-token handoff PARTIAL (Installable-gated copy + `dry_handoff_rc` fail-close). Preflight + token-ask probe day1 four-file shape via shared `scripts/lib/adopter-docs-shaped.sh`; `owner-status` next-steps name the four-file commit. Bars gate. Does **not** publish `0.1.0` or lift Suite HOLD. Still blocked on `CARGO_REGISTRY_TOKEN`.

**Day1 crates.io flip after Installable (2026-09-05 01:36 UTC, tip `65332b9`):** crates.io has `partitionline` `0.1.0` (published 2026-09-05T01:32Z). Live `day1-after-publish` flipped README+ADOPTION+guide+migrate to crates.io pins; registry adopter consumer `cargo check` OK. Next: `LAND_PARKS=1` post-Installable handoff. Does **not** lift Suite HOLD (Lab A still unsigned).

**Post-Installable bars honesty (2026-09-05 01:39 UTC, tip `7d7dfd1`):** after crates.io `0.1.0`, adopter-pin bars accept four-file crates.io shape (not only pre-Installable git tags); `adopter-docs-shaped --self-test` aligns with Installable presence; missing `CARGO_REGISTRY_TOKEN` is no longer an Installable BLOCKED once the crate is present (future-cut note only). Full bars OK (42 pass). Suite HOLD remains (Lab A unsigned).

**Installable + post-cut parks landed (2026-09-05 01:41 UTC, tip `fdb25a5`):** crates.io `partitionline` `0.1.0` live; day1 four-file crates.io flip committed; registry adopter consumer OK; full civilization bars 42/0/0; `LAND_PARKS=1` handoff landed post-cut parks onto `origin/main` (`check-parks-on-main` OK). Suite HOLD remains until signed Lab A. Trusted Publishing UI still owner if not configured.

**Handoff DRY_RUN parks-on-main path (2026-09-05 01:48 UTC, tip `187bf4c`):** bars accept handoff DRY_RUN exit 0 when parks already on main (not only PARTIAL/2 while pending). Re-land after tip refresh keeps `check-parks-on-main` green. Full bars 42/0/0. Suite HOLD remains.

**Handoff DRY_RUN parks-on-main path (2026-09-05 01:49 UTC, tip `187bf4c`):** bars accept handoff DRY_RUN exit 0 when parks already on main. Suite HOLD remains.


**Post-Installable tip absorb + plan honesty (2026-09-05, tip `c2f1bb2`):** civilization tip absorbs `origin/main` (post-cut parks already landed) so tip is no longer 462 commits behind. `docs/CIVILIZATION.md` / `docs/ADOPTION.md` / `docs/RELEASE.md` stop claiming Installable is still token-blocked / unpublished; priority order is post-Installable (tip≥main, handoff re-entry, unsigned Verifiable, Suite HOLD). `expected pre-Installable` retained as historical parks framing for bars. Does **not** lift Suite HOLD (Lab A unsigned).


**Main roadmap absorb (2026-09-05, tip `46ec304`):** `origin/main` added `docs/ROADMAP.md` + `TODO.md` (production ramp to v1.0.0) after parks land; tip honesty branch merges that commit and labels Suite HOLD still unsigned. Does **not** lift Suite HOLD.


**Main CI green: fmt + integrity/latency split (2026-09-05, tip `e38e5ce`):** `tests/fuzz_decode_smoke.rs` rustfmt (cgheartbeat import order). Actions `integrity-smoke` sets `SKIP_LATENCY_GATE=1` under `REQUIRE_INTEGRITY=1` so Lab A HW is not conflated with nested local 750µs latency under GHA load — unsigned latency stays the dedicated `latency-gate` job (`LATENCY_LIMIT_US=5000`). Auth/integrity checkout pinned to `@v7` (parks leftover `@v5`). Does **not** lift Suite HOLD. Broker-smoke 3.9.1 Docker pull reset on `95e3865` was infra flake (later main runs green for that job).


**Post-Installable token MISSING honesty (2026-09-05, tip `57e3cfd`):** `check-registry-token` MISSING path now distinguishes Installable-met (token only for future cuts / Actions) from first cut of a new crate name (still needs publish-new). Does **not** lift Suite HOLD. Token remains unset in this agent.


**Main CI green: fmt + integrity/latency split (2026-09-05, tip `1dfc192`):** `tests/fuzz_decode_smoke.rs` rustfmt (cgheartbeat import order). Actions `integrity-smoke` sets `SKIP_LATENCY_GATE=1` under `REQUIRE_INTEGRITY=1` so Lab A HW is not conflated with nested local 750µs latency under GHA load — unsigned latency stays the dedicated `latency-gate` job (`LATENCY_LIMIT_US=5000`). Auth/integrity checkout pinned to `@v7` (parks leftover `@v5`). Does **not** lift Suite HOLD. Broker-smoke 3.9.1 Docker pull reset on `95e3865` was infra flake (later main runs green for that job).


**Post-Installable token MISSING honesty (2026-09-05, tip `1afab91`):** `check-registry-token` MISSING path now distinguishes Installable-met (token only for future cuts / Actions) from first cut of a new crate name (still needs publish-new). Does **not** lift Suite HOLD. Token remains unset in this agent.


**Tip↔main unify after CI land (2026-09-05, tip `670e1ee`):** tip absorbed main CI green (`SKIP_LATENCY_GATE`, rustfmt, checkout v7) after tip-advance and main push diverged; crates.io description pure-Rust / no-C / no-librdkafka retained on tip. Does **not** lift Suite HOLD.


**Owner-status post-Installable token honesty (2026-09-05, tip `486238b`):** `owner-status` no longer labels a missing `CARGO_REGISTRY_TOKEN` as Installable-BLOCKED when crates.io already has this version (OK + future-cuts copy); bars/branch-lite fast-path skip copy distinguishes Installable-met from token-wait. Bars gate the strings. Does **not** lift Suite HOLD. Token remains unset in this agent.


**crates.io description drift honesty (2026-09-05, tip `9181ce6`):** `scripts/check-crates-io-description.sh` WARNs when the published crates.io description lags `Cargo.toml` identity (local names no-C / no-librdkafka; published page still weaker until the next cut). Wired into `owner-status`; bars gate the probe. Does **not** re-cut `0.1.0`. Does **not** lift Suite HOLD.


**Post-Installable #86 close + unblock/plan honesty (2026-09-05, tip \`6493bcd\`):** Closed crates.io first-cut tracking #86 (0.1.0 live). \`owner-unblock\` branches ALREADY_INSTALLABLE (Trusted Publishing / handoff / survey #85; do not re-cut) vs historical READY_EXCEPT_TOKEN. CIVILIZATION WP-6 + ROADMAP stop waiting on crates.io / default ruzstd P0. Merged main evidence roadmap onto tip. Does **not** lift Suite HOLD.


**Main CI green after #86 / unblock honesty (2026-09-05, tip `dc9e2ec`):** Actions `ci` on `main` HEAD green (fmt, clippy, test 1.85+stable, features, deny, package, audit, auth-smoke, integrity-smoke, latency-gate, fuzz-smoke, broker-smoke Kafka 3.9.1 + 4.1.0). Installable met; #86 closed; owner-unblock ALREADY_INSTALLABLE path live; civilization bars 47 PASS. Does **not** lift Suite HOLD (Lab A unsigned). Trusted Publishing UI still owner.


**Handoff tip-ahead-of-parks honesty (2026-09-05, tip `404fdf6`):** `owner-post-installable-handoff` DRY_RUN and live stack checks PARTIAL/exit 2 when parks are already on main but tip is not an ancestor of park heads (STATUS stamps), with `refresh-post-cut-parks` guidance — no hard FAIL that bricks bars after docs-only tip moves. Bars gate. Does **not** lift Suite HOLD.


**Post-Installable maintain timer (2026-09-05, tip `59da087`):** Recheck: `CARGO_REGISTRY_TOKEN` unset (Installable met — future cuts only; `--self-test` OK); `check-installable` present; parks on main OK; post-cut parks stack OK; civilization bars 48/0/0; tip=main. Main CI on this HEAD still running broker-smoke/fuzz at stamp time. Suite HOLD remains (Lab A unsigned). Trusted Publishing UI still owner. Do not re-cut 0.1.0.


**Registry adopter consumer post-Installable (2026-09-05, tip `a692183`):** Operable bars + cut-path + branch-lite require `MODE=registry` `verify-crates-io-consumer` when Installable is met (git MODE SKIP is not enough). CI `package` runs `ci-crate-consumer` against the packed `.crate`. `docs/schema-companion.md` notes core 0.1.0 publish gate is met. Does **not** lift Suite HOLD.


**Schema companion scaffold + finish registry wire (2026-09-05, tip `22999e1`):** `partitionline-schema` wire framing (magic+schema-id) landed workspace-excluded `publish=false`; `check-schema-companion-scaffold` gated in bars (requires lib unit tests). `owner-finish-installable` runs `MODE=registry` adopter consumer when Installable. Tip=main after FF. Does **not** publish the companion or lift Suite HOLD.

**KL-01 broker identity + portable timeout (2026-09-05, tip `d00fc56`):** broker/auth smokes stamp `requested=` vs `actual=` (docker/native/external); `pl_timeout` accepts GNU `timeout` or Homebrew `gtimeout` and fails closed otherwise; bars gate + identity `--self-test`. Does **not** close full KL-01 (oracles/fuzz campaigns remain). Does **not** lift Suite HOLD.

**KL-08 release serialize (2026-09-05, tip `acb1153`):** release-plz PR-only; owner-cut-release / owner-publish / ci-publish-ready / release.yml require exact-SHA `check-main-ci` + crate-consumer; crates.io soft-skip if version present. Rehearse-only (no new cut). Does **not** lift Suite HOLD.

**KL-08 partial-release rehearsal (2026-09-05, tip `04a4992`):** `scripts/rehearse-partial-release.sh --self-test` proves recovery without publishing (0.1.0 stays; release-plz PR-only; exact-SHA CI + crate-consumer on cut/release.yml/publish-ready; crates.io skip; publish-succeeded → day1/handoff DRY_RUN, not another publish). Wired into `ci-publish-ready` + bars. KL-08 package stays open. Does **not** lift Suite HOLD.

**KL-08 skip-gate honesty (2026-09-05, tip `9ebf3f1`):** `owner-publish` skips `cargo publish` when crates.io already has the version (PUBLISH_LOCAL re-entry cannot re-cut 0.1.0). `release.yml` grants `actions: read` so exact-SHA `gh run list` can see workflow runs. Rehearsal `--self-test` fail-closes if Authenticate/Publish skip `if:` lines are deleted. Does **not** lift Suite HOLD.

**KL-02 produce cancel contract (2026-09-05, tip `633bad3`):** guide cancellation/shutdown table; `tests/produce_cancel.rs` (completed/failed/ambiguous/Closed); durable `Producer` `closed` flag so clones cannot send after `close`. Does **not** close full KL-02 (no overload soak). Does **not** lift Suite HOLD.

**KL-01/KL-04 latency CI policy (2026-09-05, tip `cc59201`):** CI run 33938039612 nested integrity produce-ack p99 **1,344 µs** vs **750 µs** is **historical** — `integrity-smoke` no longer nests the local relative gate (`SKIP_LATENCY_GATE=1`). Dedicated `latency-gate` remains `LATENCY_LIMIT_US=5000` (GHA+Docker catastrophic ceiling, unsigned). Local default is still 500 µs + 50% slack (750 µs). This is **not** a claim that raising a limit fixed the miss. Suite HOLD unchanged. See [latency-ci-policy.json](latency-ci-policy.json).

**KL-01 fuzz campaign metadata (2026-09-05, tip `3e9a0fb`):** campaign harness/metadata landed (`scripts/ci-fuzz-campaign.sh`, `fuzz/campaign/metadata.example.json`, retained `fuzz/artifacts/minimized/`); 15s CI smoke (`scripts/ci-fuzz-smoke.sh`, `FUZZ_SECONDS=15`) unchanged; not a sustained-campaign close of KL-01; Suite HOLD unchanged.

**KL-01 protocol oracles (2026-09-05, tip `fc924e9`):** Produce/Fetch/Metadata/ListOffsets fixture tests compare decoded required fields against pinned Kafka 3.9.1 and 4.1.0 (`tests/protocol_oracles.rs`, `scripts/ci-protocol-oracles.sh`); live broker is optional (`REQUIRE_BROKER=1`) and stamps `requested=` vs `actual=`. Does **not** close full KL-01 (controlled latency reproduce and sustained campaigns remain). Does **not** lift Suite HOLD.

**KL-02 consumer close-commit honesty (2026-09-05, tip `3ec50d1`):** `ConsumerGroup::leave`/`close`/`unsubscribe` no longer auto-commit positions when `enable_auto_commit` is on; poll-interval auto-commit + explicit `commit*` unchanged. Tests: `tests/consumer_close_commit.rs`. Does **not** close full KL-02. Does **not** lift Suite HOLD.

**KL-06 credential Debug redaction (2026-09-05, tip `d813408`):** `Sasl` / `OidcConfig` / `TlsConfig` and producer/consumer/admin configs redact passwords, `client_secret`, and mTLS private key PEMs in `Debug`. Tests: `tests/credential_redact.rs`. Does **not** close full KL-06 (rotation/outage + error/span/metrics redaction remain). Does **not** lift Suite HOLD.

**KL-06 auth Error body hygiene (2026-09-05, tip `69599c1`):** OIDC token-endpoint and OAUTHBEARER authenticate failures no longer embed IdP/broker response bodies in `Error` (`oidc token endpoint HTTP {status}` / `oauthbearer: authentication failed`). Extends `tests/credential_redact.rs`. Does **not** close full KL-06 (rotation/outage + span/metrics redaction remain). Does **not** lift Suite HOLD.

**KL-06 metrics/span redaction honesty (2026-09-05, tip `e1a2647`):** Metrics snapshots are counters+latency+topic names only; optional `tracing` instruments `skip(self)` so configs with credentials are not span fields. Tests: `metrics_debug_excludes_credential_material`, `tracing_instruments_skip_self_holding_configs`. Does **not** close full KL-06 (rotation/outage recovery remains). Does **not** lift Suite HOLD.

**KL-06 OIDC outage fail-closed honesty (2026-09-05, tip `97f2fa8`):** Documents one-shot `fetch_client_credentials_token` (no `expires_in` / `session_lifetime_ms` refresh); IdP 503 → status-only `Protocol`, hang → `Timeout`. Tests: `fetch_token_rejects_http_503_fail_closed`, `fetch_token_hang_times_out_fail_closed`. Does **not** close full KL-06 (no bounded retry/refresh/rotation). Does **not** lift Suite HOLD.

**KL-06 OIDC bounded transient retry (2026-09-05, tip `3940917`):** `fetch_client_credentials_token` retries HTTP 5xx/I/O/timeout up to 3 attempts with short backoff inside `request_timeout`; HTTP 4xx still fails immediately. Tests cover 503→success, persistent 503 exhaust, and no-401-retry. Does **not** close mid-connection refresh/rotation or outage soak. Does **not** lift Suite HOLD.

**KL-06 OIDC expires_in + session_lifetime record (2026-09-05, tip `1bfae30`):** Parses IdP `expires_in` into `OidcAccessToken::expires_at`; records broker `session_lifetime_ms` / OIDC expiry on `BrokerConn` after authenticate. `token_needs_refresh` helper with skew. Does **not** close mid-connection `SaslAuthenticate` reauth / rotation soak. Does **not** lift Suite HOLD.

**KL-06 auth-lifetime reconnect (2026-09-05, tip `5517bb0`):** `BrokerConn::should_reconnect` recycles sockets when recorded SASL/OIDC lifetime is within skew (producer/consumer/admin/group/share). Reconnect re-runs full SASL; live-socket `SaslAuthenticate` rotation still open. Test: `auth_lifetimes_need_refresh_respects_skew_and_none`. Does **not** lift Suite HOLD.

**KL-02 buffer ownership + mock overload soak (2026-09-05, tip `8bd64d3`):** saturating `try_send` keeps `metrics().bytes_buffered ≤ buffer_memory` and drains to 0 after flush/close; guide documents key+value queued-until-ack model. Tests: `tests/buffer_ownership.rs`. Does **not** close full KL-02 (no 2×/24h RSS or full encode/socket/task ownership). Does **not** lift Suite HOLD.

**KL-08 support matrix honesty (2026-09-05, tip `52d2dd7`):** `docs/support.md` records CI-backed brokers (3.9.1/4.1.0), MSRV 1.85, Linux/x86_64, default pure-Rust features, and explicit non-promises; linked from RELEASE/ADOPTION/api-stability. Does **not** close full KL-08 (adopter 24h/7d + promotion/rollback remain). Does **not** lift Suite HOLD.

**KL-08 adopter exercise template (2026-09-05, tip `622dfa2`):** `docs/adopter-exercise.md` is the blank 24h/7d record format (`UNFILLED — not evidence`); linked from support.md + ADOPTION.md. Does **not** close full KL-08 (no filled adopter records, no traffic-shadow promotion/rollback proof). Does **not** lift Suite HOLD.

This file tracks holes and unsigned samples. It does not lift Suite HOLD.
Integrity harnesses (`scripts/lab-a-integrity.sh`, `lab-a-produce.sh`,
`lab-a-fetch.sh`) and the relative latency gate are **unsigned** evidence only.
Numbers and reproduce steps: [benchmark.md](benchmark.md).

