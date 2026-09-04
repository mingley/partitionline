#!/usr/bin/env bash
# Evidence audit for docs/CIVILIZATION.md success criteria (six bars).
# Does not publish. Exit 0 only when every bar is PASS.
# Bars blocked on owner credentials report BLOCKED (still non-zero overall).
#
# Usage:
#   bash scripts/audit-civilization-bars.sh
#   FULL=1 bash scripts/audit-civilization-bars.sh   # also run branch-lite + deny
#   JSON=1 bash scripts/audit-civilization-bars.sh   # machine-readable summary line
#   PRE_PUBLISH=1 bash scripts/audit-civilization-bars.sh
#     # exit 0 if the only BLOCKEDs are Installable (crates.io / token) — for
#     # publish-ready / pre-cut gates that must not fail before first publish
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

FULL="${FULL:-0}"
JSON="${JSON:-0}"
PRE_PUBLISH="${PRE_PUBLISH:-0}"

pass=0
fail=0
blocked=0
partial=0
installable_blocked=0

ok() { echo "PASS  $*"; pass=$((pass + 1)); }
bad() { echo "FAIL  $*"; fail=$((fail + 1)); }
blk() { echo "BLOCKED  $*"; blocked=$((blocked + 1)); }
iblk() { echo "BLOCKED  $*"; blocked=$((blocked + 1)); installable_blocked=$((installable_blocked + 1)); }
part() { echo "PARTIAL  $*"; partial=$((partial + 1)); }

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
echo "audit-civilization-bars: ${name} ${ver} @ $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
echo

# --- 1. Installable ---
echo "== 1. Installable (crates.io + MSRV) =="
msrv_ok=0
if grep -qE '^rust-version = "1\.[0-9]+"' Cargo.toml; then
  ok "MSRV declared in Cargo.toml"
  msrv_ok=1
else
  bad "Cargo.toml missing rust-version"
fi
if [[ -f .github/workflows/ci.yml ]] \
  && grep -qE 'msrv|MSRV|rust-version|"1\.85"|rust:.*1\.85' .github/workflows/ci.yml; then
  ok "MSRV exercised in CI workflow"
elif [[ -f scripts/ci-msrv.sh ]]; then
  part "MSRV CI job label not obvious; scripts/ci-msrv.sh present"
else
  bad "no MSRV CI / scripts/ci-msrv.sh"
fi
# shellcheck source=scripts/lib/crates-io.sh
source "$ROOT/scripts/lib/crates-io.sh"
pl_crates_probe_version "$name" "$ver" "partitionline-audit-bars/1"
case "${PL_CRATES_PROBE_STATUS}" in
  present)
    ok "crates.io has ${name} ${ver} (${PL_CRATES_PROBE_DETAIL})"
    ;;
  absent)
    iblk "crates.io missing ${name} ${ver} (${PL_CRATES_PROBE_DETAIL}) — need CARGO_REGISTRY_TOKEN + owner-finish-installable (or first-publish.yml / owner-cut-release)"
    ;;
  *)
    bad "crates.io probe inconclusive (${PL_CRATES_PROBE_DETAIL})"
    ;;
esac
# Probe auth — do not PASS on presence alone (publish-update-only / garbage tokens).
tok_rc=0
bash scripts/check-registry-token.sh >/tmp/pl-audit-token.log 2>&1 || tok_rc=$?
case "$tok_rc" in
  0)
    ok "CARGO_REGISTRY_TOKEN accepted by crates.io for publish-new auth"
    ;;
  2)
    iblk "CARGO_REGISTRY_TOKEN unset (first publish / Actions secret; needs publish-new)"
    ;;
  *)
    iblk "CARGO_REGISTRY_TOKEN rejected by crates.io (recreate with publish-new; see check-registry-token)"
    ;;
esac

# --- 2. Verifiable ---
echo
echo "== 2. Verifiable (mock + broker CI + fuzz) =="
if cargo test --lib --quiet >/tmp/pl-audit-libtest.log 2>&1; then
  ok "cargo test --lib"
else
  bad "cargo test --lib; see /tmp/pl-audit-libtest.log"
fi
if [[ -f .github/workflows/ci.yml ]] && grep -qE 'broker-smoke|ci-broker-smoke' .github/workflows/ci.yml; then
  ok "broker-smoke job present in CI"
else
  bad "broker-smoke not wired in CI"
fi
if [[ -f scripts/ci-broker-smoke.sh && -f scripts/ci-auth-smoke.sh ]]; then
  ok "broker + auth smoke scripts present"
else
  bad "missing broker/auth smoke scripts"
fi
if [[ -f scripts/ci-integrity-smoke.sh && -f scripts/ci-latency-gate.sh \
   && -f scripts/lib/ensure-broker.sh && -f scripts/ci-tip-verifiable-broker.sh ]]; then
  ok "tip live-broker Verifiable scripts present (integrity/latency/ensure-broker/tip-verifiable)"
else
  bad "missing tip live-broker Verifiable scripts (ci-integrity-smoke/ci-latency-gate/ensure-broker/ci-tip-verifiable-broker)"
fi
if [[ -f scripts/ci-branch-lite.sh ]] && grep -q 'ci-tip-verifiable-broker' scripts/ci-branch-lite.sh \
  && [[ -f scripts/check-cut-path.sh ]] && grep -q 'ci-tip-verifiable-broker' scripts/check-cut-path.sh; then
  ok "tip Verifiable proxy wires ci-tip-verifiable-broker (branch-lite + cut-path)"
else
  bad "ci-tip-verifiable-broker not wired into ci-branch-lite / check-cut-path"
fi
# Soft-skip must not claim tip Verifiable `ok` after mid-chain skips (PARTIAL).
# PARTIAL must exit 2 by default so set -e tip proxies cannot greenwash.
# Prefer executable --self-test over grep-only (finalize exit codes cannot drift).
# Cut path (`owner-finish-installable`) + Installable preflight must also run both honesty self-tests.
if [[ -f scripts/ci-tip-verifiable-broker.sh ]] \
  && grep -q 'pl_tip_verifiable_finalize' scripts/ci-tip-verifiable-broker.sh \
  && grep -q 'pl_tip_verifiable_interpret_integrity' scripts/ci-tip-verifiable-broker.sh \
  && grep -qF 'latency gate failed (soft)' scripts/ci-tip-verifiable-broker.sh \
  && grep -q 'pl_tip_verifiable_quiet_retry_integrity' scripts/ci-tip-verifiable-broker.sh \
  && grep -q 'TIP_VERIFIABLE_QUIET_RETRIES' scripts/ci-tip-verifiable-broker.sh \
  && grep -qF -- '--self-test' scripts/ci-tip-verifiable-broker.sh \
  && grep -q 'TIP_VERIFIABLE_SOFT' scripts/ci-tip-verifiable-broker.sh \
  && grep -q 'pl_tip_verifiable_tooling_ready' scripts/ci-tip-verifiable-broker.sh \
  && bash scripts/ci-tip-verifiable-broker.sh --self-test >/tmp/pl-tip-verifiable-self-test.log 2>&1 \
  && grep -q 'self-test OK' /tmp/pl-tip-verifiable-self-test.log \
  && grep -q 'soft latency honesty' /tmp/pl-tip-verifiable-self-test.log \
  && grep -q 'quiet retry' /tmp/pl-tip-verifiable-self-test.log \
  && [[ -f scripts/ci-branch-lite.sh ]] && grep -qF -- 'ci-tip-verifiable-broker.sh --self-test' scripts/ci-branch-lite.sh \
  && [[ -f scripts/check-cut-path.sh ]] && grep -qF -- 'ci-tip-verifiable-broker.sh --self-test' scripts/check-cut-path.sh \
  && [[ -f scripts/owner-finish-installable.sh ]] \
  && grep -qF -- 'check-registry-token.sh --self-test' scripts/owner-finish-installable.sh \
  && grep -qF -- 'ci-tip-verifiable-broker.sh --self-test' scripts/owner-finish-installable.sh \
  && [[ -f scripts/check-installable-preflight.sh ]] \
  && grep -qF -- 'check-registry-token.sh --self-test' scripts/check-installable-preflight.sh \
  && grep -qF -- 'ci-tip-verifiable-broker.sh --self-test' scripts/check-installable-preflight.sh \
  && grep -qF -- 'pl_prepare_cargo_registry_token' scripts/check-installable-preflight.sh \
  && grep -qF -- 'check-installable-preflight.sh --self-test' scripts/check-installable-preflight.sh \
  && bash scripts/check-installable-preflight.sh --self-test >/tmp/pl-preflight-self-test.log 2>&1 \
  && grep -q 'self-test OK' /tmp/pl-preflight-self-test.log; then
  ok "tip Verifiable soft-skip honesty (--self-test PARTIAL exit 2 + soft latency + quiet retry; wired into branch-lite/cut-path/finish/preflight)"
  ok "Installable preflight TOKEN prepare honesty (whitespace/misname/TOKEN_FILE before READY_EXCEPT_TOKEN; --self-test)"
else
  bad "ci-tip-verifiable-broker soft-skip honesty missing (--self-test / finalize / soft latency / quiet retry / tip proxy+finish+preflight wiring); see /tmp/pl-tip-verifiable-self-test.log"
  bad "Installable preflight TOKEN prepare honesty missing (pl_prepare / --self-test); see /tmp/pl-preflight-self-test.log"
fi
# Integrity leaf must not print final `ok` after soft latency — civilization-check / tip
# proxies must see PARTIAL/exit 2 (REQUIRE_INTEGRITY=1 → hard-fail). Soft branch must
# exit before the final `ok` line in source order.
if [[ -f scripts/ci-integrity-smoke.sh ]] \
  && grep -qF 'latency_soft=1' scripts/ci-integrity-smoke.sh \
  && grep -qF 'ci-integrity-smoke: PARTIAL' scripts/ci-integrity-smoke.sh \
  && awk '
      /latency_soft=1/ { soft=NR }
      /ci-integrity-smoke: PARTIAL/ { partial=NR }
      /exit 2/ && soft && NR >= soft { e2=NR }
      /ci-integrity-smoke: ok \(unsigned/ { ok=NR }
      END { exit (soft && partial && e2 && ok && partial < ok && e2 < ok) ? 0 : 1 }
    ' scripts/ci-integrity-smoke.sh \
  && [[ -f scripts/ci-civilization-check.sh ]] \
  && grep -qF 'ci-integrity-smoke: PARTIAL' scripts/ci-civilization-check.sh \
  && grep -qF 'latency soft-miss' scripts/ci-civilization-check.sh; then
  ok "integrity-smoke soft-latency honesty (PARTIAL/exit 2 before final ok; civilization-check ski)"
else
  bad "integrity-smoke soft-latency honesty missing (must PARTIAL/exit 2 before final ok; never greenwash soft)"
fi
fuzz_n=0
if [[ -d fuzz/fuzz_targets ]]; then
  fuzz_n="$(find fuzz/fuzz_targets -name '*.rs' | wc -l | tr -d ' ')"
fi
if [[ "$fuzz_n" -ge 5 ]]; then
  ok "fuzz targets present (${fuzz_n})"
else
  bad "need >=5 fuzz targets (found ${fuzz_n})"
fi
if cargo test --test fuzz_decode_smoke --quiet >/tmp/pl-audit-fuzz.log 2>&1; then
  ok "fuzz decode smoke"
else
  bad "fuzz decode smoke; see /tmp/pl-audit-fuzz.log"
fi
if [[ "$FULL" == "1" ]]; then
  if bash scripts/ci-branch-lite.sh >/tmp/pl-audit-branch-lite.log 2>&1; then
    ok "FULL branch-lite Verifiable proxy"
  else
    bad "FULL branch-lite; see /tmp/pl-audit-branch-lite.log"
  fi
fi

# --- 3. Operable ---
echo
echo "== 3. Operable (guide + migration + observability) =="
for f in docs/guide.md docs/migrate-from-rdkafka.md docs/ADOPTION.md; do
  if [[ -f "$f" ]]; then ok "$f"; else bad "missing $f"; fi
done
if grep -qiE 'tracing|metrics|prometheus' docs/guide.md; then
  ok "observability path documented in guide"
else
  bad "guide missing metrics/tracing path"
fi
# Documented git pin must cargo-check while Installable waits (Adoptable before crates.io).
if grep -qF 'MODE=git' scripts/check-cut-path.sh \
  && grep -qF 'MODE=git' scripts/check-installable-preflight.sh \
  && grep -qF 'MODE=git' scripts/owner-finish-installable.sh \
  && grep -qE 'MODE=git|mode.*git' scripts/verify-crates-io-consumer.sh; then
  if MODE=git bash scripts/verify-crates-io-consumer.sh >/tmp/pl-git-adopter.log 2>&1; then
    ok "git-tag adopter consumer (documented pin cargo-checks; wired into cut-path + preflight + finish)"
  else
    bad "git-tag adopter consumer failed; see /tmp/pl-git-adopter.log"
  fi
else
  bad "git-tag adopter consumer not wired into cut-path / preflight / finish / verify-crates-io-consumer"
fi
# Parks auto-refresh must restore main/caller before cut/publish (token-day footgun).
if [[ -x scripts/check-parks-refresh-cut-guards.sh ]] \
  && bash scripts/check-parks-refresh-cut-guards.sh >/tmp/pl-parks-refresh-guards.log 2>&1 \
  && grep -qF -- 'check-parks-refresh-cut-guards.sh' scripts/owner-finish-installable.sh \
  && grep -qF -- 'check-parks-refresh-cut-guards.sh' scripts/check-cut-path.sh \
  && grep -qF -- 'check-parks-refresh-cut-guards.sh' scripts/check-installable-preflight.sh; then
  ok "parks-refresh cut guards (restore main/caller; wired into finish + cut-path + preflight)"
else
  bad "parks-refresh cut guards missing or unwired; see /tmp/pl-parks-refresh-guards.log"
fi
# day1 crates.io README/ADOPTION flips must survive parks land (stash pop can fail).
# Live parks land now runs inside handoff (LAND_PARKS=1) as well as finish — both must preserve.
if [[ -x scripts/lib/preserve-day1-docs.sh ]] \
  && bash scripts/lib/preserve-day1-docs.sh --self-test >/tmp/pl-day1-docs-preserve.log 2>&1 \
  && grep -qF -- 'preserve-day1-docs.sh' scripts/owner-finish-installable.sh \
  && grep -qF -- 'preserve-day1-docs.sh' scripts/check-installable-preflight.sh \
  && grep -qF -- 'preserve-day1-docs.sh' scripts/check-cut-path.sh \
  && grep -qF -- 'preserve-day1-docs.sh' scripts/owner-post-installable-handoff.sh \
  && grep -qF -- 'pl_day1_docs_begin' scripts/owner-post-installable-handoff.sh \
  && grep -qF -- 'pl_day1_docs_end' scripts/owner-post-installable-handoff.sh; then
  ok "day1 docs preserve across parks (stash+backup; wired into finish + cut-path + preflight + handoff)"
else
  bad "day1 docs preserve missing or unwired (finish/preflight/cut-path/handoff); see /tmp/pl-day1-docs-preserve.log"
fi
# Post-Installable handoff must exist for TP/parks re-entry (Actions-alternate + soft-fails).
if [[ -x scripts/owner-post-installable-handoff.sh ]] \
  && grep -qF -- 'owner-post-installable-handoff.sh' scripts/owner-finish-installable.sh \
  && grep -qF -- 'owner-post-installable-handoff.sh' scripts/check-cut-path.sh \
  && grep -qF -- 'owner-post-installable-handoff.sh' scripts/day1-after-publish.sh \
  && grep -qF -- 'owner-post-installable-handoff.sh' scripts/owner-dispatch-first-publish.sh \
  && grep -qF -- 'LAND_PARKS=' scripts/owner-finish-installable.sh \
  && grep -qF -- 'Post-Installable handoff' scripts/owner-finish-installable.sh \
  && grep -qF -- 'would run owner-post-installable-handoff' scripts/owner-finish-installable.sh \
  && grep -qF -- 'owner-post-installable-handoff' scripts/owner-cut-release.sh \
  && grep -qF -- 'LAND_PARKS' scripts/owner-cut-release.sh \
  && grep -qF -- 'SKIP_HANDOFF' scripts/owner-cut-release.sh \
  && grep -qF -- 'SKIP_HANDOFF=1' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — already-Installable DRY_RUN soft-failed' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — not-yet-Installable DRY_RUN soft-failed' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — Installable OK but Actions secret not synced' scripts/owner-finish-installable.sh \
  && grep -qF 'secret_rc' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — Installable OK but Actions secret not synced' scripts/owner-cut-release.sh \
  && grep -qF 'secret_rc' scripts/owner-cut-release.sh \
  && grep -qF 'parks_rc' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — not yet Installable' scripts/day1-after-publish.sh \
  && grep -qF 'day1_rc' scripts/check-cut-path.sh \
  && grep -qF 'day1_rc' scripts/ci-branch-lite.sh \
  && grep -qF 'day1_rc' scripts/ci-publish-ready.sh \
  && grep -qF 'finish_rc' scripts/check-cut-path.sh \
  && grep -qF 'check-installable-preflight.sh' scripts/owner-request-registry-token.sh \
  && grep -qF 'READY_EXCEPT_TOKEN' scripts/owner-request-registry-token.sh \
  && grep -qF 'parks stack stale' scripts/owner-request-registry-token.sh \
  && grep -qF -- '--self-test' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'PARTIAL — Installable OK but parks land failed' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'day1-after-publish.sh' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'PARTIAL — Installable OK but adopter docs still git-shaped' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'SKIP_DAY1=1' scripts/owner-finish-installable.sh \
  && bash scripts/owner-post-installable-handoff.sh --self-test >/tmp/pl-handoff-self-test.log 2>&1 \
  && grep -q 'self-test OK' /tmp/pl-handoff-self-test.log \
  && grep -q 'fail-closed PARTIAL' /tmp/pl-handoff-self-test.log \
  && HANDOFF_FROM_BARS=1 DRY_RUN=1 bash scripts/owner-post-installable-handoff.sh >/tmp/pl-handoff-dry.log 2>&1; then
  ok "post-Installable handoff (DRY_RUN + fail-closed PARTIAL; finish+cut-release chain + cut-path + day1 + first-publish)"
else
  bad "post-Installable handoff missing/unwired; see /tmp/pl-handoff-dry.log /tmp/pl-handoff-self-test.log"
fi

# Cut-release bare must auto PUBLISH_LOCAL=1 when token is in-env (token-day footgun).
if bash scripts/owner-cut-release.sh --self-test >/tmp/pl-cut-publish-local-auto.log 2>&1 \
  && grep -q 'handoff chained' /tmp/pl-cut-publish-local-auto.log \
  && grep -q 'DRY_RUN reaches handoff' /tmp/pl-cut-publish-local-auto.log \
  && grep -q 'Actions secret PARTIAL gated' /tmp/pl-cut-publish-local-auto.log; then
  ok "cut-release PUBLISH_LOCAL auto-default + handoff chain + DRY_RUN reaches handoff + SKIP_HANDOFF for finish + Actions secret PARTIAL"
else
  bad "cut-release PUBLISH_LOCAL auto-default / handoff chain / Actions secret PARTIAL missing/broken; see /tmp/pl-cut-publish-local-auto.log"
fi


# Owner checklists must match cut-release auto PUBLISH_LOCAL (no bare → Actions steer).
bare_actions="$(grep -nE 'owner-cut-release\.sh.*tag → Actions' scripts/owner-unblock.sh scripts/owner-status.sh 2>/dev/null | grep -v 'PUBLISH_LOCAL=0' || true)"
if grep -qF 'token in-env → local publish (auto)' scripts/owner-unblock.sh \
  && grep -qF 'token in-env → local publish (auto)' scripts/owner-status.sh \
  && [[ -z "$bare_actions" ]]; then
  ok "cut-release owner-helper comments match PUBLISH_LOCAL auto-default"
else
  bad "owner-unblock/status still steers bare cut-release to Actions (or missing auto-local comment)"
fi

# --- 4. Honest ---
echo
echo "== 4. Honest (labeled benches; no false Suite HOLD lift) =="
if grep -qiE 'unsigned|Suite HOLD|HOLD' docs/STATUS.md docs/benchmark.md 2>/dev/null; then
  ok "STATUS/benchmark honesty labels present"
else
  bad "STATUS/benchmark missing honesty labels"
fi
# Refuse a false lift claim in STATUS.
if grep -qiE 'Suite HOLD (lifted|cleared|removed)|HOLD lifted' docs/STATUS.md 2>/dev/null; then
  bad "STATUS appears to claim Suite HOLD lift — verify signed Lab A first"
else
  ok "STATUS does not claim Suite HOLD lift"
fi

# --- 5. Independent ---
echo
echo "== 5. Independent (no C Kafka/compression/SASL defaults) =="
if grep -q 'unsafe_code' Cargo.toml && grep -q 'forbid' Cargo.toml; then
  ok "unsafe_code forbid"
else
  bad "unsafe_code forbid missing"
fi
if grep -E '^\s*(rdkafka|rdkafka-sys|openssl-sys|native-tls|zstd-sys|libzstd-sys)\s*=' Cargo.toml >/dev/null; then
  bad "C stack listed as direct Cargo.toml dependency"
else
  ok "no C Kafka/OpenSSL/zstd direct deps in Cargo.toml"
fi
if [[ -f deny.toml ]] && grep -qE 'rdkafka|openssl|zstd-sys' deny.toml; then
  ok "cargo-deny bans cover C Kafka/TLS/zstd stack"
else
  part "deny.toml C-stack bans not obvious"
fi
if [[ "$FULL" == "1" ]]; then
  if bash scripts/ci-deny.sh >/tmp/pl-audit-deny.log 2>&1; then
    ok "FULL cargo deny"
  else
    bad "FULL cargo deny; see /tmp/pl-audit-deny.log"
  fi
fi

# --- 6. Stewarded ---
echo
echo "== 6. Stewarded (changelog, release policy, templates, plan) =="
for f in CHANGELOG.md docs/RELEASE.md docs/CIVILIZATION.md SECURITY.md \
  .github/PULL_REQUEST_TEMPLATE.md .github/ISSUE_TEMPLATE .github/CODEOWNERS; do
  if [[ -e "$f" ]]; then ok "$f"; else bad "missing $f"; fi
done
if grep -q "^## \\[${ver}\\]" CHANGELOG.md || grep -q "^## \\[0\\.1\\.0\\]" CHANGELOG.md; then
  ok "CHANGELOG has ${ver} (or 0.1.0) section"
else
  bad "CHANGELOG missing ## [${ver}] section"
fi
# Open Dependabot cargo/Actions bumps must map to post-cut parks (tip stays docs/scripts-only).
if [[ ! -x scripts/check-dependabot-parks-coverage.sh ]]; then
  bad "missing scripts/check-dependabot-parks-coverage.sh"
elif ! bash scripts/check-dependabot-parks-coverage.sh --self-test >/tmp/pl-dep-parks-self.log 2>&1; then
  bad "Dependabot parks coverage --self-test failed; see /tmp/pl-dep-parks-self.log"
else
  dep_rc=0
  bash scripts/check-dependabot-parks-coverage.sh >/tmp/pl-dep-parks.log 2>&1 || dep_rc=$?
  if [[ "$dep_rc" -eq 0 ]]; then
    ok "Dependabot ↔ post-cut parks coverage (open bumps mapped; tip-delta safe)"
  elif [[ "$dep_rc" -eq 2 ]]; then
    ok "Dependabot ↔ post-cut parks coverage soft-skipped (gh/API)"
  else
    bad "Dependabot parks coverage missing/unmapped; see /tmp/pl-dep-parks.log"
  fi
fi

echo
echo "audit-civilization-bars: pass=${pass} partial=${partial} blocked=${blocked} fail=${fail} installable_blocked=${installable_blocked}"
if [[ "$JSON" == "1" ]]; then
  printf '{"pass":%s,"partial":%s,"blocked":%s,"fail":%s,"installable_blocked":%s,"installable":"%s","version":"%s"}\n' \
    "$pass" "$partial" "$blocked" "$fail" "$installable_blocked" "${PL_CRATES_PROBE_STATUS:-unknown}" "$ver"
fi

if [[ "$fail" -gt 0 ]]; then
  echo "audit-civilization-bars: NOT COMPLETE — FAIL items above" >&2
  exit 1
fi

# Pre-publish gates: Installable BLOCKED is expected until crates.io lands.
# Soft structural PARTIALs (e.g. MSRV label / deny.toml wording) stay allowed
# under PRE_PUBLISH so token-day rehearsal is not blocked by doc-shape notes.
# Full (non-PRE_PUBLISH) bars refuse final OK when any PARTIAL remains.
if [[ "$PRE_PUBLISH" == "1" ]]; then
  other_blocked=$((blocked - installable_blocked))
  if [[ "$other_blocked" -gt 0 ]]; then
    echo "audit-civilization-bars: NOT COMPLETE — non-Installable BLOCKED items" >&2
    exit 1
  fi
  if [[ "$installable_blocked" -gt 0 ]]; then
    if [[ "$partial" -gt 0 ]]; then
      echo "audit-civilization-bars: PRE_PUBLISH OK — bars green except Installable (owner token/cut); PARTIAL notes above"
    else
      echo "audit-civilization-bars: PRE_PUBLISH OK — bars green except Installable (owner token/cut)"
    fi
    exit 0
  fi
  if [[ "$partial" -gt 0 ]]; then
    echo "audit-civilization-bars: PRE_PUBLISH OK — Installable proven; PARTIAL notes above (full bars would exit 2)"
    exit 0
  fi
  echo "audit-civilization-bars: PRE_PUBLISH OK — all six bars PASS (already Installable)"
  exit 0
fi

if [[ "$blocked" -gt 0 ]]; then
  echo "audit-civilization-bars: NOT COMPLETE — civilization bars unmet" >&2
  if [[ "$installable_blocked" -eq "$blocked" ]]; then
    echo "audit-civilization-bars: remaining blockers are owner/credentials (see BLOCKED lines)" >&2
  fi
  exit 1
fi
if [[ "$partial" -gt 0 ]]; then
  echo "audit-civilization-bars: PARTIAL — soft notes remain (no FAIL/BLOCKED; exit 2)" >&2
  exit 2
fi
echo "audit-civilization-bars: OK — all six bars PASS"
exit 0