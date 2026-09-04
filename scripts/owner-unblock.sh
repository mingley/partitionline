#!/usr/bin/env bash
# One-shot owner checklist after CARGO_REGISTRY_TOKEN is set and/or Actions runners recover.
# Safe to run anytime: prints status, dry-run cancel targets, then the merge → tag → day1 path.
#
# Usage:
#   bash scripts/owner-unblock.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "========================================"
echo " partitionline owner unblock checklist"
echo "========================================"
echo

echo "== 1) Current status =="
bash scripts/owner-status.sh || true
echo

echo "== 2) Stale queued Actions (dry-run + hygiene) =="
bash scripts/check-actions-hygiene.sh || true
echo
DRY_RUN=1 bash scripts/owner-cancel-stuck-runs.sh || true
echo
echo "If targets listed above: cancel them as repo owner, then re-run this script."
echo "  bash scripts/owner-cancel-stuck-runs.sh"
echo
echo "Especially cancel zombie-rc-release runs (RC tags must not publish) and"
echo "stale tip CI queued from before tip auto-CI was disabled."
echo
echo "If hygiene WARN'd about missing label 'dependencies' (Dependabot):"
echo "  gh label create dependencies --repo mingley/partitionline \\"
echo "    --description 'Pull requests that update a dependency file' --color 0366d6"
echo "  (agents 403 on label create — owner-only; unblocks Dependabot label noise)"
echo

echo "== 3) Merge/tag readiness (no token required) =="
bash scripts/check-merge-ready.sh || true
echo
echo "== 3b) Installable preflight =="
bash scripts/check-installable-preflight.sh || true
echo

echo "== 3c) Post-cut parks stack (tip→Verifiable→SCRAM) =="
bash scripts/check-post-cut-parks-stack.sh || true
echo "  If stack FAIL (parks lagged tip / chain broken):"
echo "    bash scripts/refresh-post-cut-parks.sh"
echo

echo "== 4) Publish path (after token; Verifiable already green on main) =="
echo "Tracking issue: https://github.com/mingley/partitionline/issues/86"
echo
# Live tip/main relationship — do not hardcode "matches main" (tip often stays
# ahead on docs/scripts while Installable waits; thrash guard refuses sync).
git fetch origin main dev/civilization-plan-b686 >/dev/null 2>&1 || true
tip_sha="$(git rev-parse origin/dev/civilization-plan-b686 2>/dev/null || true)"
main_sha="$(git rev-parse origin/main 2>/dev/null || true)"
if [[ -n "$tip_sha" && -n "$main_sha" && "$tip_sha" == "$main_sha" ]]; then
  tip_main_note="Tip matches main."
elif [[ -n "$tip_sha" && -n "$main_sha" ]]; then
  ahead="$(git rev-list --count origin/main..origin/dev/civilization-plan-b686 2>/dev/null || echo '?')"
  tip_main_note="Tip ${tip_sha:0:7} is ahead of main ${main_sha:0:7} by ${ahead} commit(s) (intentional while Installable waits; do not tip→main thrash)."
else
  tip_main_note="Tip/main relationship unknown (fetch failed)."
fi
echo "As of 2026-09-04: main CI is green through Kafka 3.9.1 + 4.1.0 broker-smoke"
echo "and latency-gate (soft-skip kip848 on 3.9). PRE_PUBLISH bars: only Installable"
echo "blocked. ${tip_main_note} Remaining owner action is CARGO_REGISTRY_TOKEN."
echo "Probe: bash scripts/check-installable-preflight.sh   # expect READY_EXCEPT_TOKEN"
echo
echo "Inject token into this Cloud Agent (preferred for owner-finish-installable):"
echo "  Cursor → Cloud Agents → Environments → this env → Secrets"
# shellcheck source=scripts/lib/cursor-env-secrets-url.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/cursor-env-secrets-url.sh"
echo "  Direct: $PARTITIONLINE_CURSOR_ENV_SECRETS_URL"
echo "  Add CARGO_REGISTRY_TOKEN = crates.io token with publish-new (+ publish-update)"
echo "  Save, then restart/re-run the agent so the shell receives the secret."
echo "  Also set the same name as a GitHub Actions repository secret."
echo
echo "Fastest once CARGO_REGISTRY_TOKEN is in this environment (bypasses starved Actions):"
echo "  bash scripts/check-registry-token.sh   # must exit 0 (publish-new auth; not presence-only)"
echo "  bash scripts/check-cut-path.sh         # full cut rehearsal"
echo "  bash scripts/owner-finish-installable.sh"
echo "  # After Installable, finish chains owner-land-post-cut-parks by default:"
echo "  #   1) dev/verifiable-auth-integrity-fuzz-b686 (Actions auth+integrity + CGHeartbeat fuzz)"
echo "  #   2) dev/scram-crypto-bumps-b686 (SCRAM crypto + flate2 bumps)"
echo "  #   3) dev/lz4-flex-bump-b686 (lz4_flex 0.11 → 0.14)"
echo "  #   4) dev/actions-checkout-bump-b686 (actions/checkout → v7)"
echo "  # MERGE_PARKED_VERIFIABLE=0 / MERGE_POST_CUT_PARKS=0 skips parks land"
echo "  # FF-merges civilization → main (includes any tip-ahead docs/scripts), cargo publish,"
echo "  # day1, proves Installable. Real cuts default REQUIRE_MAIN_CI=1 — wait for"
echo "  # green main CI if a docs/scripts push is still running, or override with 0."
echo "  DRY_RUN=1 bash scripts/owner-finish-installable.sh"
echo
echo "Or stepwise (docs/RELEASE.md): merge civilization → main, then tag final only:"
echo "  # open/merge PR: dev/civilization-plan-b686 → main"
echo "  git fetch origin main && git checkout main && git pull origin main"
echo "  bash scripts/owner-cut-release.sh          # tag → Actions → day1"
echo "  PUBLISH_LOCAL=1 bash scripts/owner-cut-release.sh  # manual cargo publish"
echo "  DRY_RUN=1 bash scripts/owner-cut-release.sh"
echo
echo "If the token is Actions-only (not in this shell):"
echo "  1. Merge/FF civilization → main (first-publish.yml must exist on default branch)"
echo "     # or dispatch with REF=<tip-sha> without FF when tip is docs/scripts-only"
echo "  2. bash scripts/owner-cancel-stuck-runs.sh"
echo "  3. From an owner machine (Cloud Agents get HTTP 403 on workflow_dispatch):"
echo "       bash scripts/owner-dispatch-first-publish.sh"
echo "     # or: Actions → First publish → confirm=publish"
echo "  Prefer owner-finish-installable.sh when the token is already in-env."
echo
echo "Until crates.io lands, adopters pin git tag v0.1.0-rc.6 (not floating main)."
echo

echo "== 5) Day-1 after crates.io shows partitionline 0.1.0 =="
echo "  bash scripts/day1-after-publish.sh"
echo "  bash scripts/check-installable.sh"
echo "  # Rehearse without waiting on crates.io index:"
echo "  DRY_RUN=1 bash scripts/day1-after-publish.sh"
echo "  # day1 flips README via post-publish-readme.sh (DRY_RUN=1 preflight in publish-ready)"
echo "  # then: crates.io → Settings → Trusted Publishing → GitHub"
echo "  #        owner=mingley repo=partitionline workflow=release.yml"
echo "  #        (later tags need no long-lived Actions secret)"
echo
echo "Installable is proven only when crates.io returns partitionline 0.1.0"
echo "and check-installable.sh exits 0."
