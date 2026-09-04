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

echo "== 3) Merge/tag readiness (no token required) =="
bash scripts/check-merge-ready.sh || true
echo

echo "== 4) Publish path (after token; Verifiable already green on main) =="
echo "Tracking issue: https://github.com/mingley/partitionline/issues/86"
echo
echo "As of 2026-09-04: main CI is green through Kafka 3.9.1 + 4.1.0 broker-smoke"
echo "and latency-gate (soft-skip kip848 on 3.9). PRE_PUBLISH bars: only Installable"
echo "blocked. Tip matches main. Remaining owner action is CARGO_REGISTRY_TOKEN."
echo
echo "Fastest once CARGO_REGISTRY_TOKEN is in this environment (bypasses starved Actions):"
echo "  bash scripts/owner-finish-installable.sh"
echo "  # FF-merges civilization → main (no-op if already aligned), cargo publish,"
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
echo "  2. bash scripts/owner-cancel-stuck-runs.sh"
echo "  3. bash scripts/owner-dispatch-first-publish.sh"
echo "     # or: Actions → First publish → confirm=publish"
echo "  Prefer owner-finish-installable.sh when the token is already in-env."
echo
echo "Until crates.io lands, adopters pin git tag v0.1.0-rc.6 (not floating main)."
echo

echo "== 5) Day-1 after crates.io shows partitionline 0.1.0 =="
echo "  bash scripts/day1-after-publish.sh"
echo "  bash scripts/check-installable.sh"
echo "  # day1 flips README via post-publish-readme.sh (DRY_RUN=1 preflight in publish-ready)"
echo "  # then: crates.io → Settings → Trusted Publishing → GitHub"
echo "  #        owner=mingley repo=partitionline workflow=release.yml"
echo "  #        (later tags need no long-lived Actions secret)"
echo
echo "Installable is proven only when crates.io returns partitionline 0.1.0"
echo "and check-installable.sh exits 0."
