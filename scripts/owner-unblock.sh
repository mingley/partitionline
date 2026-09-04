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

echo "== 2) Stale queued Actions (dry-run) =="
DRY_RUN=1 bash scripts/owner-cancel-stuck-runs.sh || true
echo
echo "If targets listed above: cancel them as repo owner, then re-run this script."
echo "  bash scripts/owner-cancel-stuck-runs.sh"
echo

echo "== 3) Merge/tag readiness (no token required) =="
bash scripts/check-merge-ready.sh || true
echo

echo "== 4) Publish path (after token + clean Actions) =="
echo "Tracking issue: https://github.com/mingley/partitionline/issues/86"
echo "Preferred (docs/RELEASE.md): merge civilization → main, then tag final only:"
echo "  # open/merge PR: dev/civilization-plan-b686 → main"
echo "  git fetch origin main && git checkout main && git pull origin main"
echo "  git tag -a v0.1.0 -m 'partitionline 0.1.0'"
echo "  git push origin v0.1.0"
echo "  # release.yml: OIDC trusted publishing if configured, else CARGO_REGISTRY_TOKEN"
echo
echo "If publishing locally instead (token in env):"
echo "  cargo publish --dry-run && cargo publish"
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
