#!/usr/bin/env bash
# Post-Installable owner helper: enable crates.io Trusted Publishing (OIDC).
#
# First cut of a *new* crate still needs CARGO_REGISTRY_TOKEN with publish-new.
# After 0.1.0 exists, configure Trusted Publishing so later tags need no
# long-lived publish token. This script does not call crates.io with write
# credentials — it verifies workflow shape and prints the exact UI steps.
#
# Usage:
#   bash scripts/owner-enable-trusted-publishing.sh
#   DRY_RUN=1 bash scripts/owner-enable-trusted-publishing.sh   # allow absent crate
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY_RUN="${DRY_RUN:-0}"
name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"

echo "owner-enable-trusted-publishing: ${name} ${ver}"
echo

if [[ "$DRY_RUN" == "1" ]]; then
  echo "owner-enable-trusted-publishing: DRY_RUN=1 — workflow shape only (crate may be absent)"
  bash scripts/check-trusted-publishing-ready.sh
else
  REQUIRE_INSTALLABLE=1 bash scripts/check-trusted-publishing-ready.sh
fi

echo
echo "owner-enable-trusted-publishing: one-time crates.io UI checklist"
echo "  1. Open https://crates.io/crates/${name}/settings/trusted-publishing"
echo "  2. Add publisher:"
echo "       Source: GitHub"
echo "       Repository: mingley/partitionline"
echo "       Workflow: release.yml"
echo "       Environment: (leave empty unless you pin one)"
echo "  3. Keep Actions secret CARGO_REGISTRY_TOKEN until *one* OIDC tag publish succeeds"
echo "  4. Next final tag (vX.Y.Z): release.yml should mint a short-lived OIDC token"
echo "  5. After that success, delete/rotate the long-lived CARGO_REGISTRY_TOKEN secret"
echo "  6. Rehearse anytime: bash scripts/check-trusted-publishing-ready.sh"
echo
echo "Related post-cut steps (if not already done by owner-finish-installable):"
echo "  Preferred one-shot: bash scripts/owner-post-installable-handoff.sh"
echo "  LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh"
echo "  Or stepwise: bash scripts/owner-land-post-cut-parks.sh"
echo "              bash scripts/day1-after-publish.sh"
echo "  # close Dependabot PRs superseded by parks (#87–#92)"
echo
if [[ "$DRY_RUN" == "1" ]]; then
  echo "owner-enable-trusted-publishing: DRY_RUN complete — configure UI after crates.io ${ver} exists"
  exit 0
fi
echo "owner-enable-trusted-publishing: OK — complete the UI steps above, then prefer OIDC for later tags"
