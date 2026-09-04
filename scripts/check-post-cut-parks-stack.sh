#!/usr/bin/env bash
# Tip Verifiable gate: prove parked post-cut branches still stack-clean onto tip.
#
# Catches CHANGELOG/docs conflicts that only appear after tip→park1→park2
# (per-park merge-tree vs bare tip/main hides them). Failures mean rebase parks
# before the Installable cut; do not tip→main thrash for docs while waiting.
#
# Usage:
#   bash scripts/check-post-cut-parks-stack.sh
#   CIVILIZATION_TIP=dev/civilization-plan-b686 bash scripts/check-post-cut-parks-stack.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

tip_br="${CIVILIZATION_TIP:-dev/civilization-plan-b686}"

echo "check-post-cut-parks-stack: rehearsing parks onto ${tip_br}"
REQUIRE_PARKS=1 ALLOW_BEFORE_INSTALLABLE=1 TARGET_BRANCH="$tip_br" DRY_RUN=1 \
  bash scripts/owner-land-post-cut-parks.sh

# Park honesty: Actions first-publish alternate must document publish-new.
# Tip keeps first-publish.yml docs/scripts-safe (workflow edits stay parked).
checkout_park="${CHECKOUT_PARK:-dev/actions-checkout-bump-b686}"
git fetch origin "$checkout_park" >/dev/null 2>&1 || true
if git rev-parse "origin/${checkout_park}" >/dev/null 2>&1; then
  if ! git show "origin/${checkout_park}:.github/workflows/first-publish.yml" \
      | grep -q 'publish-new'; then
    echo "check-post-cut-parks-stack: FAIL — ${checkout_park} first-publish.yml missing publish-new" >&2
    echo "  First cut of a NEW crate needs crates.io publish-new (+ publish-update)." >&2
    exit 1
  fi
  echo "check-post-cut-parks-stack: parked first-publish.yml documents publish-new"
fi
echo "check-post-cut-parks-stack: OK"
