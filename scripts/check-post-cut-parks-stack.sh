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
echo "check-post-cut-parks-stack: OK"
