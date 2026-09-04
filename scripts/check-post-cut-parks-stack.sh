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

# Tip must be an ancestor of every park. Merge-clean stacks can still hide
# "parks lagged tip" drift after tip docs/scripts commits; refresh parks onto
# tip before Installable so post-cut land does not surprise.
PARKED_BRANCHES="${PARKED_BRANCHES:-dev/verifiable-auth-integrity-fuzz-b686 dev/scram-crypto-bumps-b686 dev/lz4-flex-bump-b686 dev/actions-checkout-bump-b686}"
git fetch origin "$tip_br" >/dev/null 2>&1 || true
tip_ref="origin/${tip_br}"
if ! git rev-parse "$tip_ref" >/dev/null 2>&1; then
  tip_ref="$tip_br"
fi
tip_sha="$(git rev-parse "$tip_ref")"
ancestor_fail=0
for parked in ${PARKED_BRANCHES}; do
  git fetch origin "$parked" >/dev/null 2>&1 || true
  if ! git rev-parse "origin/${parked}" >/dev/null 2>&1; then
    echo "check-post-cut-parks-stack: FAIL — missing origin/${parked}" >&2
    ancestor_fail=1
    continue
  fi
  if ! git merge-base --is-ancestor "$tip_sha" "origin/${parked}"; then
    echo "check-post-cut-parks-stack: FAIL — tip ${tip_sha:0:7} is not an ancestor of ${parked}" >&2
    echo "  Refresh: checkout ${parked}, merge ${tip_br}, push — then re-run." >&2
    ancestor_fail=1
  fi
done
if [[ "$ancestor_fail" != "0" ]]; then
  exit 1
fi
echo "check-post-cut-parks-stack: tip ${tip_sha:0:7} is ancestor of all parks"

# Parks must form a chain tip⊆park1⊆park2⊆… so parallel tip refreshes cannot
# silently fork CHANGELOG histories that only conflict when stacked at land time.
# Refresh order: merge tip→Verifiable, then Verifiable→SCRAM→lz4→checkout.
chain_fail=0
prev=""
for parked in ${PARKED_BRANCHES}; do
  if [[ -n "$prev" ]]; then
    if ! git merge-base --is-ancestor "origin/${prev}" "origin/${parked}"; then
      echo "check-post-cut-parks-stack: FAIL — ${prev} is not an ancestor of ${parked}" >&2
      echo "  Restore chain: checkout ${parked}, merge ${prev}, push — then re-run." >&2
      chain_fail=1
    fi
  fi
  prev="$parked"
done
if [[ "$chain_fail" != "0" ]]; then
  exit 1
fi
echo "check-post-cut-parks-stack: parks form tip⊆… chain (land order)"

# Default: prove stacked post-cut tree is not just merge-clean but test-green.
RUN_STACK_TESTS="${RUN_STACK_TESTS:-1}"
REQUIRE_PARKS=1 ALLOW_BEFORE_INSTALLABLE=1 TARGET_BRANCH="$tip_br" DRY_RUN=1 \
  RUN_STACK_TESTS="$RUN_STACK_TESTS" \
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
