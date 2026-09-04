#!/usr/bin/env bash
# Refresh parked post-cut branches onto tip in land order (chain-safe).
#
# Parallel "merge tip into each park" forks CHANGELOG histories and breaks
# tip⊆park1⊆park2⊆… even when tip-is-ancestor still holds. Always refresh as:
#   tip → Verifiable → SCRAM → lz4 → checkout
#
# Usage:
#   bash scripts/refresh-post-cut-parks.sh
#   DRY_RUN=1 bash scripts/refresh-post-cut-parks.sh   # print merges only
#   PUSH=0 VERIFY=0 bash scripts/refresh-post-cut-parks.sh
#   CIVILIZATION_TIP=dev/civilization-plan-b686 …
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

tip_br="${CIVILIZATION_TIP:-dev/civilization-plan-b686}"
PARKED_BRANCHES="${PARKED_BRANCHES:-dev/verifiable-auth-integrity-fuzz-b686 dev/scram-crypto-bumps-b686 dev/lz4-flex-bump-b686 dev/actions-checkout-bump-b686}"
DRY_RUN="${DRY_RUN:-0}"
PUSH="${PUSH:-1}"
VERIFY="${VERIFY:-1}"
# After push, leave caller on tip (not last park).
RETURN_TIP="${RETURN_TIP:-1}"

echo "refresh-post-cut-parks: tip=${tip_br}"
echo "refresh-post-cut-parks: parks: ${PARKED_BRANCHES}"

git fetch origin "$tip_br"
if ! git rev-parse "origin/${tip_br}" >/dev/null 2>&1; then
  echo "refresh-post-cut-parks: FAIL — missing origin/${tip_br}" >&2
  exit 1
fi

prev="origin/${tip_br}"
start_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"

for parked in ${PARKED_BRANCHES}; do
  echo
  echo "== refresh ${parked} from ${prev#origin/} =="
  git fetch origin "$parked"
  if ! git rev-parse "origin/${parked}" >/dev/null 2>&1; then
    echo "refresh-post-cut-parks: FAIL — missing origin/${parked}" >&2
    exit 1
  fi
  if git merge-base --is-ancestor "$prev" "origin/${parked}"; then
    echo "refresh-post-cut-parks: already ${prev#origin/} ⊆ ${parked}"
  else
    if [[ "$DRY_RUN" == "1" ]]; then
      echo "refresh-post-cut-parks: DRY_RUN — would merge ${prev#origin/} into ${parked}"
    else
      git checkout -B "$parked" "origin/${parked}"
      git merge --no-edit "$prev"
      if [[ -n "$(git ls-files -u)" ]]; then
        echo "refresh-post-cut-parks: FAIL — conflict merging ${prev#origin/} into ${parked}" >&2
        git diff --name-only --diff-filter=U >&2 || true
        exit 1
      fi
      if [[ "$PUSH" == "1" ]]; then
        git push -u origin "HEAD:refs/heads/${parked}"
      else
        echo "refresh-post-cut-parks: PUSH=0 — local ${parked} updated only"
      fi
    fi
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    prev="origin/${parked}"
  else
    # Prefer just-pushed tip of park when available.
    git fetch origin "$parked" >/dev/null 2>&1 || true
    prev="origin/${parked}"
  fi
done

if [[ "$RETURN_TIP" == "1" && "$DRY_RUN" != "1" ]]; then
  git checkout -q "$tip_br" 2>/dev/null || git checkout -q "origin/${tip_br}"
  if [[ -n "$start_branch" && "$start_branch" != "HEAD" && "$start_branch" != "$tip_br" ]]; then
    : # tip is the intended post-refresh branch for agents
  fi
fi

if [[ "$VERIFY" == "1" ]]; then
  echo
  echo "== verify stacked parks =="
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "refresh-post-cut-parks: DRY_RUN — skip VERIFY (no pushes)"
  else
    # Ancestor/chain gates only unless RUN_STACK_TESTS=1 requested by caller.
    RUN_STACK_TESTS="${RUN_STACK_TESTS:-0}" bash scripts/check-post-cut-parks-stack.sh
  fi
fi

echo "refresh-post-cut-parks: OK"
