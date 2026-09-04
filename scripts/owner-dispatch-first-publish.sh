#!/usr/bin/env bash
# Dispatch .github/workflows/first-publish.yml when CARGO_REGISTRY_TOKEN is an
# Actions secret but not in this shell. Prefer owner-finish-installable.sh when
# the token is already exported (no Actions queue wait).
#
# GitHub only lists workflow_dispatch workflows from the *default* branch.
# Options to make first-publish.yml visible:
#   A) Merge thin PR that adds only first-publish.yml onto main, or
#   B) FF/merge full civilization tip → main
# Then dispatch packages REF (default: main once tip is merged).
#
# Typical sequence:
#   1. bash scripts/owner-cancel-stuck-runs.sh   # owner machine
#   2. Land first-publish.yml on main (thin PR or full tip FF)
#   3. bash scripts/owner-dispatch-first-publish.sh
#   4. bash scripts/check-installable.sh
#   5. bash scripts/day1-after-publish.sh
#
# Usage:
#   bash scripts/owner-dispatch-first-publish.sh
#   REF=main bash scripts/owner-dispatch-first-publish.sh
#   DRY_RUN=1 bash scripts/owner-dispatch-first-publish.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY_RUN="${DRY_RUN:-0}"
REF="${REF:-main}"
CONFIRM="${CONFIRM:-publish}"
WORKFLOW="${WORKFLOW:-first-publish.yml}"

if ! command -v gh >/dev/null 2>&1; then
  echo "owner-dispatch-first-publish: gh CLI required" >&2
  exit 1
fi

if [[ "$CONFIRM" != "publish" ]]; then
  echo "owner-dispatch-first-publish: CONFIRM must be exactly 'publish' (got '$CONFIRM')" >&2
  exit 1
fi

echo "owner-dispatch-first-publish: workflow=${WORKFLOW} ref=${REF} confirm=${CONFIRM}"

if ! gh workflow view "$WORKFLOW" >/tmp/pl-first-publish-wf.txt 2>/tmp/pl-first-publish-wf.err; then
  echo "owner-dispatch-first-publish: FAIL — cannot see ${WORKFLOW}" >&2
  cat /tmp/pl-first-publish-wf.err >&2 || true
  echo >&2
  echo "workflow_dispatch entries are only listed from the default branch." >&2
  echo "Merge/FF civilization tip → main first (so first-publish.yml exists on main)," >&2
  echo "or use in-env publish instead:" >&2
  echo "  bash scripts/owner-finish-installable.sh" >&2
  exit 1
fi

echo "owner-dispatch-first-publish: workflow is visible:"
sed -n '1,8p' /tmp/pl-first-publish-wf.txt || true

if [[ "$DRY_RUN" == "1" ]]; then
  echo "owner-dispatch-first-publish: DRY_RUN=1 — would run:"
  echo "  gh workflow run ${WORKFLOW} -f confirm=publish -f ref=${REF}"
  exit 0
fi

gh workflow run "$WORKFLOW" -f confirm=publish -f "ref=${REF}"
echo "owner-dispatch-first-publish: dispatched. Watch:"
echo "  gh run list --workflow=${WORKFLOW} --limit 5"
echo "After green: bash scripts/check-installable.sh && bash scripts/day1-after-publish.sh"
