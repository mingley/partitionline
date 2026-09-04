#!/usr/bin/env bash
# Probe whether post-cut park tips are ancestors of origin/main.
#
# Stack readiness (check-post-cut-parks-stack) ≠ landed. Installable can be true
# while parks remain off main — handoff / status / preflight must not soft-OK that.
#
# Exit codes:
#   0  — every park is an ancestor of origin/main
#   2  — one or more parks pending (PARTIAL)
#   1  — hard failure (e.g. missing origin/main)
#
# Usage:
#   bash scripts/check-parks-on-main.sh
#   bash scripts/check-parks-on-main.sh --self-test
#   PARKED_BRANCHES="dev/a-b686 dev/b-b686" bash scripts/check-parks-on-main.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "--self-test" ]]; then
  echo "check-parks-on-main: self-test — must probe merge-base --is-ancestor vs origin/main"
  if ! grep -qF 'merge-base --is-ancestor' "$ROOT/scripts/check-parks-on-main.sh"; then
    echo "check-parks-on-main: self-test FAIL — missing merge-base --is-ancestor probe" >&2
    exit 1
  fi
  if ! grep -qF 'PARTIAL — parks not on main' "$ROOT/scripts/check-parks-on-main.sh"; then
    echo "check-parks-on-main: self-test FAIL — missing PARTIAL string" >&2
    exit 1
  fi
  if ! grep -qF 'check-parks-on-main.sh' "$ROOT/scripts/owner-post-installable-handoff.sh"; then
    echo "check-parks-on-main: self-test FAIL — handoff must call check-parks-on-main.sh" >&2
    exit 1
  fi
  if ! grep -qF 'check-parks-on-main.sh' "$ROOT/scripts/check-installable-preflight.sh"; then
    echo "check-parks-on-main: self-test FAIL — preflight must call check-parks-on-main.sh" >&2
    exit 1
  fi
  if ! grep -qF 'check-parks-on-main.sh' "$ROOT/scripts/owner-status.sh"; then
    echo "check-parks-on-main: self-test FAIL — owner-status must call check-parks-on-main.sh" >&2
    exit 1
  fi
  echo "check-parks-on-main: self-test OK — probe + handoff/preflight/status wiring"
  exit 0
fi

PARKED_BRANCHES="${PARKED_BRANCHES:-dev/verifiable-auth-integrity-fuzz-b686 dev/scram-crypto-bumps-b686 dev/lz4-flex-bump-b686 dev/actions-checkout-bump-b686}"

git fetch origin main ${PARKED_BRANCHES} >/dev/null 2>&1 || true
if ! git rev-parse -q --verify origin/main >/dev/null; then
  echo "check-parks-on-main: FAIL — missing origin/main" >&2
  exit 1
fi

pending=()
for parked in ${PARKED_BRANCHES}; do
  if ! git rev-parse -q --verify "origin/${parked}" >/dev/null; then
    pending+=("${parked} (missing)")
    continue
  fi
  if ! git merge-base --is-ancestor "origin/${parked}" origin/main; then
    pending+=("${parked}")
  fi
done

if [[ "${#pending[@]}" -eq 0 ]]; then
  echo "check-parks-on-main: OK — parks are on origin/main"
  exit 0
fi

echo "check-parks-on-main: PARTIAL — parks not on main"
for p in "${pending[@]}"; do
  echo "  - ${p}"
done
echo "  Re-enter: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh"
echo "  Or: bash scripts/owner-land-post-cut-parks.sh"
echo "  Intentional deferral: ALLOW_PARKS_PENDING=1 …"
exit 2
