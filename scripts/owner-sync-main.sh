#!/usr/bin/env bash
# Fast-forward origin/main to the civilization tip.
#
# Prefer this over ad-hoc `git push origin HEAD:main` from agents: main CI uses
# cancel-in-progress, so repeated tip→main syncs cancel each other and starve
# Verifiable. Sync once when ready to publish, then stop until Installable
# lands (or until a real main fix is required).
#
# Usage:
#   CONFIRM=1 bash scripts/owner-sync-main.sh
#   DRY_RUN=1 bash scripts/owner-sync-main.sh
#   TIP=dev/civilization-plan-b686 CONFIRM=1 bash scripts/owner-sync-main.sh
#   ALLOW_BUSY_MAIN=1 …  # push even when main HEAD CI is still running (cancels it)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY_RUN="${DRY_RUN:-0}"
CONFIRM="${CONFIRM:-0}"
TIP="${TIP:-dev/civilization-plan-b686}"
ALLOW_BUSY_MAIN="${ALLOW_BUSY_MAIN:-0}"

git fetch origin main "$TIP"

tip_sha="$(git rev-parse "origin/${TIP}")"
main_sha="$(git rev-parse origin/main)"

echo "owner-sync-main: origin/main=${main_sha:0:7} tip(${TIP})=${tip_sha:0:7}"

if [[ "$main_sha" == "$tip_sha" ]]; then
  echo "owner-sync-main: main already matches tip — nothing to do"
  exit 0
fi

if ! git merge-base --is-ancestor origin/main "origin/${TIP}"; then
  echo "owner-sync-main: origin/main is not an ancestor of ${TIP}" >&2
  echo "owner-sync-main: open a PR instead of fast-forward." >&2
  exit 1
fi

echo "owner-sync-main: note — main CI cancel-in-progress=true; this push cancels any in-flight main CI."
echo "owner-sync-main: prefer syncing once before owner-finish-installable, not on every tip commit."

# Refuse to cancel an in-flight Verifiable run on the current main HEAD unless
# explicitly overridden. Red main CI is allowed (that is how fixes land).
ci_rc=0
bash scripts/check-main-ci.sh || ci_rc=$?
if [[ "$ci_rc" -eq 2 ]]; then
  if [[ "$ALLOW_BUSY_MAIN" == "1" ]]; then
    echo "owner-sync-main: ALLOW_BUSY_MAIN=1 — continuing despite in-flight main CI" >&2
  else
    echo "owner-sync-main: main HEAD CI is still running — refusing tip→main sync" >&2
    echo "  Wait for it to finish (bash scripts/check-main-ci.sh), then re-run." >&2
    echo "  Or set ALLOW_BUSY_MAIN=1 to cancel in-flight CI intentionally." >&2
    exit 1
  fi
fi

if [[ "$DRY_RUN" == "1" ]]; then
  echo "owner-sync-main: DRY_RUN=1 — would: git push origin ${tip_sha}:main"
  exit 0
fi

if [[ "$CONFIRM" != "1" ]]; then
  echo "owner-sync-main: refusing without CONFIRM=1 (avoids accidental tip→main thrash)" >&2
  echo "  CONFIRM=1 bash scripts/owner-sync-main.sh" >&2
  echo "  DRY_RUN=1 bash scripts/owner-sync-main.sh" >&2
  exit 1
fi

git push origin "${tip_sha}:main"
echo "owner-sync-main: fast-forwarded main to ${tip_sha:0:7}"
echo "owner-sync-main: next — wait for main CI, then bash scripts/owner-finish-installable.sh (token required)"
