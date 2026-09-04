#!/usr/bin/env bash
# After Installable: land parked post-cut branches onto main (in order).
#
# Kept off tip during READY_EXCEPT_TOKEN so PUBLISH_LOCAL tip-delta stays
# docs/scripts-only. Default parks:
#   1. Verifiable auth/integrity Actions + ConsumerGroupHeartbeat fuzz
#   2. SCRAM crypto hmac/pbkdf2 0.13 + sha2 0.11 (includes flate2 1.1.10 gzip fix)
#
# Usage:
#   bash scripts/owner-land-post-cut-parks.sh
#   DRY_RUN=1 bash scripts/owner-land-post-cut-parks.sh
#   ALLOW_BEFORE_INSTALLABLE=1 …   # not recommended
#   PARKED_BRANCHES="dev/foo-b686 dev/bar-b686" …
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY_RUN="${DRY_RUN:-0}"
ALLOW_BEFORE_INSTALLABLE="${ALLOW_BEFORE_INSTALLABLE:-0}"
TARGET_BRANCH="${TARGET_BRANCH:-main}"
PUSH="${PUSH:-1}"
# Space-separated; override to land a subset.
PARKED_BRANCHES="${PARKED_BRANCHES:-dev/verifiable-auth-integrity-fuzz-b686 dev/scram-crypto-bumps-b686}"

echo "owner-land-post-cut-parks: → ${TARGET_BRANCH}"
echo "owner-land-post-cut-parks: parks: ${PARKED_BRANCHES}"

echo
echo "== 0) Installable gate =="
if bash scripts/check-installable.sh; then
  echo "owner-land-post-cut-parks: crates.io Installable OK"
else
  if [[ "$ALLOW_BEFORE_INSTALLABLE" == "1" ]]; then
    echo "owner-land-post-cut-parks: ALLOW_BEFORE_INSTALLABLE=1 — continuing without crates.io" >&2
  else
    echo "owner-land-post-cut-parks: refusing — Installable not proven yet" >&2
    echo "  Publish first: bash scripts/owner-finish-installable.sh" >&2
    exit 1
  fi
fi

git fetch origin "${TARGET_BRANCH}"
for b in ${PARKED_BRANCHES}; do
  git fetch origin "$b" || {
    echo "owner-land-post-cut-parks: WARN — missing origin/${b}; skipping" >&2
    continue
  }
done

land_one() {
  local parked="$1"
  local target_sha parked_sha
  target_sha="$(git rev-parse "origin/${TARGET_BRANCH}")"
  parked_sha="$(git rev-parse "origin/${parked}")"
  echo
  echo "== park ${parked} =="
  echo "owner-land-post-cut-parks: origin/${TARGET_BRANCH}=${target_sha:0:7} park=${parked_sha:0:7}"
  if git merge-base --is-ancestor "$parked_sha" "$target_sha"; then
    echo "owner-land-post-cut-parks: ${parked} already on ${TARGET_BRANCH}"
    return 0
  fi
  if ! git merge-tree --write-tree --no-messages "$target_sha" "$parked_sha" \
      >/tmp/pl-post-cut-tree 2>/tmp/pl-post-cut.err; then
    echo "owner-land-post-cut-parks: merge-tree failed for ${parked} — resolve via PR" >&2
    git merge-tree --write-tree --messages "$target_sha" "$parked_sha" 2>&1 | tail -30 >&2 || true
    return 1
  fi
  echo "owner-land-post-cut-parks: merge-tree clean → $(tr -d '\n' </tmp/pl-post-cut-tree | head -c 12)"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-land-post-cut-parks: DRY_RUN=1 — would merge --no-ff origin/${parked} into ${TARGET_BRANCH}"
    return 0
  fi
  git checkout "${TARGET_BRANCH}"
  git pull --ff-only origin "${TARGET_BRANCH}"
  if git merge-base --is-ancestor "$parked_sha" HEAD; then
    echo "owner-land-post-cut-parks: already contains ${parked} after pull"
  else
    git merge --no-ff "origin/${parked}" -m "Merge ${parked}: post-cut parked civilization land"
  fi
  if [[ "$PUSH" == "1" ]]; then
    git push origin "${TARGET_BRANCH}"
    echo "owner-land-post-cut-parks: pushed ${TARGET_BRANCH} with ${parked}"
  fi
  # Refresh target tip for next park.
  git fetch origin "${TARGET_BRANCH}"
}

fail=0
for b in ${PARKED_BRANCHES}; do
  if ! land_one "$b"; then
    fail=1
  fi
done

if [[ "$DRY_RUN" == "1" ]]; then
  echo
  echo "owner-land-post-cut-parks: DRY_RUN complete"
  exit "$fail"
fi

if [[ "$fail" -ne 0 ]]; then
  echo "owner-land-post-cut-parks: completed with failures — inspect PRs for remaining parks" >&2
  exit 1
fi

echo
echo "== next =="
echo "  Wait for main CI: bash scripts/check-main-ci.sh"
echo "  Then: bash scripts/audit-civilization-bars.sh"
echo "  Configure crates.io Trusted Publishing → release.yml if needed"
echo "owner-land-post-cut-parks: OK"
