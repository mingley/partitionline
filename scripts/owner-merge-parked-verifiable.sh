#!/usr/bin/env bash
# After Installable: land parked Verifiable work onto main.
#
# Parked branch (kept off tip during READY_EXCEPT_TOKEN so PUBLISH_LOCAL stays
# one-shot) adds:
#   - Actions auth-smoke (REQUIRE_AUTH=1)
#   - Actions integrity-smoke (REQUIRE_INTEGRITY=1)
#   - ConsumerGroupHeartbeat decode fuzz + decode-smoke coverage
#
# Usage:
#   bash scripts/owner-merge-parked-verifiable.sh
#   DRY_RUN=1 bash scripts/owner-merge-parked-verifiable.sh
#   ALLOW_BEFORE_INSTALLABLE=1 …   # not recommended; tip/main thrash risk
#   TARGET_BRANCH=main …           # default main
#   PARKED_BRANCH=dev/verifiable-auth-integrity-fuzz-b686
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY_RUN="${DRY_RUN:-0}"
ALLOW_BEFORE_INSTALLABLE="${ALLOW_BEFORE_INSTALLABLE:-0}"
TARGET_BRANCH="${TARGET_BRANCH:-main}"
PARKED_BRANCH="${PARKED_BRANCH:-dev/verifiable-auth-integrity-fuzz-b686}"
PUSH="${PUSH:-1}"

echo "owner-merge-parked-verifiable: parked=${PARKED_BRANCH} → ${TARGET_BRANCH}"

echo
echo "== 0) Installable gate =="
if bash scripts/check-installable.sh; then
  echo "owner-merge-parked-verifiable: crates.io Installable OK"
else
  if [[ "$ALLOW_BEFORE_INSTALLABLE" == "1" ]]; then
    echo "owner-merge-parked-verifiable: ALLOW_BEFORE_INSTALLABLE=1 — continuing without crates.io" >&2
  else
    echo "owner-merge-parked-verifiable: refusing — Installable not proven yet" >&2
    echo "  Publish first: bash scripts/owner-finish-installable.sh" >&2
    echo "  Or override: ALLOW_BEFORE_INSTALLABLE=1 (not recommended while tip is cut-critical)" >&2
    exit 1
  fi
fi

echo
echo "== 1) Fetch =="
git fetch origin "${TARGET_BRANCH}" "${PARKED_BRANCH}"

target_sha="$(git rev-parse "origin/${TARGET_BRANCH}")"
parked_sha="$(git rev-parse "origin/${PARKED_BRANCH}")"
echo "owner-merge-parked-verifiable: origin/${TARGET_BRANCH}=${target_sha:0:7}"
echo "owner-merge-parked-verifiable: origin/${PARKED_BRANCH}=${parked_sha:0:7}"

if git merge-base --is-ancestor "$parked_sha" "$target_sha"; then
  echo "owner-merge-parked-verifiable: parked commits already on ${TARGET_BRANCH}"
  exit 0
fi

if ! git merge-base --is-ancestor "$target_sha" "$parked_sha"; then
  # Allow merge commit path when tip moved; still require clean merge-tree.
  echo "owner-merge-parked-verifiable: ${TARGET_BRANCH} is not a strict ancestor of parked — will merge"
fi

echo
echo "== 2) Merge-tree probe =="
# Modern merge-tree: prints tree OID on success; conflicts → nonzero.
if ! git merge-tree --write-tree --no-messages "$target_sha" "$parked_sha"     >/tmp/pl-merge-verifiable-tree 2>/tmp/pl-merge-verifiable.err; then
  echo "owner-merge-parked-verifiable: merge-tree failed — resolve conflicts on a PR" >&2
  git merge-tree --write-tree --messages "$target_sha" "$parked_sha" 2>&1 | tail -40 >&2 || true
  cat /tmp/pl-merge-verifiable.err >&2 || true
  exit 1
fi
echo "owner-merge-parked-verifiable: merge-tree clean → $(tr -d '\n' </tmp/pl-merge-verifiable-tree | head -c 12)"

echo
echo "== 3) Land on ${TARGET_BRANCH} =="
if [[ "$DRY_RUN" == "1" ]]; then
  echo "owner-merge-parked-verifiable: DRY_RUN=1 — would checkout ${TARGET_BRANCH}, merge --no-ff ${PARKED_BRANCH}, push"
  echo "owner-merge-parked-verifiable: DRY_RUN=1 — would wait for main CI then audit-civilization-bars"
  exit 0
fi

git checkout "${TARGET_BRANCH}"
git pull --ff-only origin "${TARGET_BRANCH}"
if git merge-base --is-ancestor "$parked_sha" HEAD; then
  echo "owner-merge-parked-verifiable: already contains parked after pull"
else
  git merge --no-ff "origin/${PARKED_BRANCH}" -m "Merge ${PARKED_BRANCH}: Actions auth+integrity smoke + ConsumerGroupHeartbeat fuzz"
fi

if [[ "$PUSH" == "1" ]]; then
  git push origin "${TARGET_BRANCH}"
  echo "owner-merge-parked-verifiable: pushed ${TARGET_BRANCH}"
else
  echo "owner-merge-parked-verifiable: PUSH=0 — local merge only"
fi

echo
echo "== 4) Next =="
echo "  Wait for main CI green: bash scripts/check-main-ci.sh"
echo "  Then: bash scripts/audit-civilization-bars.sh"
echo "  Configure crates.io Trusted Publishing → release.yml if not done"
echo "owner-merge-parked-verifiable: OK"
