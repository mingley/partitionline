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
#   ALLOW_BEFORE_INSTALLABLE=1 …
#   REQUIRE_PARKS=1 …   # missing park is FAIL (tip Verifiable gate)
#   PARKED_BRANCHES="dev/foo-b686 dev/bar-b686" …
#   TARGET_BRANCH=dev/civilization-plan-b686 DRY_RUN=1 ALLOW_BEFORE_INSTALLABLE=1 …
#     # rehearse stack onto tip before cut (finish FFs tip→main first)
#
# DRY_RUN uses a disposable git worktree (never `checkout -f` on the caller's
# branch) so tip WIP / Verifiable gate edits are not discarded mid-edit.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY_RUN="${DRY_RUN:-0}"
ALLOW_BEFORE_INSTALLABLE="${ALLOW_BEFORE_INSTALLABLE:-0}"
REQUIRE_PARKS="${REQUIRE_PARKS:-0}"
TARGET_BRANCH="${TARGET_BRANCH:-main}"
PUSH="${PUSH:-1}"
# Space-separated; override to land a subset.
PARKED_BRANCHES="${PARKED_BRANCHES:-dev/verifiable-auth-integrity-fuzz-b686 dev/scram-crypto-bumps-b686 dev/lz4-flex-bump-b686 dev/actions-checkout-bump-b686}"

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

# Prefer local TARGET when it is ahead of origin (pre-push tip rehearsal).
resolve_target_sha() {
  local origin_sha="" local_sha=""
  git fetch origin "${TARGET_BRANCH}" 2>/dev/null || true
  if git rev-parse --verify "origin/${TARGET_BRANCH}" >/dev/null 2>&1; then
    origin_sha="$(git rev-parse "origin/${TARGET_BRANCH}")"
  fi
  if git rev-parse --verify "${TARGET_BRANCH}" >/dev/null 2>&1; then
    local_sha="$(git rev-parse "${TARGET_BRANCH}")"
  fi
  if [[ -n "$local_sha" && -n "$origin_sha" \
      && "$local_sha" != "$origin_sha" ]] \
      && git merge-base --is-ancestor "$origin_sha" "$local_sha"; then
    echo "owner-land-post-cut-parks: local ${TARGET_BRANCH}=${local_sha:0:7} ahead of origin — using local" >&2
    printf '%s' "$local_sha"
    return 0
  fi
  if [[ -n "$origin_sha" ]]; then
    printf '%s' "$origin_sha"
    return 0
  fi
  if [[ -n "$local_sha" ]]; then
    printf '%s' "$local_sha"
    return 0
  fi
  echo "owner-land-post-cut-parks: cannot resolve ${TARGET_BRANCH}" >&2
  return 1
}

TARGET_SHA="$(resolve_target_sha)"

for b in ${PARKED_BRANCHES}; do
  if ! git fetch origin "$b"; then
    if [[ "$REQUIRE_PARKS" == "1" ]]; then
      echo "owner-land-post-cut-parks: FAIL — missing origin/${b} (REQUIRE_PARKS=1)" >&2
      exit 1
    fi
    echo "owner-land-post-cut-parks: WARN — missing origin/${b}; skipping" >&2
    continue
  fi
done

# DRY_RUN must stack parks the same way a real land does. Per-park merge-tree
# against bare TARGET hides conflicts that only appear after earlier parks land
# (seen 2026-09-04: SCRAM CHANGELOG vs tip+Verifiable).
# Use a worktree so we never `checkout -f` the caller's branch (that discarded
# tip WIP while wiring this gate).
dry_run_stack() {
  # EXIT traps run after locals are torn down under `set -u` — keep cleanup
  # paths in globals for the trap body.
  local base_sha fail=0 parked parked_sha saw_park=0
  base_sha="${TARGET_SHA}"
  PL_POST_CUT_DRY_WT="$(mktemp -d /tmp/pl-post-cut-dry-XXXXXX)"
  PL_POST_CUT_DRY_BRANCH="tmp/post-cut-dry-run-$$"
  git branch -D "$PL_POST_CUT_DRY_BRANCH" 2>/dev/null || true
  git worktree add -b "$PL_POST_CUT_DRY_BRANCH" "$PL_POST_CUT_DRY_WT" "$base_sha" >/dev/null
  cleanup() {
    git worktree remove --force "${PL_POST_CUT_DRY_WT:-}" 2>/dev/null || true
    rm -rf "${PL_POST_CUT_DRY_WT:-}"
    git branch -D "${PL_POST_CUT_DRY_BRANCH:-}" 2>/dev/null || true
    unset PL_POST_CUT_DRY_WT PL_POST_CUT_DRY_BRANCH
  }
  trap cleanup EXIT
  echo
  echo "== DRY_RUN stacked merges from ${TARGET_BRANCH}=${base_sha:0:7} (worktree) =="
  (
    cd "$PL_POST_CUT_DRY_WT"
    for parked in ${PARKED_BRANCHES}; do
      if ! git rev-parse "origin/${parked}" >/dev/null 2>&1; then
        if [[ "$REQUIRE_PARKS" == "1" ]]; then
          echo "owner-land-post-cut-parks: FAIL — missing origin/${parked} (REQUIRE_PARKS=1)" >&2
          exit 2
        fi
        echo "owner-land-post-cut-parks: WARN — missing origin/${parked}; skipping" >&2
        continue
      fi
      saw_park=1
      parked_sha="$(git rev-parse "origin/${parked}")"
      echo
      echo "== park ${parked} =="
      echo "owner-land-post-cut-parks: base=${base_sha:0:7} park=${parked_sha:0:7}"
      if git merge-base --is-ancestor "$parked_sha" "$base_sha"; then
        echo "owner-land-post-cut-parks: ${parked} already contained"
        continue
      fi
      if git merge --no-ff "origin/${parked}" -m "DRY_RUN merge ${parked}"; then
        base_sha="$(git rev-parse HEAD)"
        echo "owner-land-post-cut-parks: DRY_RUN merge clean → ${base_sha:0:7}"
      else
        echo "owner-land-post-cut-parks: DRY_RUN CONFLICT on ${parked}" >&2
        git diff --name-only --diff-filter=U >&2 || true
        git merge --abort 2>/dev/null || true
        exit 3
      fi
    done
    if [[ "$REQUIRE_PARKS" == "1" && "$saw_park" -eq 0 ]]; then
      echo "owner-land-post-cut-parks: FAIL — no parks resolved (REQUIRE_PARKS=1)" >&2
      exit 2
    fi
  )
  local rc=$?
  trap - EXIT
  cleanup
  if [[ "$rc" -ne 0 ]]; then
    echo "owner-land-post-cut-parks: DRY_RUN failed — rebase parks onto tip before cut" >&2
    return 1
  fi
  echo
  echo "owner-land-post-cut-parks: DRY_RUN complete — stacked merges clean"
  return 0
}

if [[ "$DRY_RUN" == "1" ]]; then
  dry_run_stack
  exit $?
fi

land_one() {
  local parked="$1"
  local target_sha parked_sha
  git fetch origin "${TARGET_BRANCH}"
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
  git fetch origin "${TARGET_BRANCH}"
}

fail=0
for b in ${PARKED_BRANCHES}; do
  if ! land_one "$b"; then
    fail=1
  fi
done

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
