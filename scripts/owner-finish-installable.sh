#!/usr/bin/env bash
# One-shot owner finish for civilization Installable (WP-0.5).
#
# Once CARGO_REGISTRY_TOKEN is available in this environment, this script:
#   1. Confirms tip merge-readiness
#   2. Best-effort cancels stuck Actions (agents often 403 — non-fatal)
#   3. Fast-forwards main to the civilization tip (MERGE_CIVILIZATION=1)
#   4. Cuts vX.Y.Z via PUBLISH_LOCAL=1 by default (bypasses starved Actions)
#   5. Runs day1 + check-installable + audit-civilization-bars
#
# Prefer this over hand-assembling merge → tag → Actions when the Cloud Agent
# (or owner shell) already has the crates.io token. Actions + OIDC remain the
# preferred path for *later* tags after Trusted Publishing is configured.
#
# Usage:
#   bash scripts/owner-finish-installable.sh
#   DRY_RUN=1 bash scripts/owner-finish-installable.sh
#   PUBLISH_LOCAL=0 bash scripts/owner-finish-installable.sh   # tag → Actions instead
#   MERGE_CIVILIZATION=0 bash scripts/owner-finish-installable.sh  # require already-on-main
#   ALLOW_RED_MAIN=1 …   # override red main CI refuse (not recommended)
#   REQUIRE_MAIN_CI=1 …  # also refuse when main CI is inconclusive
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY_RUN="${DRY_RUN:-0}"
# Default local publish when a token is in-env — avoids the starved Actions queue
# for the first crates.io cut. Set PUBLISH_LOCAL=0 to push a tag for release.yml.
PUBLISH_LOCAL="${PUBLISH_LOCAL:-1}"
MERGE_CIVILIZATION="${MERGE_CIVILIZATION:-1}"
CIVILIZATION_BRANCH="${CIVILIZATION_BRANCH:-dev/civilization-plan-b686}"
CANCEL_STUCK="${CANCEL_STUCK:-1}"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
tag="v${ver}"

echo "owner-finish-installable: ${name} ${ver}"
echo

echo "== 0) Already Installable? =="
if bash scripts/check-installable.sh; then
  echo "owner-finish-installable: crates.io already has ${name} ${ver}"
  echo "owner-finish-installable: running day1 + bars audit"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-finish-installable: DRY_RUN=1 — would run day1-after-publish + audit-civilization-bars"
    exit 0
  fi
  bash scripts/day1-after-publish.sh
  bash scripts/audit-civilization-bars.sh
  exit 0
fi

echo
echo "== 1) Token =="
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-finish-installable: CARGO_REGISTRY_TOKEN unset — DRY_RUN=1 continues (rehearsal only)"
  else
    echo "owner-finish-installable: CARGO_REGISTRY_TOKEN is NOT set" >&2
    echo >&2
    echo "Installable cannot finish without a crates.io publish token." >&2
    echo "  1. Create a crates.io token (publish-update for ${name})" >&2
    echo "  2. Add it as Cloud Agent secret CARGO_REGISTRY_TOKEN" >&2
    echo "  3. Also add Actions secret CARGO_REGISTRY_TOKEN (release.yml / first-publish.yml)" >&2
    echo "  4. Re-run: bash scripts/owner-finish-installable.sh" >&2
    echo >&2
    echo "If the token is only an Actions secret (not in this shell):" >&2
    echo "  After canceling stuck runs → Actions → First publish → confirm=publish" >&2
    echo "  (workflow: .github/workflows/first-publish.yml; prefer this script when in-env)" >&2
    echo "  or: bash scripts/owner-dispatch-first-publish.sh" >&2
    echo >&2
    echo "Meanwhile (no token required):" >&2
    echo "  bash scripts/owner-unblock.sh" >&2
    echo "  bash scripts/owner-cancel-stuck-runs.sh   # owner machine; agents 403" >&2
    echo "  DRY_RUN=1 bash scripts/owner-finish-installable.sh  # rehearse merge/cut" >&2
    exit 1
  fi
else
  echo "owner-finish-installable: CARGO_REGISTRY_TOKEN is set (len=${#CARGO_REGISTRY_TOKEN})"
fi

echo
echo "== 2) Merge/tag readiness =="
bash scripts/check-merge-ready.sh

echo
echo "== 2b) Main CI (Verifiable) =="
# Red main CI refuses the cut (ALLOW_RED_MAIN=1 to override).
# Inconclusive warns by default; REQUIRE_MAIN_CI=1 hard-gates that too.
ci_rc=0
bash scripts/check-main-ci.sh || ci_rc=$?
if [[ "$ci_rc" -eq 1 ]]; then
  if [[ "${ALLOW_RED_MAIN:-0}" == "1" ]]; then
    echo "owner-finish-installable: ALLOW_RED_MAIN=1 — continuing despite red main CI" >&2
  else
    echo "owner-finish-installable: main HEAD CI is red — refusing Installable cut" >&2
    echo "  Wait for green CI, or set ALLOW_RED_MAIN=1 to override (not recommended)." >&2
    echo "  Probe: bash scripts/check-main-ci.sh" >&2
    exit 1
  fi
elif [[ "$ci_rc" -eq 2 ]]; then
  if [[ "${REQUIRE_MAIN_CI:-0}" == "1" ]]; then
    echo "owner-finish-installable: REQUIRE_MAIN_CI=1 and main CI inconclusive — refusing" >&2
    exit 1
  fi
  echo "owner-finish-installable: main CI inconclusive — continuing (set REQUIRE_MAIN_CI=1 to hard-gate)"
fi

if [[ "$CANCEL_STUCK" == "1" ]]; then
  echo
  echo "== 3) Best-effort cancel stuck Actions =="
  if [[ "$DRY_RUN" == "1" ]]; then
    DRY_RUN=1 bash scripts/owner-cancel-stuck-runs.sh || true
  else
    bash scripts/owner-cancel-stuck-runs.sh || {
      echo "owner-finish-installable: note — cancel failed (agents often 403); continuing"
    }
  fi
fi

echo
echo "== 4) Ensure clean main has civilization tip =="
git fetch origin main "${CIVILIZATION_BRANCH}"

if [[ "$MERGE_CIVILIZATION" == "1" ]]; then
  tip_sha="$(git rev-parse "origin/${CIVILIZATION_BRANCH}")"
  main_sha="$(git rev-parse origin/main)"
  echo "owner-finish-installable: origin/main=${main_sha:0:7} tip=${tip_sha:0:7}"
  if [[ "$main_sha" == "$tip_sha" ]]; then
    echo "owner-finish-installable: main already at civilization tip"
  else
    # Refuse if main has commits tip lacks (would need a real merge/PR).
    if ! git merge-base --is-ancestor origin/main "origin/${CIVILIZATION_BRANCH}"; then
      echo "owner-finish-installable: origin/main is not an ancestor of ${CIVILIZATION_BRANCH}" >&2
      echo "owner-finish-installable: open/merge a PR instead of fast-forward." >&2
      exit 1
    fi
    if [[ "$DRY_RUN" == "1" ]]; then
      echo "owner-finish-installable: DRY_RUN=1 — would fast-forward main to ${tip_sha:0:7}"
    else
      git checkout main
      git pull --ff-only origin main
      git merge --ff-only "origin/${CIVILIZATION_BRANCH}"
      git push origin main
      echo "owner-finish-installable: fast-forwarded main to ${tip_sha:0:7}"
    fi
  fi
fi

branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$DRY_RUN" != "1" && "$branch" != "main" ]]; then
  echo "owner-finish-installable: checking out main for cut"
  git checkout main
  git pull --ff-only origin main
fi

echo
echo "== 5) Cut release =="
if [[ "$PUBLISH_LOCAL" == "1" ]]; then
  echo "owner-finish-installable: PUBLISH_LOCAL=1 (token in-env; bypasses Actions queue)"
else
  echo "owner-finish-installable: PUBLISH_LOCAL=0 (tag → release.yml)"
fi

if [[ "$DRY_RUN" == "1" ]]; then
  DRY_RUN=1 PUBLISH_LOCAL="$PUBLISH_LOCAL" \
    SKIP_PUBLISH_READY="${SKIP_PUBLISH_READY:-1}" \
    bash scripts/owner-cut-release.sh
  echo
  echo "owner-finish-installable: DRY_RUN complete — no merge/tag/publish performed"
  exit 0
fi

PUBLISH_LOCAL="$PUBLISH_LOCAL" bash scripts/owner-cut-release.sh

echo
echo "== 6) Prove Installable =="
bash scripts/check-installable.sh
echo "owner-finish-installable: verify adopter crates.io consumer compiles"
bash scripts/verify-crates-io-consumer.sh
bash scripts/audit-civilization-bars.sh

echo
echo "== 7) Best-effort sync Actions secret for later tag publishes =="
if command -v gh >/dev/null 2>&1; then
  if printf '%s' "${CARGO_REGISTRY_TOKEN}" | gh secret set CARGO_REGISTRY_TOKEN 2>/tmp/pl-finish-secret.log; then
    echo "owner-finish-installable: Actions secret CARGO_REGISTRY_TOKEN synced"
  else
    echo "owner-finish-installable: note — could not set Actions secret (need admin; agents often 403)"
    tail -3 /tmp/pl-finish-secret.log 2>/dev/null | sed 's/^/  /' || true
    echo "  Owner: gh secret set CARGO_REGISTRY_TOKEN <<< \"\$CARGO_REGISTRY_TOKEN\""
  fi
else
  echo "owner-finish-installable: note — gh not available; set Actions secret manually"
fi

echo
echo "owner-finish-installable: OK — ${name} ${ver} is Installable"
echo "owner-finish-installable: commit README crates.io line if day1 changed it"
echo "owner-finish-installable: then crates.io → Trusted Publishing → release.yml"
exit 0
