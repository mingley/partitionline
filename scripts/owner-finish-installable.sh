#!/usr/bin/env bash
# One-shot owner finish for civilization Installable (WP-0.5).
#
# Once CARGO_REGISTRY_TOKEN is available in this environment, this script:
#   0a. Runs honesty self-tests (registry token units + tip Verifiable PARTIAL exits)
#   1. Confirms tip merge-readiness
#   2. Best-effort cancels stuck Actions (agents often 403 — non-fatal)
#   3. Fast-forwards main to the civilization tip (MERGE_CIVILIZATION=1)
#   4. Cuts vX.Y.Z via PUBLISH_LOCAL=1 by default (bypasses starved Actions)
#   5. Runs day1 + check-installable + audit-civilization-bars
#   6. Optionally lands parked post-cut parks (MERGE_PARKED_VERIFIABLE=1 default)
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
#   ALLOW_RED_MAIN=1 …        # override red main CI refuse (not recommended)
#   REQUIRE_MAIN_CI=0 …       # allow inconclusive main CI (default: require green when not DRY_RUN)
#   ALLOW_UNVERIFIED_TIP=1 …  # allow PUBLISH_LOCAL of tip code not yet on green main (not recommended)
#   MERGE_PARKED_VERIFIABLE=0 …  # skip post-cut merge of parked Verifiable branch
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY_RUN="${DRY_RUN:-0}"
# Real Installable cuts require terminal green main CI by default. DRY_RUN keeps
# the soft warn so rehearsal works while CI is still queued. Set REQUIRE_MAIN_CI=0
# to override (not recommended for the first crates.io cut).
if [[ -z "${REQUIRE_MAIN_CI:-}" ]]; then
  if [[ "$DRY_RUN" == "1" ]]; then
    REQUIRE_MAIN_CI=0
  else
    REQUIRE_MAIN_CI=1
  fi
fi
export REQUIRE_MAIN_CI
# Default local publish when a token is in-env — avoids the starved Actions queue
# for the first crates.io cut. Set PUBLISH_LOCAL=0 to push a tag for release.yml.
PUBLISH_LOCAL="${PUBLISH_LOCAL:-1}"
MERGE_CIVILIZATION="${MERGE_CIVILIZATION:-1}"
CIVILIZATION_BRANCH="${CIVILIZATION_BRANCH:-dev/civilization-plan-b686}"
CANCEL_STUCK="${CANCEL_STUCK:-1}"
ALLOW_UNVERIFIED_TIP="${ALLOW_UNVERIFIED_TIP:-0}"
# After a successful Installable prove, land parked post-cut parks on main
# (Verifiable auth/integrity/fuzz + flate2 gzip fix + SCRAM crypto bumps).
# Set 0 to skip. Alias: MERGE_PARKED_VERIFIABLE still honored.
MERGE_PARKED_VERIFIABLE="${MERGE_PARKED_VERIFIABLE:-1}"
MERGE_POST_CUT_PARKS="${MERGE_POST_CUT_PARKS:-$MERGE_PARKED_VERIFIABLE}"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
tag="v${ver}"

echo "owner-finish-installable: ${name} ${ver}"
echo

echo "== 0a) Honesty self-tests (no token required) =="
# Cut path must not drift: registry probe units + tip Verifiable PARTIAL exit codes.
# These are executable (not grep-only) and run before Installable short-circuit / token gate.
bash scripts/check-registry-token.sh --self-test
bash scripts/ci-tip-verifiable-broker.sh --self-test

echo
echo "== 0) Already Installable? =="
if bash scripts/check-installable.sh; then
  echo "owner-finish-installable: crates.io already has ${name} ${ver}"
  echo "owner-finish-installable: running day1 + bars audit"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-finish-installable: DRY_RUN=1 — would run day1-after-publish + audit-civilization-bars"
    echo "owner-finish-installable: DRY_RUN=1 — would then run owner-enable-trusted-publishing.sh"
    if [[ "${MERGE_POST_CUT_PARKS:-${MERGE_PARKED_VERIFIABLE:-1}}" == "1" ]]; then
      echo "owner-finish-installable: DRY_RUN=1 — would then run owner-land-post-cut-parks.sh"
      # Hard-fail: soft-skipping parks here lied about cut readiness.
      DRY_RUN=1 REQUIRE_PARKS=1 bash scripts/owner-land-post-cut-parks.sh
    fi
    exit 0
  fi
  bash scripts/day1-after-publish.sh
  bash scripts/audit-civilization-bars.sh
  echo
  echo "== Trusted Publishing (post-Installable) =="
  bash scripts/owner-enable-trusted-publishing.sh || {
    echo "owner-finish-installable: WARN — Trusted Publishing helper failed; Installable still OK" >&2
    echo "  Retry: bash scripts/owner-enable-trusted-publishing.sh" >&2
  }
  if [[ "${MERGE_POST_CUT_PARKS:-${MERGE_PARKED_VERIFIABLE:-1}}" == "1" ]]; then
    echo
    echo "== Land parked Verifiable on main =="
    bash scripts/owner-land-post-cut-parks.sh || {
      echo "owner-finish-installable: WARN — post-cut parks land failed; Installable still OK" >&2
      echo "  Retry: bash scripts/owner-land-post-cut-parks.sh" >&2
    }
  fi
  exit 0
fi

echo
echo "== 1) Token =="
# Load TOKEN_FILE + normalize whitespace into *this* shell so cargo publish sees it.
# (check-registry-token alone runs in a subshell — its export would not stick.)
# shellcheck source=scripts/lib/cargo-registry-token.sh
source "$ROOT/scripts/lib/cargo-registry-token.sh"
pl_prepare_cargo_registry_token "owner-finish-installable"

tok_rc=0
bash scripts/check-registry-token.sh || tok_rc=$?
if [[ "$tok_rc" -eq 0 ]]; then
  echo "owner-finish-installable: CARGO_REGISTRY_TOKEN accepted for publish-new auth"
elif [[ "$tok_rc" -eq 2 || -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-finish-installable: CARGO_REGISTRY_TOKEN unset — DRY_RUN=1 continues (rehearsal only)"
  else
    echo "owner-finish-installable: CARGO_REGISTRY_TOKEN is NOT set" >&2
    echo >&2
    echo "Installable cannot finish without a crates.io publish token." >&2
    echo "One-screen owner ask:" >&2
    echo "  bash scripts/owner-request-registry-token.sh" >&2
    echo >&2
    # Print the ask inline so the owner does not have to hunt for the helper.
    bash scripts/owner-request-registry-token.sh >&2 || true
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
  echo "owner-finish-installable: CARGO_REGISTRY_TOKEN set but crates.io rejected it" >&2
  echo "  Recreate with publish-new (+ publish-update); see check-registry-token output." >&2
  exit 1
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
  if [[ "${REQUIRE_MAIN_CI}" == "1" ]]; then
    echo "owner-finish-installable: REQUIRE_MAIN_CI=1 and main CI inconclusive — refusing" >&2
    echo "  Wait for green main CI (bash scripts/check-main-ci.sh), or REQUIRE_MAIN_CI=0 to override." >&2
    exit 1
  fi
  echo "owner-finish-installable: main CI inconclusive — continuing (REQUIRE_MAIN_CI=0)"
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

# shellcheck source=scripts/lib/tip-delta.sh
source "$ROOT/scripts/lib/tip-delta.sh"

tip_sha="$(git rev-parse "origin/${CIVILIZATION_BRANCH}")"
main_sha="$(git rev-parse origin/main)"
echo "owner-finish-installable: origin/main=${main_sha:0:7} tip=${tip_sha:0:7}"

# Trust rule: PUBLISH_LOCAL after FF is only safe when tip delta is docs/scripts
# (library bytes already Verifiable-green on main) OR tip == main. Non-docs tip
# drift must land on main and get green CI before the crates.io cut.
tip_code_unverified=0
if [[ "$main_sha" != "$tip_sha" ]] && ! pl_tip_delta_is_docs_only "$main_sha" "$tip_sha"; then
  tip_code_unverified=1
fi
if [[ "$tip_code_unverified" -eq 1 && "$PUBLISH_LOCAL" == "1" && "$ALLOW_UNVERIFIED_TIP" != "1" ]]; then
  echo "owner-finish-installable: refusing PUBLISH_LOCAL — tip has non-docs commits not on main" >&2
  echo "  Pre-FF main CI does not cover tip library/workflow changes." >&2
  echo "  1. CONFIRM=1 bash scripts/owner-sync-main.sh   # FF tip→main (code delta)" >&2
  echo "  2. Wait until bash scripts/check-main-ci.sh is green on the new main HEAD" >&2
  echo "  3. MERGE_CIVILIZATION=0 bash scripts/owner-finish-installable.sh" >&2
  echo "  Override (not recommended): ALLOW_UNVERIFIED_TIP=1" >&2
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-finish-installable: DRY_RUN=1 — would refuse here; continuing rehearsal" >&2
  else
    exit 1
  fi
fi
if [[ "$tip_code_unverified" -eq 1 && "$ALLOW_UNVERIFIED_TIP" == "1" ]]; then
  echo "owner-finish-installable: ALLOW_UNVERIFIED_TIP=1 — publishing tip code without tip-SHA Verifiable" >&2
fi

if [[ "$MERGE_CIVILIZATION" == "1" ]]; then
  if [[ "$main_sha" == "$tip_sha" ]]; then
    echo "owner-finish-installable: main already at civilization tip"
  else
    # Refuse if main has commits tip lacks (would need a real merge/PR).
    if ! git merge-base --is-ancestor origin/main "origin/${CIVILIZATION_BRANCH}"; then
      echo "owner-finish-installable: origin/main is not an ancestor of ${CIVILIZATION_BRANCH}" >&2
      echo "owner-finish-installable: open/merge a PR instead of fast-forward." >&2
      exit 1
    fi
    if [[ "$tip_code_unverified" -eq 0 ]]; then
      echo "owner-finish-installable: tip delta is docs/scripts-only — FF+publish is Verifiable-safe"
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
  echo "owner-finish-installable: DRY_RUN=1 — would then run owner-enable-trusted-publishing.sh"
  # Rehearse workflow-shape + UI checklist (crate may be absent).
  DRY_RUN=1 bash scripts/owner-enable-trusted-publishing.sh
  if [[ "${MERGE_POST_CUT_PARKS:-${MERGE_PARKED_VERIFIABLE:-1}}" == "1" ]]; then
    echo "owner-finish-installable: DRY_RUN=1 — would then run owner-land-post-cut-parks.sh"
    # Finish FFs tip→main before parks; rehearse stack on tip when tip is ahead.
    # Hard-fail parks rehearsal — || true previously greenwashed dirty stacks.
    tip_br="${CIVILIZATION_TIP:-dev/civilization-plan-b686}"
    git fetch origin main "$tip_br" >/dev/null 2>&1 || true
    if git rev-parse "origin/${tip_br}" >/dev/null 2>&1 \
        && ! git merge-base --is-ancestor "origin/${tip_br}" origin/main; then
      echo "owner-finish-installable: tip ahead of main — DRY_RUN parks against ${tip_br}"
      DRY_RUN=1 REQUIRE_PARKS=1 ALLOW_BEFORE_INSTALLABLE=1 TARGET_BRANCH="$tip_br" \
        bash scripts/owner-land-post-cut-parks.sh
    else
      DRY_RUN=1 REQUIRE_PARKS=1 ALLOW_BEFORE_INSTALLABLE=1 \
        bash scripts/owner-land-post-cut-parks.sh
    fi
  fi
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

echo
echo "== 8) Trusted Publishing (post-Installable) =="
# Prints exact crates.io UI steps so later tags can drop the long-lived token.
# Non-fatal: Installable is already proven above.
bash scripts/owner-enable-trusted-publishing.sh || {
  echo "owner-finish-installable: WARN — Trusted Publishing helper failed; Installable still OK" >&2
  echo "  Retry: bash scripts/owner-enable-trusted-publishing.sh" >&2
}

if [[ "${MERGE_POST_CUT_PARKS}" == "1" ]]; then
  echo
  echo "== 9) Land parked Verifiable on main =="
  echo "owner-finish-installable: MERGE_PARKED_VERIFIABLE=1 — landing post-cut parks (Verifiable + flate2 + SCRAM crypto + lz4_flex + actions/checkout)"
  # Installable already proven above; merge script rechecks crates.io.
  if bash scripts/owner-land-post-cut-parks.sh; then
    echo "owner-finish-installable: post-cut parks landed on main"
  else
    echo "owner-finish-installable: WARN — post-cut parks land failed; Installable still OK" >&2
    echo "  Retry: bash scripts/owner-land-post-cut-parks.sh" >&2
    echo "  Skip later: MERGE_PARKED_VERIFIABLE=0 bash scripts/owner-finish-installable.sh" >&2
  fi
else
  echo "owner-finish-installable: MERGE_PARKED_VERIFIABLE=0 — skipped post-cut parks land"
  echo "  Later: bash scripts/owner-land-post-cut-parks.sh"
fi
exit 0
