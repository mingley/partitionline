#!/usr/bin/env bash
# One-shot owner cut: main → tag vX.Y.Z → crates.io → day1.
# Preferred civilization path when a token is already in-env: PUBLISH_LOCAL=1
# (cargo publish here; bypasses starved Actions). If CARGO_REGISTRY_TOKEN is
# present and PUBLISH_LOCAL is unset, defaults to 1. Set PUBLISH_LOCAL=0 to
# push the final tag and let release.yml publish (OIDC Trusted Publishing if
# configured, else the Actions secret). Prefer owner-finish-installable for
# tip→main FF + parks/handoff chaining.
#
# Refuses dirty trees and non-main branches (ALLOW_NON_MAIN_PUBLISH=1 to override).
# Does not redefine Installable: check-installable.sh must still exit 0.
#
# Usage (on a clean main that already contains the civilization tip):
#   bash scripts/owner-cut-release.sh                 # auto PUBLISH_LOCAL=1 when token in-env
#   PUBLISH_LOCAL=0 bash scripts/owner-cut-release.sh # tag → release.yml
#   PUBLISH_LOCAL=1 bash scripts/owner-cut-release.sh # force local publish
#   DRY_RUN=1 bash scripts/owner-cut-release.sh
#   SKIP_HANDOFF=1 …  # finish calls this so secret sync + one handoff stay in finish
#   REQUIRE_ACTIONS_SECRET=1 bash scripts/owner-cut-release.sh  # fail if Actions secret missing
#   bash scripts/owner-cut-release.sh --self-test
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "--self-test" ]]; then
  # Prove unset PUBLISH_LOCAL + token → auto 1; explicit 0 stays 0.
  # shellcheck source=scripts/lib/cargo-registry-token.sh
  source "$ROOT/scripts/lib/cargo-registry-token.sh"
  auto_from_token() {
    local PUBLISH_LOCAL_EXPLICIT PUBLISH_LOCAL CARGO_REGISTRY_TOKEN
    unset PUBLISH_LOCAL || true
    if [[ -z "${PUBLISH_LOCAL+x}" ]]; then
      PUBLISH_LOCAL_EXPLICIT=0
      PUBLISH_LOCAL=0
    else
      PUBLISH_LOCAL_EXPLICIT=1
    fi
    CARGO_REGISTRY_TOKEN="fake-token-for-self-test"
    if [[ "$PUBLISH_LOCAL_EXPLICIT" != "1" && -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
      PUBLISH_LOCAL=1
    fi
    [[ "$PUBLISH_LOCAL" == "1" ]] || {
      echo "owner-cut-release --self-test: FAIL — token+unset should auto PUBLISH_LOCAL=1" >&2
      exit 1
    }
  }
  keep_explicit_zero() {
    local PUBLISH_LOCAL_EXPLICIT PUBLISH_LOCAL CARGO_REGISTRY_TOKEN
    PUBLISH_LOCAL=0
    if [[ -z "${PUBLISH_LOCAL+x}" ]]; then
      PUBLISH_LOCAL_EXPLICIT=0
      PUBLISH_LOCAL=0
    else
      PUBLISH_LOCAL_EXPLICIT=1
    fi
    CARGO_REGISTRY_TOKEN="fake-token-for-self-test"
    if [[ "$PUBLISH_LOCAL_EXPLICIT" != "1" && -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
      PUBLISH_LOCAL=1
    fi
    [[ "$PUBLISH_LOCAL" == "0" ]] || {
      echo "owner-cut-release --self-test: FAIL — explicit PUBLISH_LOCAL=0 must stick" >&2
      exit 1
    }
  }
  auto_from_token
  keep_explicit_zero
  grep -qF 'PUBLISH_LOCAL_EXPLICIT' "$ROOT/scripts/owner-cut-release.sh"     || { echo "owner-cut-release --self-test: FAIL — missing PUBLISH_LOCAL_EXPLICIT wiring" >&2; exit 1; }
  grep -qF 'owner-post-installable-handoff' "$ROOT/scripts/owner-cut-release.sh" \
    || { echo "owner-cut-release --self-test: FAIL — must chain owner-post-installable-handoff after day1" >&2; exit 1; }
  grep -qF 'LAND_PARKS' "$ROOT/scripts/owner-cut-release.sh" \
    || { echo "owner-cut-release --self-test: FAIL — LAND_PARKS wiring missing for handoff chain" >&2; exit 1; }
  grep -qF 'SKIP_HANDOFF' "$ROOT/scripts/owner-cut-release.sh" \
    || { echo "owner-cut-release --self-test: FAIL — SKIP_HANDOFF knob missing (finish single-handoff)" >&2; exit 1; }
  grep -qF 'secret_rc' "$ROOT/scripts/owner-cut-release.sh" \
    || { echo "owner-cut-release --self-test: FAIL — secret_rc missing (bare-cut Actions secret PARTIAL)" >&2; exit 1; }
  grep -qF 'PARTIAL — Installable OK but Actions secret not synced' "$ROOT/scripts/owner-cut-release.sh" \
    || { echo "owner-cut-release --self-test: FAIL — Actions secret PARTIAL string missing" >&2; exit 1; }
  # DRY_RUN must rehearse handoff (or honor SKIP_HANDOFF) — never exit 0 before that.
  if ! awk '
    /owner-post-installable-handoff/ && !dry { hand_before=NR }
    /DRY_RUN complete/ { dry=NR }
    /SKIP_HANDOFF/ { skip=NR }
    END { exit (dry && hand_before && skip && hand_before < dry) ? 0 : 1 }
  ' "$ROOT/scripts/owner-cut-release.sh"; then
    echo "owner-cut-release --self-test: FAIL — DRY_RUN must reach handoff before DRY_RUN complete" >&2
    exit 1
  fi
  echo "owner-cut-release: --self-test OK — token auto PUBLISH_LOCAL=1; explicit 0 preserved; handoff chained; DRY_RUN reaches handoff; Actions secret PARTIAL gated"
  exit 0
fi


DRY_RUN="${DRY_RUN:-0}"
# Distinguish unset (auto) from explicit PUBLISH_LOCAL=0 (tag→Actions).
if [[ -z "${PUBLISH_LOCAL+x}" ]]; then
  PUBLISH_LOCAL_EXPLICIT=0
  PUBLISH_LOCAL=0
else
  PUBLISH_LOCAL_EXPLICIT=1
fi
SKIP_PUBLISH_READY="${SKIP_PUBLISH_READY:-0}"
WAIT_CRATES_ATTEMPTS="${WAIT_CRATES_ATTEMPTS:-36}" # ~6 minutes at 10s

# TOKEN_FILE + whitespace normalize into this shell before PUBLISH_LOCAL probe/publish.
# shellcheck source=scripts/lib/cargo-registry-token.sh
source "$ROOT/scripts/lib/cargo-registry-token.sh"
pl_prepare_cargo_registry_token "owner-cut-release"

# Token-day footgun: stepwise docs call cut-release bare. If a publish-new token
# is already in-env and the caller did not force PUBLISH_LOCAL=0, prefer local
# publish (same default as owner-finish-installable) instead of tag→starved Actions.
if [[ "$PUBLISH_LOCAL_EXPLICIT" != "1" && -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  PUBLISH_LOCAL=1
  echo "owner-cut-release: CARGO_REGISTRY_TOKEN in-env — defaulting PUBLISH_LOCAL=1"
  echo "  (set PUBLISH_LOCAL=0 to force tag → release.yml / Actions)"
fi

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
tag="v${ver}"

echo "owner-cut-release: ${name} ${ver} (tag ${tag})"
echo

branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$branch" != "main" && "${ALLOW_NON_MAIN_PUBLISH:-}" != "1" ]]; then
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-cut-release: DRY_RUN=1 on branch '${branch}' (not main) — rehearsal only; no tag/push."
  else
    echo "owner-cut-release: refuse to cut from branch '${branch}' (need main)." >&2
    echo "owner-cut-release: merge civilization → main first, or set ALLOW_NON_MAIN_PUBLISH=1." >&2
    echo "owner-cut-release: tip rehearsal: DRY_RUN=1 bash scripts/owner-cut-release.sh" >&2
    exit 1
  fi
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "owner-cut-release: working tree is dirty; commit or stash first." >&2
  exit 1
fi

echo "== merge/tag readiness =="
bash scripts/check-merge-ready.sh

# Best-effort Actions secret probe (agents often get 403 — never fails the cut
# unless REQUIRE_ACTIONS_SECRET=1). First crates.io publish needs the secret
# until Trusted Publishing is configured post-0.1.0.
if [[ "$PUBLISH_LOCAL" != "1" ]]; then
  echo
  echo "== Actions secret preflight (best-effort) =="
  if command -v gh >/dev/null 2>&1; then
    sec=""
    sec_rc=0
    sec="$(gh secret list 2>/dev/null)" && sec_rc=0 || sec_rc=$?
    if [[ "$sec_rc" -ne 0 ]]; then
      echo "owner-cut-release: note — cannot list Actions secrets (need repo admin; agents often 403)."
      echo "  Owner: gh secret list | grep CARGO_REGISTRY_TOKEN"
    elif printf '%s\n' "$sec" | grep -q '^CARGO_REGISTRY_TOKEN\b'; then
      echo "owner-cut-release: Actions secret CARGO_REGISTRY_TOKEN is present"
    else
      echo "owner-cut-release: WARN — CARGO_REGISTRY_TOKEN not listed in Actions secrets." >&2
      echo "  First publish will fail until the secret exists (or use PUBLISH_LOCAL=1)." >&2
      echo "  Prefer: bash scripts/owner-finish-installable.sh (syncs secret before tag when PUBLISH_LOCAL=0)." >&2
      if [[ "${REQUIRE_ACTIONS_SECRET:-0}" == "1" ]]; then
        echo "owner-cut-release: REQUIRE_ACTIONS_SECRET=1 — refusing to cut." >&2
        exit 1
      fi
    fi
  fi
fi

if [[ "$SKIP_PUBLISH_READY" != "1" ]]; then
  echo
  echo "== publish-ready (full gate) =="
  bash scripts/ci-publish-ready.sh
fi

if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
  existing="$(git rev-list -n1 "${tag}")"
  head="$(git rev-parse HEAD)"
  if [[ "$existing" != "$head" ]]; then
    echo "owner-cut-release: tag ${tag} exists but points at ${existing:0:7}, not HEAD ${head:0:7}." >&2
    exit 1
  fi
  echo "owner-cut-release: tag ${tag} already on HEAD"
else
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-cut-release: DRY_RUN=1 — would create annotated tag ${tag}"
  else
    git tag -a "${tag}" -m "${name} ${ver}"
    echo "owner-cut-release: created annotated tag ${tag}"
  fi
fi

if [[ "$PUBLISH_LOCAL" == "1" ]]; then
  # Probe publish-new auth before cargo publish (structured empty-tarball; not /me).
  if [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
    bash scripts/check-registry-token.sh
  fi
  if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
    if [[ "$DRY_RUN" == "1" ]]; then
      echo "owner-cut-release: DRY_RUN=1 — would require CARGO_REGISTRY_TOKEN then owner-publish + push ${tag}"
    else
      echo "owner-cut-release: PUBLISH_LOCAL=1 requires CARGO_REGISTRY_TOKEN in the environment." >&2
      exit 1
    fi
  elif [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-cut-release: DRY_RUN=1 — would run owner-publish.sh then push ${tag}"
  else
    # owner-publish already runs day1 by default; avoid double day1 below.
    RUN_DAY1_AFTER_PUBLISH=0 bash scripts/owner-publish.sh
    git push origin main "${tag}"
  fi
else
  echo
  echo "owner-cut-release: PUBLISH_LOCAL=0 — push tag; release.yml publishes"
  if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
    echo "owner-cut-release: note — no local CARGO_REGISTRY_TOKEN; Actions must have"
    echo "  the secret (first cut) or crates.io Trusted Publishing (later cuts)."
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-cut-release: DRY_RUN=1 — would: git push origin ${tag}"
  else
    git push origin "${tag}"
  fi
fi

# Post-cut handoff (TP + parks + bars). Finish sets SKIP_HANDOFF=1 so it can
# sync Actions secrets once, then run a single handoff (no double parks land).
# Bare cut (SKIP_HANDOFF=0) owns Actions secret sync here — same fail-closed
# PARTIAL as finish when sync fails after Installable.
SKIP_HANDOFF="${SKIP_HANDOFF:-0}"
land_parks="${LAND_PARKS:-${MERGE_POST_CUT_PARKS:-${MERGE_PARKED_VERIFIABLE:-1}}}"
secret_rc=0

pl_cut_sync_actions_secret() {
  # Finish owns secret sync when SKIP_HANDOFF=1. Bare cut must not soft-OK.
  if [[ "$SKIP_HANDOFF" == "1" ]]; then
    return 0
  fi
  echo
  echo "== sync Actions secret for later tag publishes =="
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-cut-release: DRY_RUN=1 — would sync Actions secret CARGO_REGISTRY_TOKEN"
    return 0
  fi
  if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
    # Tag→Actions path may rely on a pre-set Actions secret; cannot sync from here.
    echo "owner-cut-release: note — no in-env CARGO_REGISTRY_TOKEN to sync (Actions-only path)"
    return 0
  fi
  if command -v gh >/dev/null 2>&1; then
    if printf '%s' "${CARGO_REGISTRY_TOKEN}" | gh secret set CARGO_REGISTRY_TOKEN 2>/tmp/pl-cut-secret.log; then
      echo "owner-cut-release: Actions secret CARGO_REGISTRY_TOKEN synced"
    else
      secret_rc=1
      echo "owner-cut-release: WARN — could not set Actions secret (need admin; agents often 403)"
      tail -3 /tmp/pl-cut-secret.log 2>/dev/null | sed 's/^/  /' || true
      echo "  Owner: gh secret set CARGO_REGISTRY_TOKEN <<< \"\$CARGO_REGISTRY_TOKEN\""
    fi
  else
    secret_rc=1
    echo "owner-cut-release: WARN — gh not available; set Actions secret manually"
    echo "  Owner: gh secret set CARGO_REGISTRY_TOKEN <<< \"\$CARGO_REGISTRY_TOKEN\""
  fi
}

pl_cut_run_handoff() {
  local handoff_rc=0
  if [[ "$SKIP_HANDOFF" == "1" ]]; then
    echo "owner-cut-release: SKIP_HANDOFF=1 — TP/parks left to caller (finish single-handoff)"
    return 0
  fi
  echo
  echo "== post-Installable handoff (TP + parks + bars) =="
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "owner-cut-release: DRY_RUN=1 — would run owner-post-installable-handoff (LAND_PARKS=${land_parks})"
    LAND_PARKS=0 DRY_RUN=1 bash scripts/owner-post-installable-handoff.sh || handoff_rc=$?
  else
    SKIP_DAY1="${SKIP_DAY1:-0}" LAND_PARKS="$land_parks" bash scripts/owner-post-installable-handoff.sh || handoff_rc=$?
  fi
  if [[ "$handoff_rc" -eq 2 ]]; then
    echo "owner-cut-release: PARTIAL — ${name} ${ver} is Installable on crates.io but handoff soft-failed"
    echo "owner-cut-release: commit the README crates.io line if day1 changed it."
    echo "  Re-enter: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh"
    return 2
  elif [[ "$handoff_rc" -ne 0 ]]; then
    echo "owner-cut-release: FAIL — post-Installable handoff rc=${handoff_rc}" >&2
    return "$handoff_rc"
  fi
  return 0
}

if [[ "$DRY_RUN" == "1" ]]; then
  echo
  echo "owner-cut-release: DRY_RUN — skipping crates.io wait / day1; rehearsing secret sync + handoff"
  pl_cut_sync_actions_secret
  pl_cut_run_handoff
  echo
  echo "owner-cut-release: DRY_RUN complete — no publish / day1 performed; handoff rehearsed (or SKIP_HANDOFF)"
  exit 0
fi

echo
echo "== wait for crates.io ${name} ${ver} =="
ok=0
for i in $(seq 1 "${WAIT_CRATES_ATTEMPTS}"); do
  if bash scripts/check-installable.sh >/tmp/pl-cut-installable.log 2>&1; then
    ok=1
    break
  fi
  echo "owner-cut-release: waiting for crates.io (${i}/${WAIT_CRATES_ATTEMPTS})..."
  sleep 10
done
if [[ "$ok" != "1" ]]; then
  cat /tmp/pl-cut-installable.log >&2 || true
  echo "owner-cut-release: crates.io does not yet show ${name} ${ver}." >&2
  echo "owner-cut-release: check Actions release run / token / Trusted Publishing." >&2
  exit 1
fi

echo
echo "== day1 (README flip + remaining owner steps) =="
bash scripts/day1-after-publish.sh
bash scripts/check-installable.sh

# day1 already flipped adopter docs; handoff must not re-run day1 as a second flip.
SKIP_DAY1=1
pl_cut_sync_actions_secret

handoff_rc=0
pl_cut_run_handoff || handoff_rc=$?
if [[ "$handoff_rc" -ne 0 ]]; then
  exit "$handoff_rc"
fi
if [[ "$secret_rc" -ne 0 ]]; then
  echo
  echo "owner-cut-release: PARTIAL — Installable OK but Actions secret not synced"
  echo "owner-cut-release: ${name} ${ver} is on crates.io; later tag/Actions cuts need the secret"
  echo "  Owner: gh secret set CARGO_REGISTRY_TOKEN <<< \"\$CARGO_REGISTRY_TOKEN\""
  echo "  Then: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh   # if parks/TP still pending"
  echo "owner-cut-release: commit the README crates.io line if day1 changed it."
  exit 2
fi
echo
echo "owner-cut-release: OK — ${name} ${ver} is Installable on crates.io"
echo "owner-cut-release: commit the README crates.io line if day1 changed it."
echo "  Re-enter handoff anytime: bash scripts/owner-post-installable-handoff.sh"
echo "  LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh   # if parks/TP soft-failed"
exit 0
