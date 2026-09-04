#!/usr/bin/env bash
# One-shot owner cut: main → tag vX.Y.Z → crates.io → day1.
# Preferred civilization path (docs/RELEASE.md): push the final tag and let
# release.yml publish (OIDC Trusted Publishing if configured, else
# CARGO_REGISTRY_TOKEN Actions secret). Optional PUBLISH_LOCAL=1 uses
# scripts/owner-publish.sh instead of waiting on Actions.
#
# Refuses dirty trees and non-main branches (ALLOW_NON_MAIN_PUBLISH=1 to override).
# Does not redefine Installable: check-installable.sh must still exit 0.
#
# Usage (on a clean main that already contains the civilization tip):
#   bash scripts/owner-cut-release.sh
#   PUBLISH_LOCAL=1 bash scripts/owner-cut-release.sh
#   DRY_RUN=1 bash scripts/owner-cut-release.sh
#   REQUIRE_ACTIONS_SECRET=1 bash scripts/owner-cut-release.sh  # fail if Actions secret missing
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY_RUN="${DRY_RUN:-0}"
PUBLISH_LOCAL="${PUBLISH_LOCAL:-0}"
SKIP_PUBLISH_READY="${SKIP_PUBLISH_READY:-0}"
WAIT_CRATES_ATTEMPTS="${WAIT_CRATES_ATTEMPTS:-36}" # ~6 minutes at 10s

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
  echo "owner-cut-release: preferred path — push tag; release.yml publishes"
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

if [[ "$DRY_RUN" == "1" ]]; then
  echo
  echo "owner-cut-release: DRY_RUN complete — no publish / day1 performed"
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

echo
echo "== civilization bars (post-publish) =="
bash scripts/audit-civilization-bars.sh

echo
echo "owner-cut-release: OK — ${name} ${ver} is Installable on crates.io"
echo "owner-cut-release: commit the README crates.io line if day1 changed it, then"
echo "  configure crates.io Trusted Publishing for release.yml (drop long-lived secret)."
exit 0
