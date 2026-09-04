#!/usr/bin/env bash
# Pre-publish Installable preflight (WP-0.5). Does not publish.
#
# Exit codes:
#   0  — READY_EXCEPT_TOKEN: structurally ready; only CARGO_REGISTRY_TOKEN / crates.io cut remains
#   1  — other blockers (merge-ready fail, red main CI, metadata, etc.)
#   2  — already Installable (crates.io has this version)
#   3  — inconclusive main CI (still running); structural gates otherwise OK
#
# Env:
#   REQUIRE_MAIN_CI=1  treat inconclusive main CI (exit 3) as failure (exit 1) — default 0
#   SKIP_MAIN_CI=1     skip the main CI probe entirely
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REQUIRE_MAIN_CI="${REQUIRE_MAIN_CI:-0}"
SKIP_MAIN_CI="${SKIP_MAIN_CI:-0}"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"

echo "check-installable-preflight: ${name} ${ver}"
echo

echo "== honesty self-tests (no token required) =="
# READY_EXCEPT_TOKEN must not be claimable if probe/PARTIAL units regress.
# Same executable units as cut-path / owner-finish-installable step 0a.
bash scripts/check-registry-token.sh --self-test
bash scripts/ci-tip-verifiable-broker.sh --self-test

# Already published?
if bash scripts/check-installable.sh >/tmp/pl-preflight-installable.log 2>&1; then
  echo "check-installable-preflight: ALREADY_INSTALLABLE — crates.io has ${name} ${ver}"
  tail -5 /tmp/pl-preflight-installable.log | sed 's/^/  /'
  exit 2
fi
# check-installable fails when absent — that is expected pre-cut.
if ! grep -q 'not on crates.io\|status=absent\|FAIL' /tmp/pl-preflight-installable.log; then
  echo "check-installable-preflight: unexpected installable probe output:" >&2
  cat /tmp/pl-preflight-installable.log >&2
  exit 1
fi
echo "check-installable-preflight: crates.io absent (first cut still needed) — expected"

echo
echo "== merge/tag readiness =="
if ! bash scripts/check-merge-ready.sh; then
  echo "check-installable-preflight: FAIL — merge-ready blockers above" >&2
  exit 1
fi

echo
echo "== crate metadata =="
bash scripts/check-crate-metadata.sh

echo
echo "== main CI (Verifiable) =="
ci_rc=0
if [[ "$SKIP_MAIN_CI" == "1" ]]; then
  echo "check-installable-preflight: SKIP_MAIN_CI=1 — not probing"
else
  bash scripts/check-main-ci.sh || ci_rc=$?
  if [[ "$ci_rc" -eq 1 ]]; then
    echo "check-installable-preflight: FAIL — main HEAD CI is red" >&2
    exit 1
  elif [[ "$ci_rc" -eq 2 ]]; then
    if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
      echo "check-installable-preflight: FAIL — REQUIRE_MAIN_CI=1 and main CI inconclusive" >&2
      exit 1
    fi
    echo
    if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
      echo "check-installable-preflight: READY_EXCEPT_TOKEN (main CI still running — wait or REQUIRE_MAIN_CI=0)"
      echo "  Next: bash scripts/owner-request-registry-token.sh"
      echo "        then export CARGO_REGISTRY_TOKEN, wait for green main CI, then:"
      echo "        bash scripts/owner-finish-installable.sh"
      exit 3
    fi
    echo "check-installable-preflight: token set but main CI inconclusive — wait for green before cut"
    exit 3
  fi
fi

echo
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  # Soft parks honesty: cut path hard-requires tip⊆parks; do not claim "token only"
  # when parks are stale (refresh is tip-safe and unblocks the post-token cut).
  parks_rc=0
  bash scripts/check-post-cut-parks-stack.sh >/tmp/pl-preflight-parks.log 2>&1 || parks_rc=$?
  if [[ "$parks_rc" -ne 0 ]]; then
    echo "check-installable-preflight: READY_EXCEPT_TOKEN (also parks stack stale — cut would fail after token)"
    echo "  Structural + Verifiable gates OK; crates.io cut needs CARGO_REGISTRY_TOKEN AND refreshed parks."
    echo "  Refresh: bash scripts/refresh-post-cut-parks.sh"
    tail -8 /tmp/pl-preflight-parks.log | sed 's/^/  /' || true
  else
    echo "check-installable-preflight: READY_EXCEPT_TOKEN"
    echo "  Structural + Verifiable + parks stack OK; crates.io cut blocked only on CARGO_REGISTRY_TOKEN."
  fi
  echo "  Token scope: first cut of a NEW crate needs crates.io publish-new (+ usually publish-update)."
  echo "  publish-update alone cannot create the crate. Trusted Publishing is configured after 0.1.0."
  echo "  One-screen owner ask: bash scripts/owner-request-registry-token.sh"
  echo "  Next: set Cloud Agent secret CARGO_REGISTRY_TOKEN (Cursor → Environments → Secrets),"
  # shellcheck source=scripts/lib/cursor-env-secrets-url.sh
  source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/cursor-env-secrets-url.sh"
  echo "        Direct: $PARTITIONLINE_CURSOR_ENV_SECRETS_URL"
  echo "        also add Actions secret CARGO_REGISTRY_TOKEN, restart agent, then:"
  echo "        bash scripts/owner-finish-installable.sh"
  echo "        # publishes 0.1.0 then chains post-cut parks land by default"
  echo "        # (MERGE_PARKED_VERIFIABLE=0 to skip Verifiable+flate2+SCRAM+lz4+checkout parks land)"
  echo "  Actions-only alternate: bash scripts/owner-dispatch-first-publish.sh (owner machine; agents 403)."
  exit 0
fi

echo "== registry token =="
if ! bash scripts/check-registry-token.sh; then
  echo "check-installable-preflight: FAIL — CARGO_REGISTRY_TOKEN set but crates.io rejected it" >&2
  exit 1
fi

echo "== post-cut parks stack (token present) =="
# When TOKEN was unset we only soft-warned; with TOKEN set, stale parks would fail the cut.
if ! bash scripts/check-post-cut-parks-stack.sh; then
  echo "check-installable-preflight: FAIL — parks lag tip; refresh before cut" >&2
  echo "  bash scripts/refresh-post-cut-parks.sh" >&2
  echo "  (owner-finish-installable also auto-refreshes, then restores main)" >&2
  exit 1
fi

echo "check-installable-preflight: READY — token present; run bash scripts/owner-finish-installable.sh"
exit 0
