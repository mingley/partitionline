#!/usr/bin/env bash
# One-screen owner ask for the Installable blocker: crates.io publish-new token.
# Does not publish. Safe anytime. Prefer this over scrolling owner-unblock when the
# only remaining gap is CARGO_REGISTRY_TOKEN.
#
# Refuses to claim READY_EXCEPT_TOKEN unless check-installable-preflight agrees
# (stale parks / red merge-ready / prepare honesty must not greenwash the ask).
#
# Usage:
#   bash scripts/owner-request-registry-token.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# shellcheck source=scripts/lib/cursor-env-secrets-url.sh
source "$ROOT/scripts/lib/cursor-env-secrets-url.sh"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"

echo "========================================"
echo " ${name} ${ver} — request CARGO_REGISTRY_TOKEN"
echo "========================================"
echo

# Honesty first: only claim READY_EXCEPT_TOKEN when preflight says so.
pf_rc=0
bash scripts/check-installable-preflight.sh >/tmp/pl-request-preflight.log 2>&1 || pf_rc=$?
case "$pf_rc" in
  0)
    if ! grep -q 'READY_EXCEPT_TOKEN' /tmp/pl-request-preflight.log; then
      echo "owner-request-registry-token: PARTIAL — preflight exit 0 but READY_EXCEPT_TOKEN missing" >&2
      tail -12 /tmp/pl-request-preflight.log | sed 's/^/  /' >&2 || true
      exit 2
    fi
    # Belt-and-suspenders: preflight now exits 1 on stale parks, but refuse the
    # "everything rehearsed" claim if a soft parks note ever regresses into exit 0.
    if grep -qiE 'parks stack stale|also parks' /tmp/pl-request-preflight.log; then
      echo "owner-request-registry-token: refusing READY_EXCEPT_TOKEN claim — parks stack stale" >&2
      echo "  Refresh parks first, then re-run this ask (token alone cannot cut a stale stack)." >&2
      echo "  bash scripts/refresh-post-cut-parks.sh" >&2
      tail -12 /tmp/pl-request-preflight.log | sed 's/^/  /' >&2 || true
      exit 1
    fi
    echo "Installable is READY_EXCEPT_TOKEN. Everything else for the first cut is rehearsed."
    ;;
  2)
    echo "Installable is ALREADY_INSTALLABLE (crates.io has ${name} ${ver})."
    echo "No token ask needed for first cut. Re-enter: bash scripts/owner-post-installable-handoff.sh"
    exit 0
    ;;
  3)
    echo "Installable is READY_EXCEPT_TOKEN (main CI still running — wait or REQUIRE_MAIN_CI=0)."
    echo "Token ask still valid once CI is green; structural gates otherwise OK."
    ;;
  *)
    echo "owner-request-registry-token: refusing READY_EXCEPT_TOKEN claim — preflight exit ${pf_rc}" >&2
    echo "  Fix blockers below, then re-run this ask (do not inject a token into a broken cut path)." >&2
    if grep -qiE 'parks stack stale|refresh-post-cut-parks' /tmp/pl-request-preflight.log; then
      echo "  Parks look stale — refresh before the token ask:" >&2
      echo "  bash scripts/refresh-post-cut-parks.sh" >&2
    fi
    tail -20 /tmp/pl-request-preflight.log | sed 's/^/  /' >&2 || true
    echo "  Probe: bash scripts/check-installable-preflight.sh" >&2
    exit 1
    ;;
esac

echo "crates.io still lacks ${name} ${ver}. The cut needs a crates.io token with"
echo "scope publish-new (+ publish-update). publish-update alone cannot create the crate."
echo
echo "1) Create token:"
echo "   https://crates.io/settings/tokens"
echo "   Enable: publish-new + publish-update"
echo
echo "2) Inject into this Cloud Agent env (exact name, non-whitespace value):"
echo "   ${PARTITIONLINE_CURSOR_ENV_SECRETS_URL}"
echo "   Name: CARGO_REGISTRY_TOKEN"
echo "   Also add the same name as a GitHub Actions repository secret."
echo
echo "3) Restart / re-run the agent so the shell receives the secret, then:"
echo "   bash scripts/check-registry-token.sh    # must exit 0 (publish-new auth)"
echo "   bash scripts/owner-finish-installable.sh"
echo "   # FF tip→main, cargo publish, day1, post-cut parks land"
echo
echo "Actions-only alternate (owner machine; agents 403 on workflow_dispatch):"
echo "   bash scripts/owner-dispatch-first-publish.sh"
echo
echo "Tracking: https://github.com/mingley/partitionline/issues/86"
echo "Probe now: bash scripts/check-installable-preflight.sh  # expect READY_EXCEPT_TOKEN"
echo

tok_rc=0
bash scripts/check-registry-token.sh >/tmp/pl-request-token.log 2>&1 || tok_rc=$?
case "$tok_rc" in
  0)
    echo "STATUS: CARGO_REGISTRY_TOKEN is already accepted for publish-new."
    echo "Next: bash scripts/owner-finish-installable.sh"
    ;;
  2)
    echo "STATUS: CARGO_REGISTRY_TOKEN is unset in this shell."
    # Surface Secrets typos that leave Installable stuck (length-only).
    if grep -q 'misnamed token' /tmp/pl-request-token.log 2>/dev/null; then
      grep 'misnamed token\|Rename to exactly' /tmp/pl-request-token.log | sed 's/^/  /' || true
    fi
    ;;
  *)
    echo "STATUS: CARGO_REGISTRY_TOKEN is set but crates.io rejected it."
    tail -8 /tmp/pl-request-token.log | sed 's/^/  /' || true
    echo "Recreate with publish-new (+ publish-update), replace the Secret, restart."
    ;;
esac
