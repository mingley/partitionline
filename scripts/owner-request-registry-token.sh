#!/usr/bin/env bash
# One-screen status for publication credentials.
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
    if grep -qF 'READY — token present' /tmp/pl-request-preflight.log; then
      echo "Preflight reports a token is present; authorization and scope remain unverified."
      echo "Complete the release gates before publication."
      exit 0
    fi
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
    echo "Preflight reports READY_EXCEPT_TOKEN. Token authorization remains unverified."
    echo "Post-cut parks stay off main until after crates.io 0.1.0 (expected; tip⊆parks stack is the pre-cut gate)."
    ;;
  2)
    echo "Registry confirms ${name} ${ver}; the external consumer still needs verification."
    echo "No token is needed to confirm the existing registry version."
    parks_main_rc=0
    bash scripts/check-parks-on-main.sh >/tmp/pl-request-parks-main.log 2>&1 || parks_main_rc=$?
    if [[ "$parks_main_rc" -eq 2 ]]; then
      echo "owner-request-registry-token: PARTIAL — parks not on main yet. Re-enter:" >&2
      echo "  LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh" >&2
      exit 2
    elif [[ "$parks_main_rc" -ne 0 ]]; then
      echo "owner-request-registry-token: FAIL — parks-on-main probe rc=${parks_main_rc}" >&2
      exit 1
    fi
    # shellcheck source=scripts/lib/adopter-docs-shaped.sh
    source "$ROOT/scripts/lib/adopter-docs-shaped.sh"
    if ! pl_adopter_docs_crates_io_shaped; then
      echo "owner-request-registry-token: PARTIAL — registry confirmed but adopter docs still git-shaped" >&2
      echo "  Day1 must flip README + ADOPTION + guide + migrate to crates.io." >&2
      echo "  Re-enter: bash scripts/day1-after-publish.sh" >&2
      echo "  Then: bash scripts/owner-post-installable-handoff.sh" >&2
      exit 2
    fi
    echo "Re-enter handoff if TP/parks still pending: bash scripts/owner-post-installable-handoff.sh"
    exit 0
    ;;
  3)
    echo "Installable is READY_EXCEPT_TOKEN (main CI still running — wait or REQUIRE_MAIN_CI=0)."
    echo "Token ask still valid once CI is green; structural gates otherwise OK."
    echo "Post-cut parks stay off main until after crates.io 0.1.0 (expected pre-Installable)."
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

echo "The candidate version ${name} ${ver} is absent. Local publication needs credentials."
echo
echo "1) Create token:"
echo "   https://crates.io/settings/tokens"
echo "   Use a token authorized to update this existing crate."
echo
echo "2) Inject into this Cloud Agent env (exact name, non-whitespace value):"
echo "   ${PARTITIONLINE_CURSOR_ENV_SECRETS_URL}"
echo "   Name: CARGO_REGISTRY_TOKEN"
echo "   Also add the same name as a GitHub Actions repository secret."
echo
echo "3) Restart / re-run the agent so the shell receives the secret, then:"
echo "   bash scripts/check-registry-token.sh    # exit 3 means present; scope is unverified"
echo "   bash scripts/owner-finish-installable.sh"
echo "   # FF tip→main, cargo publish, day1, post-cut parks land"
echo
echo "Actions-only alternate after the release gates pass:"
echo "   PUBLISH_LOCAL=0 bash scripts/owner-cut-release.sh"
echo
echo "Tracking: https://github.com/mingley/partitionline/issues/86"
echo "Probe now: bash scripts/check-installable-preflight.sh  # expect READY_EXCEPT_TOKEN"
echo

tok_rc=0
bash scripts/check-registry-token.sh >/tmp/pl-request-token.log 2>&1 || tok_rc=$?
case "$tok_rc" in
  3)
    echo "STATUS: CARGO_REGISTRY_TOKEN is present; authorization and scope unverified."
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
    echo "STATUS: CARGO_REGISTRY_TOKEN availability check failed (rc=${tok_rc})."
    tail -8 /tmp/pl-request-token.log | sed 's/^/  /' || true
    exit 1
    ;;
esac
