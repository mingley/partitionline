#!/usr/bin/env bash
# One-screen owner ask for the Installable blocker: crates.io publish-new token.
# Does not publish. Safe anytime. Prefer this over scrolling owner-unblock when the
# only remaining gap is CARGO_REGISTRY_TOKEN.
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
echo "Installable is READY_EXCEPT_TOKEN. Everything else for the first cut is rehearsed."
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
    ;;
  *)
    echo "STATUS: CARGO_REGISTRY_TOKEN is set but crates.io rejected it."
    tail -8 /tmp/pl-request-token.log | sed 's/^/  /' || true
    echo "Recreate with publish-new (+ publish-update), replace the Secret, restart."
    ;;
esac
