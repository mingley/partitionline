#!/usr/bin/env bash
# Post-Installable: verify release.yml is Trusted-Publishing-shaped and print
# the crates.io owner steps. First publish still needs CARGO_REGISTRY_TOKEN;
# OIDC Trusted Publishing can only be configured after the crate exists.
#
# Usage:
#   bash scripts/check-trusted-publishing-ready.sh
#   REQUIRE_INSTALLABLE=1 bash scripts/check-trusted-publishing-ready.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REQUIRE_INSTALLABLE="${REQUIRE_INSTALLABLE:-0}"
wf=".github/workflows/release.yml"
name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"

echo "check-trusted-publishing-ready: ${name} ${ver}"
fail=0

if [[ ! -f "$wf" ]]; then
  echo "FAIL  missing ${wf}" >&2
  exit 1
fi

check() {
  local label="$1" pattern="$2"
  if grep -qE "$pattern" "$wf"; then
    echo "PASS  ${label}"
  else
    echo "FAIL  ${label}" >&2
    fail=1
  fi
}

check "id-token: write (OIDC)" 'id-token:[[:space:]]*write'
check "crates-io-auth-action" 'crates-io-auth-action'
check "CARGO_REGISTRY_TOKEN fallback" 'CARGO_REGISTRY_TOKEN'
check "final vX.Y.Z tag filter (no RC)" 'tags:'
check "ghost-noop job (branch pushes stay green)" 'ghost-noop'

# shellcheck source=scripts/lib/crates-io.sh
source "$ROOT/scripts/lib/crates-io.sh"
pl_crates_probe_version "$name" "$ver" || true
case "${PL_CRATES_PROBE_STATUS:-}" in
  present)
    echo "PASS  crates.io has ${name} ${ver} — Trusted Publishing can be configured now"
    echo
    echo "Owner next (one-time after first cut):"
    echo "  1. https://crates.io/crates/${name}/settings/trusted-publishing"
    echo "  2. Add GitHub → repository mingley/partitionline → workflow release.yml"
    echo "  3. Keep Actions secret CARGO_REGISTRY_TOKEN until one OIDC publish succeeds"
    echo "  4. Re-tag / next release should prefer OIDC (see docs/RELEASE.md)"
    ;;
  absent)
    echo "INFO  crates.io missing ${name} ${ver} — Trusted Publishing waits on first cut"
    echo "  First publish still needs CARGO_REGISTRY_TOKEN:"
    echo "    bash scripts/owner-finish-installable.sh"
    if [[ "$REQUIRE_INSTALLABLE" == "1" ]]; then
      echo "check-trusted-publishing-ready: FAIL — REQUIRE_INSTALLABLE=1 and crate absent" >&2
      exit 1
    fi
    ;;
  *)
    echo "WARN  crates.io probe inconclusive (${PL_CRATES_PROBE_DETAIL:-unknown})"
    ;;
esac

if [[ "$fail" -ne 0 ]]; then
  echo "check-trusted-publishing-ready: FAIL — fix release.yml shape" >&2
  exit 1
fi
echo "check-trusted-publishing-ready: OK — release.yml is Trusted-Publishing-shaped"
