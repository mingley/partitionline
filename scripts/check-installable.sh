#!/usr/bin/env bash
# Check that the API and sparse index both identify an unyanked version.
# This registry check does not compile an external consumer or publish a crate.
# An optional argument selects a version; otherwise the manifest version is used.
# Exit 0 for confirmed identity, 3 for confirmed absence, and 1 for an
# inconclusive probe or an error.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=scripts/lib/crates-io.sh
source "$ROOT/scripts/lib/crates-io.sh"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
if [[ "$#" -gt 1 ]]; then
  echo "usage: bash scripts/check-installable.sh [version]" >&2
  exit 1
fi
ver="${1:-$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)}"
if [[ ! "$ver" =~ ^[0-9A-Za-z.+-]+$ ]]; then
  echo "check-installable: invalid version" >&2
  exit 1
fi
ua="partitionline-check-installable/1"

echo "check-installable: probing ${name} ${ver}"
pl_crates_probe_version "$name" "$ver" "$ua"
echo "check-installable: registry_identity=${PL_CRATES_IDENTITY_STATUS} (${PL_CRATES_PROBE_DETAIL})"

token=0
if [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  token=1
  echo "check-installable: CARGO_REGISTRY_TOKEN is set"
else
  echo "check-installable: CARGO_REGISTRY_TOKEN is NOT set"
fi

if [[ "$PL_CRATES_IDENTITY_STATUS" == "present" ]]; then
  echo "check-installable: API and index confirm ${name} ${ver}"
  echo "check-installable: external consumer has not been checked"
  if [[ "$token" == "1" ]]; then
    echo "check-installable: token present for future cuts"
  else
    echo "check-installable: token missing for future cuts"
  fi
  exit 0
fi

if [[ "$PL_CRATES_IDENTITY_STATUS" != "absent" ]]; then
  echo "check-installable: FAIL — could not confirm ${name} ${ver} (${PL_CRATES_PROBE_DETAIL})" >&2
  echo "check-installable: ensure HTTPS to crates.io + index.crates.io; send a User-Agent" >&2
  exit 1
fi

echo "check-installable: FAIL — ${name} ${ver} not on crates.io (Installable bar unmet)" >&2
echo "check-installable: owner path: docs/RELEASE.md + docs/ADOPTION.md (WP-0.5)" >&2
exit 3
