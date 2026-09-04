#!/usr/bin/env bash
# Probe civilization Installable bar (WP-0.5). Does not publish.
# Exit 0 only when crates.io (API and/or sparse index) serves this crate version
# and (optionally) a token is present for future publishes.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=scripts/lib/crates-io.sh
source "$ROOT/scripts/lib/crates-io.sh"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ua="partitionline-check-installable/1"

echo "check-installable: probing ${name} ${ver}"
pl_crates_probe_version "$name" "$ver" "$ua"
echo "check-installable: status=${PL_CRATES_PROBE_STATUS} (${PL_CRATES_PROBE_DETAIL})"

token=0
if [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  token=1
  echo "check-installable: CARGO_REGISTRY_TOKEN is set"
else
  echo "check-installable: CARGO_REGISTRY_TOKEN is NOT set"
fi

if [[ "$PL_CRATES_PROBE_STATUS" == "present" ]]; then
  echo "check-installable: crates.io has ${name} ${ver}"
  if [[ "$token" == "1" ]]; then
    echo "check-installable: ok (published + token present for future cuts)"
  else
    echo "check-installable: ok (published); token missing for future cuts"
  fi
  exit 0
fi

if [[ "$PL_CRATES_PROBE_STATUS" == "unknown" ]]; then
  echo "check-installable: FAIL — could not confirm ${name} ${ver} (${PL_CRATES_PROBE_DETAIL})" >&2
  echo "check-installable: ensure HTTPS to crates.io + index.crates.io; send a User-Agent" >&2
  exit 1
fi

echo "check-installable: FAIL — ${name} ${ver} not on crates.io (Installable bar unmet)" >&2
echo "check-installable: owner path: docs/RELEASE.md + docs/ADOPTION.md (WP-0.5)" >&2
exit 1
