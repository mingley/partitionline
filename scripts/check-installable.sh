#!/usr/bin/env bash
# Probe civilization Installable bar (WP-0.5). Does not publish.
# Exit 0 only when crates.io serves this crate version and (optionally) a token
# is present for future publishes.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
url="https://crates.io/api/v1/crates/${name}/${ver}"

echo "check-installable: probing ${url}"
code="$(curl -sS -A 'partitionline-check-installable/1' -o /tmp/pl-installable.json -w '%{http_code}' "$url" || true)"
echo "check-installable: http=${code}"

token=0
if [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  token=1
  echo "check-installable: CARGO_REGISTRY_TOKEN is set"
else
  echo "check-installable: CARGO_REGISTRY_TOKEN is NOT set"
fi

if [[ "$code" == "200" ]]; then
  echo "check-installable: crates.io has ${name} ${ver}"
  if [[ "$token" == "1" ]]; then
    echo "check-installable: ok (published + token present for future cuts)"
  else
    echo "check-installable: ok (published); token missing for future cuts"
  fi
  exit 0
fi

echo "check-installable: FAIL — ${name} ${ver} not on crates.io (Installable bar unmet)" >&2
echo "check-installable: owner path: docs/RELEASE.md + docs/ADOPTION.md (WP-0.5)" >&2
exit 1
