#!/usr/bin/env bash
# Day-1 owner checklist after partitionline lands on crates.io (WP-0.5).
# Does not publish. Verifies the crate, flips README, prints remaining steps.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
echo "day1-after-publish: expecting crates.io partitionline ${ver}"

ok=0
for i in $(seq 1 30); do
  if curl -fsSA 'partitionline-day1/1' \
    "https://crates.io/api/v1/crates/partitionline/${ver}" >/tmp/pl-day1.json; then
    ok=1
    break
  fi
  echo "day1-after-publish: waiting for crates.io index (${i}/30)..."
  sleep 10
done
if [[ "$ok" != "1" ]]; then
  echo "day1-after-publish: crates.io does not yet have partitionline ${ver}" >&2
  echo "day1-after-publish: publish first (docs/RELEASE.md / scripts/owner-publish.sh)." >&2
  exit 1
fi
echo "day1-after-publish: crates.io has partitionline ${ver}"

bash scripts/post-publish-readme.sh

echo
echo "day1-after-publish: next owner steps"
echo "  1. Review and commit README crates.io dependency line"
echo "  2. Ensure GitHub Actions secret CARGO_REGISTRY_TOKEN is set for future tags"
echo "  3. Confirm docs.rs build for partitionline ${ver}"
echo "  4. Comment on adoption survey #85 that crates.io install works"
echo "  5. Update docs/CIVILIZATION.md WP-0 → done when MSRV CI is also green"
echo "  6. Only then consider partitionline-schema companion (WP-6.3)"
