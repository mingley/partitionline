#!/usr/bin/env bash
# Day-1 owner checklist after partitionline lands on crates.io (WP-0.5).
# Does not publish. Verifies the crate, flips README, prints remaining steps.
#
# Usage:
#   bash scripts/day1-after-publish.sh
#   DRY_RUN=1 bash scripts/day1-after-publish.sh   # one probe, no index wait; README dry-run
#   ATTEMPTS=5 SLEEP_SECS=5 bash scripts/day1-after-publish.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRY_RUN="${DRY_RUN:-0}"
ATTEMPTS="${ATTEMPTS:-30}"
SLEEP_SECS="${SLEEP_SECS:-10}"

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
echo "day1-after-publish: expecting crates.io partitionline ${ver}"
if [[ "$DRY_RUN" == "1" ]]; then
  echo "day1-after-publish: DRY_RUN=1 — single probe, no index wait loop"
  ATTEMPTS=1
  SLEEP_SECS=0
fi

ok=0
for i in $(seq 1 "$ATTEMPTS"); do
  if bash scripts/check-installable.sh >/tmp/pl-day1-installable.log 2>&1; then
    ok=1
    break
  fi
  if [[ "$i" -lt "$ATTEMPTS" ]]; then
    echo "day1-after-publish: waiting for crates.io index (${i}/${ATTEMPTS})..."
    sleep "$SLEEP_SECS"
  fi
done

if [[ "$ok" != "1" ]]; then
  cat /tmp/pl-day1-installable.log >&2 || true
  echo "day1-after-publish: crates.io does not yet have partitionline ${ver}" >&2
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "day1-after-publish: DRY_RUN=1 — would verify crates.io consumer + flip README after publish" >&2
    echo "day1-after-publish: rehearsing README flip only (post-publish-readme DRY_RUN=1)" >&2
    DRY_RUN=1 bash scripts/post-publish-readme.sh
    echo
    echo "day1-after-publish: DRY_RUN complete — publish first, then re-run without DRY_RUN"
    echo "  Preferred cut: bash scripts/owner-finish-installable.sh"
    exit 0
  fi
  echo "day1-after-publish: publish first (docs/RELEASE.md / scripts/owner-publish.sh)." >&2
  exit 1
fi
echo "day1-after-publish: crates.io has partitionline ${ver}"

echo "day1-after-publish: verify adopter crates.io consumer compiles"
if [[ "$DRY_RUN" == "1" ]]; then
  echo "day1-after-publish: DRY_RUN=1 — would run verify-crates-io-consumer.sh"
else
  bash scripts/verify-crates-io-consumer.sh
fi

if [[ "$DRY_RUN" == "1" ]]; then
  DRY_RUN=1 bash scripts/post-publish-readme.sh
else
  bash scripts/post-publish-readme.sh
fi

echo
echo "day1-after-publish: next owner steps"
echo "  1. Review and commit README crates.io dep + status blurb + crates.io/docs.rs badges"
echo "  2. Prefer crates.io Trusted Publishing for later tags (Settings → Trusted"
echo "     Publishing → GitHub: mingley/partitionline, workflow release.yml)."
echo "     Keep Actions secret CARGO_REGISTRY_TOKEN until an OIDC publish succeeds,"
echo "     then remove the long-lived secret."
echo "  3. Confirm docs.rs build for partitionline ${ver}"
echo "  4. Comment on adoption survey #85 that crates.io install works"
echo "  5. Update docs/CIVILIZATION.md WP-0 → done when MSRV CI is also green"
echo "  6. Only then consider partitionline-schema companion (WP-6.3)"
if [[ "$DRY_RUN" == "1" ]]; then
  echo
  echo "day1-after-publish: DRY_RUN complete — no README commit performed"
fi
