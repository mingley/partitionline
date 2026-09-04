#!/usr/bin/env bash
# Owner-only crates.io publish for partitionline 0.1.x (WP-0.5).
# Agents without CARGO_REGISTRY_TOKEN must stop after ci-publish-ready.sh.
#
# Preferred path after merge to main:
#   1. Set CARGO_REGISTRY_TOKEN (and GitHub Actions secret of the same name)
#   2. bash scripts/ci-publish-ready.sh
#   3. bash scripts/owner-publish.sh          # publishes from a clean tree
#   4. git tag "v$(cargo pkgid | sed 's/.*#//')" && git push origin "v..."
#      (or let .github/workflows/release.yml publish from the tag instead)
#
# Prefer the tag → Actions path in docs/RELEASE.md when Actions runners work.
# Use this script only for a manual publish from a clean main checkout.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "owner-publish: CARGO_REGISTRY_TOKEN is not set" >&2
  echo "owner-publish: create a crates.io token and export it, then re-run." >&2
  exit 1
fi

branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$branch" != "main" && "${ALLOW_NON_MAIN_PUBLISH:-}" != "1" ]]; then
  echo "owner-publish: refuse to publish from branch '$branch' (need main)." >&2
  echo "owner-publish: merge first, or set ALLOW_NON_MAIN_PUBLISH=1 only if intentional." >&2
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "owner-publish: working tree is dirty; commit or stash first." >&2
  exit 1
fi

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
echo "owner-publish: publishing partitionline ${ver}"

bash scripts/ci-publish-ready.sh
cargo publish

echo "owner-publish: published partitionline ${ver}"
echo "owner-publish: next — tag v${ver}, push tag, flip README to crates.io dep,"
echo "  confirm https://crates.io/crates/partitionline"
