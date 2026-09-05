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

# Load TOKEN_FILE + normalize whitespace into *this* shell (standalone path;
# owner-finish already prepares, but direct owner-publish must too).
# shellcheck source=scripts/lib/cargo-registry-token.sh
source "$ROOT/scripts/lib/cargo-registry-token.sh"
pl_prepare_cargo_registry_token "owner-publish"

if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "owner-publish: CARGO_REGISTRY_TOKEN is not set" >&2
  echo "owner-publish: create a crates.io token (or set CARGO_REGISTRY_TOKEN_FILE) and re-run." >&2
  echo "  First cut of a NEW crate needs publish-new (+ publish-update)." >&2
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

# KL-08: exact-SHA CI before local publish (soft when REQUIRE_MAIN_CI=0).
if [[ -z "${REQUIRE_MAIN_CI:-}" ]]; then
  REQUIRE_MAIN_CI=1
fi
export REQUIRE_MAIN_CI
ci_rc=0
CHECK_SHA="$(git rev-parse HEAD)" bash scripts/check-main-ci.sh || ci_rc=$?
if [[ "$ci_rc" -eq 1 && "${ALLOW_RED_MAIN:-0}" != "1" ]]; then
  echo "owner-publish: main CI red for this SHA — refusing publish" >&2
  exit 1
fi
if [[ "$ci_rc" -eq 2 && "${REQUIRE_MAIN_CI}" == "1" ]]; then
  echo "owner-publish: REQUIRE_MAIN_CI=1 and CI inconclusive — refusing" >&2
  exit 1
fi

bash scripts/ci-publish-ready.sh
cargo publish

echo "owner-publish: published partitionline ${ver}"
echo "owner-publish: confirm https://crates.io/crates/partitionline"

# Manual publish path: flip day1 adopter docs once the index sees the crate.
# Tag → Actions remains preferred (docs/RELEASE.md); set RUN_DAY1_AFTER_PUBLISH=0 to skip.
if [[ "${RUN_DAY1_AFTER_PUBLISH:-1}" == "1" ]]; then
  echo "owner-publish: running day1-after-publish (README/ADOPTION/guide/migrate flip + installable probe)"
  bash scripts/day1-after-publish.sh
  echo "owner-publish: commit day1 crates.io lines (README + ADOPTION + guide + migrate), then tag if release.yml did not:"
  echo "  git add README.md docs/ADOPTION.md docs/guide.md docs/migrate-from-rdkafka.md && git commit -m \"docs: crates.io ${ver} install pins\""
  echo "  git tag -a v${ver} -m \"partitionline ${ver}\" && git push origin main v${ver}"
else
  echo "owner-publish: next — tag v${ver}, push tag (or rely on release.yml),"
  echo "  bash scripts/day1-after-publish.sh, commit README + ADOPTION + guide + migrate"
fi
