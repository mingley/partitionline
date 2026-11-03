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
name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"

# Refuse non-final or prerelease version tags (matches release.yml gate).
case "$ver" in
  *-*|*+*)
    echo "owner-publish: refusing prerelease/build version '$ver' — crates.io publish is for final X.Y.Z only." >&2
    exit 1
    ;;
esac
if ! printf '%s' "$ver" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "owner-publish: refusing non-final version '$ver' (need X.Y.Z)." >&2
  exit 1
fi

# KL-08: never cargo-publish a version crates.io already has (owner-cut
# PUBLISH_LOCAL=1 re-entry after 0.1.0 must recover via day1/handoff).
# shellcheck source=scripts/lib/crates-io.sh
source "$ROOT/scripts/lib/crates-io.sh"
pl_crates_probe_version "$name" "$ver" "partitionline-owner-publish/1"
if [[ "${PL_CRATES_PROBE_STATUS}" != "present" && "${PL_CRATES_PROBE_STATUS}" != "absent" ]]; then
  echo "owner-publish: registry unavailable or version status unknown (${PL_CRATES_PROBE_DETAIL}) — refusing publish" >&2
  exit 1
fi
if [[ "${PL_CRATES_PROBE_STATUS}" == "present" ]]; then
  echo "owner-publish: index lists ${name} ${ver} — skipping cargo publish (idempotent; not a re-cut)"
else
  echo "owner-publish: publishing partitionline ${ver}"

  # A real upload must use live, full-profile evidence for this exact SHA.
  # Fixture inputs and profile overrides are useful in isolated tests only.
  ci_rc=0
  env -u GH_RUNS_JSON -u GH_JOBS_JSON -u GH_ARTIFACTS_JSON \
    -u REQUIRED_JOBS -u REQUIRED_ARTIFACTS -u GH_RUN_LIMIT \
    MAIN_BRANCH=main REQUIRE_MAIN_CI=1 REQUIRE_PROFILE_EVIDENCE=1 \
    CHECK_SHA="$(git rev-parse HEAD)" bash scripts/check-main-ci.sh || ci_rc=$?
  if [[ "$ci_rc" -ne 0 ]]; then
    echo "owner-publish: exact-SHA full-profile CI not confirmed (rc=${ci_rc}) — refusing publish" >&2
    exit 1
  fi

  bash scripts/ci-publish-ready.sh
  cargo publish

  echo "owner-publish: upload accepted for partitionline ${ver}; waiting for registry confirmation"
fi

# An index entry prevents a duplicate upload, but does not establish that the
# API also identifies this unyanked version. Recheck both after an upload or
# skipped upload. A successful external consumer remains a separate gate.
confirmed=0
if [[ "${PL_CRATES_IDENTITY_STATUS}" == "present" ]]; then
  confirmed=1
else
  attempts="${WAIT_CRATES_ATTEMPTS:-36}"
  if ! [[ "$attempts" =~ ^[1-9][0-9]*$ ]]; then
    echo "owner-publish: WAIT_CRATES_ATTEMPTS must be a positive integer" >&2
    exit 1
  fi
  for ((i=1; i<=attempts; i++)); do
    pl_crates_probe_version "$name" "$ver" "partitionline-owner-publish/1"
    if [[ "${PL_CRATES_IDENTITY_STATUS}" == "present" ]]; then
      confirmed=1
      break
    fi
    if (( i < attempts )); then sleep 10; fi
  done
fi
if [[ "$confirmed" != "1" ]]; then
  echo "owner-publish: PARTIAL — upload skipped or accepted, but API and index do not both confirm ${name} ${ver} (${PL_CRATES_PROBE_DETAIL}); do not retry upload without rechecking" >&2
  exit 2
fi
echo "owner-publish: API and index confirm ${name} ${ver}; external registry consumer is a separate gate"

# Manual publish path: flip day1 adopter docs once the index sees the crate.
# Tag → Actions remains preferred (docs/RELEASE.md); set RUN_DAY1_AFTER_PUBLISH=0 to skip.
if [[ "${RUN_DAY1_AFTER_PUBLISH:-1}" == "1" ]]; then
  echo "owner-publish: running day1-after-publish (README/ADOPTION/guide/migrate flip + installable probe)"
  bash scripts/day1-after-publish.sh
  echo "owner-publish: commit day1 crates.io lines (README + ADOPTION + guide + migrate), then tag if release.yml did not:"
  echo "  git add README.md docs/ADOPTION.md docs/guide.md docs/migrate-from-rdkafka.md && git commit -m \"docs: crates.io ${ver} install pins\""
  echo "  git tag -a v${ver} -m \"partitionline ${ver}\" && git push origin main v${ver}"
else
  echo "owner-publish: external consumer and day1 handoff are outstanding; next — tag v${ver}, push tag (or rely on release.yml),"
  echo "  bash scripts/day1-after-publish.sh, commit README + ADOPTION + guide + migrate"
fi
