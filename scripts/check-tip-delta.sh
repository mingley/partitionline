#!/usr/bin/env bash
# Self-check for scripts/lib/tip-delta.sh (civilization cut/sync trust guard).
#
# Verifies the docs/scripts-only classifier used by owner-sync-main (thrash
# refuse) and owner-finish-installable (PUBLISH_LOCAL safety). Optionally
# reports whether origin/main…tip is docs-only.
#
# Usage:
#   bash scripts/check-tip-delta.sh
#   REPORT_TIP=1 bash scripts/check-tip-delta.sh
#   TIP=dev/civilization-plan-b686 REPORT_TIP=1 bash scripts/check-tip-delta.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# shellcheck source=scripts/lib/tip-delta.sh
source "$ROOT/scripts/lib/tip-delta.sh"

REPORT_TIP="${REPORT_TIP:-1}"
TIP="${TIP:-dev/civilization-plan-b686}"
fail=0

expect_docs() {
  local path="$1"
  if ! pl_tip_path_is_docs_only "$path"; then
    echo "check-tip-delta: FAIL — expected docs-only: ${path}" >&2
    fail=1
  fi
}

expect_code() {
  local path="$1"
  if pl_tip_path_is_docs_only "$path"; then
    echo "check-tip-delta: FAIL — expected non-docs: ${path}" >&2
    fail=1
  fi
}

echo "check-tip-delta: unit checks"
expect_docs "docs/CIVILIZATION.md"
expect_docs "scripts/owner-finish-installable.sh"
expect_docs "scripts/owner-merge-parked-verifiable.sh"
expect_docs "scripts/owner-land-post-cut-parks.sh"
expect_docs "scripts/lib/tip-delta.sh"
expect_docs "CHANGELOG.md"
expect_docs "README.md"
expect_docs ".github/PULL_REQUEST_TEMPLATE.md"
expect_docs ".github/ISSUE_TEMPLATE/adoption.md"
expect_code "src/lib.rs"
expect_code "Cargo.toml"
expect_code "Cargo.lock"
expect_code ".github/workflows/ci.yml"
expect_code "fuzz/fuzz_targets/decode.rs"
expect_code "tests/integration.rs"

# Equal SHAs are not "docs-only" — callers handle tip==main separately.
if pl_tip_delta_is_docs_only HEAD HEAD; then
  echo "check-tip-delta: FAIL — equal SHAs must not classify as docs-only" >&2
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  echo "check-tip-delta: FAIL — classifier unit checks" >&2
  exit 1
fi
echo "check-tip-delta: unit checks OK"

if [[ "$REPORT_TIP" == "1" ]]; then
  git fetch origin main "$TIP" >/dev/null 2>&1 || true
  if git rev-parse "origin/${TIP}" >/dev/null 2>&1 && git rev-parse origin/main >/dev/null 2>&1; then
    main_sha="$(git rev-parse origin/main)"
    tip_sha="$(git rev-parse "origin/${TIP}")"
    echo "check-tip-delta: origin/main=${main_sha:0:7} tip(${TIP})=${tip_sha:0:7}"
    if [[ "$main_sha" == "$tip_sha" ]]; then
      echo "check-tip-delta: tip matches main (empty delta)"
    elif pl_tip_delta_is_docs_only "$main_sha" "$tip_sha"; then
      echo "check-tip-delta: tip delta is docs/scripts-only — PUBLISH_LOCAL after FF is Verifiable-safe"
      git diff --name-only "$main_sha" "$tip_sha" | sed 's/^/  /' || true
    else
      echo "check-tip-delta: tip delta includes non-docs paths — PUBLISH_LOCAL requires green main CI on tip SHA first"
      git diff --name-only "$main_sha" "$tip_sha" | sed 's/^/  /' || true
    fi
  else
    echo "check-tip-delta: note — could not resolve origin/main or origin/${TIP} for tip report"
  fi
fi

echo "check-tip-delta: OK"
exit 0
