#!/usr/bin/env bash
# Stewardship gate: open Dependabot dep bumps must be covered by post-cut parks.
#
# While Installable waits, tip stays docs/scripts-only. Cargo/Actions bumps land
# via the parks chain after crates.io 0.1.0 — merging Dependabot onto tip/main
# breaks tip-delta and races cancel-in-progress main CI.
#
# This check makes that policy executable: every open Dependabot PR that touches
# Cargo.lock or actions/checkout must map to a known park branch that still
# exists on origin. Unmapped Dependabot PRs fail so stewardship cannot drift.
#
# Usage:
#   bash scripts/check-dependabot-parks-coverage.sh
#   bash scripts/check-dependabot-parks-coverage.sh --self-test
#
# Exit:
#   0  all open Dependabot dep bumps mapped (or none open)
#   1  unmapped open Dependabot PR(s), or park branch missing
#   2  soft skip (gh unavailable / API error) — not a pass under REQUIRE=1
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REPO="${GITHUB_REPOSITORY:-mingley/partitionline}"

# headRefName glob → covering park (must match check-post-cut-parks-stack order).
# Keep in sync with docs/ADOPTION.md "Dependabot vs post-cut parks".
park_for_dependabot_head() {
  local head="$1"
  case "$head" in
    dependabot/cargo/lz4_flex-*)
      echo "dev/lz4-flex-bump-b686"
      ;;
    dependabot/cargo/hmac-*|dependabot/cargo/pbkdf2-*|dependabot/cargo/sha2-*|dependabot/cargo/flate2-*)
      echo "dev/scram-crypto-bumps-b686"
      ;;
    dependabot/github_actions/actions/checkout-*)
      echo "dev/actions-checkout-bump-b686"
      ;;
    *)
      echo ""
      ;;
  esac
}

is_dep_bump_head() {
  local head="$1"
  [[ "$head" == dependabot/cargo/* || "$head" == dependabot/github_actions/* ]]
}

if [[ "${1:-}" == "--self-test" ]]; then
  echo "check-dependabot-parks-coverage: self-test — map known heads"
  got="$(park_for_dependabot_head 'dependabot/cargo/lz4_flex-0.14.0')"
  [[ "$got" == "dev/lz4-flex-bump-b686" ]] || {
    echo "FAIL — lz4_flex map → '$got'" >&2
    exit 1
  }
  got="$(park_for_dependabot_head 'dependabot/cargo/hmac-0.13.0')"
  [[ "$got" == "dev/scram-crypto-bumps-b686" ]] || {
    echo "FAIL — hmac map → '$got'" >&2
    exit 1
  }
  got="$(park_for_dependabot_head 'dependabot/github_actions/actions/checkout-7')"
  [[ "$got" == "dev/actions-checkout-bump-b686" ]] || {
    echo "FAIL — checkout map → '$got'" >&2
    exit 1
  }
  got="$(park_for_dependabot_head 'dependabot/cargo/serde-1.0.0')"
  [[ -z "$got" ]] || {
    echo "FAIL — unknown crate must be unmapped" >&2
    exit 1
  }
  echo "check-dependabot-parks-coverage: self-test OK"
  exit 0
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "check-dependabot-parks-coverage: SKIP — gh CLI not available" >&2
  exit 2
fi

echo "check-dependabot-parks-coverage: probing open Dependabot PRs on ${REPO}"

json="$(gh pr list --repo "$REPO" --state open --limit 50 \
  --json number,title,headRefName,author 2>/tmp/pl-dependabot-parks.err)" || {
  echo "check-dependabot-parks-coverage: SKIP — gh pr list failed" >&2
  sed 's/^/  /' /tmp/pl-dependabot-parks.err >&2 || true
  exit 2
}

# Filter Dependabot author + dep-bump heads (cargo / github_actions).
mapfile -t rows < <(REPO_JSON="$json" python3 - <<'PY'
import json, os
prs = json.loads(os.environ["REPO_JSON"])
for pr in prs:
    author = ((pr.get("author") or {}).get("login") or "").lower()
    head = pr.get("headRefName") or ""
    if author not in {"dependabot", "dependabot[bot]"} and not head.startswith("dependabot/"):
        continue
    if not (head.startswith("dependabot/cargo/") or head.startswith("dependabot/github_actions/")):
        continue
    print(f"{pr.get('number')}\t{head}\t{pr.get('title') or ''}")
PY
)

if [[ "${#rows[@]}" -eq 0 ]]; then
  echo "check-dependabot-parks-coverage: OK — no open Dependabot cargo/Actions bumps"
  exit 0
fi

fail=0
covered=0
for row in "${rows[@]}"; do
  number="${row%%$'\t'*}"
  rest="${row#*$'\t'}"
  head="${rest%%$'\t'*}"
  title="${rest#*$'\t'}"
  park="$(park_for_dependabot_head "$head")"
  if [[ -z "$park" ]]; then
    echo "FAIL  #$number $head — no post-cut park mapping" >&2
    echo "  title: $title" >&2
    echo "  Either park this bump post-cut, or update park_for_dependabot_head in" >&2
    echo "  scripts/check-dependabot-parks-coverage.sh (+ docs/ADOPTION.md)." >&2
    echo "  Do NOT merge onto tip/main while Installable waits (breaks tip-delta)." >&2
    fail=1
    continue
  fi
  git fetch -q origin "$park" 2>/dev/null || true
  if ! git rev-parse "origin/${park}" >/dev/null 2>&1; then
    echo "FAIL  #$number $head → park ${park} missing on origin" >&2
    fail=1
    continue
  fi
  echo "OK  #$number $head → ${park}"
  covered=$((covered + 1))
done

if [[ "$fail" -ne 0 ]]; then
  echo "check-dependabot-parks-coverage: FAIL — unmapped or missing park coverage" >&2
  exit 1
fi

echo "check-dependabot-parks-coverage: OK — ${covered} open Dependabot bump(s) covered by parks"
echo "  After crates.io 0.1.0: bash scripts/owner-land-post-cut-parks.sh then close superseded PRs."
exit 0
