#!/usr/bin/env bash
# Probe whether origin/main's HEAD has a terminal green CI conclusion.
# Used before Installable cut so we do not publish over a red Verifiable tip.
#
# Exit codes:
#   0  — latest completed CI on main HEAD is success
#   1  — completed CI on main HEAD failed / non-success conclusion
#   2  — inconclusive (no gh, no runs yet, still in progress)
#
# Env:
#   REQUIRE_MAIN_CI=1  treat inconclusive (exit 2) as failure (exit 1)
#   MAIN_BRANCH        default main
#   CHECK_SHA          optional exact commit to probe (default: origin/$MAIN_BRANCH).
#                      Use for tag publish so the gate is the release SHA, not "latest main".
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MAIN_BRANCH="${MAIN_BRANCH:-main}"
REQUIRE_MAIN_CI="${REQUIRE_MAIN_CI:-0}"

git fetch origin "${MAIN_BRANCH}" --quiet 2>/dev/null || true
if [[ -n "${CHECK_SHA:-}" ]]; then
  main_sha="$(git rev-parse "${CHECK_SHA}" 2>/dev/null || true)"
else
  main_sha="$(git rev-parse "origin/${MAIN_BRANCH}" 2>/dev/null || true)"
fi
if [[ -z "$main_sha" ]]; then
  echo "check-main-ci: WARN — cannot resolve ${CHECK_SHA:-origin/${MAIN_BRANCH}}"
  if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
    exit 1
  fi
  exit 2
fi

echo "check-main-ci: sha=${main_sha:0:7} (branch=${MAIN_BRANCH}${CHECK_SHA:+ check_sha=${CHECK_SHA:0:7}})"

if ! command -v gh >/dev/null 2>&1; then
  echo "check-main-ci: SKIP — gh CLI not available"
  if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
    exit 1
  fi
  exit 2
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
if ! gh run list --branch "$MAIN_BRANCH" --limit 30 \
  --json databaseId,status,conclusion,headSha,name,event,displayTitle,createdAt \
  >"$tmp" 2>/dev/null; then
  echo "check-main-ci: WARN — gh run list failed"
  if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
    exit 1
  fi
  exit 2
fi

if [[ ! -s "$tmp" || "$(cat "$tmp")" == "[]" ]]; then
  echo "check-main-ci: WARN — no Actions runs listed for ${MAIN_BRANCH}"
  if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
    exit 1
  fi
  exit 2
fi

HEAD_SHA="$main_sha" REQUIRE_MAIN_CI="$REQUIRE_MAIN_CI" python3 - "$tmp" <<'PY'
import json, os, sys
path = sys.argv[1]
head = os.environ["HEAD_SHA"]
require = os.environ.get("REQUIRE_MAIN_CI", "0") == "1"
try:
    with open(path, encoding="utf-8") as f:
        runs = json.load(f)
except Exception as e:
    print(f"check-main-ci: WARN — could not parse gh JSON ({e})")
    sys.exit(1 if require else 2)
if not isinstance(runs, list):
    print("check-main-ci: WARN — unexpected gh JSON shape")
    sys.exit(1 if require else 2)

match = [r for r in runs if r.get("headSha") == head]
ci = [r for r in match if (r.get("name") or "").lower() in ("ci", "ci.yml")] or match
if not ci:
    print(f"check-main-ci: WARN — no Actions runs for HEAD {head[:7]} yet")
    sys.exit(1 if require else 2)

run = ci[0]  # gh list is newest-first
status = run.get("status") or ""
conclusion = run.get("conclusion") or ""
rid = run.get("databaseId")
title = (run.get("displayTitle") or run.get("name") or "")[:70]
print(f"check-main-ci: run {rid} status={status} conclusion={conclusion or '-'} — {title}")

if status != "completed":
    print("check-main-ci: INCONCLUSIVE — CI still running/queued for this HEAD")
    sys.exit(1 if require else 2)

if conclusion == "success":
    print("check-main-ci: OK — main HEAD CI is green")
    sys.exit(0)

print(f"check-main-ci: FAIL — main HEAD CI conclusion={conclusion}")
sys.exit(1)
PY
