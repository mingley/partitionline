#!/usr/bin/env bash
# Probe whether origin/main's HEAD has a terminal green CI conclusion.
# Used before Installable cut so we do not publish over a red Verifiable tip.
#
# Outcome mapping:
#   Outcome      Exit (REQUIRE=0)  Exit (REQUIRE=1)  Stderr / Stdout
#   ----------   ----------------  ----------------  -------------------------------------------------------------
#   success      0                 0                 stdout: check-main-ci: OK — [outcome=success] main HEAD CI is green
#   failed       1                 1                 stderr: check-main-ci: FAIL — [outcome=failed] failed: ...
#   cancelled    1                 1                 stderr: check-main-ci: FAIL — [outcome=cancelled] cancelled: ...
#   pending      2                 1                 stderr: check-main-ci: INCONCLUSIVE/FAIL — [outcome=pending] pending: ...
#   missing      2                 1                 stderr: check-main-ci: WARN/FAIL — [outcome=missing] missing: ...
#   wrong_sha    2                 1                 stderr: check-main-ci: FAIL — [outcome=wrong_sha] wrong SHA: ...
#
# Exit codes:
#   0  — latest completed CI on main HEAD is success
#   1  — completed CI failed / cancelled / non-success conclusion / rejected when REQUIRE_MAIN_CI=1
#   2  — inconclusive (no runs yet, still in progress, missing, wrong SHA when REQUIRE_MAIN_CI=0)
#
# Env:
#   REQUIRE_MAIN_CI=1  treat inconclusive / missing (exit 2) as failure (exit 1)
#   MAIN_BRANCH        default main
#   CHECK_SHA          optional exact commit to probe (default: origin/$MAIN_BRANCH).
#                      Use for tag publish so the gate is the release SHA, not "latest main".
#   GH_RUNS_JSON       optional path to fixture JSON file (or colon-separated paths).
#                      When set, skips git fetch and gh CLI, operating offline for tests.
#   GH_RUN_LIMIT       optional limit for gh run list (default: 50)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MAIN_BRANCH="${MAIN_BRANCH:-main}"
REQUIRE_MAIN_CI="${REQUIRE_MAIN_CI:-0}"

if [[ -z "${GH_RUNS_JSON:-}" ]]; then
  git fetch origin "${MAIN_BRANCH}" --quiet 2>/dev/null || true
fi

if [[ -n "${CHECK_SHA:-}" ]]; then
  main_sha="$(git rev-parse -q --verify "${CHECK_SHA}^{commit}" 2>/dev/null || true)"
  if [[ -z "$main_sha" && "${CHECK_SHA}" =~ ^[0-9a-fA-F]{7,40}$ ]]; then
    main_sha="${CHECK_SHA}"
  fi
else
  main_sha="$(git rev-parse -q --verify "origin/${MAIN_BRANCH}^{commit}" 2>/dev/null || true)"
fi
if [[ -z "$main_sha" ]]; then
  echo "check-main-ci: FAIL — [outcome=wrong_sha] wrong SHA: cannot resolve ${CHECK_SHA:-origin/${MAIN_BRANCH}}" >&2
  if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
    exit 1
  fi
  exit 2
fi

echo "check-main-ci: sha=${main_sha:0:7} (branch=${MAIN_BRANCH}${CHECK_SHA:+ check_sha=${CHECK_SHA:0:7}})"

if [[ -n "${GH_RUNS_JSON:-}" ]]; then
  input_json="$GH_RUNS_JSON"
else
  if ! command -v gh >/dev/null 2>&1; then
    echo "check-main-ci: SKIP — [outcome=missing] missing: gh CLI not available" >&2
    if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
      exit 1
    fi
    exit 2
  fi

  tmp="$(mktemp)"
  trap 'rm -f "$tmp"' EXIT
  limit="${GH_RUN_LIMIT:-50}"
  if ! gh run list --branch "$MAIN_BRANCH" --limit "$limit" \
    --json databaseId,status,conclusion,headSha,name,workflowName,event,displayTitle,createdAt,attempt \
    >"$tmp" 2>/dev/null; then
    echo "check-main-ci: WARN — [outcome=missing] missing: gh run list failed" >&2
    if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
      exit 1
    fi
    exit 2
  fi

  if [[ ! -s "$tmp" || "$(cat "$tmp")" == "[]" ]]; then
    echo "check-main-ci: WARN — [outcome=missing] missing: no Actions runs listed for ${MAIN_BRANCH}" >&2
    if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
      exit 1
    fi
    exit 2
  fi
  input_json="$tmp"
fi

HEAD_SHA="$main_sha" REQUIRE_MAIN_CI="$REQUIRE_MAIN_CI" python3 - "$input_json" <<'PY'
import json, os, sys

paths = sys.argv[1].split(":")
head = os.environ["HEAD_SHA"]
require = os.environ.get("REQUIRE_MAIN_CI", "0") == "1"

all_runs = []
for p in paths:
    if not p:
        continue
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        sys.stderr.write(f"check-main-ci: WARN — [outcome=missing] could not parse gh JSON from {p} ({e})\n")
        sys.exit(1 if require else 2)

    if isinstance(data, dict):
        if "pages" in data and isinstance(data["pages"], list):
            for page in data["pages"]:
                if isinstance(page, list):
                    all_runs.extend(page)
                elif isinstance(page, dict):
                    all_runs.append(page)
        elif "workflow_runs" in data and isinstance(data["workflow_runs"], list):
            all_runs.extend(data["workflow_runs"])
        else:
            sys.stderr.write(f"check-main-ci: WARN — [outcome=missing] unexpected gh JSON object shape in {p}\n")
            sys.exit(1 if require else 2)
    elif isinstance(data, list):
        if data and isinstance(data[0], list):
            for page in data:
                if isinstance(page, list):
                    all_runs.extend(page)
        else:
            all_runs.extend(data)
    else:
        sys.stderr.write(f"check-main-ci: WARN — [outcome=missing] unexpected gh JSON shape in {p}\n")
        sys.exit(1 if require else 2)

if not all_runs:
    sys.stderr.write("check-main-ci: WARN — [outcome=missing] missing: no Actions runs found\n")
    sys.exit(1 if require else 2)

def sha_matches(run_sha, target_sha):
    if not run_sha or not target_sha:
        return False
    r = str(run_sha).strip().lower()
    t = str(target_sha).strip().lower()
    if len(r) == 40 and len(t) == 40:
        return r == t
    if len(t) >= 7 and r.startswith(t):
        return True
    if len(r) >= 7 and t.startswith(r):
        return True
    return False

match = [r for r in all_runs if isinstance(r, dict) and sha_matches(r.get("headSha"), head)]
if not match:
    sys.stderr.write(
        f"check-main-ci: FAIL — [outcome=wrong_sha] wrong SHA: runs exist for other commits, but no runs found for HEAD {head[:7]}\n"
    )
    sys.exit(1 if require else 2)

def is_ci_workflow(r):
    name = (r.get("name") or "").strip().lower()
    wf_name = (r.get("workflowName") or "").strip().lower()
    return name in ("ci", "ci.yml") or wf_name in ("ci", "ci.yml")

ci = [r for r in match if is_ci_workflow(r)]
if not ci:
    non_ci = sorted(set(r.get("name") or r.get("workflowName") or "unnamed" for r in match))
    non_ci_desc = ", ".join(non_ci)
    sys.stderr.write(
        f"check-main-ci: FAIL — [outcome=missing] missing: no CI workflow run found for HEAD {head[:7]} (found non-CI runs: {non_ci_desc})\n"
    )
    sys.exit(1 if require else 2)

def run_sort_key(r):
    created = r.get("createdAt") or r.get("startedAt") or r.get("updatedAt") or ""
    attempt = r.get("attempt") or 1
    if not isinstance(attempt, int):
        try:
            attempt = int(attempt)
        except (ValueError, TypeError):
            attempt = 1
    db_id = r.get("databaseId") or r.get("id") or 0
    if not isinstance(db_id, int):
        try:
            db_id = int(db_id)
        except (ValueError, TypeError):
            db_id = 0
    return (created, attempt, db_id)

ci.sort(key=run_sort_key, reverse=True)
run = ci[0]

status = (run.get("status") or "").strip().lower()
conclusion = (run.get("conclusion") or "").strip().lower()
rid = run.get("databaseId") or run.get("id") or "unknown"
attempt = run.get("attempt") or 1
title = (run.get("displayTitle") or run.get("name") or "")[:70]
print(f"check-main-ci: run {rid} (attempt {attempt}) status={status} conclusion={conclusion or '-'} — {title}")

if status != "completed":
    prefix = "FAIL" if require else "INCONCLUSIVE"
    sys.stderr.write(
        f"check-main-ci: {prefix} — [outcome=pending] pending: CI run {rid} still running/queued for HEAD {head[:7]} (status={status})\n"
    )
    sys.exit(1 if require else 2)

if conclusion == "success":
    print(f"check-main-ci: OK — [outcome=success] main HEAD CI is green (run {rid})")
    sys.exit(0)

if conclusion == "cancelled":
    sys.stderr.write(
        f"check-main-ci: FAIL — [outcome=cancelled] cancelled: main HEAD CI run {rid} was cancelled\n"
    )
    sys.exit(1)

sys.stderr.write(
    f"check-main-ci: FAIL — [outcome=failed] failed: main HEAD CI run {rid} conclusion={conclusion}\n"
)
sys.exit(1)
PY
