#!/usr/bin/env bash
# Probe whether origin/main's HEAD has a terminal green CI conclusion.
# Used before Installable cut so we do not publish over a red Verifiable tip.
#
# KL08-02 full-profile gate: a green workflow conclusion alone does not
# release. When job/artifact evidence is available (live `gh` fetch, or
# GH_JOBS_JSON / GH_ARTIFACTS_JSON fixtures), every required CI job and
# matrix cell must be green at the exact release SHA/attempt, the required
# conformance artifacts must exist, and packed-crate consumer evidence (the
# `package` job, which runs scripts/ci-crate-consumer.sh against the packed
# .crate) must be bound to the same run. Missing/skipped cells, absent
# artifacts, and stale or unbound evidence fail the gate — readiness is never
# inferred from an unrelated successful workflow, a template, or a stale
# artifact.
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
#   missing_jobs 2                 1                 stderr: check-main-ci: FAIL — [outcome=missing_jobs] required job absent: ...
#   skipped      2                 1                 stderr: check-main-ci: FAIL — [outcome=skipped] required job skipped/neutral: ...
#   stale        2                 1                 stderr: check-main-ci: FAIL — [outcome=stale] stale/unbound evidence: ...
#   missing_artifacts 2            1                 stderr: check-main-ci: FAIL — [outcome=missing_artifacts] required artifact absent: ...
#
# Exit codes:
#   0  — latest completed CI on main HEAD is success AND the required profile
#        (jobs + artifacts, when evidence is in scope) is fully evidenced
#   1  — completed CI failed / cancelled / non-success conclusion / profile
#        evidence rejected when REQUIRE_MAIN_CI=1
#   2  — inconclusive (no runs yet, still in progress, missing, wrong SHA,
#        missing/skipped cells, stale or absent artifacts when REQUIRE_MAIN_CI=0)
#
# Env:
#   REQUIRE_MAIN_CI=1  treat inconclusive / missing (exit 2) as failure (exit 1)
#   MAIN_BRANCH        default main
#   CHECK_SHA          optional exact commit to probe (default: origin/$MAIN_BRANCH).
#                      Use for tag publish so the gate is the release SHA, not "latest main".
#   GH_RUNS_JSON       optional path to fixture JSON file (or colon-separated paths).
#                      When set, skips git fetch and gh CLI, operating offline for tests.
#   GH_JOBS_JSON       optional path to jobs fixture JSON (or colon-separated paths).
#                      Shape: {"run_id": N, "head_sha": "...", "attempt": A, "jobs": [...]}
#                      (also accepts {"jobs": [...]}, a plain [...] list, or the
#                      paginated list-of-pages / {"pages": [...]} forms).
#                      When set with GH_RUNS_JSON, enables the required-job phase offline.
#   GH_ARTIFACTS_JSON  optional path to artifacts fixture JSON (or colon-separated).
#                      Shape: {"artifacts": [{"name": ..., "expired": false,
#                      "workflow_run": {"id": N, "head_sha": "..."}}]} (also accepts
#                      a plain [...] list or paginated forms). Enables the
#                      required-artifact phase offline.
#   REQUIRE_PROFILE_EVIDENCE=1  in fixture mode (GH_RUNS_JSON set), fail closed
#                      when GH_JOBS_JSON or GH_ARTIFACTS_JSON is absent instead
#                      of skipping that phase. Live mode always requires the
#                      full profile. The release workflow sets this.
#   REQUIRED_JOBS      comma-separated required job names (default: ci.yml
#                      main-push lanes; matrix cells named e.g. "test (1.85)").
#   REQUIRED_ARTIFACTS comma-separated required artifact names
#                      (default: conformance-fixture-artifacts).
#   GH_RUN_LIMIT       optional limit for gh run list (default: 50)
#
# Args:
#   none, or --self-test (offline fixture rehearsal of the gate, including
#   missing/skipped/stale/positive cases; creates no tag and publishes nothing).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

pl_kl08_self_test() {
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/pl-kl08-ci-gate.XXXXXX")"
  # shellcheck disable=SC2064
  trap "rm -rf '$tmp'" EXIT
  SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  OTHER="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  RUN=424242
  python3 - "$tmp" "$SHA" "$OTHER" "$RUN" <<'PY'
import json, sys
tmp, sha, other, run = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
runs = [{"databaseId": run, "name": "ci", "workflowName": "ci",
         "status": "completed", "conclusion": "success", "headSha": sha,
         "createdAt": "2026-09-21T12:00:00Z", "attempt": 1}]
jobs_all = ["fmt", "clippy", "docs", "test (1.85)", "test (stable)", "audit",
            "deny", "package", "features", "fuzz-smoke",
            "broker-smoke (apache/kafka:3.9.1)", "broker-smoke (apache/kafka:4.1.0)",
            "latency-gate", "auth-smoke", "integrity-smoke", "conformance-fixtures"]
jobs = [{"name": n, "status": "completed", "conclusion": "success",
         "runId": run, "headSha": sha} for n in jobs_all]
arts = {"artifacts": [{"name": "conformance-fixture-artifacts", "expired": False,
                       "workflow_run": {"id": run, "head_sha": sha}}]}
open(f"{tmp}/runs.json", "w").write(json.dumps(runs))
open(f"{tmp}/jobs_ok.json", "w").write(
    json.dumps({"run_id": run, "head_sha": sha, "attempt": 1, "jobs": jobs}))
open(f"{tmp}/art_ok.json", "w").write(json.dumps(arts))
PY
  pass_count=0
  check_case() {
    name="$1"; want_rc="$2"; want_token="$3"; shift 3
    out="$("$@" 2>&1)"; rc=$?
    if [[ "$rc" == "$want_rc" && "$out" == *"$want_token"* ]]; then
      echo "self-test: PASS ${name}"
      pass_count=$((pass_count + 1))
    else
      echo "self-test: FAIL ${name} (want rc=${want_rc} token=${want_token}; got rc=${rc})" >&2
      echo "$out" >&2
      return 1
    fi
  }
  base_env="env CHECK_SHA=${SHA} REQUIRE_MAIN_CI=1 GH_RUNS_JSON=${tmp}/runs.json"
  # 1. Complete positive evidence.
  check_case "positive-full-profile" 0 "[outcome=success]" \
    $base_env GH_JOBS_JSON="${tmp}/jobs_ok.json" GH_ARTIFACTS_JSON="${tmp}/art_ok.json" \
    bash scripts/check-main-ci.sh || return 1
  # 2. Missing required job (drop conformance-fixtures) must fail, not pass on workflow green.
  python3 - "$tmp" <<'PY'
import json, sys
tmp = sys.argv[1]
d = json.load(open(f"{tmp}/jobs_ok.json"))
d["jobs"] = [j for j in d["jobs"] if j["name"] != "conformance-fixtures"]
json.dump(d, open(f"{tmp}/jobs_missing.json", "w"))
PY
  check_case "missing-required-job" 1 "[outcome=missing_jobs]" \
    $base_env GH_JOBS_JSON="${tmp}/jobs_missing.json" GH_ARTIFACTS_JSON="${tmp}/art_ok.json" \
    bash scripts/check-main-ci.sh || return 1
  # 3. Skipped matrix cell must fail.
  python3 - "$tmp" <<'PY'
import json, sys
tmp = sys.argv[1]
d = json.load(open(f"{tmp}/jobs_ok.json"))
for j in d["jobs"]:
    if j["name"] == "test (stable)":
        j["conclusion"] = "skipped"
json.dump(d, open(f"{tmp}/jobs_skipped.json", "w"))
PY
  check_case "skipped-matrix-cell" 1 "[outcome=skipped]" \
    $base_env GH_JOBS_JSON="${tmp}/jobs_skipped.json" GH_ARTIFACTS_JSON="${tmp}/art_ok.json" \
    bash scripts/check-main-ci.sh || return 1
  # 4. Failed required job must fail with failed (mutation: conclusion flip is read).
  python3 - "$tmp" <<'PY'
import json, sys
tmp = sys.argv[1]
d = json.load(open(f"{tmp}/jobs_ok.json"))
for j in d["jobs"]:
    if j["name"] == "package":
        j["conclusion"] = "failure"
json.dump(d, open(f"{tmp}/jobs_failed.json", "w"))
PY
  check_case "failed-package-job" 1 "[outcome=failed]" \
    $base_env GH_JOBS_JSON="${tmp}/jobs_failed.json" GH_ARTIFACTS_JSON="${tmp}/art_ok.json" \
    bash scripts/check-main-ci.sh || return 1
  # 5. Stale artifact (bound to another run) must fail.
  python3 - "$tmp" "$OTHER" <<'PY'
import json, sys
tmp, other = sys.argv[1], sys.argv[2]
json.dump({"artifacts": [{"name": "conformance-fixture-artifacts", "expired": False,
          "workflow_run": {"id": 999999, "head_sha": other}}]},
          open(f"{tmp}/art_stale.json", "w"))
PY
  check_case "stale-artifact" 1 "[outcome=stale]" \
    $base_env GH_JOBS_JSON="${tmp}/jobs_ok.json" GH_ARTIFACTS_JSON="${tmp}/art_stale.json" \
    bash scripts/check-main-ci.sh || return 1
  # 6. Expired artifact must fail.
  python3 - "$tmp" "$SHA" "$RUN" <<'PY'
import json, sys
tmp, sha, run = sys.argv[1], sys.argv[2], int(sys.argv[3])
json.dump({"artifacts": [{"name": "conformance-fixture-artifacts", "expired": True,
          "workflow_run": {"id": run, "head_sha": sha}}]},
          open(f"{tmp}/art_expired.json", "w"))
PY
  check_case "expired-artifact" 1 "[outcome=stale]" \
    $base_env GH_JOBS_JSON="${tmp}/jobs_ok.json" GH_ARTIFACTS_JSON="${tmp}/art_expired.json" \
    bash scripts/check-main-ci.sh || return 1
  # 7. Absent conformance artifact must fail.
  echo '{"artifacts": []}' >"${tmp}/art_missing.json"
  check_case "missing-artifact" 1 "[outcome=missing_artifacts]" \
    $base_env GH_JOBS_JSON="${tmp}/jobs_ok.json" GH_ARTIFACTS_JSON="${tmp}/art_missing.json" \
    bash scripts/check-main-ci.sh || return 1
  # 8. Unrelated successful workflow never substitutes (wrong-SHA / non-CI still rejected).
  python3 - "$tmp" "$OTHER" <<'PY'
import json, sys
tmp, other = sys.argv[1], sys.argv[2]
json.dump([{"databaseId": 777001, "name": "release", "workflowName": "release",
            "status": "completed", "conclusion": "success", "headSha": other,
            "createdAt": "2026-09-21T12:00:00Z", "attempt": 1}],
          open(f"{tmp}/runs_other.json", "w"))
PY
  check_case "unrelated-workflow-rejected" 1 "[outcome=wrong_sha]" \
    env CHECK_SHA="${SHA}" REQUIRE_MAIN_CI=1 GH_RUNS_JSON="${tmp}/runs_other.json" \
    bash scripts/check-main-ci.sh || return 1
  # 9. Jobs evidence for another run is stale, not readiness.
  python3 - "$tmp" "$SHA" <<'PY'
import json, sys
tmp, sha = sys.argv[1], sys.argv[2]
d = json.load(open(f"{tmp}/jobs_ok.json"))
d["run_id"] = 999999
json.dump(d, open(f"{tmp}/jobs_otherrun.json", "w"))
PY
  check_case "jobs-for-other-run-stale" 1 "[outcome=stale]" \
    $base_env GH_JOBS_JSON="${tmp}/jobs_otherrun.json" GH_ARTIFACTS_JSON="${tmp}/art_ok.json" \
    bash scripts/check-main-ci.sh || return 1
  echo "self-test: OK — ${pass_count} cases (no tag created, nothing published)"
}

if [[ "${1:-}" == "--self-test" ]]; then
  shift
  if [[ $# -gt 0 ]]; then
    echo "usage: bash scripts/check-main-ci.sh [--self-test]" >&2
    exit 2
  fi
  pl_kl08_self_test
  exit $?
fi
if [[ $# -gt 0 ]]; then
  echo "usage: bash scripts/check-main-ci.sh [--self-test]" >&2
  echo "check-main-ci: refuses args (never publishes)" >&2
  exit 2
fi

MAIN_BRANCH="${MAIN_BRANCH:-main}"
REQUIRE_MAIN_CI="${REQUIRE_MAIN_CI:-0}"
REQUIRE_PROFILE_EVIDENCE="${REQUIRE_PROFILE_EVIDENCE:-0}"

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

selected_out="$(mktemp "${TMPDIR:-/tmp}/pl-kl08-selected.XXXXXX")"
trap 'rm -f "$selected_out"' EXIT
HEAD_SHA="$main_sha" REQUIRE_MAIN_CI="$REQUIRE_MAIN_CI" SELECTED_OUT="$selected_out" python3 - "$input_json" <<'PY'
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
    with open(os.environ["SELECTED_OUT"], "w", encoding="utf-8") as f:
        json.dump({"run_id": rid, "attempt": attempt}, f)
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

# Workflow-level gate passed. Now require the complete release profile:
# every required job/matrix cell green at this SHA/attempt, required
# artifacts present and bound to this run.
selected_run_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["run_id"])' "$selected_out")"
selected_attempt="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["attempt"])' "$selected_out")"

DEFAULT_REQUIRED_JOBS="fmt,clippy,docs,test (1.85),test (stable),audit,deny,package,features,fuzz-smoke,broker-smoke (apache/kafka:3.9.1),broker-smoke (apache/kafka:4.1.0),latency-gate,auth-smoke,integrity-smoke,conformance-fixtures"
DEFAULT_REQUIRED_ARTIFACTS="conformance-fixture-artifacts"
REQUIRED_JOBS="${REQUIRED_JOBS:-$DEFAULT_REQUIRED_JOBS}"
REQUIRED_ARTIFACTS="${REQUIRED_ARTIFACTS:-$DEFAULT_REQUIRED_ARTIFACTS}"

fixture_mode=0
if [[ -n "${GH_RUNS_JSON:-}" ]]; then
  fixture_mode=1
fi

profile_tmp="$(mktemp -d "${TMPDIR:-/tmp}/pl-kl08-profile.XXXXXX")"
trap 'rm -f "$selected_out" ${tmp:-}; rm -rf "$profile_tmp"' EXIT

resolve_evidence() {
  # $1 = fixture env value, $2 = kind (jobs|artifacts), $3 = outfile; sets RESOLVED_PATH or returns 1 (skip) / 2 (fail closed).
  local fixture_val="$1" kind="$2" outfile="$3"
  if [[ -n "$fixture_val" ]]; then
    printf '%s' "$fixture_val" >"${outfile}.paths"
    RESOLVED_PATH="$(cat "${outfile}.paths")"
    return 0
  fi
  if [[ "$fixture_mode" == "1" ]]; then
    if [[ "$REQUIRE_PROFILE_EVIDENCE" == "1" ]]; then
      return 2
    fi
    return 1
  fi
  if ! command -v gh >/dev/null 2>&1; then
    return 2
  fi
  if [[ "$kind" == "jobs" ]]; then
    if gh run view "$selected_run_id" --attempt "$selected_attempt" --json jobs >"$outfile" 2>/dev/null; then
      RESOLVED_PATH="$outfile"
      return 0
    fi
    if gh run view "$selected_run_id" --json jobs >"$outfile" 2>/dev/null; then
      RESOLVED_PATH="$outfile"
      return 0
    fi
    return 2
  fi
  repo_slug="$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)"
  if [[ -z "$repo_slug" ]]; then
    return 2
  fi
  if gh api "repos/${repo_slug}/actions/runs/${selected_run_id}/artifacts?per_page=100" >"$outfile" 2>/dev/null; then
    RESOLVED_PATH="$outfile"
    return 0
  fi
  return 2
}

profile_fail() {
  # $1 = outcome token suffix, $2 = message; exit 1 when required else 2.
  echo "check-main-ci: FAIL — [outcome=$1] $2" >&2
  if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
    exit 1
  fi
  exit 2
}

JOBS_PATHS=""
if resolve_evidence "${GH_JOBS_JSON:-}" jobs "${profile_tmp}/jobs.json"; then
  JOBS_PATHS="$RESOLVED_PATH"
else
  rc=$?
  if [[ "$rc" == "2" ]]; then
    profile_fail "missing_jobs" "missing_jobs: required job evidence unavailable for run ${selected_run_id} (no GH_JOBS_JSON and live jobs fetch failed or profile evidence required)"
  else
    echo "check-main-ci: note: skipping required-job phase (no GH_JOBS_JSON; set REQUIRE_PROFILE_EVIDENCE=1 to require it)"
  fi
fi

ARTS_PATHS=""
if resolve_evidence "${GH_ARTIFACTS_JSON:-}" artifacts "${profile_tmp}/artifacts.json"; then
  ARTS_PATHS="$RESOLVED_PATH"
else
  rc=$?
  if [[ "$rc" == "2" ]]; then
    profile_fail "missing_artifacts" "missing_artifacts: required artifact evidence unavailable for run ${selected_run_id} (no GH_ARTIFACTS_JSON and live artifacts fetch failed or profile evidence required)"
  else
    echo "check-main-ci: note: skipping required-artifact phase (no GH_ARTIFACTS_JSON; set REQUIRE_PROFILE_EVIDENCE=1 to require it)"
  fi
fi

if [[ -z "$JOBS_PATHS" && -z "$ARTS_PATHS" ]]; then
  exit 0
fi

HEAD_SHA="$main_sha" RUN_ID="$selected_run_id" RUN_ATTEMPT="$selected_attempt" \
  REQUIRE_MAIN_CI="$REQUIRE_MAIN_CI" REQUIRED_JOBS="$REQUIRED_JOBS" \
  REQUIRED_ARTIFACTS="$REQUIRED_ARTIFACTS" JOBS_PATHS="$JOBS_PATHS" ARTS_PATHS="$ARTS_PATHS" \
  python3 - <<'PY'
import json, os, sys

head = os.environ["HEAD_SHA"]
run_id = os.environ["RUN_ID"]
require = os.environ.get("REQUIRE_MAIN_CI", "0") == "1"
try:
    attempt = int(os.environ.get("RUN_ATTEMPT") or 1)
except ValueError:
    attempt = 1
required_jobs = [j.strip() for j in os.environ.get("REQUIRED_JOBS", "").split(",") if j.strip()]
required_arts = [a.strip() for a in os.environ.get("REQUIRED_ARTIFACTS", "").split(",") if a.strip()]
jobs_paths = os.environ.get("JOBS_PATHS", "")
arts_paths = os.environ.get("ARTS_PATHS", "")

def fail(outcome, message):
    sys.stderr.write(f"check-main-ci: FAIL — [outcome={outcome}] {message}\n")
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

def load_items(paths, list_keys):
    items = []
    envelope = {}
    for p in paths.split(":"):
        if not p:
            continue
        try:
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            fail("missing", f"missing: could not parse evidence JSON from {p} ({e})")
        if isinstance(data, dict):
            for k in ("run_id", "runId", "workflowRunId", "workflow_run_id"):
                if data.get(k) is not None:
                    envelope.setdefault("run_id", data[k])
            for k in ("head_sha", "headSha"):
                if data.get(k):
                    envelope.setdefault("head_sha", data[k])
            if data.get("attempt") is not None:
                envelope.setdefault("attempt", data["attempt"])
            found = False
            for k in list_keys:
                if isinstance(data.get(k), list):
                    items.extend(data[k])
                    found = True
            if found:
                continue
            if isinstance(data.get("pages"), list):
                for page in data["pages"]:
                    if isinstance(page, list):
                        items.extend(page)
                    elif isinstance(page, dict):
                        items.append(page)
                continue
            fail("missing", f"missing: unexpected evidence JSON object shape in {p}")
        elif isinstance(data, list):
            if data and isinstance(data[0], list):
                for page in data:
                    if isinstance(page, list):
                        items.extend(page)
            else:
                items.extend(data)
        else:
            fail("missing", f"missing: unexpected evidence JSON shape in {p}")
    return items, envelope

def check_binding(envelope, items, id_keys, sha_keys, kind):
    """Bind evidence to the exact release run/SHA/attempt. Fail closed as stale."""
    env_run = envelope.get("run_id")
    env_sha = envelope.get("head_sha")
    env_attempt = envelope.get("attempt")
    if env_run is not None and str(env_run) != str(run_id):
        fail("stale", f"stale: {kind} evidence is for run {env_run}, not release run {run_id} (exact-SHA binding)")
    if env_sha and not sha_matches(env_sha, head):
        fail("stale", f"stale: {kind} evidence is for SHA {str(env_sha)[:7]}, not release SHA {head[:7]}")
    if env_attempt is not None:
        try:
            if int(env_attempt) != attempt:
                fail("stale", f"stale: {kind} evidence is for attempt {env_attempt}, not release attempt {attempt}")
        except (ValueError, TypeError):
            fail("stale", f"stale: {kind} evidence has unparseable attempt {env_attempt!r}")
    if env_run is not None:
        return
    if not items:
        return
    for it in items:
        if not isinstance(it, dict):
            fail("stale", f"stale: {kind} evidence entry is not an object (cannot bind to run {run_id})")
        bound = False
        for k in id_keys:
            if it.get(k) is not None:
                bound = True
                if str(it[k]) != str(run_id):
                    fail("stale", f"stale: {kind} evidence is for run {it[k]}, not release run {run_id} (exact-SHA binding)")
        for k in sha_keys:
            if it.get(k) and not sha_matches(it[k], head):
                fail("stale", f"stale: {kind} evidence is for SHA {str(it[k])[:7]}, not release SHA {head[:7]}")
        if not bound:
            fail("stale", f"stale: {kind} evidence carries no run binding (refusing unbound {kind} for run {run_id})")

if jobs_paths:
    jobs, job_env = load_items(jobs_paths, ["jobs"])
    check_binding(job_env, jobs, ("runId", "run_id", "workflowRunId", "workflow_run_id"), ("headSha", "head_sha"), "job")
    by_name = {}
    for j in jobs:
        if isinstance(j, dict) and j.get("name"):
            by_name.setdefault(j["name"], []).append(j)
    for req in required_jobs:
        entries = by_name.get(req, [])
        if not entries:
            fail("missing_jobs", f"missing_jobs: required job absent: '{req}' has no result on release run {run_id} (missing/skipped cells cannot release)")
        for e in entries:
            status = (e.get("status") or "").strip().lower()
            conclusion = (e.get("conclusion") or "").strip().lower()
            if status != "completed":
                fail("pending", f"pending: required job '{req}' not completed on run {run_id} (status={status or '-'})")
            if conclusion == "success":
                continue
            if conclusion in ("skipped", "neutral"):
                fail("skipped", f"skipped: required job '{req}' concluded {conclusion} on run {run_id} (skipped cells cannot release)")
            if conclusion == "cancelled":
                fail("cancelled", f"cancelled: required job '{req}' was cancelled on run {run_id}")
            fail("failed", f"failed: required job '{req}' conclusion={conclusion or '-'} on run {run_id}")
    print(f"check-main-ci: profile jobs OK — {len(required_jobs)} required jobs green on run {run_id} (attempt {attempt})")

if arts_paths:
    arts, _ = load_items(arts_paths, ["artifacts"])
    for req in required_arts:
        candidates = [a for a in arts if isinstance(a, dict) and a.get("name") == req]
        if not candidates:
            fail("missing_artifacts", f"missing_artifacts: required artifact absent: '{req}' not found on release run {run_id}")
        valid = False
        for a in candidates:
            wr = a.get("workflow_run") or {}
            wid = wr.get("id") if isinstance(wr, dict) else None
            wsha = wr.get("head_sha") if isinstance(wr, dict) else None
            if wid is None or str(wid) != str(run_id):
                continue
            if wsha and not sha_matches(wsha, head):
                continue
            if a.get("expired") is True:
                continue
            valid = True
            break
        if not valid:
            fail("stale", f"stale: required artifact '{req}' has no live copy bound to release run {run_id} SHA {head[:7]} (expired, unbound, or another run's artifact)")
    print(f"check-main-ci: profile artifacts OK — {len(required_arts)} required artifacts bound to run {run_id} SHA {head[:7]}")

print(f"check-main-ci: OK — [outcome=success] release profile complete (run {run_id} attempt {attempt} sha {head[:7]})")
PY
