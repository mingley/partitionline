#!/usr/bin/env bash
# Owner-only: cancel stuck queued GitHub Actions runs that starve org runners.
#
# Cloud Agents typically get HTTP 403 on `gh run cancel`. A human with
# Actions write permission should run this when Verifiable is blocked by a
# multi-hour `queued` backlog (main, tip, obsolete RC release jobs, etc.).
#
# Usage:
#   bash scripts/owner-cancel-stuck-runs.sh           # cancel queued runs older than 15m
#   MIN_AGE_MINUTES=5 bash scripts/owner-cancel-stuck-runs.sh
#   DRY_RUN=1 bash scripts/owner-cancel-stuck-runs.sh # print only
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v gh >/dev/null 2>&1; then
  echo "owner-cancel-stuck-runs: gh CLI required" >&2
  exit 1
fi

MIN_AGE_MINUTES="${MIN_AGE_MINUTES:-15}"
DRY_RUN="${DRY_RUN:-0}"
REPO="${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)}"
if [[ -z "$REPO" ]]; then
  echo "owner-cancel-stuck-runs: cannot resolve repo (set GITHUB_REPOSITORY or run from a gh-authenticated clone)" >&2
  exit 1
fi

echo "owner-cancel-stuck-runs: repo=$REPO min_age_minutes=$MIN_AGE_MINUTES dry_run=$DRY_RUN"

# List queued runs as TSV: databaseId, createdAt, workflowName, headBranch, displayTitle
mapfile -t rows < <(gh run list --repo "$REPO" --status queued --limit 50 \
  --json databaseId,createdAt,workflowName,headBranch,displayTitle \
  --jq '.[] | [.databaseId, .createdAt, .workflowName, .headBranch, .displayTitle] | @tsv' 2>/dev/null || true)

if [[ ${#rows[@]} -eq 0 ]]; then
  echo "owner-cancel-stuck-runs: no queued runs"
  exit 0
fi

now_epoch="$(date -u +%s)"
cancelled=0
skipped=0
failed=0

for row in "${rows[@]}"; do
  IFS=$'\t' read -r id created workflow branch title <<<"$row"
  # createdAt is ISO8601; GNU/BSD date both accept -d / -u with care
  created_epoch="$(date -u -d "$created" +%s 2>/dev/null || date -u -j -f "%Y-%m-%dT%H:%M:%SZ" "${created%.*}Z" +%s 2>/dev/null || echo 0)"
  if [[ "$created_epoch" -eq 0 ]]; then
    echo "SKIP  parse createdAt=$created id=$id"
    skipped=$((skipped + 1))
    continue
  fi
  age_min=$(( (now_epoch - created_epoch) / 60 ))
  if [[ "$age_min" -lt "$MIN_AGE_MINUTES" ]]; then
    echo "KEEP  age=${age_min}m < ${MIN_AGE_MINUTES}m  id=$id  $workflow  $branch  $title"
    skipped=$((skipped + 1))
    continue
  fi
  echo "CANCEL age=${age_min}m  id=$id  $workflow  $branch  $title"
  if [[ "$DRY_RUN" == "1" ]]; then
    continue
  fi
  if gh run cancel "$id" --repo "$REPO" 2>/tmp/pl-cancel-err; then
    cancelled=$((cancelled + 1))
  else
    echo "  FAIL: $(tr '\n' ' ' </tmp/pl-cancel-err)" >&2
    failed=$((failed + 1))
  fi
done

echo "owner-cancel-stuck-runs: cancelled=$cancelled kept_or_skipped=$skipped failed=$failed"
if [[ "$failed" -gt 0 ]]; then
  echo "owner-cancel-stuck-runs: cancel requires Actions write (agents often get 403)" >&2
  exit 1
fi
