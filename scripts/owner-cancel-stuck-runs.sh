#!/usr/bin/env bash
# Cancel GitHub Actions runs that are stuck in `queued` longer than STALE_MINUTES.
# Requires repo write access (agent tokens often get 403 — run as repo owner).
#
# Usage:
#   bash scripts/owner-cancel-stuck-runs.sh           # cancel stale queued runs
#   DRY_RUN=1 bash scripts/owner-cancel-stuck-runs.sh # print targets + copy-paste cancel lines
#   STALE_MINUTES=30 bash scripts/owner-cancel-stuck-runs.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

STALE_MINUTES="${STALE_MINUTES:-15}"
NOW_EPOCH="$(date -u +%s)"
CUTOFF=$((NOW_EPOCH - STALE_MINUTES * 60))

need() { command -v "$1" >/dev/null 2>&1 || { echo "need $1" >&2; exit 1; }; }
need gh
need python3

REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
echo "== owner cancel stuck runs: ${REPO} (queued older than ${STALE_MINUTES}m) =="

json="$(gh run list --limit 100 --json databaseId,status,createdAt,displayTitle,headBranch,event,url)"

mapfile -t cancels < <(printf '%s' "$json" | CUTOFF="$CUTOFF" python3 -c '
import json, os, sys
from datetime import datetime
cutoff = int(os.environ["CUTOFF"])
runs = json.load(sys.stdin)
for r in runs:
    if r.get("status") != "queued":
        continue
    created = datetime.fromisoformat(r["createdAt"].replace("Z", "+00:00"))
    if int(created.timestamp()) > cutoff:
        continue
    print("|".join([
        str(r["databaseId"]),
        r.get("headBranch") or "",
        r.get("event") or "",
        (r.get("displayTitle") or "").replace("|", "/"),
        r.get("url") or "",
    ]))
')

if [[ "${#cancels[@]}" -eq 0 ]]; then
  echo "No stale queued runs older than ${STALE_MINUTES}m (among last 100)."
  exit 0
fi

echo "Targets (${#cancels[@]}):"
rc_zombies=0
tip_zombies=0
for line in "${cancels[@]}"; do
  IFS='|' read -r id branch event title url <<<"$line"
  note=""
  if [[ "$branch" =~ (-rc\.|-alpha\.|-beta\.|-pre) ]] && [[ "${title,,}" == *release* || "$event" == "push" ]]; then
    # release.yml on tip is final-tag-only; RC tag pushes that still sit
    # queued are pre-filter zombies starving runners.
    if [[ "$branch" =~ ^v[0-9] ]]; then
      note="  [zombie-rc-tag — safe to cancel; final-only release filter]"
      rc_zombies=$((rc_zombies + 1))
    fi
  fi
  if [[ "$branch" == dev/* ]]; then
    note="  [stale tip run — tip auto-CI disabled; safe to cancel]"
    tip_zombies=$((tip_zombies + 1))
  fi
  echo "  run ${id}  branch=${branch}  event=${event}  ${title}${note}"
  echo "    ${url}"
done
if (( rc_zombies + tip_zombies > 0 )); then
  echo "Note: ${rc_zombies} RC-tag zombie(s), ${tip_zombies} tip-branch stale run(s) — cancelling frees runners for main/release."
fi

emit_cancel_lines() {
  echo "----"
  for line in "${cancels[@]}"; do
    IFS='|' read -r id _ <<<"$line"
    echo "gh run cancel ${id} --repo ${REPO}"
  done
  echo "----"
}

if [[ "${DRY_RUN:-}" == "1" ]]; then
  echo
  echo "DRY_RUN=1 — not cancelling. Copy-paste (owner shell with write access):"
  emit_cancel_lines
  exit 0
fi

failed=0
for line in "${cancels[@]}"; do
  IFS='|' read -r id branch event title url <<<"$line"
  echo "Cancelling ${id} (${branch}/${event})..."
  if gh run cancel "$id"; then
    echo "  cancelled ${id}"
  else
    echo "  FAILED to cancel ${id} (need repo write access?)" >&2
    failed=1
  fi
done

if [[ "$failed" -ne 0 ]]; then
  echo
  echo "Some cancels failed. Copy-paste as repo owner:"
  emit_cancel_lines
  echo "Or open each run URL above → Cancel workflow."
  exit 1
fi

echo "Done. Re-check: gh run list --limit 20"
