#!/usr/bin/env bash
# Informational Actions hygiene probe (does not cancel anything).
# Flags stale queued runs that starve runners — especially:
#   - tip (dev/**) CI queued after tip auto-CI was disabled
#   - release.yml runs on prerelease/RC tags (current filter is final-only)
#
# Exit 0 always (status printer). Owner cancel: scripts/owner-cancel-stuck-runs.sh
#
# Usage:
#   bash scripts/check-actions-hygiene.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STALE_MINUTES="${STALE_MINUTES:-15}"
NOW_EPOCH="$(date -u +%s)"
CUTOFF=$((NOW_EPOCH - STALE_MINUTES * 60))

echo "check-actions-hygiene: queued runs older than ${STALE_MINUTES}m"

if ! command -v gh >/dev/null 2>&1; then
  echo "SKIP  gh CLI not available"
  exit 0
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP  python3 not available"
  exit 0
fi

if ! gh run list --status queued --limit 100 \
  --json databaseId,name,headBranch,event,displayTitle,createdAt,url \
  >/tmp/pl-actions-hygiene.json 2>/dev/null; then
  echo "WARN  could not list queued Actions runs"
  exit 0
fi

CUTOFF="$CUTOFF" python3 - <<'PY'
import json, os, re
from datetime import datetime, timezone

cutoff = int(os.environ["CUTOFF"])
try:
    runs = json.load(open("/tmp/pl-actions-hygiene.json"))
except Exception as e:
    print(f"WARN  parse failed: {e}")
    raise SystemExit(0)

if not isinstance(runs, list):
    print("WARN  unexpected run list shape")
    raise SystemExit(0)

rc_re = re.compile(r"(?i)(-rc\.|-alpha\.|-beta\.|-pre)")
stale = []
for r in runs:
    created = r.get("createdAt") or ""
    try:
        ts = datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp()
    except Exception:
        continue
    if ts > cutoff:
        continue
    branch = r.get("headBranch") or ""
    name = (r.get("name") or "").lower()
    kind = "stale-queued"
    if "release" in name and rc_re.search(branch):
        kind = "zombie-rc-release"
    elif branch.startswith("dev/") and ("ci" in name or name in ("ci", "test", "check")):
        kind = "stale-tip-ci"
    elif branch.startswith("dev/"):
        kind = "stale-tip-run"
    stale.append((kind, r, int(ts)))

if not stale:
    print("OK  no stale queued runs in last 100")
    raise SystemExit(0)

by = {}
for kind, r, _ in stale:
    by.setdefault(kind, []).append(r)

labels = {
    "zombie-rc-release": "RC/prerelease release.yml zombies (current filter is final vX.Y.Z only — cancel)",
    "stale-tip-ci": "tip CI still queued after tip auto-CI disable — cancel",
    "stale-tip-run": "other tip-branch queued runs — cancel",
    "stale-queued": "other stale queued runs — cancel",
}
print(f"WARN  {len(stale)} stale queued run(s):")
for kind, items in by.items():
    print(f"  [{kind}] {labels.get(kind, kind)} ({len(items)})")
    for r in items[:8]:
        print(
            f"    run {r.get('databaseId')}\tbranch={r.get('headBranch')}\t"
            f"event={r.get('event')}\t{(r.get('displayTitle') or '')[:70]}"
        )
        if r.get("url"):
            print(f"      {r['url']}")
print("  owner: DRY_RUN=1 bash scripts/owner-cancel-stuck-runs.sh")
print("         bash scripts/owner-cancel-stuck-runs.sh")
PY

echo
echo "check-actions-hygiene: Dependabot label probe"
# .github/dependabot.yml requests label `dependencies`. Missing label does not
# block PRs, but Dependabot comments on every PR and stewardship looks broken.
if gh label list --json name --jq '.[].name' 2>/dev/null | grep -Fxq 'dependencies'; then
  echo "OK  GitHub label 'dependencies' present (Dependabot)"
else
  echo "WARN  GitHub label 'dependencies' missing — Dependabot cannot apply it"
  echo "  Owner one-shot (agents get 403 on label create):"
  echo "    gh label create dependencies \\"
  echo "      --repo mingley/partitionline \\"
  echo "      --description 'Pull requests that update a dependency file' \\"
  echo "      --color 0366d6"
fi

exit 0
