#!/usr/bin/env bash
# One-shot owner/agent probe for civilization Installable + Verifiable blockers.
# Does not publish. Exit 0 always (status printer); use check-installable.sh to gate.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
echo "owner-status: partitionline ${ver}"
echo

echo "== Installable =="
if [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "OK  CARGO_REGISTRY_TOKEN is set (len=${#CARGO_REGISTRY_TOKEN})"
else
  echo "BLOCKED  CARGO_REGISTRY_TOKEN unset (export for Cloud Agent + Actions secret)"
fi
# shellcheck source=scripts/lib/crates-io.sh
source "$ROOT/scripts/lib/crates-io.sh"
pl_crates_probe_version "partitionline" "$ver" "partitionline-owner-status/1"
if [[ "$PL_CRATES_PROBE_STATUS" == "present" ]]; then
  echo "OK  crates.io has partitionline ${ver} (${PL_CRATES_PROBE_DETAIL})"
elif [[ "$PL_CRATES_PROBE_STATUS" == "absent" ]]; then
  echo "BLOCKED  crates.io: partitionline ${ver} does not exist yet (need publish; ${PL_CRATES_PROBE_DETAIL})"
else
  echo "WARN  crates.io probe inconclusive (${PL_CRATES_PROBE_DETAIL})"
fi

echo
echo "== Verifiable (GitHub Actions) =="
if ! command -v gh >/dev/null 2>&1; then
  echo "SKIP  gh CLI not available"
else
  queued="$(gh run list --status queued --limit 50 --json databaseId --jq 'length' 2>/dev/null || echo "?")"
  echo "queued runs (repo, up to 50): ${queued}"
  if [[ "$queued" != "?" && "$queued" != "0" ]]; then
    echo "  owner: bash scripts/owner-cancel-stuck-runs.sh   # or DRY_RUN=1 first"
    bash scripts/check-actions-hygiene.sh || true
  fi
  echo "-- main (latest 2) --"
  gh run list --branch main --limit 2 2>/dev/null || echo "WARN  gh run list main failed"
  echo "-- tip branch (HEAD-aware) --"
  branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
  head_sha="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "  HEAD ${head_sha:0:7} on ${branch}"
  # Prefer runs for this exact SHA so a fixed tip is not shadowed by an older
  # empty-job release failure on the same branch name.
  if command -v python3 >/dev/null 2>&1; then
    if gh run list --branch "$branch" --limit 30 \
      --json databaseId,status,conclusion,name,event,headSha,displayTitle,createdAt \
      >/tmp/pl-owner-tip-runs.json 2>/dev/null; then
      HEAD_SHA="$head_sha" python3 - <<'PY' || echo "WARN  could not interpret tip run list"
import json, os
head = os.environ.get("HEAD_SHA", "")
try:
    runs = json.load(open("/tmp/pl-owner-tip-runs.json"))
except Exception:
    print("WARN  could not parse gh run list JSON")
    raise SystemExit(0)
if not isinstance(runs, list):
    print("WARN  unexpected gh run list JSON shape")
    raise SystemExit(0)
match = [r for r in runs if r.get("headSha") == head]
if match:
    print(f"  runs for HEAD ({len(match)}):")
    for r in match[:5]:
        print(
            f"    {r.get('status')}\t{r.get('conclusion') or ''}\t"
            f"{r.get('name')}\t{r.get('event')}\t{r.get('databaseId')}\t"
            f"{(r.get('displayTitle') or '')[:60]}"
        )
else:
    print("  no Actions runs for this HEAD yet (tip auto-CI may be disabled)")
    print("  latest on branch (may be older SHAs):")
    for r in runs[:2]:
        sha = (r.get("headSha") or "")[:7]
        print(
            f"    {sha}\t{r.get('status')}\t{r.get('conclusion') or ''}\t"
            f"{r.get('name')}\t{r.get('event')}\t{r.get('databaseId')}"
        )
PY
    else
      echo "WARN  gh run list ${branch} failed"
    fi
  else
    gh run list --branch "$branch" --limit 2 2>/dev/null || echo "WARN  gh run list ${branch} failed"
  fi
fi

echo
echo "== Local trust snapshot =="
echo "  tip: $(git rev-parse --short HEAD 2>/dev/null || echo unknown) on $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
if [[ -n "$(git status --porcelain 2>/dev/null || true)" ]]; then
  echo "  working tree: dirty"
else
  echo "  working tree: clean"
fi
if bash scripts/check-adopter-pin.sh >/tmp/pl-owner-adopter-pin.log 2>&1; then
  echo "  adopter pin: $(tail -1 /tmp/pl-owner-adopter-pin.log)"
else
  echo "  adopter pin: FAIL (see /tmp/pl-owner-adopter-pin.log)"
  tail -5 /tmp/pl-owner-adopter-pin.log | sed 's/^/    /' || true
fi
if bash scripts/ci-branch-lite.sh >/tmp/pl-owner-branch-lite.log 2>&1; then
  echo "  branch-lite (local Actions mirror): ok"
else
  echo "  branch-lite (local Actions mirror): FAIL (see /tmp/pl-owner-branch-lite.log)"
fi
echo
if bash scripts/check-merge-ready.sh >/tmp/pl-owner-merge-ready.log 2>&1; then
  echo "  merge-ready: $(grep -E '^check-merge-ready: OK' /tmp/pl-owner-merge-ready.log | tail -1)"
else
  echo "  merge-ready: FAIL (see /tmp/pl-owner-merge-ready.log)"
  grep -E '^(FAIL|WARN|check-merge-ready:)' /tmp/pl-owner-merge-ready.log | tail -12 | sed 's/^/    /' || true
fi

echo
echo "== Civilization bars =="
if bash scripts/audit-civilization-bars.sh >/tmp/pl-owner-bars.log 2>&1; then
  echo "  bars: $(tail -1 /tmp/pl-owner-bars.log)"
else
  echo "  bars: NOT COMPLETE (see summary)"
  grep -E '^(PASS|PARTIAL|BLOCKED|FAIL|audit-civilization-bars:)' /tmp/pl-owner-bars.log \
    | grep -E 'BLOCKED|FAIL|audit-civilization-bars:' | tail -8 | sed 's/^/    /' || true
fi

echo "owner-status: next"
echo "  0. One-shot checklist: bash scripts/owner-unblock.sh"
echo "  1. Set CARGO_REGISTRY_TOKEN (Cloud Agent env + Actions secret)"
echo "  2. Preferred once token is in-env (bypasses starved Actions):"
echo "       bash scripts/owner-finish-installable.sh"
echo "  3. Or stepwise: cancel stuck runs, merge civilization → main, then:"
echo "       bash scripts/owner-cancel-stuck-runs.sh   # owner machine; agents 403"
echo "       bash scripts/owner-cut-release.sh         # tag → publish → day1"
echo "  4. bash scripts/check-installable.sh   # must exit 0"
echo "  5. crates.io → Trusted Publishing → release.yml"
