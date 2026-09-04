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
code="$(curl -sS -o /tmp/pl-owner-crates.json -w '%{http_code}' \
  -H 'User-Agent: partitionline-owner-status' \
  'https://crates.io/api/v1/crates/partitionline' || true)"
if [[ "$code" == "200" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    python3 - <<'PY'
import json
d=json.load(open("/tmp/pl-owner-crates.json"))
vers=[v.get("num") for v in d.get("versions",[])[:5]]
print("OK  crates.io has partitionline; recent:", ", ".join(vers) or "(none)")
PY
  else
    echo "OK  crates.io has partitionline (HTTP 200)"
  fi
elif [[ "$code" == "404" ]]; then
  echo "BLOCKED  crates.io: partitionline does not exist yet (need publish)"
else
  echo "WARN  crates.io HTTP ${code} (see /tmp/pl-owner-crates.json)"
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
  fi
  echo "-- main (latest 2) --"
  gh run list --branch main --limit 2 2>/dev/null || echo "WARN  gh run list main failed"
  echo "-- civilization tip (latest 2) --"
  branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
  gh run list --branch "$branch" --limit 2 2>/dev/null || echo "WARN  gh run list ${branch} failed"
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
echo "owner-status: next"
echo "  0. One-shot checklist: bash scripts/owner-unblock.sh"
echo "  1. Set CARGO_REGISTRY_TOKEN (Cloud + Actions)"
echo "  2. Restore Actions runners: bash scripts/owner-cancel-stuck-runs.sh"
echo "     (needs Actions write; agents usually get 403 — owner must run it)"
echo "  3. Merge civilization → main, tag v${ver} (docs/RELEASE.md)"
echo "  4. bash scripts/day1-after-publish.sh"
echo "  5. bash scripts/check-installable.sh   # must exit 0"
