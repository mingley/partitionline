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
# Presence alone is not enough: publish-update-only / garbage tokens must not look OK.
tok_rc=0
bash scripts/check-registry-token.sh >/tmp/pl-owner-token.log 2>&1 || tok_rc=$?
case "$tok_rc" in
  0)
    echo "OK  CARGO_REGISTRY_TOKEN accepted by crates.io for publish-new auth (len=${#CARGO_REGISTRY_TOKEN})"
    ;;
  2)
    echo "BLOCKED  CARGO_REGISTRY_TOKEN unset (export for Cloud Agent + Actions secret)"
    echo "         First cut needs publish-new (+ publish-update); publish-update alone cannot create the crate."
    # Surface Secrets typos that leave Installable stuck (length-only; never print values).
    if grep -q 'misnamed token' /tmp/pl-owner-token.log 2>/dev/null; then
      grep 'misnamed token\|Rename to exactly' /tmp/pl-owner-token.log | sed 's/^/         /' || true
    fi
    ;;
  *)
    echo "BLOCKED  CARGO_REGISTRY_TOKEN rejected by crates.io (see scripts/check-registry-token.sh)"
    tail -5 /tmp/pl-owner-token.log 2>/dev/null | sed 's/^/         /' || true
    ;;
esac
# Compact preflight verdict (does not fail owner-status).
pf=0
bash scripts/check-installable-preflight.sh >/tmp/pl-owner-preflight.log 2>&1 || pf=$?
preflight_already=0
if [[ "$pf" -eq 0 ]]; then
  echo "  preflight: $(grep -E 'READY_EXCEPT_TOKEN|READY —' /tmp/pl-owner-preflight.log | tail -1)"
elif [[ "$pf" -eq 2 ]]; then
  preflight_already=1
  echo "  preflight: ALREADY_INSTALLABLE"
elif [[ "$pf" -eq 3 ]]; then
  echo "  preflight: READY_EXCEPT_TOKEN (main CI still running — see check-main-ci)"
else
  echo "  preflight: not ready (exit ${pf}); bash scripts/check-installable-preflight.sh"
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
  echo "-- main HEAD CI probe --"
  bash scripts/check-main-ci.sh || true
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
# Full tip Verifiable + bars take many minutes (broker chain). While Installable
# waits only on CARGO_REGISTRY_TOKEN, default to a fast snapshot so the owner ask
# path stays usable. OWNER_STATUS_FULL=1 restores the heavy local mirror.
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" && "${OWNER_STATUS_FULL:-0}" != "1" ]]; then
  echo "  branch-lite (local Actions mirror): skipped (TOKEN unset; OWNER_STATUS_FULL=1 to run)"
else
  bl_rc=0
  bash scripts/ci-branch-lite.sh >/tmp/pl-owner-branch-lite.log 2>&1 || bl_rc=$?
  if [[ "$bl_rc" -eq 0 ]]; then
    if grep -qE 'ok with PARTIAL|PARTIAL —' /tmp/pl-owner-branch-lite.log; then
      echo "  branch-lite (local Actions mirror): PARTIAL (exit 0 with soft notes; see /tmp/pl-owner-branch-lite.log)"
      grep -E 'ok with PARTIAL|PARTIAL —' /tmp/pl-owner-branch-lite.log | tail -3 | sed 's/^/    /' || true
    else
      echo "  branch-lite (local Actions mirror): ok"
    fi
  elif [[ "$bl_rc" -eq 2 ]] && grep -qE 'PARTIAL —' /tmp/pl-owner-branch-lite.log; then
    # Post-Installable tip proxy fail-closes PARTIAL/2 — not a hard FAIL.
    echo "  branch-lite (local Actions mirror): PARTIAL (exit 2; Installable met, post-cut re-entry; see /tmp/pl-owner-branch-lite.log)"
    grep -E 'PARTIAL —' /tmp/pl-owner-branch-lite.log | tail -3 | sed 's/^/    /' || true
  else
    echo "  branch-lite (local Actions mirror): FAIL (rc=${bl_rc}; see /tmp/pl-owner-branch-lite.log)"
  fi
fi
echo
if bash scripts/check-merge-ready.sh >/tmp/pl-owner-merge-ready.log 2>&1; then
  echo "  merge-ready: $(grep -E '^check-merge-ready: OK' /tmp/pl-owner-merge-ready.log | tail -1)"
else
  echo "  merge-ready: FAIL (see /tmp/pl-owner-merge-ready.log)"
  grep -E '^(FAIL|WARN|check-merge-ready:)' /tmp/pl-owner-merge-ready.log | tail -12 | sed 's/^/    /' || true
fi

if bash scripts/check-post-cut-parks-stack.sh >/tmp/pl-owner-parks-stack.log 2>&1; then
  echo "  post-cut parks stack: ok"
else
  echo "  post-cut parks stack: FAIL (see /tmp/pl-owner-parks-stack.log)"
  echo "  fix: bash scripts/refresh-post-cut-parks.sh   # tip→Verifiable→SCRAM→lz4→checkout"
fi
# Stack ≠ landed. Live parks-on-main probe (same honesty as handoff).
# Pre-Installable: parks intentionally stay off main — do not label that PARTIAL
# (looks like a cut blocker). Post-Installable: PARTIAL + handoff re-entry.
parks_main_rc=0
bash scripts/check-parks-on-main.sh >/tmp/pl-owner-parks-main.log 2>&1 || parks_main_rc=$?
if [[ "$parks_main_rc" -eq 0 ]]; then
  echo "  parks on main: ok"
elif [[ "$parks_main_rc" -eq 2 ]]; then
  if bash scripts/check-installable.sh >/dev/null 2>&1; then
    echo "  parks on main: PARTIAL — Installable OK but parks not on main"
    grep -E '^  - |Re-enter:' /tmp/pl-owner-parks-main.log | sed 's/^/    /' || true
    echo "    LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh"
  else
    echo "  parks on main: pending (expected pre-Installable; land after crates.io cut)"
    echo "    tip⊆parks stack is the pre-cut gate; do not FF parks onto main before 0.1.0"
    echo "    After Installable: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh"
  fi
else
  echo "  parks on main: FAIL rc=${parks_main_rc} (see /tmp/pl-owner-parks-main.log)"
fi
if bash scripts/check-parks-refresh-cut-guards.sh >/tmp/pl-owner-parks-guards.log 2>&1; then
  echo "  parks-refresh cut guards: ok (finish restores main; publish-ready restores caller)"
else
  echo "  parks-refresh cut guards: FAIL (see /tmp/pl-owner-parks-guards.log)"
fi
if MODE=git bash scripts/verify-crates-io-consumer.sh >/tmp/pl-owner-git-adopter.log 2>&1; then
  echo "  git-tag adopter consumer: ok (documented pin cargo-checks)"
else
  echo "  git-tag adopter consumer: FAIL (see /tmp/pl-owner-git-adopter.log)"
fi
if bash scripts/check-trusted-publishing-ready.sh >/tmp/pl-owner-tp.log 2>&1; then
  echo "  trusted-publishing shape: $(grep -E 'OK|INFO|FAIL' /tmp/pl-owner-tp.log | tail -1)"
else
  echo "  trusted-publishing shape: FAIL (see /tmp/pl-owner-tp.log)"
fi

echo
echo "== Civilization bars =="
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" && "${OWNER_STATUS_FULL:-0}" != "1" ]]; then
  echo "  bars: skipped (TOKEN unset; OWNER_STATUS_FULL=1 to run; PRE_PUBLISH=1 bash scripts/audit-civilization-bars.sh)"
elif bash scripts/audit-civilization-bars.sh >/tmp/pl-owner-bars.log 2>&1; then
  echo "  bars: $(tail -1 /tmp/pl-owner-bars.log)"
else
  echo "  bars: NOT COMPLETE (see summary)"
  # Include PARTIAL — soft notes must not vanish when bars exit 2.
  grep -E '^(PASS|PARTIAL|BLOCKED|FAIL|audit-civilization-bars:)' /tmp/pl-owner-bars.log \
    | grep -E 'PARTIAL|BLOCKED|FAIL|audit-civilization-bars:' | tail -12 | sed 's/^/    /' || true
fi

echo "owner-status: next"
if [[ "${preflight_already:-0}" -eq 1 ]]; then
  echo "  crates.io already has this version — do not re-cut."
  echo "  Re-enter post-cut: bash scripts/owner-finish-installable.sh"
  echo "    (or LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh)"
  echo "  Finish uses SKIP_HANDOFF=1 into cut so parks/TP land once after secret sync."
  echo "  If finish/cut exited PARTIAL (parks/TP): re-enter handoff — do not re-publish."
  echo "  If PARTIAL was Actions secret not synced:"
  echo "    gh secret set CARGO_REGISTRY_TOKEN <<< \"\$CARGO_REGISTRY_TOKEN\""
  echo "  If PARTIAL was parks not on main (or parks/TP soft-fail):"
  echo "    LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh"
else
  echo "  0. Token missing? one screen: bash scripts/owner-request-registry-token.sh"
  echo "  0b. Full checklist: bash scripts/owner-unblock.sh"
  echo "  1. Set CARGO_REGISTRY_TOKEN (Cloud Agent env + Actions secret; scope publish-new)"
  # shellcheck source=scripts/lib/cursor-env-secrets-url.sh
  source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/cursor-env-secrets-url.sh"
  echo "     Direct: $PARTITIONLINE_CURSOR_ENV_SECRETS_URL"
  echo "  1b. Rehearse cut path (no publish): bash scripts/check-cut-path.sh"
  echo "  2. Preferred once token is in-env (bypasses starved Actions):"
  echo "       bash scripts/owner-finish-installable.sh"
  echo "       # finish SKIP_HANDOFF=1 into cut, syncs Actions secret, then one handoff"
  echo "       # finish chains post-cut parks land by default after Installable"
  echo "       # MERGE_PARKED_VERIFIABLE=0 to skip"
  echo "       # finish FF tip→main when tip is ahead; do not tip→main thrash beforehand"
  tip_sha="$(git rev-parse origin/dev/civilization-plan-b686 2>/dev/null || true)"
  main_sha="$(git rev-parse origin/main 2>/dev/null || true)"
  if [[ -n "$tip_sha" && -n "$main_sha" && "$tip_sha" != "$main_sha" ]]; then
    echo "  tip/main: tip ${tip_sha:0:7} ≠ main ${main_sha:0:7} (intentional while Installable waits)"
    echo "       DRY_RUN=1 bash scripts/owner-sync-main.sh   # show FF; CONFIRM=1 to push"
    echo "       # refuses while main HEAD CI is running unless ALLOW_BUSY_MAIN=1"
  fi
  echo "  3. Actions-only alternate (first-publish.yml must be on main):"
  echo "       bash scripts/owner-cancel-stuck-runs.sh   # owner machine; agents 403"
  echo "       CONFIRM=1 bash scripts/owner-sync-main.sh # if tip ahead; wait for idle main CI"
  echo "       bash scripts/owner-dispatch-first-publish.sh"
  echo "       # or: Actions → First publish → confirm=publish"
  echo "  4. Or stepwise tag path after cancel + tip on main:"
  echo "       bash scripts/owner-cut-release.sh         # token in-env → local publish (auto)"
  echo "       PUBLISH_LOCAL=0 bash scripts/owner-cut-release.sh  # force tag → Actions"
  echo "  5. bash scripts/check-installable.sh   # must exit 0"
  echo "  6. After Installable (any cut path): bash scripts/owner-post-installable-handoff.sh"
  echo "       LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh   # TP+parks+bars"
  echo "       # If cut/finish exited PARTIAL (parks/TP): re-enter handoff — do not re-cut"
  echo "       # If PARTIAL was Actions secret: gh secret set CARGO_REGISTRY_TOKEN <<< \"\$CARGO_REGISTRY_TOKEN\""
  echo "       # If PARTIAL was parks not on main: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh"
  echo "  7. Or stepwise: bash scripts/owner-enable-trusted-publishing.sh  # after crates.io 0.1.0"
fi
