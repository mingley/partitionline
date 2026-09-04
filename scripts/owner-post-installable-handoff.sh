#!/usr/bin/env bash
# Post-Installable civilization handoff.
#
# Re-run anytime after crates.io 0.1.0 exists — including when Installable was
# cut via Actions first-publish, or when finish soft-failed Trusted Publishing /
# parks land. Proves Installable, adopter docs, Trusted Publishing checklist,
# post-cut parks readiness, and full civilization bars (no PRE_PUBLISH).
#
# Usage:
#   bash scripts/owner-post-installable-handoff.sh
#   DRY_RUN=1 bash scripts/owner-post-installable-handoff.sh   # rehearse before token/cut
#   LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh  # also land post-cut parks
#   ENABLE_TP=0 bash scripts/owner-post-installable-handoff.sh   # skip Trusted Publishing helper
#   SKIP_DAY1=1 …  # caller already ran day1-after-publish (finish/cut)
#   ALLOW_PARKS_PENDING=1 …  # OK even if parks not yet ancestors of main (defer land)
#   bash scripts/owner-post-installable-handoff.sh --self-test   # preserve + day1 + parks-on-main
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "--self-test" ]]; then
  echo "owner-post-installable-handoff: self-test — LAND_PARKS path must preserve day1 docs"
  if ! grep -qF 'preserve-day1-docs.sh' "$ROOT/scripts/owner-post-installable-handoff.sh"; then
    echo "owner-post-installable-handoff: self-test FAIL — missing preserve-day1-docs.sh source" >&2
    exit 1
  fi
  if ! grep -qF 'pl_day1_docs_begin' "$ROOT/scripts/owner-post-installable-handoff.sh" \
    || ! grep -qF 'pl_day1_docs_end' "$ROOT/scripts/owner-post-installable-handoff.sh"; then
    echo "owner-post-installable-handoff: self-test FAIL — missing pl_day1_docs_begin/end around parks land" >&2
    exit 1
  fi
  if ! grep -qF 'LAND_PARKS' "$ROOT/scripts/owner-post-installable-handoff.sh"; then
    echo "owner-post-installable-handoff: self-test FAIL — LAND_PARKS knob missing" >&2
    exit 1
  fi
  if ! grep -qF 'PARTIAL — Installable OK but parks land failed' "$ROOT/scripts/owner-post-installable-handoff.sh" \
    || ! grep -qF 'exit 2' "$ROOT/scripts/owner-post-installable-handoff.sh"; then
    echo "owner-post-installable-handoff: self-test FAIL — LAND_PARKS failure must PARTIAL/exit 2 (not final OK)" >&2
    exit 1
  fi
  # Actions-alternate / handoff-only re-entry must flip adopter docs (finish already does).
  if ! grep -qF 'day1-after-publish.sh' "$ROOT/scripts/owner-post-installable-handoff.sh"; then
    echo "owner-post-installable-handoff: self-test FAIL — must chain day1-after-publish after Installable" >&2
    exit 1
  fi
  if ! grep -qF 'PARTIAL — Installable OK but adopter docs still git-shaped' "$ROOT/scripts/owner-post-installable-handoff.sh"; then
    echo "owner-post-installable-handoff: self-test FAIL — git-shaped docs after day1 must PARTIAL (not final OK)" >&2
    exit 1
  fi
  if ! grep -qF 'PARTIAL — Installable OK but parks not on main' "$ROOT/scripts/owner-post-installable-handoff.sh"; then
    echo "owner-post-installable-handoff: self-test FAIL — parks-not-on-main must PARTIAL (not final OK)" >&2
    exit 1
  fi
  if ! grep -qF 'REQUIRE_PARKS=1' "$ROOT/scripts/owner-post-installable-handoff.sh"; then
    echo "owner-post-installable-handoff: self-test FAIL — LAND_PARKS land must REQUIRE_PARKS=1" >&2
    exit 1
  fi
  # DRY_RUN must probe parks-on-main (not only stack). Already-Installable DRY_RUN
  # exits PARTIAL/2; pre-token holds exit 0 with a PARTIAL note (like day1).
  if ! grep -qF 'DRY_RUN: parks on main' "$ROOT/scripts/owner-post-installable-handoff.sh" \
    || ! grep -qF 'PARTIAL — parks not on main (DRY_RUN' "$ROOT/scripts/owner-post-installable-handoff.sh" \
    || ! grep -qF 'check-parks-on-main.sh' "$ROOT/scripts/owner-post-installable-handoff.sh"; then
    echo "owner-post-installable-handoff: self-test FAIL — DRY_RUN must probe parks-on-main via check-parks-on-main.sh (Installable→exit 2)" >&2
    exit 1
  fi
  bash "$ROOT/scripts/check-parks-on-main.sh" --self-test
  bash "$ROOT/scripts/lib/preserve-day1-docs.sh" --self-test
  echo "owner-post-installable-handoff: self-test OK — preserve + day1 chain + parks-on-main + fail-closed PARTIAL"
  exit 0
fi

DRY_RUN="${DRY_RUN:-0}"
LAND_PARKS="${LAND_PARKS:-0}"
ENABLE_TP="${ENABLE_TP:-1}"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"

echo "owner-post-installable-handoff: ${name} ${ver}"
echo

if [[ "$DRY_RUN" == "1" ]]; then
  echo "== DRY_RUN: Installable probe (may be absent) =="
  bash scripts/check-installable.sh >/tmp/pl-handoff-installable.log 2>&1 || true
  tail -6 /tmp/pl-handoff-installable.log | sed 's/^/  /' || true
  echo
  echo "== DRY_RUN: day1 after-publish rehearsal =="
  # Absent crate → day1 PARTIAL/2. Capture so bars/cut-path DRY_RUN cannot abort.
  day1_rc=0
  DRY_RUN=1 bash scripts/day1-after-publish.sh || day1_rc=$?
  if [[ "$day1_rc" -eq 2 ]]; then
    echo "owner-post-installable-handoff: PARTIAL — day1 DRY_RUN not yet Installable (expected pre-token; rehearsal held)"
  elif [[ "$day1_rc" -ne 0 ]]; then
    echo "owner-post-installable-handoff: FAIL — day1 DRY_RUN rc=${day1_rc}" >&2
    exit "$day1_rc"
  fi
  echo
  echo "== DRY_RUN: adopter pin honesty =="
  bash scripts/check-adopter-pin.sh
  echo
  echo "== DRY_RUN: path adopter consumer =="
  MODE=path bash scripts/verify-crates-io-consumer.sh
  echo
  # Bars gate calls this script with HANDOFF_FROM_BARS=1 — skip nested bars
  # to avoid audit-civilization-bars ↔ handoff recursion.
  if [[ "${HANDOFF_FROM_BARS:-0}" != "1" ]]; then
    echo "== DRY_RUN: civilization bars (PRE_PUBLISH) =="
    PRE_PUBLISH=1 bash scripts/audit-civilization-bars.sh
    echo
  else
    echo "== DRY_RUN: civilization bars skipped (nested from bars gate) =="
    echo
  fi
  if [[ "$ENABLE_TP" == "1" ]]; then
    echo "== DRY_RUN: Trusted Publishing shape =="
    DRY_RUN=1 bash scripts/owner-enable-trusted-publishing.sh
    echo
  fi
  echo "== DRY_RUN: post-cut parks stack =="
  bash scripts/check-post-cut-parks-stack.sh
  echo
  echo "== DRY_RUN: parks on main =="
  # Stack check ≠ landed. Shared probe so already-Installable DRY_RUN cannot
  # soft-green unfinished post-cut land. Pre-token parks pending is expected —
  # hold exit 0 with PARTIAL note (same pattern as day1).
  dry_parks_rc=0
  bash scripts/check-parks-on-main.sh || dry_parks_rc=$?
  if [[ "$dry_parks_rc" -eq 0 ]]; then
    : # probe printed OK
  elif [[ "$dry_parks_rc" -eq 1 ]]; then
    echo "owner-post-installable-handoff: FAIL — parks-on-main probe rc=1" >&2
    exit 1
  elif [[ "${ALLOW_PARKS_PENDING:-0}" == "1" ]]; then
    echo "owner-post-installable-handoff: WARN — ALLOW_PARKS_PENDING=1; parks still off main (deferred)"
  else
    dry_installable=0
    if bash scripts/check-installable.sh >/dev/null 2>&1; then
      dry_installable=1
    fi
    if [[ "$dry_installable" -eq 1 ]]; then
      echo "owner-post-installable-handoff: PARTIAL — parks not on main (DRY_RUN; already Installable)" >&2
      echo "  Re-enter: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh" >&2
      exit 2
    fi
    echo "owner-post-installable-handoff: PARTIAL — parks not on main (DRY_RUN; expected pre-token; rehearsal held)"
    echo "  After Installable: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh"
  fi
  echo
  echo "owner-post-installable-handoff: DRY_RUN complete — after crates.io ${ver}:"
  echo "  bash scripts/owner-post-installable-handoff.sh"
  echo "  # or preferred cut: bash scripts/owner-finish-installable.sh"
  exit 0
fi

echo "== 1) Installable =="
bash scripts/check-installable.sh

echo
echo "== 1b) Day1 (README/ADOPTION crates.io flip) =="
# Actions-alternate / handoff-only re-entry must not OK with git-shaped adopter docs.
# Finish/cut already run day1; re-run is idempotent. SKIP_DAY1=1 skips when caller
# just flipped docs in-process.
if [[ "${SKIP_DAY1:-0}" == "1" ]]; then
  echo "owner-post-installable-handoff: SKIP_DAY1=1 — caller already ran day1-after-publish"
else
  bash scripts/day1-after-publish.sh
fi
if ! grep -qE '^partitionline = "[0-9]' README.md; then
  echo "owner-post-installable-handoff: PARTIAL — Installable OK but adopter docs still git-shaped" >&2
  echo "  Day1 must flip README/ADOPTION to crates.io before handoff can OK." >&2
  echo "  Re-enter: bash scripts/day1-after-publish.sh" >&2
  echo "  Then: bash scripts/owner-post-installable-handoff.sh" >&2
  exit 2
fi

echo
echo "== 2) Adopter pin (crates.io-shaped docs) =="
bash scripts/check-adopter-pin.sh

echo
echo "== 3) Registry adopter consumer =="
MODE=registry bash scripts/verify-crates-io-consumer.sh

echo
echo "== 4) Civilization bars (full — no PRE_PUBLISH) =="
bash scripts/audit-civilization-bars.sh

echo
tp_rc=0
if [[ "$ENABLE_TP" == "1" ]]; then
  echo "== 5) Trusted Publishing =="
  bash scripts/owner-enable-trusted-publishing.sh || {
    tp_rc=$?
    echo "owner-post-installable-handoff: WARN — Trusted Publishing helper failed (rc=${tp_rc})" >&2
    echo "  Retry: bash scripts/owner-enable-trusted-publishing.sh" >&2
  }
  echo
fi

echo "== 6) Post-cut parks =="
bash scripts/check-post-cut-parks-stack.sh
parks_rc=0
if [[ "$LAND_PARKS" == "1" ]]; then
  echo "owner-post-installable-handoff: LAND_PARKS=1 — landing parks onto main"
  # Preserve any uncommitted day1 README/ADOPTION edits across park merges.
  # shellcheck source=scripts/lib/preserve-day1-docs.sh
  source "$ROOT/scripts/lib/preserve-day1-docs.sh"
  pl_day1_docs_begin
  REQUIRE_PARKS=1 bash scripts/owner-land-post-cut-parks.sh || parks_rc=$?
  pl_day1_docs_end
  if [[ "$parks_rc" -ne 0 ]]; then
    echo "owner-post-installable-handoff: WARN — parks land failed (Installable still OK)" >&2
    echo "  Retry: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh" >&2
    echo "  Or: bash scripts/owner-land-post-cut-parks.sh" >&2
  fi
else
  echo "owner-post-installable-handoff: LAND_PARKS=0 — stack checked only"
  echo "  Land later: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh"
  echo "  Or: bash scripts/owner-land-post-cut-parks.sh"
fi

echo
echo "== 7) Parks on main (post-cut land honesty) =="
# Stack check proves tip⊆parks merge readiness. Final OK still requires each park
# to be an ancestor of origin/main — otherwise Actions-alternate / LAND_PARKS=0
# handoff greenwashes unfinished post-cut land.
parks_on_main_rc=0
bash scripts/check-parks-on-main.sh || parks_on_main_rc=$?
if [[ "$parks_on_main_rc" -eq 1 ]]; then
  echo "owner-post-installable-handoff: FAIL — parks-on-main probe rc=1" >&2
  exit 1
fi

echo
echo "== 8) Remaining honesty =="
echo "owner-post-installable-handoff: Suite HOLD remains until signed Lab A (docs/STATUS.md)."
echo "owner-post-installable-handoff: Schema Registry stays a companion after core publish"
echo "  (docs/schema-companion.md)."
echo
if [[ "$LAND_PARKS" == "1" && "$parks_rc" -ne 0 ]]; then
  echo "owner-post-installable-handoff: PARTIAL — Installable OK but parks land failed (rc=${parks_rc})" >&2
  echo "  Re-enter: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh" >&2
  exit 2
fi
if [[ "$ENABLE_TP" == "1" && "$tp_rc" -ne 0 ]]; then
  echo "owner-post-installable-handoff: PARTIAL — Installable OK but Trusted Publishing helper failed (rc=${tp_rc})" >&2
  echo "  Re-enter: bash scripts/owner-enable-trusted-publishing.sh" >&2
  exit 2
fi
if [[ "$parks_on_main_rc" -eq 2 && "${ALLOW_PARKS_PENDING:-0}" != "1" ]]; then
  echo "owner-post-installable-handoff: PARTIAL — Installable OK but parks not on main" >&2
  echo "  Post-cut parks still pending on origin/main — handoff must not final-OK." >&2
  echo "  Re-enter: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh" >&2
  echo "  Or: bash scripts/owner-land-post-cut-parks.sh" >&2
  echo "  Intentional deferral: ALLOW_PARKS_PENDING=1 bash scripts/owner-post-installable-handoff.sh" >&2
  exit 2
fi
if [[ "$parks_on_main_rc" -eq 2 && "${ALLOW_PARKS_PENDING:-0}" == "1" ]]; then
  echo "owner-post-installable-handoff: WARN — ALLOW_PARKS_PENDING=1; parks still off main (deferred)"
fi
# Automated handoff steps succeeded. Trusted Publishing UI remains an owner action
# (helper only checks workflow shape — never claim TP is configured).
echo "owner-post-installable-handoff: OK — ${name} ${ver} post-Installable automated handoff complete"
echo "owner-post-installable-handoff: Trusted Publishing UI still owner (crates.io settings) if not already configured"
