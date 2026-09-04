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
#   bash scripts/owner-post-installable-handoff.sh --self-test   # preserve wiring units
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
  bash "$ROOT/scripts/lib/preserve-day1-docs.sh" --self-test
  echo "owner-post-installable-handoff: self-test OK — preserve wired for LAND_PARKS + fail-closed PARTIAL"
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
  echo "owner-post-installable-handoff: DRY_RUN complete — after crates.io ${ver}:"
  echo "  bash scripts/owner-post-installable-handoff.sh"
  echo "  # or preferred cut: bash scripts/owner-finish-installable.sh"
  exit 0
fi

echo "== 1) Installable =="
bash scripts/check-installable.sh

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
  bash scripts/owner-land-post-cut-parks.sh || parks_rc=$?
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
echo "== 7) Remaining honesty =="
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
echo "owner-post-installable-handoff: OK — ${name} ${ver} post-Installable handoff complete"
