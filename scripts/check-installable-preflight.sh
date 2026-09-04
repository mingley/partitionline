#!/usr/bin/env bash
# Pre-publish Installable preflight (WP-0.5). Does not publish.
#
# Exit codes:
#   0  — READY_EXCEPT_TOKEN: structurally ready; only CARGO_REGISTRY_TOKEN / crates.io cut remains
#   1  — other blockers (merge-ready fail, red main CI, metadata, etc.)
#   2  — already Installable (crates.io has this version)
#   3  — inconclusive main CI (still running); structural gates otherwise OK
#
# Env:
#   REQUIRE_MAIN_CI=1  treat inconclusive main CI (exit 3) as failure (exit 1) — default 0
#   SKIP_MAIN_CI=1     skip the main CI probe entirely
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REQUIRE_MAIN_CI="${REQUIRE_MAIN_CI:-0}"
SKIP_MAIN_CI="${SKIP_MAIN_CI:-0}"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"

echo "check-installable-preflight: ${name} ${ver}"
echo

# Already published?
if bash scripts/check-installable.sh >/tmp/pl-preflight-installable.log 2>&1; then
  echo "check-installable-preflight: ALREADY_INSTALLABLE — crates.io has ${name} ${ver}"
  tail -5 /tmp/pl-preflight-installable.log | sed 's/^/  /'
  exit 2
fi
# check-installable fails when absent — that is expected pre-cut.
if ! grep -q 'not on crates.io\|status=absent\|FAIL' /tmp/pl-preflight-installable.log; then
  echo "check-installable-preflight: unexpected installable probe output:" >&2
  cat /tmp/pl-preflight-installable.log >&2
  exit 1
fi
echo "check-installable-preflight: crates.io absent (first cut still needed) — expected"

echo
echo "== merge/tag readiness =="
if ! bash scripts/check-merge-ready.sh; then
  echo "check-installable-preflight: FAIL — merge-ready blockers above" >&2
  exit 1
fi

echo
echo "== crate metadata =="
bash scripts/check-crate-metadata.sh

echo
echo "== main CI (Verifiable) =="
ci_rc=0
if [[ "$SKIP_MAIN_CI" == "1" ]]; then
  echo "check-installable-preflight: SKIP_MAIN_CI=1 — not probing"
else
  bash scripts/check-main-ci.sh || ci_rc=$?
  if [[ "$ci_rc" -eq 1 ]]; then
    echo "check-installable-preflight: FAIL — main HEAD CI is red" >&2
    exit 1
  elif [[ "$ci_rc" -eq 2 ]]; then
    if [[ "$REQUIRE_MAIN_CI" == "1" ]]; then
      echo "check-installable-preflight: FAIL — REQUIRE_MAIN_CI=1 and main CI inconclusive" >&2
      exit 1
    fi
    echo
    if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
      echo "check-installable-preflight: READY_EXCEPT_TOKEN (main CI still running — wait or REQUIRE_MAIN_CI=0)"
      echo "  Next: export CARGO_REGISTRY_TOKEN, wait for green main CI, then:"
      echo "        bash scripts/owner-finish-installable.sh"
      exit 3
    fi
    echo "check-installable-preflight: token set but main CI inconclusive — wait for green before cut"
    exit 3
  fi
fi

echo
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "check-installable-preflight: READY_EXCEPT_TOKEN"
  echo "  Structural + Verifiable gates OK; crates.io cut blocked only on CARGO_REGISTRY_TOKEN."
  echo "  Next: export CARGO_REGISTRY_TOKEN (Cloud Agent + Actions), then:"
  echo "        bash scripts/owner-finish-installable.sh"
  echo "        # publishes 0.1.0 then chains post-cut parks land by default"
  echo "        # (MERGE_PARKED_VERIFIABLE=0 to skip Verifiable+flate2+SCRAM+lz4+checkout parks land)"
  exit 0
fi

echo "== registry token =="
if ! bash scripts/check-registry-token.sh; then
  echo "check-installable-preflight: FAIL — CARGO_REGISTRY_TOKEN set but crates.io rejected it" >&2
  exit 1
fi
echo "check-installable-preflight: READY — token present; run bash scripts/owner-finish-installable.sh"
exit 0
