#!/usr/bin/env bash
# Pre-publish Installable preflight (WP-0.5). Does not publish.
#
# Exit codes:
#   0  — READY_EXCEPT_TOKEN: structurally ready; only CARGO_REGISTRY_TOKEN / crates.io cut remains
#   1  — other blockers (merge-ready fail, red main CI, metadata, stale parks without token, etc.)
#   2  — already Installable (crates.io has this version)
#   3  — inconclusive main CI (still running); structural gates otherwise OK
#
# Env:
#   REQUIRE_MAIN_CI=1  treat inconclusive main CI (exit 3) as failure (exit 1) — default 0
#   SKIP_MAIN_CI=1     skip the main CI probe entirely
#
# Usage:
#   bash scripts/check-installable-preflight.sh
#   bash scripts/check-installable-preflight.sh --self-test
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# shellcheck source=scripts/lib/cargo-registry-token.sh
source "$ROOT/scripts/lib/cargo-registry-token.sh"

if [[ "${1:-}" == "--self-test" ]]; then
  echo "check-installable-preflight: self-test — must prepare TOKEN (file/whitespace/misname) before READY_EXCEPT_TOKEN"
  if ! grep -qF 'pl_prepare_cargo_registry_token' "$ROOT/scripts/check-installable-preflight.sh"; then
    echo "check-installable-preflight: self-test FAIL — missing pl_prepare_cargo_registry_token" >&2
    exit 1
  fi
  if ! grep -qF 'cargo-registry-token.sh' "$ROOT/scripts/check-installable-preflight.sh"; then
    echo "check-installable-preflight: self-test FAIL — must source cargo-registry-token.sh" >&2
    exit 1
  fi
  # Whitespace-only must become unset (else READY_EXCEPT_TOKEN is skipped and probe "rejects").
  CARGO_REGISTRY_TOKEN='   '
  export CARGO_REGISTRY_TOKEN
  pl_prepare_cargo_registry_token "preflight-self-test" >/tmp/pl-preflight-prep.log 2>&1 || true
  if [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
    echo "check-installable-preflight: self-test FAIL — whitespace TOKEN still set after prepare" >&2
    exit 1
  fi
  # Misname must WARN when exact TOKEN unset.
  unset CARGO_REGISTRY_TOKEN || true
  CARGO_TOKEN='fake-misname-for-self-test'
  export CARGO_TOKEN
  pl_prepare_cargo_registry_token "preflight-self-test" >/tmp/pl-preflight-misname.log 2>&1 || true
  if ! grep -q 'misnamed token' /tmp/pl-preflight-misname.log; then
    echo "check-installable-preflight: self-test FAIL — misname WARN missing" >&2
    exit 1
  fi
  unset CARGO_TOKEN || true
  # TOKEN_FILE must load into this shell.
  _pl_tf="$(mktemp)"
  printf 'file-token-self-test' >"$_pl_tf"
  unset CARGO_REGISTRY_TOKEN || true
  CARGO_REGISTRY_TOKEN_FILE="$_pl_tf"
  export CARGO_REGISTRY_TOKEN_FILE
  pl_prepare_cargo_registry_token "preflight-self-test" >/tmp/pl-preflight-file.log 2>&1 || true
  rm -f "$_pl_tf"
  unset CARGO_REGISTRY_TOKEN_FILE || true
  if [[ "${CARGO_REGISTRY_TOKEN:-}" != "file-token-self-test" ]]; then
    echo "check-installable-preflight: self-test FAIL — TOKEN_FILE did not load (got len=${#CARGO_REGISTRY_TOKEN})" >&2
    exit 1
  fi
  unset CARGO_REGISTRY_TOKEN || true
  echo "check-installable-preflight: self-test OK — prepare wires whitespace/misname/TOKEN_FILE before READY_EXCEPT_TOKEN"
  exit 0
fi

REQUIRE_MAIN_CI="${REQUIRE_MAIN_CI:-0}"
SKIP_MAIN_CI="${SKIP_MAIN_CI:-0}"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"

echo "check-installable-preflight: ${name} ${ver}"
echo

# Normalize TOKEN_FILE / whitespace / misnames into *this* shell before any
# READY_EXCEPT_TOKEN claim (raw [[ -z TOKEN ]] greenwashes Secrets typos).
pl_prepare_cargo_registry_token "check-installable-preflight"

echo "== honesty self-tests (no token required) =="
# READY_EXCEPT_TOKEN must not be claimable if probe/PARTIAL units regress.
# Same executable units as cut-path / owner-finish-installable step 0a.
bash scripts/check-registry-token.sh --self-test
bash scripts/check-installable-preflight.sh --self-test
bash scripts/ci-tip-verifiable-broker.sh --self-test
# Parks auto-refresh must restore main/caller before cut/publish (token-day footgun).
bash scripts/check-parks-refresh-cut-guards.sh
# Documented git pin must cargo-check while Installable waits (Adoptable before crates.io).
MODE=git bash scripts/verify-crates-io-consumer.sh
# day1 README/ADOPTION/guide/migrate must survive parks land even if stash pop fails.
bash scripts/lib/preserve-day1-docs.sh --self-test

# Already published?
if bash scripts/check-installable.sh >/tmp/pl-preflight-installable.log 2>&1; then
  echo "check-installable-preflight: ALREADY_INSTALLABLE — crates.io has ${name} ${ver}"
  tail -5 /tmp/pl-preflight-installable.log | sed 's/^/  /'
  # Installable ≠ post-cut complete. Surface parks-on-main + day1 four-file shape so
  # ALREADY_INSTALLABLE cannot soft-OK unfinished park land or git-shaped adopter docs.
  # shellcheck source=scripts/lib/adopter-docs-shaped.sh
  source "$ROOT/scripts/lib/adopter-docs-shaped.sh"
  echo "== parks on main (post-Installable honesty) =="
  parks_main_rc=0
  bash scripts/check-parks-on-main.sh || parks_main_rc=$?
  if [[ "$parks_main_rc" -eq 2 ]]; then
    echo "check-installable-preflight: PARTIAL — Installable OK but parks not on main"
    echo "  Re-enter: LAND_PARKS=1 bash scripts/owner-post-installable-handoff.sh"
    echo "  Or: bash scripts/owner-finish-installable.sh  # already-Installable short-circuit"
  elif [[ "$parks_main_rc" -ne 0 ]]; then
    echo "check-installable-preflight: WARN — parks-on-main probe rc=${parks_main_rc}" >&2
  fi
  echo "== day1 adopter docs crates.io shape =="
  if ! pl_adopter_docs_crates_io_shaped; then
    echo "check-installable-preflight: PARTIAL — Installable OK but adopter docs still git-shaped"
    echo "  Day1 must flip README + ADOPTION + guide + migrate to crates.io."
    echo "  Re-enter: bash scripts/day1-after-publish.sh"
    echo "  Then: bash scripts/owner-post-installable-handoff.sh"
  fi
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
      echo "  Parks stay off main until after crates.io 0.1.0 (expected pre-Installable; tip⊆parks stack is the pre-cut gate)."
      echo "  Next: bash scripts/owner-request-registry-token.sh"
      echo "        then export CARGO_REGISTRY_TOKEN, wait for green main CI, then:"
      echo "        bash scripts/owner-finish-installable.sh"
      exit 3
    fi
    echo "check-installable-preflight: token set but main CI inconclusive — wait for green before cut"
    exit 3
  fi
fi

echo
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  # Fail closed: cut path hard-requires tip⊆parks. Soft-exit READY_EXCEPT_TOKEN while
  # parks are stale greenwashes the token ask ("everything else is rehearsed").
  parks_rc=0
  bash scripts/check-post-cut-parks-stack.sh >/tmp/pl-preflight-parks.log 2>&1 || parks_rc=$?
  if [[ "$parks_rc" -ne 0 ]]; then
    echo "check-installable-preflight: FAIL — parks stack stale (also needs CARGO_REGISTRY_TOKEN)" >&2
    echo "  Structural + Verifiable gates OK, but tip⊈parks — refresh before claiming READY_EXCEPT_TOKEN." >&2
    echo "  Refresh: bash scripts/refresh-post-cut-parks.sh" >&2
    tail -8 /tmp/pl-preflight-parks.log | sed 's/^/  /' >&2 || true
    exit 1
  fi
  echo "check-installable-preflight: READY_EXCEPT_TOKEN"
  echo "  Structural + Verifiable + parks stack OK; crates.io cut blocked only on CARGO_REGISTRY_TOKEN."
  echo "  Parks stay off main until after crates.io 0.1.0 (expected pre-Installable; tip⊆parks stack is the pre-cut gate)."
  echo "  Token scope: first cut of a NEW crate needs crates.io publish-new (+ usually publish-update)."
  echo "  publish-update alone cannot create the crate. Trusted Publishing is configured after 0.1.0."
  echo "  One-screen owner ask: bash scripts/owner-request-registry-token.sh"
  echo "  Next: set Cloud Agent secret CARGO_REGISTRY_TOKEN (Cursor → Environments → Secrets),"
  # shellcheck source=scripts/lib/cursor-env-secrets-url.sh
  source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/cursor-env-secrets-url.sh"
  echo "        Direct: $PARTITIONLINE_CURSOR_ENV_SECRETS_URL"
  echo "        also add Actions secret CARGO_REGISTRY_TOKEN, restart agent, then:"
  echo "        bash scripts/owner-finish-installable.sh"
  echo "        # publishes 0.1.0 then chains post-cut parks land by default"
  echo "        # (MERGE_PARKED_VERIFIABLE=0 to skip Verifiable+flate2+SCRAM+lz4+checkout parks land)"
  echo "  Actions-only alternate: bash scripts/owner-dispatch-first-publish.sh (owner machine; agents 403)."
  exit 0
fi

echo "== registry token =="
tok_rc=0
bash scripts/check-registry-token.sh || tok_rc=$?
if [[ "$tok_rc" -eq 2 ]]; then
  # Prepare should have cleared whitespace; unset mid-flight is still EXCEPT_TOKEN.
  echo "check-installable-preflight: READY_EXCEPT_TOKEN (token unset after prepare)"
  echo "  Parks stay off main until after crates.io 0.1.0 (expected pre-Installable; tip⊆parks stack is the pre-cut gate)."
  exit 0
elif [[ "$tok_rc" -ne 0 ]]; then
  echo "check-installable-preflight: FAIL — CARGO_REGISTRY_TOKEN set but crates.io rejected it (rc=${tok_rc})" >&2
  exit 1
fi

echo "== post-cut parks stack (token present) =="
# TOKEN unset already fail-closes on stale parks above; keep the hard check when TOKEN is set.
if ! bash scripts/check-post-cut-parks-stack.sh; then
  echo "check-installable-preflight: FAIL — parks lag tip; refresh before cut" >&2
  echo "  bash scripts/refresh-post-cut-parks.sh" >&2
  echo "  (owner-finish-installable also auto-refreshes, then restores main)" >&2
  exit 1
fi

echo "check-installable-preflight: READY — token present; run bash scripts/owner-finish-installable.sh"
exit 0
