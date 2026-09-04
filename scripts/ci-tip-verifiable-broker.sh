#!/usr/bin/env bash
# Tip Verifiable live-broker chain (docs/scripts tip proxy for civilization bars).
#
# Tip pushes do not auto-queue Actions. ci-branch-lite historically covered
# fmt/clippy/lib/fuzz/docs but never a live broker — so tip "Verifiable" could
# look green while broker/auth/integrity regressed until main CI. This script
# closes that gap when a broker can be ensured; soft-skips honestly when not.
#
# Soft-skip honesty:
#   - Missing broker + cannot start native → SKIP (exit 0) unless REQUIRE_BROKER=1
#   - Auth missing tooling ("skipping") → SKIP unless REQUIRE_AUTH=1
#   - Integrity missing broker → SKIP unless REQUIRE_INTEGRITY=1 or REQUIRE_BROKER=1
#   - Any other failure → FAIL (exit 1). Soft-skip must not greenwash breaks.
#   - Final line is `ok` only when broker+auth+integrity all passed.
#     Mid-chain soft-skips print `PARTIAL` (not evidence) — never `ok`.
#   - PARTIAL exits 2 by default so tip proxies (`ci-branch-lite` / `check-cut-path`,
#     which use `set -e`) cannot treat incomplete evidence as green. Early SKIP
#     (no broker at all) stays exit 0. Opt-in: TIP_VERIFIABLE_SOFT=1 → PARTIAL
#     exits 0 for constrained sandboxes that explicitly accept incomplete evidence.
#
# When Java/openssl/keytool/python3 and Kafka are present, defaults REQUIRE_BROKER=1
# and REQUIRE_AUTH=1 so a capable agent VM cannot soft-skip into a fake green.
# Opt out: TIP_VERIFIABLE_SOFT=1 (constrained sandboxes without wanting hard-fail).
#
# Exit codes:
#   0 — full `ok`, or early SKIP (no broker), or PARTIAL under TIP_VERIFIABLE_SOFT=1
#   1 — FAIL (smoke/tooling break)
#   2 — PARTIAL (mid-chain soft-skip; not tip Verifiable evidence)
#
# Does not lift Suite HOLD. Integrity/latency remain unsigned.
#
# Usage:
#   bash scripts/ci-tip-verifiable-broker.sh
#   REQUIRE_BROKER=1 REQUIRE_AUTH=1 bash scripts/ci-tip-verifiable-broker.sh
#   TIP_VERIFIABLE_SOFT=1 bash scripts/ci-tip-verifiable-broker.sh
#   bash scripts/ci-tip-verifiable-broker.sh --self-test   # PARTIAL exit-code units
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REQUIRE_BROKER="${REQUIRE_BROKER:-0}"
REQUIRE_AUTH="${REQUIRE_AUTH:-0}"
REQUIRE_INTEGRITY="${REQUIRE_INTEGRITY:-0}"

# Final gate: ok only when both auth+integrity passed; else PARTIAL (exit 2 unless soft).
# Shared by live path and --self-test so exit-code honesty cannot drift.
pl_tip_verifiable_finalize() {
  local auth_ok="$1"
  local integ_ok="$2"
  local soft_skip="${3:-0}"
  if [[ "$auth_ok" == "1" && "$integ_ok" == "1" ]]; then
    echo "ci-tip-verifiable-broker: ok (tip live-broker Verifiable; unsigned)"
    return 0
  fi
  # Soft-skipped mid-chain: never claim ok (not Verifiable evidence).
  # Default exit 2 so set -e tip proxies fail closed; TIP_VERIFIABLE_SOFT=1 → exit 0.
  echo "ci-tip-verifiable-broker: PARTIAL — soft-skipped stage(s); not full tip Verifiable evidence (soft_skip=${soft_skip})"
  if [[ "${TIP_VERIFIABLE_SOFT:-0}" == "1" ]]; then
    return 0
  fi
  return 2
}

if [[ "${1:-}" == "--self-test" ]]; then
  echo "ci-tip-verifiable-broker: self-test — full pass must exit 0"
  TIP_VERIFIABLE_SOFT=0
  set +e
  out="$(pl_tip_verifiable_finalize 1 1 0 2>&1)"
  rc=$?
  set -e
  if [[ "$rc" -ne 0 ]] || ! grep -q 'ci-tip-verifiable-broker: ok' <<<"$out"; then
    echo "ci-tip-verifiable-broker: self-test FAIL — expected ok/exit 0, got rc=$rc out=$out" >&2
    exit 1
  fi

  echo "ci-tip-verifiable-broker: self-test — PARTIAL default must exit 2"
  TIP_VERIFIABLE_SOFT=0
  set +e
  out="$(pl_tip_verifiable_finalize 0 1 1 2>&1)"
  rc=$?
  set -e
  if [[ "$rc" -ne 2 ]] || ! grep -q 'PARTIAL' <<<"$out"; then
    echo "ci-tip-verifiable-broker: self-test FAIL — expected PARTIAL/exit 2, got rc=$rc out=$out" >&2
    exit 1
  fi
  if grep -q 'ci-tip-verifiable-broker: ok' <<<"$out"; then
    echo "ci-tip-verifiable-broker: self-test FAIL — PARTIAL path printed ok" >&2
    exit 1
  fi

  echo "ci-tip-verifiable-broker: self-test — PARTIAL under TIP_VERIFIABLE_SOFT=1 must exit 0"
  TIP_VERIFIABLE_SOFT=1
  set +e
  out="$(pl_tip_verifiable_finalize 1 0 1 2>&1)"
  rc=$?
  set -e
  if [[ "$rc" -ne 0 ]] || ! grep -q 'PARTIAL' <<<"$out"; then
    echo "ci-tip-verifiable-broker: self-test FAIL — expected PARTIAL/exit 0 under soft, got rc=$rc out=$out" >&2
    exit 1
  fi
  if grep -q 'ci-tip-verifiable-broker: ok' <<<"$out"; then
    echo "ci-tip-verifiable-broker: self-test FAIL — soft PARTIAL printed ok" >&2
    exit 1
  fi

  echo "ci-tip-verifiable-broker: self-test OK — finalize ok/PARTIAL exit 2/soft exit 0"
  exit 0
fi

pl_tip_verifiable_tooling_ready() {
  local kver="${KAFKA_VERSION:-4.1.0}"
  local kdir="${KAFKA_HOME:-/tmp/kafka_${kver}}"
  command -v openssl >/dev/null 2>&1 \
    && command -v keytool >/dev/null 2>&1 \
    && command -v java >/dev/null 2>&1 \
    && command -v python3 >/dev/null 2>&1 \
    && [[ -d "$kdir/bin" ]]
}

# Capable env → require broker+auth unless explicitly soft.
if [[ "${TIP_VERIFIABLE_SOFT:-0}" != "1" ]] && pl_tip_verifiable_tooling_ready; then
  if [[ "$REQUIRE_BROKER" != "1" ]]; then
    REQUIRE_BROKER=1
    echo "ci-tip-verifiable-broker: tooling present — REQUIRE_BROKER=1 (TIP_VERIFIABLE_SOFT=1 to soft-skip)"
  fi
  if [[ "$REQUIRE_AUTH" != "1" ]]; then
    REQUIRE_AUTH=1
    echo "ci-tip-verifiable-broker: tooling present — REQUIRE_AUTH=1 (TIP_VERIFIABLE_SOFT=1 to soft-skip)"
  fi
fi

auth_ok=0
integ_ok=0
soft_skip=0

echo "== ci-tip-verifiable-broker: ensure broker =="
# shellcheck source=scripts/lib/ensure-broker.sh
source "$ROOT/scripts/lib/ensure-broker.sh"
if ! pl_ensure_broker "ci-tip-verifiable-broker"; then
  if [[ "$REQUIRE_BROKER" == "1" ]]; then
    echo "ci-tip-verifiable-broker: FAIL — no broker (REQUIRE_BROKER=1)" >&2
    exit 1
  fi
  echo "ci-tip-verifiable-broker: SKIP — no broker available (not evidence of Verifiable)"
  exit 0
fi

echo "== ci-tip-verifiable-broker: broker-smoke (kip848+share when 4.x) =="
if ! SKIP_DOCKER=1 bash scripts/ci-broker-smoke.sh >/tmp/pl-tip-broker.log 2>&1 \
  || ! grep -q 'ci-broker-smoke: ok' /tmp/pl-tip-broker.log; then
  echo "ci-tip-verifiable-broker: FAIL — broker-smoke; see /tmp/pl-tip-broker.log" >&2
  tail -40 /tmp/pl-tip-broker.log >&2 || true
  exit 1
fi
echo "ci-tip-verifiable-broker: broker-smoke ok"
# Leave shared native broker up for integrity/latency (do not stop here).

echo "== ci-tip-verifiable-broker: auth-smoke =="
# Auth brings its own SASL_SSL listeners; soft-skips without Java/Kafka tooling.
set +e
bash scripts/ci-auth-smoke.sh >/tmp/pl-tip-auth.log 2>&1
auth_rc=$?
set -e
if grep -q 'ci-auth-smoke: ok' /tmp/pl-tip-auth.log; then
  echo "ci-tip-verifiable-broker: auth-smoke ok"
  auth_ok=1
elif grep -q 'ci-auth-smoke: skipping' /tmp/pl-tip-auth.log; then
  if [[ "$REQUIRE_AUTH" == "1" ]]; then
    echo "ci-tip-verifiable-broker: FAIL — auth-smoke skipped (REQUIRE_AUTH=1)" >&2
    tail -40 /tmp/pl-tip-auth.log >&2 || true
    exit 1
  fi
  echo "ci-tip-verifiable-broker: SKIP — auth-smoke (missing Java/openssl/Kafka tooling)"
  soft_skip=1
else
  echo "ci-tip-verifiable-broker: FAIL — auth-smoke; see /tmp/pl-tip-auth.log" >&2
  tail -40 /tmp/pl-tip-auth.log >&2 || true
  exit 1
fi

echo "== ci-tip-verifiable-broker: integrity-smoke (unsigned; includes latency gate) =="
# STOP_NATIVE_ON_EXIT defaults off so we do not kill the shared tip broker.
set +e
bash scripts/ci-integrity-smoke.sh >/tmp/pl-tip-integrity.log 2>&1
integ_rc=$?
set -e
if grep -q 'ci-integrity-smoke: ok' /tmp/pl-tip-integrity.log; then
  echo "ci-tip-verifiable-broker: integrity-smoke ok (unsigned; not a Suite HOLD lift)"
  integ_ok=1
elif grep -q 'ci-integrity-smoke: skipping' /tmp/pl-tip-integrity.log; then
  if [[ "$REQUIRE_INTEGRITY" == "1" || "$REQUIRE_BROKER" == "1" ]]; then
    echo "ci-tip-verifiable-broker: FAIL — integrity-smoke skipped (required)" >&2
    tail -40 /tmp/pl-tip-integrity.log >&2 || true
    exit 1
  fi
  echo "ci-tip-verifiable-broker: SKIP — integrity-smoke (no broker)"
  soft_skip=1
else
  echo "ci-tip-verifiable-broker: FAIL — integrity-smoke; see /tmp/pl-tip-integrity.log" >&2
  tail -40 /tmp/pl-tip-integrity.log >&2 || true
  exit 1
fi

pl_tip_verifiable_finalize "$auth_ok" "$integ_ok" "$soft_skip"
exit $?
