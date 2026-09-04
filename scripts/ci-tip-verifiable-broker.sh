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
#   - Any other failure → FAIL (exit 1). Soft-skip must not greenwash breaks.
#
# Does not lift Suite HOLD. Integrity/latency remain unsigned.
#
# Usage:
#   bash scripts/ci-tip-verifiable-broker.sh
#   REQUIRE_BROKER=1 REQUIRE_AUTH=1 bash scripts/ci-tip-verifiable-broker.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REQUIRE_BROKER="${REQUIRE_BROKER:-0}"
REQUIRE_AUTH="${REQUIRE_AUTH:-0}"
REQUIRE_INTEGRITY="${REQUIRE_INTEGRITY:-0}"

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
elif grep -q 'ci-auth-smoke: skipping' /tmp/pl-tip-auth.log; then
  if [[ "$REQUIRE_AUTH" == "1" ]]; then
    echo "ci-tip-verifiable-broker: FAIL — auth-smoke skipped (REQUIRE_AUTH=1)" >&2
    tail -40 /tmp/pl-tip-auth.log >&2 || true
    exit 1
  fi
  echo "ci-tip-verifiable-broker: SKIP — auth-smoke (missing Java/openssl/Kafka tooling)"
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
elif grep -q 'ci-integrity-smoke: skipping' /tmp/pl-tip-integrity.log; then
  if [[ "$REQUIRE_INTEGRITY" == "1" || "$REQUIRE_BROKER" == "1" ]]; then
    echo "ci-tip-verifiable-broker: FAIL — integrity-smoke skipped (required)" >&2
    tail -40 /tmp/pl-tip-integrity.log >&2 || true
    exit 1
  fi
  echo "ci-tip-verifiable-broker: SKIP — integrity-smoke (no broker)"
else
  echo "ci-tip-verifiable-broker: FAIL — integrity-smoke; see /tmp/pl-tip-integrity.log" >&2
  tail -40 /tmp/pl-tip-integrity.log >&2 || true
  exit 1
fi

echo "ci-tip-verifiable-broker: ok (tip live-broker Verifiable; unsigned)"
exit 0
