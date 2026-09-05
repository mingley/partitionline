#!/usr/bin/env bash
# Local / CI integrity smoke (WP-5): small-count Lab A produce→HW→fetch plus
# unsigned latency gate. Soft-skips when no broker is available unless
# REQUIRE_INTEGRITY=1. Never a Suite HOLD lift.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COUNT="${COUNT:-5000}"
PARTITIONS="${PARTITIONS:-1}"
RUNS="${RUNS:-1}"
TOPIC="${TOPIC:-pl-ci-integrity}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
# Leave shared native brokers up for subsequent gates (latency / broker-smoke).
# Only stop on EXIT when *this* invocation started native Kafka from a cold port.
started_native=0
broker_was_up=0

# shellcheck source=scripts/lib/ensure-broker.sh
source "$ROOT/scripts/lib/ensure-broker.sh"

cleanup() {
  if [[ "$started_native" -eq 1 && "$broker_was_up" -eq 0 ]]; then
    # We brought the broker up; stop only when STOP_NATIVE_ON_EXIT=1 (default off
    # so agent Verifiable sequences can chain integrity → latency → fuzz).
    if [[ "${STOP_NATIVE_ON_EXIT:-0}" == "1" ]]; then
      bash scripts/ci-native-kafka.sh stop >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT

broker_ready=0
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  # Prefer an already-running lab broker; otherwise try native (Docker overlay
  # often broken in nested VMs).
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${BROKER_NAME:-pl-lab-a-kafka}"; then
    broker_ready=1
    broker_was_up=1
  fi
fi

if [[ "$broker_ready" -eq 0 ]]; then
  if pl_broker_tcp_ready "$BOOTSTRAP"; then
    broker_ready=1
    broker_was_up=1
  elif pl_ensure_broker "ci-integrity-smoke"; then
    broker_ready=1
    if [[ "${PL_ENSURE_BROKER_STARTED:-0}" -eq 1 ]]; then
      started_native=1
    else
      broker_was_up=1
    fi
  fi
fi

if [[ "$broker_ready" -eq 0 ]]; then
  if [[ "${REQUIRE_INTEGRITY:-}" == "1" ]]; then
    echo "ci-integrity-smoke: no broker available" >&2
    exit 1
  fi
  echo "ci-integrity-smoke: skipping (no Docker/native broker)"
  exit 0
fi

echo "ci-integrity-smoke: Lab A integrity COUNT=$COUNT RUNS=$RUNS topic=$TOPIC"
export KAFKA_BOOTSTRAP="$BOOTSTRAP"
export TOPIC COUNT PARTITIONS RUNS
export PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
export ACKS="${ACKS:-1}"
export LINGER_MS="${LINGER_MS:-5}"
export WARMUP_SECS=0
export MEASURE_SECS=0

if ! bash scripts/lab-a-integrity.sh; then
  echo "ci-integrity-smoke: lab-a-integrity failed" >&2
  exit 1
fi

# Unsigned relative latency gate (not Suite HOLD). Soft-miss must not print
# final `ok` (civilization-check / tip Verifiable must not greenwash under load).
# REQUIRE_INTEGRITY=1 hard-fails soft miss. Otherwise exit 2 + PARTIAL.
latency_soft=0
if [[ "${SKIP_LATENCY_GATE:-}" != "1" ]]; then
  echo "ci-integrity-smoke: latency gate (unsigned)"
  export KAFKA_TOPIC="${LATENCY_TOPIC:-pl-ci-latency}"
  export COUNT="${LATENCY_COUNT:-2000}"
  export WARMUP="${LATENCY_WARMUP:-200}"
  export LATENCY_SLACK_PCT="${LATENCY_SLACK_PCT:-50}"
  if ! bash scripts/ci-latency-gate.sh; then
    if [[ "${REQUIRE_INTEGRITY:-}" == "1" ]]; then
      echo "ci-integrity-smoke: latency gate failed (REQUIRE_INTEGRITY=1)" >&2
      exit 1
    fi
    echo "ci-integrity-smoke: latency gate failed (soft) — Lab A integrity held; latency not full evidence (set REQUIRE_INTEGRITY=1 to hard-fail)" >&2
    latency_soft=1
  fi
fi

if [[ "$latency_soft" == "1" ]]; then
  echo "ci-integrity-smoke: PARTIAL — Lab A integrity ok but latency soft-miss (unsigned; not a Suite HOLD lift)"
  exit 2
fi

echo "ci-integrity-smoke: ok (unsigned; not a Suite HOLD lift)"
