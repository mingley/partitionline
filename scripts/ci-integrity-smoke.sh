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
started_native=0

cleanup() {
  if [[ "$started_native" -eq 1 ]]; then
    bash scripts/ci-native-kafka.sh stop >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

broker_ready=0
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  # Prefer an already-running lab broker; otherwise try native (Docker overlay
  # often broken in nested VMs).
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${BROKER_NAME:-pl-lab-a-kafka}"; then
    broker_ready=1
  fi
fi

if [[ "$broker_ready" -eq 0 ]]; then
  if bash scripts/ci-native-kafka.sh start >/tmp/pl-integrity-native.log 2>&1; then
    started_native=1
    broker_ready=1
    BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
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

# Unsigned relative latency gate (not Suite HOLD). Soft-fail only if required.
if [[ "${SKIP_LATENCY_GATE:-}" != "1" ]]; then
  echo "ci-integrity-smoke: latency gate (unsigned)"
  export KAFKA_TOPIC="${LATENCY_TOPIC:-pl-ci-latency}"
  export COUNT="${LATENCY_COUNT:-2000}"
  export WARMUP="${LATENCY_WARMUP:-200}"
  export LATENCY_SLACK_PCT="${LATENCY_SLACK_PCT:-50}"
  if ! bash scripts/ci-latency-gate.sh; then
    if [[ "${REQUIRE_INTEGRITY:-}" == "1" ]]; then
      echo "ci-integrity-smoke: latency gate failed" >&2
      exit 1
    fi
    echo "ci-integrity-smoke: latency gate failed (soft); see above" >&2
    exit 1
  fi
fi

echo "ci-integrity-smoke: ok (unsigned; not a Suite HOLD lift)"
