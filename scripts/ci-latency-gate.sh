#!/usr/bin/env bash
# Produce-ack p99 regression gate against a live broker (WP-5.3).
# Relative to docs/latency-baseline.json — not vs librdkafka C.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASELINE="${LATENCY_BASELINE:-docs/latency-baseline.json}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${KAFKA_TOPIC:-pl-ci-latency}"
COUNT="${COUNT:-2000}"
WARMUP="${WARMUP:-200}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
# Allow CI noise; fail only on large regressions.
SLACK_PCT="${LATENCY_SLACK_PCT:-50}"
# Optional absolute ceiling (µs). When set, the gate is max(relative limit, this).
# GitHub-hosted runners + Docker Kafka often need ~3–5ms; local native is <<1ms.
LIMIT_US_ABS="${LATENCY_LIMIT_US:-}"

if [[ ! -f "$BASELINE" ]]; then
  echo "ci-latency-gate: missing baseline $BASELINE" >&2
  exit 1
fi

export KAFKA_BOOTSTRAP="$BOOTSTRAP"
export KAFKA_TOPIC="$TOPIC"
export COUNT WARMUP PAYLOAD_BYTES
export ACKS=1
export LINGER_MS=0
export MODE=produce

echo "ci-latency-gate: running bench_latency COUNT=$COUNT WARMUP=$WARMUP"
out="$(cargo run --release --example bench_latency 2>/dev/null | tee /dev/stderr | grep '"kind":"produce_ack"' | tail -1)"
if [[ -z "$out" ]]; then
  echo "ci-latency-gate: no produce_ack JSON line from bench_latency" >&2
  exit 1
fi

p99="$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['p99_us'])" "$out")"
base_p99="$(python3 -c "import json; print(json.load(open('$BASELINE'))['produce_ack_p99_us'])")"
rel_limit="$(python3 -c "base=int('$base_p99'); slack=int('$SLACK_PCT'); print(base + (base * slack // 100))")"
if [[ -n "$LIMIT_US_ABS" ]]; then
  limit="$(python3 -c "print(max(int('$rel_limit'), int('$LIMIT_US_ABS')))")"
else
  limit="$rel_limit"
fi

echo "ci-latency-gate: measured_p99_us=$p99 baseline_p99_us=$base_p99 limit_us=$limit (slack=${SLACK_PCT}% abs=${LIMIT_US_ABS:-none})"
if (( p99 > limit )); then
  echo "ci-latency-gate: FAIL produce-ack p99 ${p99}us exceeds limit ${limit}us" >&2
  exit 1
fi
echo "ci-latency-gate: ok"
