#!/usr/bin/env bash
# Produce-ack p99 regression gate against a live broker (WP-5.3).
# Relative to docs/latency-baseline.json — not vs librdkafka C.
#
# Env:
#   KAFKA_BOOTSTRAP     default 127.0.0.1:9092
#   KAFKA_TOPIC         default pl-ci-latency
#   COUNT / WARMUP      sample sizes
#   LATENCY_SLACK_PCT   default 50 (relative slack over baseline)
#   LATENCY_LIMIT_US    optional absolute ceiling (µs); max(relative, abs)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASELINE="${LATENCY_BASELINE:-docs/latency-baseline.json}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${KAFKA_TOPIC:-pl-ci-latency}"
COUNT="${COUNT:-2000}"
WARMUP="${WARMUP:-200}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
SLACK_PCT="${LATENCY_SLACK_PCT:-50}"
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

# CI creates the topic before invoking this gate; locally, best-effort create when
# kafka-topics.sh is on PATH / KAFKA_HOME so missing-topic does not look like a
# client regression.
create_topic_best_effort() {
  local tip tip_bin
  for tip in "${KAFKA_HOME:-}" /tmp/kafka_4.1.0 /tmp/kafka_4.0.0 /tmp/kafka_3.9.1; do
    tip_bin="${tip}/bin/kafka-topics.sh"
    if [[ -x "$tip_bin" ]]; then
      "$tip_bin" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
      return 0
    fi
  done
  if command -v kafka-topics.sh >/dev/null 2>&1; then
    kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
  fi
}
create_topic_best_effort

bench_log="$(mktemp)"
set +e
cargo run --release --example bench_latency >"$bench_log" 2>&1
bench_rc=$?
set -e

if [[ "$bench_rc" -ne 0 ]]; then
  echo "ci-latency-gate: bench_latency failed (rc=$bench_rc) against ${BOOTSTRAP} topic=${TOPIC}" >&2
  tail -20 "$bench_log" >&2 || true
  rm -f "$bench_log"
  exit 1
fi

# Prefer the produce_ack JSON object; fall back to any line with p99_us.
out="$(grep -E '"kind"[[:space:]]*:[[:space:]]*"produce_ack"' "$bench_log" | tail -1 || true)"
if [[ -z "$out" ]]; then
  out="$(grep -E '"p99_us"' "$bench_log" | tail -1 || true)"
fi
if [[ -z "$out" ]]; then
  echo "ci-latency-gate: no produce_ack JSON line from bench_latency" >&2
  tail -20 "$bench_log" >&2 || true
  rm -f "$bench_log"
  exit 1
fi
# Surface the measured line for CI logs / operators.
echo "$out"
rm -f "$bench_log"

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
