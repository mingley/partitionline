#!/usr/bin/env bash
# Lab A fetch integrity harness (WP-5 honesty).
# Seeds a known record count, verifies broker HW delta == acked, fetches them
# back, and refuses to print a win unless consumed == seeded. Throughput
# numbers stay unsigned / not a Suite HOLD.
#
# Requires: Docker Kafka or native kafka-topics.sh + kafka-get-offsets.sh,
# examples/bench_produce + examples/bench_fetch.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=lab-a-common.sh
source "$ROOT/scripts/lab-a-common.sh"

BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${TOPIC:-${KAFKA_TOPIC:-plbench-fetch}}"
COUNT="${COUNT:-50000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
ACKS="${ACKS:-1}"
LINGER_MS="${LINGER_MS:-5}"
PARTITIONS="${PARTITIONS:-1}"
BROKER_NAME="${BROKER_NAME:-pl-lab-a-kafka}"
LAB_A_LABEL="lab-a-fetch"

lab_a_prepare_broker || exit 1

export KAFKA_BOOTSTRAP="$BOOTSTRAP"
export KAFKA_TOPIC="$TOPIC"
export COUNT PAYLOAD_BYTES ACKS LINGER_MS
# bench_produce still runs a timed warmup before COUNT; keep integrity seed exact.
export WARMUP_SECS="${WARMUP_SECS:-0}"
export MEASURE_SECS="${MEASURE_SECS:-0}"

echo "Lab A fetch: seed COUNT=$COUNT topic=$TOPIC then fetch; win only if HW==acked and consumed==seeded"
echo "Integrity only — unsigned. Not a Suite HOLD lift. Not a vs-C claim."

if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then
  lab_a_reset_topic || exit 1
fi

before_hw="$(lab_a_hw_sum)" || exit 1
if [[ "$before_hw" != "0" && -z "${SKIP_TOPIC_RESET:-}" ]]; then
  echo "lab-a-fetch: topic HW sum is $before_hw after reset (expected 0)" >&2
  exit 1
fi

produce_line="$(
  cargo run --release --example bench_produce 2>/dev/null \
    | tee /dev/stderr \
    | grep '"acked":' \
    | tail -1
)"
if [[ -z "$produce_line" ]]; then
  echo "lab-a-fetch: no acked JSON from bench_produce" >&2
  exit 1
fi
acked="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["acked"])' "$produce_line")"
if [[ "$acked" != "$COUNT" ]]; then
  echo "lab-a-fetch: FAIL seed acked ($acked) != COUNT ($COUNT)" >&2
  exit 1
fi
hw="$(lab_a_hw_sum)" || exit 1
delta=$((hw - before_hw))
echo "lab-a-fetch: acked=$acked hw_sum=$hw hw_delta=$delta"
if [[ "$delta" != "$acked" ]]; then
  echo "lab-a-fetch: FAIL hw_delta ($delta) != acked ($acked) — refusing win" >&2
  exit 1
fi

fetch_line="$(
  cargo run --release --example bench_fetch 2>/dev/null \
    | tee /dev/stderr \
    | grep '"consumed":' \
    | tail -1
)"
if [[ -z "$fetch_line" ]]; then
  echo "lab-a-fetch: no consumed JSON from bench_fetch" >&2
  exit 1
fi
consumed="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["consumed"])' "$fetch_line")"
echo "lab-a-fetch: seeded=$acked consumed=$consumed"
if [[ "$consumed" != "$acked" ]]; then
  echo "lab-a-fetch: FAIL consumed ($consumed) != seeded ($acked) — refusing win" >&2
  exit 1
fi

echo "lab-a-fetch: ok — HW==acked and consumed==seeded ($consumed)."
echo "lab-a-fetch: unsigned integrity check only. Not a Suite HOLD lift. Not a vs-C claim."
