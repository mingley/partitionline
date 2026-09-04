#!/usr/bin/env bash
# Combined Lab A integrity path (WP-5): produce → HW delta == acked → fetch →
# consumed == seeded. Unsigned only — not a Suite HOLD lift, not a vs-C claim.
#
# Defaults match a local smoke (small COUNT). Full Lab A knobs via env:
#   COUNT=8000000 PARTITIONS=6 RUNS=3 bash scripts/lab-a-integrity.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=lab-a-common.sh
source "$ROOT/scripts/lab-a-common.sh"

BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${TOPIC:-${KAFKA_TOPIC:-plbench-integrity}}"
COUNT="${COUNT:-5000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
ACKS="${ACKS:-1}"
LINGER_MS="${LINGER_MS:-5}"
WARMUP_SECS="${WARMUP_SECS:-0}"
MEASURE_SECS="${MEASURE_SECS:-0}"
RUNS="${RUNS:-1}"
PARTITIONS="${PARTITIONS:-1}"
BROKER_NAME="${BROKER_NAME:-pl-lab-a-kafka}"
LAB_A_LABEL="lab-a-integrity"

lab_a_prepare_broker || exit 1

export KAFKA_BOOTSTRAP="$BOOTSTRAP"
export KAFKA_TOPIC="$TOPIC"
export COUNT PAYLOAD_BYTES ACKS LINGER_MS WARMUP_SECS MEASURE_SECS

echo "Lab A integrity: COUNT=$COUNT PARTITIONS=$PARTITIONS RUNS=$RUNS topic=$TOPIC"
echo "Win: each run HW_delta==acked AND consumed==seeded. Unsigned. Not Suite HOLD."

for i in $(seq 1 "$RUNS"); do
  echo "=== integrity run $i / $RUNS ==="
  if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then
    lab_a_reset_topic || exit 1
  fi
  before_hw="$(lab_a_hw_sum)" || exit 1
  if [[ "$before_hw" != "0" && -z "${SKIP_TOPIC_RESET:-}" ]]; then
    echo "lab-a-integrity: topic HW sum is $before_hw after reset (expected 0)" >&2
    exit 1
  fi

  produce_line="$(
    cargo run --release --example bench_produce 2>/dev/null \
      | tee /dev/stderr \
      | grep '"acked":' \
      | tail -1
  )"
  if [[ -z "$produce_line" ]]; then
    echo "lab-a-integrity: no acked JSON from bench_produce" >&2
    exit 1
  fi
  acked="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["acked"])' "$produce_line")"
  hw="$(lab_a_hw_sum)" || exit 1
  delta=$((hw - before_hw))
  echo "lab-a-integrity: run $i acked=$acked hw_sum=$hw hw_delta=$delta"
  if [[ "$delta" != "$acked" ]]; then
    echo "lab-a-integrity: FAIL run $i hw_delta ($delta) != acked ($acked)" >&2
    exit 1
  fi
  if [[ "$acked" != "$COUNT" ]]; then
    echo "lab-a-integrity: FAIL seed acked ($acked) != COUNT ($COUNT)" >&2
    exit 1
  fi

  fetch_line="$(
    cargo run --release --example bench_fetch 2>/dev/null \
      | tee /dev/stderr \
      | grep '"consumed":' \
      | tail -1
  )"
  if [[ -z "$fetch_line" ]]; then
    echo "lab-a-integrity: no consumed JSON from bench_fetch" >&2
    exit 1
  fi
  consumed="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["consumed"])' "$fetch_line")"
  echo "lab-a-integrity: run $i seeded=$acked consumed=$consumed"
  if [[ "$consumed" != "$acked" ]]; then
    echo "lab-a-integrity: FAIL consumed ($consumed) != seeded ($acked)" >&2
    exit 1
  fi
done

echo "lab-a-integrity: ok — $RUNS run(s) with HW==acked and consumed==seeded."
echo "lab-a-integrity: unsigned integrity only. Not a Suite HOLD lift. Not a vs-C claim."
