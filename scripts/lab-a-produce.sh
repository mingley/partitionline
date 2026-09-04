#!/usr/bin/env bash
# Lab A produce harness (WP-5.1).
# Recreates the topic, runs locked produce runs, and refuses to print a win
# unless broker high-watermark sum equals records acked.
#
# Requires: Docker Kafka or native kafka-topics.sh + kafka-get-offsets.sh,
# and examples/bench_produce. Comparison vs librdkafka C is manual
# (see docs/benchmark.md).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=lab-a-common.sh
source "$ROOT/scripts/lab-a-common.sh"

BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
# Prefer TOPIC= for operators; KAFKA_TOPIC remains the wire env for examples.
TOPIC="${TOPIC:-${KAFKA_TOPIC:-plbench}}"
COUNT="${COUNT:-8000000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
ACKS="${ACKS:-1}"
LINGER_MS="${LINGER_MS:-5}"
WARMUP_SECS="${WARMUP_SECS:-0}"
RUNS="${RUNS:-3}"
PARTITIONS="${PARTITIONS:-6}"
BROKER_NAME="${BROKER_NAME:-pl-lab-a-kafka}"
LAB_A_LABEL="lab-a-produce"

lab_a_prepare_broker || exit 1

export KAFKA_BOOTSTRAP="$BOOTSTRAP"
export KAFKA_TOPIC="$TOPIC"
export COUNT PAYLOAD_BYTES ACKS LINGER_MS WARMUP_SECS

echo "Lab A knobs: COUNT=$COUNT PAYLOAD_BYTES=$PAYLOAD_BYTES ACKS=$ACKS LINGER_MS=$LINGER_MS topic=$TOPIC partitions=$PARTITIONS"
echo "Win condition: each run must report acked == broker HW sum (not a Suite HOLD lift)."

declare -a RUN_ACKED=()
for i in $(seq 1 "$RUNS"); do
  echo "=== run $i / $RUNS ==="
  if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then
    lab_a_reset_topic || exit 1
  fi
  before_hw="$(lab_a_hw_sum)" || exit 1
  if [[ "$before_hw" != "0" && -z "${SKIP_TOPIC_RESET:-}" ]]; then
    echo "lab-a-produce: topic HW sum is $before_hw after reset (expected 0)" >&2
    exit 1
  fi
  line="$(cargo run --release --example bench_produce 2>/dev/null | tee /dev/stderr | grep '"acked":' | tail -1)"
  if [[ -z "$line" ]]; then
    echo "lab-a-produce: no acked JSON from bench_produce" >&2
    exit 1
  fi
  acked="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["acked"])' "$line")"
  hw="$(lab_a_hw_sum)" || exit 1
  # When SKIP_TOPIC_RESET=1, compare delta HW to acked.
  delta=$((hw - before_hw))
  echo "lab-a-produce: run $i acked=$acked hw_sum=$hw hw_delta=$delta"
  if [[ "$delta" != "$acked" ]]; then
    echo "lab-a-produce: FAIL run $i hw_delta ($delta) != acked ($acked) — refusing win" >&2
    exit 1
  fi
  RUN_ACKED+=("$acked")
done

# Median acked rate is not printed as a "win vs C"; only confirm HW integrity.
IFS=$'\n' sorted=($(printf '%s\n' "${RUN_ACKED[@]}" | sort -n))
mid="${sorted[$(( (RUNS - 1) / 2 ))]}"
echo "lab-a-produce: ok — $RUNS runs with HW==acked each time (median acked=$mid)."
echo "lab-a-produce: unsigned integrity check only. Not a Suite HOLD lift. Not a vs-C claim."
