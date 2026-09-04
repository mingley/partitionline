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

BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${KAFKA_TOPIC:-plbench}"
COUNT="${COUNT:-8000000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
ACKS="${ACKS:-1}"
LINGER_MS="${LINGER_MS:-5}"
WARMUP_SECS="${WARMUP_SECS:-0}"
RUNS="${RUNS:-3}"
PARTITIONS="${PARTITIONS:-6}"
BROKER_NAME="${BROKER_NAME:-pl-lab-a-kafka}"

find_kafka_bin() {
  local name="$1"
  if command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
    return 0
  fi
  local cand
  for cand in \
    "/tmp/kafka_4.1.0/bin/${name}" \
    "/tmp/kafka_4.0.0/bin/${name}" \
    "/tmp/kafka_3.9.1/bin/${name}" \
    "${KAFKA_HOME:-}/bin/${name}"; do
    if [[ -x "$cand" ]]; then
      echo "$cand"
      return 0
    fi
  done
  return 1
}

find_topics_bin() { find_kafka_bin kafka-topics.sh; }
find_offsets_bin() { find_kafka_bin kafka-get-offsets.sh; }

ensure_broker() {
  if docker ps --format '{{.Names}}' | grep -qx "$BROKER_NAME"; then
    return 0
  fi
  if docker ps -a --format '{{.Names}}' | grep -qx "$BROKER_NAME"; then
    docker start "$BROKER_NAME" >/dev/null || return 1
  else
    if ! docker run -d --name "$BROKER_NAME" -p 9092:9092 \
      "${KAFKA_IMAGE:-apache/kafka:3.9.1}" >/dev/null; then
      echo "lab-a-produce: docker run failed (overlay often broken in nested VMs)" >&2
      return 1
    fi
  fi
  for _ in $(seq 1 90); do
    if docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "lab-a-produce: docker broker not ready" >&2
  return 1
}

reset_topic() {
  local topics_bin=""
  if [[ -n "${USE_DOCKER_BROKER:-}" ]] || docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$BROKER_NAME"; then
    docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1 || true
    sleep 2
    docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1
    return 0
  fi
  if topics_bin="$(find_topics_bin)"; then
    "$topics_bin" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" >/dev/null 2>&1 || true
    sleep 2
    "$topics_bin" --bootstrap-server "$BOOTSTRAP" \
      --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1
    return 0
  fi
  echo "lab-a-produce: no kafka-topics.sh and no docker broker for topic reset" >&2
  echo "lab-a-produce: set SKIP_TOPIC_RESET=1 or start scripts/ci-native-kafka.sh" >&2
  exit 1
}

# Sum of log-end offsets across partitions (empty topic → 0).
hw_sum() {
  local offsets_bin="" out
  if [[ -n "${USE_DOCKER_BROKER:-}" ]] || docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$BROKER_NAME"; then
    out="$(docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server localhost:9092 --topic "$TOPIC" --time -1)"
  elif offsets_bin="$(find_offsets_bin)"; then
    out="$("$offsets_bin" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)"
  else
    echo "lab-a-produce: kafka-get-offsets.sh required to verify HW==acked" >&2
    exit 1
  fi
  python3 -c '
import sys
s = 0
for line in sys.stdin.read().splitlines():
    line = line.strip()
    if not line or line.startswith("Option"):
        continue
    parts = line.split(":")
    if len(parts) >= 3 and parts[-1].lstrip("-").isdigit():
        s += int(parts[-1])
print(s)
' <<<"$out"
}

if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 \
    && ensure_broker; then
    :
  elif find_topics_bin >/dev/null; then
    echo "lab-a-produce: using native kafka tools against $BOOTSTRAP"
  else
    echo "lab-a-produce: no docker broker and no kafka-topics.sh; set SKIP_TOPIC_RESET=1" >&2
    exit 1
  fi
fi

export KAFKA_BOOTSTRAP="$BOOTSTRAP"
export KAFKA_TOPIC="$TOPIC"
export COUNT PAYLOAD_BYTES ACKS LINGER_MS WARMUP_SECS

echo "Lab A knobs: COUNT=$COUNT PAYLOAD_BYTES=$PAYLOAD_BYTES ACKS=$ACKS LINGER_MS=$LINGER_MS topic=$TOPIC partitions=$PARTITIONS"
echo "Win condition: each run must report acked == broker HW sum (not a Suite HOLD lift)."

declare -a RUN_ACKED=()
for i in $(seq 1 "$RUNS"); do
  echo "=== run $i / $RUNS ==="
  if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then
    reset_topic
  fi
  before_hw="$(hw_sum)"
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
  hw="$(hw_sum)"
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
