#!/usr/bin/env bash
# Lab A produce harness (WP-5.1).
# Recreates the topic, runs three locked produce runs, refuses to print a
# win unless broker high watermark equals records sent.
#
# Requires: Docker Kafka (or existing broker), Admin/ListOffsets via examples.
# Comparison vs librdkafka C is manual (see docs/benchmark.md).
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

if ! command -v docker >/dev/null 2>&1 && [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then
  echo "lab-a-produce: docker not found; set SKIP_TOPIC_RESET=1 to use an existing topic" >&2
  exit 1
fi

BROKER_NAME="${BROKER_NAME:-pl-lab-a-kafka}"

ensure_broker() {
  if docker ps --format '{{.Names}}' | grep -qx "$BROKER_NAME"; then
    return 0
  fi
  if docker ps -a --format '{{.Names}}' | grep -qx "$BROKER_NAME"; then
    docker start "$BROKER_NAME" >/dev/null
  else
    docker run -d --name "$BROKER_NAME" -p 9092:9092 "${KAFKA_IMAGE:-apache/kafka:3.9.1}"
  fi
  for _ in $(seq 1 90); do
    if docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "lab-a-produce: broker not ready" >&2
  exit 1
}

reset_topic() {
  docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1 || true
  sleep 2
  docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic "$TOPIC" --partitions 6 --replication-factor 1
}

if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then
  ensure_broker
fi

export KAFKA_BOOTSTRAP="$BOOTSTRAP"
export KAFKA_TOPIC="$TOPIC"
export COUNT PAYLOAD_BYTES ACKS LINGER_MS WARMUP_SECS

echo "Lab A knobs: COUNT=$COUNT PAYLOAD_BYTES=$PAYLOAD_BYTES ACKS=$ACKS LINGER_MS=$LINGER_MS topic=$TOPIC"
echo "Do not publish a win unless HW equals records sent (see docs/benchmark.md)."

for i in $(seq 1 "$RUNS"); do
  echo "=== run $i / $RUNS ==="
  if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then
    reset_topic
  fi
  cargo run --release --example bench_produce
done

echo "lab-a-produce: finished $RUNS runs. Compare medians only when each run reports HW=$COUNT."
