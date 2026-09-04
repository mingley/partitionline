#!/usr/bin/env bash
# Lab A fetch integrity harness (WP-5 honesty).
# Seeds a known record count, fetches them back, and refuses to print a win
# unless consumed == seeded. Throughput numbers stay unsigned / not a Suite HOLD.
#
# Requires: Docker Kafka or native kafka-topics.sh, examples/bench_produce +
# examples/bench_fetch.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${TOPIC:-${KAFKA_TOPIC:-plbench-fetch}}"
COUNT="${COUNT:-50000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
ACKS="${ACKS:-1}"
LINGER_MS="${LINGER_MS:-5}"
PARTITIONS="${PARTITIONS:-1}"
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

ensure_broker() {
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$BROKER_NAME"; then
    return 0
  fi
  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$BROKER_NAME"; then
    docker start "$BROKER_NAME" >/dev/null || return 1
  else
    if ! docker run -d --name "$BROKER_NAME" -p 9092:9092 \
      "${KAFKA_IMAGE:-apache/kafka:3.9.1}" >/dev/null; then
      echo "lab-a-fetch: docker run failed (overlay often broken in nested VMs)" >&2
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
  echo "lab-a-fetch: docker broker not ready" >&2
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
  if topics_bin="$(find_kafka_bin kafka-topics.sh)"; then
    "$topics_bin" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" >/dev/null 2>&1 || true
    sleep 2
    "$topics_bin" --bootstrap-server "$BOOTSTRAP" \
      --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1
    return 0
  fi
  echo "lab-a-fetch: no kafka-topics.sh and no docker broker for topic reset" >&2
  exit 1
}

if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 \
    && ensure_broker; then
    :
  elif find_kafka_bin kafka-topics.sh >/dev/null; then
    echo "lab-a-fetch: using native kafka tools against $BOOTSTRAP"
  else
    echo "lab-a-fetch: no docker broker and no kafka-topics.sh" >&2
    exit 1
  fi
fi

export KAFKA_BOOTSTRAP="$BOOTSTRAP"
export KAFKA_TOPIC="$TOPIC"
export COUNT PAYLOAD_BYTES ACKS LINGER_MS
# bench_produce still runs a timed warmup before COUNT; keep integrity seed exact.
export WARMUP_SECS="${WARMUP_SECS:-0}"
export MEASURE_SECS="${MEASURE_SECS:-0}"

echo "Lab A fetch: seed COUNT=$COUNT topic=$TOPIC then fetch; win only if consumed==seeded"
echo "Integrity only — unsigned. Not a Suite HOLD lift. Not a vs-C claim."

if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then
  reset_topic
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

echo "lab-a-fetch: ok — consumed==seeded ($consumed)."
echo "lab-a-fetch: unsigned integrity check only. Not a Suite HOLD lift. Not a vs-C claim."
