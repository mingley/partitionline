#!/usr/bin/env bash
# Real-broker smoke against Apache Kafka in Docker (KRaft).
# Used by CI (broker-smoke job) and local verification.
#
# Env:
#   KAFKA_IMAGE   default apache/kafka:3.9.1 (CI also runs apache/kafka:4.0.0)
#   KAFKA_BOOTSTRAP default 127.0.0.1:9092
#   CI=true       fail if docker is missing (GitHub Actions)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.1}"
BROKER_NAME="${BROKER_NAME:-pl-ci-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${KAFKA_TOPIC:-pl-ci-smoke}"
OUT_TOPIC="${KAFKA_OUTPUT_TOPIC:-pl-ci-smoke-out}"

if ! command -v docker >/dev/null 2>&1; then
  echo "ci-broker-smoke: docker not found" >&2
  if [[ "${CI:-}" == "true" ]]; then
    exit 1
  fi
  echo "ci-broker-smoke: skipping (no docker; set CI=true to fail)" >&2
  exit 0
fi

cleanup() {
  docker rm -f "$BROKER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
# Official apache/kafka image defaults to KRaft and advertises localhost:9092.
docker run -d --name "$BROKER_NAME" -p 9092:9092 "$KAFKA_IMAGE"

echo "waiting for broker on $BOOTSTRAP ($KAFKA_IMAGE)"
ready=0
for _ in $(seq 1 90); do
  if docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" != "1" ]]; then
  echo "ci-broker-smoke: broker did not become ready" >&2
  docker logs "$BROKER_NAME" >&2 || true
  exit 1
fi

docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic "$TOPIC" --partitions 1 --replication-factor 1

docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic "$OUT_TOPIC" --partitions 1 --replication-factor 1

export KAFKA_BOOTSTRAP="$BOOTSTRAP"
export KAFKA_TOPIC="$TOPIC"
export KAFKA_OUTPUT_TOPIC="$OUT_TOPIC"
export KAFKA_GROUP="pl-ci-group"
export KAFKA_TRANSACTIONAL_ID="pl-ci-txn"

echo "== roundtrip =="
cargo run --release --example roundtrip

echo "== produce =="
cargo run --release --example produce

echo "== admin =="
cargo run --release --example admin

echo "== txn =="
cargo run --release --example txn

echo "ci-broker-smoke: ok ($KAFKA_IMAGE)"
