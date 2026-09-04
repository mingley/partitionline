#!/usr/bin/env bash
# Real-broker smoke against Apache Kafka (KRaft).
# Used by CI (broker-smoke job) and local verification.
#
# Env:
#   KAFKA_IMAGE      default apache/kafka:3.9.1 (CI also runs apache/kafka:4.0.0)
#   KAFKA_BOOTSTRAP  default 127.0.0.1:9092
#   SKIP_DOCKER=1    use an already-running broker at KAFKA_BOOTSTRAP (no Docker)
#   CI=true          fail if docker is missing (unless SKIP_DOCKER=1)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.1}"
BROKER_NAME="${BROKER_NAME:-pl-ci-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${KAFKA_TOPIC:-pl-ci-smoke}"
OUT_TOPIC="${KAFKA_OUTPUT_TOPIC:-pl-ci-smoke-out}"

wait_tcp() {
  local host="${1%:*}" port="${1##*:}" i
  for i in $(seq 1 90); do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

# group/eos examples loop forever — succeed if they print progress before timeout.
run_until_progress() {
  local label="$1"
  local pattern="$2"
  shift 2
  local log
  log="$(mktemp)"
  set +e
  timeout 45s "$@" >"$log" 2>&1
  local rc=$?
  set -e
  if grep -E -- "$pattern" "$log" >/dev/null; then
    echo "ci-broker-smoke: $label ok"
    rm -f "$log"
    return 0
  fi
  echo "ci-broker-smoke: $label failed (rc=$rc); log:" >&2
  cat "$log" >&2 || true
  rm -f "$log"
  return 1
}

run_examples() {
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

  # Transaction / group coordinators can lag briefly after broker start.
  echo "== txn =="
  local attempt
  for attempt in 1 2 3 4 5 6 7 8; do
    if cargo run --release --example txn; then
      break
    fi
    if [[ "$attempt" -eq 8 ]]; then
      echo "ci-broker-smoke: txn example failed after retries" >&2
      exit 1
    fi
    echo "ci-broker-smoke: txn not ready yet; retry $attempt"
    sleep 3
  done

  # Seed records then exercise classic group consume/commit.
  echo "== group =="
  cargo run --release --example produce >/dev/null
  cargo run --release --example produce >/dev/null
  for attempt in 1 2 3 4 5 6; do
    if run_until_progress "group" '@[0-9]+' cargo run --release --example group; then
      break
    fi
    if [[ "$attempt" -eq 6 ]]; then
      echo "ci-broker-smoke: group example failed after retries" >&2
      exit 1
    fi
    echo "ci-broker-smoke: group coordinator not ready yet; retry $attempt"
    sleep 3
  done

  # Exactly-once consume→produce path (needs source records + output topic).
  echo "== eos =="
  cargo run --release --example produce >/dev/null
  export KAFKA_TRANSACTIONAL_ID="pl-ci-eos"
  export KAFKA_GROUP="pl-ci-eos"
  for attempt in 1 2 3 4 5 6; do
    if run_until_progress "eos" '-> ' cargo run --release --example eos; then
      break
    fi
    if [[ "$attempt" -eq 6 ]]; then
      echo "ci-broker-smoke: eos example failed after retries" >&2
      exit 1
    fi
    echo "ci-broker-smoke: eos not ready yet; retry $attempt"
    sleep 3
  done
}

if [[ "${SKIP_DOCKER:-}" == "1" ]]; then
  echo "ci-broker-smoke: SKIP_DOCKER=1; using existing broker at $BOOTSTRAP"
  if ! wait_tcp "$BOOTSTRAP"; then
    echo "ci-broker-smoke: broker not reachable at $BOOTSTRAP" >&2
    exit 1
  fi
  # Topic create via kafka CLI when available (PATH or common native install).
  topics_bin=""
  if command -v kafka-topics.sh >/dev/null 2>&1; then
    topics_bin="$(command -v kafka-topics.sh)"
  elif [[ -x /tmp/kafka_3.9.1/bin/kafka-topics.sh ]]; then
    topics_bin=/tmp/kafka_3.9.1/bin/kafka-topics.sh
  fi
  if [[ -n "$topics_bin" ]]; then
    "$topics_bin" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$TOPIC" --partitions 1 --replication-factor 1 || true
    "$topics_bin" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$OUT_TOPIC" --partitions 1 --replication-factor 1 || true
  fi
  run_examples
  echo "ci-broker-smoke: ok (existing broker $BOOTSTRAP)"
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "ci-broker-smoke: docker not found" >&2
  if [[ "${CI:-}" == "true" ]]; then
    exit 1
  fi
  echo "ci-broker-smoke: skipping (no docker; set CI=true to fail, or SKIP_DOCKER=1)" >&2
  exit 0
fi

cleanup() {
  docker rm -f "$BROKER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
# Official apache/kafka image defaults to KRaft and advertises localhost:9092.
if ! docker run -d --name "$BROKER_NAME" -p 9092:9092 "$KAFKA_IMAGE"; then
  echo "ci-broker-smoke: docker run failed (overlay often broken in nested VMs)." >&2
  echo "ci-broker-smoke: start a native broker and re-run with SKIP_DOCKER=1" >&2
  if [[ "${CI:-}" == "true" ]]; then
    exit 1
  fi
  exit 0
fi

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

run_examples
echo "ci-broker-smoke: ok ($KAFKA_IMAGE)"
