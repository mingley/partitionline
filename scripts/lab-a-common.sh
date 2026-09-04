#!/usr/bin/env bash
# Shared Lab A broker helpers (sourced by lab-a-*.sh). Not a Suite HOLD lift.
# Caller must set: ROOT, BOOTSTRAP, TOPIC, PARTITIONS, BROKER_NAME, LAB_A_LABEL

lab_a_find_kafka_bin() {
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

lab_a_ensure_broker() {
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$BROKER_NAME"; then
    return 0
  fi
  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$BROKER_NAME"; then
    docker start "$BROKER_NAME" >/dev/null || return 1
  else
    if ! docker run -d --name "$BROKER_NAME" -p 9092:9092 \
      "${KAFKA_IMAGE:-apache/kafka:3.9.1}" >/dev/null; then
      echo "${LAB_A_LABEL}: docker run failed (overlay often broken in nested VMs)" >&2
      return 1
    fi
  fi
  local _
  for _ in $(seq 1 90); do
    if docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "${LAB_A_LABEL}: docker broker not ready" >&2
  return 1
}

lab_a_using_docker_broker() {
  [[ -n "${USE_DOCKER_BROKER:-}" ]] \
    || docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$BROKER_NAME"
}

lab_a_reset_topic() {
  local topics_bin=""
  if lab_a_using_docker_broker; then
    docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1 || true
    sleep 2
    docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1
    return 0
  fi
  if topics_bin="$(lab_a_find_kafka_bin kafka-topics.sh)"; then
    "$topics_bin" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" >/dev/null 2>&1 || true
    sleep 2
    "$topics_bin" --bootstrap-server "$BOOTSTRAP" \
      --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1
    return 0
  fi
  echo "${LAB_A_LABEL}: no kafka-topics.sh and no docker broker for topic reset" >&2
  return 1
}

# Sum of log-end offsets across partitions (empty topic → 0).
lab_a_hw_sum() {
  local offsets_bin="" out
  if lab_a_using_docker_broker; then
    out="$(docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server localhost:9092 --topic "$TOPIC" --time -1)"
  elif offsets_bin="$(lab_a_find_kafka_bin kafka-get-offsets.sh)"; then
    out="$("$offsets_bin" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)"
  else
    echo "${LAB_A_LABEL}: kafka-get-offsets.sh required to verify HW==acked" >&2
    return 1
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

lab_a_prepare_broker() {
  if [[ -n "${SKIP_TOPIC_RESET:-}" ]]; then
    return 0
  fi
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 \
    && lab_a_ensure_broker; then
    return 0
  fi
  if lab_a_find_kafka_bin kafka-topics.sh >/dev/null; then
    echo "${LAB_A_LABEL}: using native kafka tools against $BOOTSTRAP"
    return 0
  fi
  echo "${LAB_A_LABEL}: no docker broker and no kafka-topics.sh; set SKIP_TOPIC_RESET=1 or start scripts/ci-native-kafka.sh" >&2
  return 1
}
