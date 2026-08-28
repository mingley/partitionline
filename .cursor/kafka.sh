#!/usr/bin/env bash
# On-demand local Kafka broker for the examples/benchmarks.
#
#   bash .cursor/kafka.sh start    # launch broker on 127.0.0.1:9092 + create demo topic
#   bash .cursor/kafka.sh stop     # stop the broker
#   bash .cursor/kafka.sh status   # report whether the broker is up
#
# The broker is intentionally NOT started on boot: the test suite runs against
# an in-tree mock broker and needs no external Kafka, and the opportunistic
# `admin_against_kafka_if_present` integration test only activates when a broker
# is listening on 127.0.0.1:9092. Start the broker explicitly before running the
# examples (`cargo run --release --example roundtrip`, etc.).
set -euo pipefail

KAFKA_HOME=/opt/kafka
KAFKA_CFG="${KAFKA_HOME}/config/kraft/pl-server.properties"
BOOTSTRAP=127.0.0.1:9092
LOG="${KAFKA_HOME}/logs/broker.out"
PIDFILE="${KAFKA_HOME}/broker.pid"

ready() { "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; }

start() {
  if ready; then
    echo "kafka already running on ${BOOTSTRAP}"
  else
    mkdir -p "${KAFKA_HOME}/logs"
    nohup "${KAFKA_HOME}/bin/kafka-server-start.sh" "${KAFKA_CFG}" > "${LOG}" 2>&1 &
    echo "$!" > "${PIDFILE}"
    for _ in $(seq 1 60); do
      ready && break
      sleep 1
    done
    if ! ready; then
      echo "kafka failed to start; last log lines:" >&2
      tail -n 40 "${LOG}" >&2 || true
      exit 1
    fi
    echo "kafka started on ${BOOTSTRAP} (pid $(cat "${PIDFILE}"))"
  fi
  # Examples default to the `partitionline` topic; the client does not
  # auto-create topics, so make sure it exists.
  "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists --topic partitionline --partitions 3 --replication-factor 1
  echo "kafka ready on ${BOOTSTRAP} (topic: partitionline)"
}

stop() {
  if [ -f "${PIDFILE}" ] && kill -0 "$(cat "${PIDFILE}")" 2>/dev/null; then
    kill "$(cat "${PIDFILE}")"
    rm -f "${PIDFILE}"
    echo "kafka stopped"
  else
    "${KAFKA_HOME}/bin/kafka-server-stop.sh" >/dev/null 2>&1 || true
    rm -f "${PIDFILE}"
    echo "kafka stop requested"
  fi
}

status() {
  if ready; then echo "kafka: up on ${BOOTSTRAP}"; else echo "kafka: down"; fi
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) echo "usage: $0 {start|stop|status}" >&2; exit 2 ;;
esac
