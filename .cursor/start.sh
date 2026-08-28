#!/usr/bin/env bash
# Per-boot startup: bring up the local Kafka broker the examples talk to.
#
# Launches the KRaft broker in the background, waits for it to accept
# connections on 127.0.0.1:9092, ensures the default demo topic exists, then
# returns. Safe to run repeatedly: it no-ops if the broker is already up.
set -euo pipefail

KAFKA_HOME=/opt/kafka
KAFKA_CFG="${KAFKA_HOME}/config/kraft/pl-server.properties"
BOOTSTRAP=127.0.0.1:9092
LOG="${KAFKA_HOME}/logs/broker.out"

ready() { "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; }

if ready; then
  echo "kafka already running on ${BOOTSTRAP}"
else
  mkdir -p "${KAFKA_HOME}/logs"
  nohup "${KAFKA_HOME}/bin/kafka-server-start.sh" "${KAFKA_CFG}" > "${LOG}" 2>&1 &
  for _ in $(seq 1 60); do
    ready && break
    sleep 1
  done
  if ! ready; then
    echo "kafka failed to start; last log lines:" >&2
    tail -n 40 "${LOG}" >&2 || true
    exit 1
  fi
  echo "kafka started on ${BOOTSTRAP}"
fi

# Examples default to the `partitionline` topic; the client does not
# auto-create topics, so make sure it is present.
"${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists --topic partitionline --partitions 3 --replication-factor 1

echo "kafka ready on ${BOOTSTRAP} (topic: partitionline)"
