#!/usr/bin/env bash
# Start a local Apache Kafka KRaft broker without Docker (agent/nested VM fallback).
# Downloads Apache Kafka once under /tmp and writes kraft.properties.
#
# Usage:
#   bash scripts/ci-native-kafka.sh start
#   SKIP_DOCKER=1 bash scripts/ci-broker-smoke.sh
#   bash scripts/ci-native-kafka.sh stop
#
# Defaults to Kafka 4.1.0 so KIP-932 share groups work (group.share.enable +
# share.version feature level 1). Override with KAFKA_VERSION=3.9.1 for older.
set -euo pipefail

KVER="${KAFKA_VERSION:-4.1.0}"
KDIR="${KAFKA_HOME:-/tmp/kafka_${KVER}}"
PROPS="${KAFKA_PROPS:-/tmp/partitionline-kraft.properties}"
LOGDIR="${KAFKA_LOG_DIRS:-/tmp/partitionline-kraft-logs}"
PIDFILE="${KAFKA_PIDFILE:-/tmp/partitionline-kafka.pid}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"

ensure_kafka() {
  if [[ -d "$KDIR/bin" ]]; then
    return 0
  fi
  local tgz="/tmp/kafka_${KVER}.tgz"
  echo "ci-native-kafka: downloading Apache Kafka ${KVER}"
  curl -fsSL "https://archive.apache.org/dist/kafka/${KVER}/kafka_2.13-${KVER}.tgz" -o "$tgz"
  rm -rf "/tmp/kafka_extract_${KVER}"
  mkdir -p "/tmp/kafka_extract_${KVER}"
  tar -xzf "$tgz" -C "/tmp/kafka_extract_${KVER}"
  rm -rf "$KDIR"
  mv "/tmp/kafka_extract_${KVER}/kafka_2.13-${KVER}" "$KDIR"
}

write_props() {
  cat >"$PROPS" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=${LOGDIR}
num.network.threads=2
num.io.threads=4
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
unstable.api.versions.enable=true
group.share.enable=true
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
EOF
}

upgrade_share_feature() {
  # Kafka 4.1 ships share.version supported=1 but finalized=0 until upgraded.
  # Without level 1, ShareGroupHeartbeat works but ShareFetch returns no records.
  if [[ ! -x "$KDIR/bin/kafka-features.sh" ]]; then
    return 0
  fi
  if "$KDIR/bin/kafka-features.sh" --bootstrap-server "$BOOTSTRAP" upgrade --feature share.version=1 >/tmp/pl-share-feature.log 2>&1; then
    echo "ci-native-kafka: share.version=1"
  else
    # 3.x or already at level 1 — not fatal for classic smoke.
    echo "ci-native-kafka: share.version upgrade skipped (see /tmp/pl-share-feature.log)"
  fi
}

cmd="${1:-start}"
case "$cmd" in
  start)
    ensure_kafka
    write_props
    if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "ci-native-kafka: already running pid=$(cat "$PIDFILE") bootstrap=$BOOTSTRAP"
      upgrade_share_feature || true
      exit 0
    fi
    rm -rf "$LOGDIR"
    CLUSTER_ID="$("$KDIR/bin/kafka-storage.sh" random-uuid)"
    "$KDIR/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$PROPS" >/dev/null
    "$KDIR/bin/kafka-server-start.sh" "$PROPS" >/tmp/partitionline-kafka.log 2>&1 &
    echo $! >"$PIDFILE"
    echo "ci-native-kafka: waiting for $BOOTSTRAP"
    for _ in $(seq 1 60); do
      if "$KDIR/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
        upgrade_share_feature || true
        echo "ci-native-kafka: ready pid=$(cat "$PIDFILE")"
        exit 0
      fi
      sleep 2
    done
    echo "ci-native-kafka: broker failed to start; see /tmp/partitionline-kafka.log" >&2
    exit 1
    ;;
  stop)
    if [[ -f "$PIDFILE" ]]; then
      kill "$(cat "$PIDFILE")" 2>/dev/null || true
      rm -f "$PIDFILE"
    fi
    echo "ci-native-kafka: stopped"
    ;;
  *)
    echo "usage: $0 {start|stop}" >&2
    exit 2
    ;;
esac
