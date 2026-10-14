#!/usr/bin/env bash
# One-node KRaft SASL_PLAINTEXT fixture for KL03-21 secure_reconnect cells.
# Addresses only the PID and directories recorded in CLUSTER_STATE_DIR.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${CLUSTER_STATE_DIR:-/tmp/partitionline-kl03-21-sasl}"
KAFKA_HOME="${KAFKA_HOME:-/tmp/kafka_4.1.0}"
CLIENT_PORT="${SASL_PLAIN_PORT:-19192}"
CONTROLLER_PORT="${SASL_PLAIN_CONTROLLER:-19193}"
USER="${SASL_PLAIN_USER:-pl-group}"
PASS="${SASL_PLAIN_PASS:-pl-group-secret}"
APPROVED="${CLUSTER_FIXTURE_APPROVED:-0}"

log() { echo "sasl-plain-node: $*"; }
err() { echo "sasl-plain-node: error: $*" >&2; }
fail() { err "$*"; exit 1; }

cmd="${1:-}"
shift || true

if [[ "$APPROVED" != "1" ]]; then
  fail "CLUSTER_FIXTURE_APPROVED=1 is required"
fi
[[ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]] || fail "KAFKA_HOME=$KAFKA_HOME is not a Kafka tree"

mkdir -p "$STATE_DIR"
PROPS="$STATE_DIR/server.properties"
LOGDIR="$STATE_DIR/kraft-logs"
PIDFILE="$STATE_DIR/kafka.pid"
LOGFILE="$STATE_DIR/kafka.log"

write_props() {
  cat >"$PROPS" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:${CONTROLLER_PORT}
listeners=SASL_PLAINTEXT://127.0.0.1:${CLIENT_PORT},CONTROLLER://127.0.0.1:${CONTROLLER_PORT}
advertised.listeners=SASL_PLAINTEXT://127.0.0.1:${CLIENT_PORT}
controller.listener.names=CONTROLLER
inter.broker.listener.name=SASL_PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN
listener.name.sasl_plaintext.sasl.enabled.mechanisms=PLAIN
listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${USER}" password="${PASS}" user_${USER}="${PASS}";
log.dirs=${LOGDIR}
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
num.partitions=3
group.share.enable=true
unstable.api.versions.enable=true
allow.everyone.if.no.acl.found=true
EOF
}

wait_ready() {
  local i
  for i in $(seq 1 60); do
    if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "127.0.0.1:${CLIENT_PORT}" \
      --command-config "$STATE_DIR/client.properties" --list >/dev/null 2>&1; then
      log "ready 127.0.0.1:${CLIENT_PORT}"
      return 0
    fi
    sleep 1
  done
  fail "node did not become ready; see $LOGFILE"
}

write_client_props() {
  cat >"$STATE_DIR/client.properties" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${USER}" password="${PASS}";
EOF
}

start_node() {
  if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    log "already running pid=$(cat "$PIDFILE")"
    return 0
  fi
  write_props
  write_client_props
  mkdir -p "$LOGDIR"
  if [[ ! -f "$STATE_DIR/formatted" ]]; then
    local uuid
    uuid="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
    "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$uuid" -c "$PROPS" >/dev/null
    echo "$uuid" >"$STATE_DIR/formatted"
  fi
  export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx256m}"
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$PROPS" >"$LOGFILE" 2>&1 &
  echo $! >"$PIDFILE"
  wait_ready
}

stop_node() {
  if [[ -f "$PIDFILE" ]]; then
    local pid
    pid="$(cat "$PIDFILE")"
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
      local i
      for i in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.2
      done
      if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
      fi
    fi
    rm -f "$PIDFILE"
  fi
}

restart_node() {
  stop_node
  start_node
}

case "$cmd" in
  start) start_node ;;
  stop) stop_node ;;
  restart) restart_node ;;
  status)
    if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "{\"running\":true,\"pid\":$(cat "$PIDFILE"),\"bootstrap\":\"127.0.0.1:${CLIENT_PORT}\"}"
    else
      echo '{"running":false}'
    fi
    ;;
  cleanup)
    stop_node
    rm -rf "$STATE_DIR"
    ;;
  *)
    fail "usage: sasl-plain-node.sh start|stop|restart|status|cleanup"
    ;;
esac
