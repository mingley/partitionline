#!/usr/bin/env bash
# Real-broker TLS + SCRAM (SASL_SSL) smoke against native Apache Kafka.
# Generates a private CA, boots an isolated KRaft broker, creates a SCRAM
# user over a PLAINTEXT admin listener, then produces via examples/sasl with
# TLS_CA_PEM set (SASL_SSL). Soft-skips when Java/openssl/keytool/Kafka are
# missing unless REQUIRE_AUTH=1.
#
# Usage:
#   bash scripts/ci-auth-smoke.sh
#   REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

KVER="${KAFKA_VERSION:-4.1.0}"
KDIR="${KAFKA_HOME:-/tmp/kafka_${KVER}}"
WORKDIR="${AUTH_SMOKE_DIR:-/tmp/partitionline-auth-smoke}"
PROPS="${WORKDIR}/kraft.properties"
LOGDIR="${WORKDIR}/kraft-logs"
PIDFILE="${WORKDIR}/kafka.pid"
CERTDIR="${WORKDIR}/certs"
CA_PEM="${CERTDIR}/ca.pem"
SSL_BOOTSTRAP="${AUTH_SSL_BOOTSTRAP:-127.0.0.1:9192}"
ADMIN_BOOTSTRAP="${AUTH_ADMIN_BOOTSTRAP:-127.0.0.1:9194}"
CONTROLLER="${AUTH_CONTROLLER:-127.0.0.1:9193}"
TOPIC="${AUTH_TOPIC:-pl-auth-smoke}"
SCRAM_USER="${AUTH_USERNAME:-alice}"
SCRAM_PASS="${AUTH_PASSWORD:-secret-change-me}"
STOREPASS="${AUTH_STORE_PASS:-changeit}"

soft_skip() {
  echo "ci-auth-smoke: skipping ($*)" >&2
  if [[ "${REQUIRE_AUTH:-}" == "1" ]]; then
    exit 1
  fi
  exit 0
}

need_bin() {
  command -v "$1" >/dev/null 2>&1 || soft_skip "missing $1"
}

need_bin openssl
need_bin keytool
need_bin java

if [[ ! -d "$KDIR/bin" ]]; then
  soft_skip "Kafka not installed at $KDIR (run scripts/ci-native-kafka.sh start once)"
fi

wait_tcp() {
  local host="${1%:*}" port="${1##*:}"
  local i
  for i in $(seq 1 60); do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

cleanup() {
  if [[ -f "$PIDFILE" ]]; then
    kill "$(cat "$PIDFILE")" 2>/dev/null || true
    rm -f "$PIDFILE"
  fi
}
trap cleanup EXIT

mkdir -p "$WORKDIR" "$CERTDIR"
rm -rf "$LOGDIR"
mkdir -p "$LOGDIR"

echo "== generate TLS material =="
rm -f "${CERTDIR:?}"/*
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -keyout "$CERTDIR/ca.key" -out "$CA_PEM" \
  -subj "/CN=partitionline-auth-ca" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -days 2 \
  -keyout "$CERTDIR/broker.key" -out "$CERTDIR/broker.csr" \
  -subj "/CN=localhost" >/dev/null 2>&1
openssl x509 -req -in "$CERTDIR/broker.csr" -CA "$CA_PEM" -CAkey "$CERTDIR/ca.key" \
  -CAcreateserial -out "$CERTDIR/broker.crt" -days 2 \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") >/dev/null 2>&1
openssl pkcs12 -export -in "$CERTDIR/broker.crt" -inkey "$CERTDIR/broker.key" \
  -certfile "$CA_PEM" -out "$CERTDIR/kafka.keystore.p12" \
  -name kafka -password "pass:${STOREPASS}" >/dev/null 2>&1
keytool -importcert -noprompt -alias ca -file "$CA_PEM" \
  -keystore "$CERTDIR/kafka.truststore.p12" -storetype PKCS12 \
  -storepass "$STOREPASS" >/dev/null 2>&1

echo "== write broker props (PLAINTEXT admin + SASL_SSL client) =="
cat >"$PROPS" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@${CONTROLLER}
listeners=SASL_SSL://127.0.0.1:9192,CONTROLLER://127.0.0.1:9193,PLAINTEXT://127.0.0.1:9194
advertised.listeners=SASL_SSL://127.0.0.1:9192,PLAINTEXT://127.0.0.1:9194
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_SSL:SASL_SSL
log.dirs=${LOGDIR}
num.network.threads=2
num.io.threads=4
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
ssl.keystore.location=${CERTDIR}/kafka.keystore.p12
ssl.keystore.password=${STOREPASS}
ssl.keystore.type=PKCS12
ssl.key.password=${STOREPASS}
ssl.truststore.location=${CERTDIR}/kafka.truststore.p12
ssl.truststore.password=${STOREPASS}
ssl.truststore.type=PKCS12
ssl.endpoint.identification.algorithm=
sasl.enabled.mechanisms=SCRAM-SHA-256,SCRAM-SHA-512
listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.sasl_ssl.ssl.client.auth=none
super.users=User:${SCRAM_USER}
EOF

echo "== format + start broker =="
CLUSTER_ID="$("$KDIR/bin/kafka-storage.sh" random-uuid)"
"$KDIR/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$PROPS" >/dev/null
"$KDIR/bin/kafka-server-start.sh" "$PROPS" >"${WORKDIR}/kafka.log" 2>&1 &
echo $! >"$PIDFILE"

if ! wait_tcp "$ADMIN_BOOTSTRAP"; then
  echo "ci-auth-smoke: admin listener did not come up; log:" >&2
  tail -n 80 "${WORKDIR}/kafka.log" >&2 || true
  soft_skip "broker failed to start"
fi
ready=0
for _ in $(seq 1 60); do
  if "$KDIR/bin/kafka-topics.sh" --bootstrap-server "$ADMIN_BOOTSTRAP" --list >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" != "1" ]]; then
  echo "ci-auth-smoke: topics CLI not ready; log:" >&2
  tail -n 80 "${WORKDIR}/kafka.log" >&2 || true
  soft_skip "broker not ready for admin"
fi

echo "== create SCRAM user + topic =="
"$KDIR/bin/kafka-configs.sh" --bootstrap-server "$ADMIN_BOOTSTRAP" \
  --alter --add-config "SCRAM-SHA-256=[password=${SCRAM_PASS}]" \
  --entity-type users --entity-name "$SCRAM_USER"
"$KDIR/bin/kafka-configs.sh" --bootstrap-server "$ADMIN_BOOTSTRAP" \
  --alter --add-config "SCRAM-SHA-512=[password=${SCRAM_PASS}]" \
  --entity-type users --entity-name "$SCRAM_USER"
"$KDIR/bin/kafka-topics.sh" --bootstrap-server "$ADMIN_BOOTSTRAP" \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1

echo "== build examples =="
cargo build --release --example sasl --example tls

export KAFKA_BOOTSTRAP="$SSL_BOOTSTRAP"
export KAFKA_TOPIC="$TOPIC"
export KAFKA_USERNAME="$SCRAM_USER"
export KAFKA_PASSWORD="$SCRAM_PASS"
export TLS_CA_PEM="$CA_PEM"
export TLS_SERVER_NAME="localhost"

echo "== SASL_SSL produce (SCRAM-SHA-256 + rustls) =="
export SASL_MECHANISM="SCRAM-SHA-256"
cargo run --release --example sasl

echo "== SASL_SSL produce (SCRAM-SHA-512 + rustls) =="
export SASL_MECHANISM="SCRAM-SHA-512"
cargo run --release --example sasl

echo "== SSL-only produce (no SASL) against SASL_SSL should fail closed =="
set +e
timeout 20s env -u KAFKA_USERNAME -u KAFKA_PASSWORD -u SASL_MECHANISM \
  KAFKA_BOOTSTRAP="$SSL_BOOTSTRAP" KAFKA_TOPIC="$TOPIC" \
  TLS_CA_PEM="$CA_PEM" TLS_SERVER_NAME="localhost" \
  cargo run --release --example tls >/tmp/pl-auth-tls-only.log 2>&1
tls_only_rc=$?
set -e
if grep -E -- '@[0-9]+' /tmp/pl-auth-tls-only.log >/dev/null; then
  echo "ci-auth-smoke: TLS-only produce unexpectedly succeeded on SASL_SSL" >&2
  cat /tmp/pl-auth-tls-only.log >&2
  exit 1
fi
echo "ci-auth-smoke: TLS-only correctly failed (rc=${tls_only_rc})"

echo "ci-auth-smoke: ok (SASL_SSL SCRAM-SHA-256/512 @ ${SSL_BOOTSTRAP})"
