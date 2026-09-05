#!/usr/bin/env bash
# Ensure a Kafka broker is listening on KAFKA_BOOTSTRAP (default 127.0.0.1:9092).
# Prefer an already-running broker; otherwise start native Kafka via
# scripts/ci-native-kafka.sh (Docker overlay often fails in nested Cloud Agent VMs).
#
# Usage:
#   # shellcheck source=scripts/lib/ensure-broker.sh
#   source "$ROOT/scripts/lib/ensure-broker.sh"
#   pl_ensure_broker            # exit 1 if unavailable
#   pl_ensure_broker || soft    # caller handles failure
#
# Sets PL_ENSURE_BROKER_STARTED=1 when this helper started native Kafka (so
# callers can choose to leave it running for subsequent gates — default leave up).

pl_broker_tcp_ready() {
  local bootstrap="${1:-${KAFKA_BOOTSTRAP:-127.0.0.1:9092}}"
  local host="${bootstrap%:*}"
  local port="${bootstrap##*:}"
  # Bash /dev/tcp alone can false-accept a dying listener after kafka stop.
  if ! (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
    return 1
  fi
  local tip tip_bin
  for tip in "${KAFKA_HOME:-}" /tmp/kafka_4.1.0 /tmp/kafka_4.0.0 /tmp/kafka_3.9.1; do
    tip_bin="${tip}/bin/kafka-topics.sh"
    if [[ -x "$tip_bin" ]]; then
      if "$tip_bin" --bootstrap-server "$bootstrap" --list >/dev/null 2>&1; then
        return 0
      fi
      return 1
    fi
  done
  # No kafka-topics.sh: require the TCP accept to stay up briefly.
  sleep 0.2
  (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1
}

pl_ensure_broker() {
  local bootstrap="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
  local root="${ROOT:-}"
  local prefix="${1:-ensure-broker}"
  PL_ENSURE_BROKER_STARTED=0
  export PL_ENSURE_BROKER_STARTED

  if [[ -z "$root" ]]; then
    root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  fi

  if pl_broker_tcp_ready "$bootstrap"; then
    # shellcheck source=scripts/lib/broker-identity.sh
    source "${root}/scripts/lib/broker-identity.sh"
    pl_broker_identity_resolve_existing "$bootstrap"
    pl_broker_identity_print "${prefix}"
    echo "${prefix}: broker already up at ${bootstrap}"
    return 0
  fi

  if [[ "${ALLOW_NATIVE_FALLBACK:-1}" != "1" ]]; then
    echo "${prefix}: no broker at ${bootstrap} (ALLOW_NATIVE_FALLBACK=0)" >&2
    return 1
  fi
  if [[ ! -x "${root}/scripts/ci-native-kafka.sh" ]]; then
    echo "${prefix}: no broker at ${bootstrap} and ci-native-kafka.sh missing" >&2
    return 1
  fi

  echo "${prefix}: no broker at ${bootstrap} — starting native Kafka"
  local start_out
  if ! start_out="$(bash "${root}/scripts/ci-native-kafka.sh" start 2>&1)"; then
    echo "$start_out" >&2
    echo "${prefix}: native Kafka start failed" >&2
    return 1
  fi
  echo "$start_out"
  if [[ "$start_out" == *"already running"* ]]; then
    PL_ENSURE_BROKER_STARTED=0
  else
    PL_ENSURE_BROKER_STARTED=1
  fi
  export PL_ENSURE_BROKER_STARTED

  if ! pl_broker_tcp_ready "$bootstrap"; then
    echo "${prefix}: native Kafka started but ${bootstrap} still closed" >&2
    return 1
  fi
  # shellcheck source=scripts/lib/broker-identity.sh
  source "${root}/scripts/lib/broker-identity.sh"
  pl_broker_identity_set_native
  pl_broker_identity_print "${prefix}"
  echo "${prefix}: native Kafka ready at ${bootstrap}"
  return 0
}
