#!/usr/bin/env bash
# Resolve and stamp the *actual* broker behind Verifiable smokes.
# Requested matrix cells (KAFKA_IMAGE) must not silently report when Docker
# falls back to native Kafka or an external listener.
#
# Exports:
#   PL_BROKER_MODE    docker | native | external
#   PL_BROKER_ACTUAL  stable identity string (e.g. docker:apache/kafka:4.1.0,
#                     native:4.1.0 path=/tmp/kafka_4.1.0, external:127.0.0.1:9092)
#
# Usage:
#   # shellcheck source=scripts/lib/broker-identity.sh
#   source "$ROOT/scripts/lib/broker-identity.sh"
#   pl_broker_identity_set_docker "$KAFKA_IMAGE"
#   pl_broker_identity_resolve_existing "$BOOTSTRAP"
#   pl_broker_identity_print "ci-broker-smoke"

pl_broker_identity_stamp_file() {
  echo "${PL_BROKER_IDENTITY_FILE:-/tmp/partitionline-broker-identity}"
}

pl_broker_identity_native_version() {
  local ver="${KAFKA_VERSION:-}"
  local home="${KAFKA_HOME:-}"
  if [[ -n "$ver" ]]; then
    echo "$ver"
    return 0
  fi
  if [[ -n "$home" && "$home" =~ kafka_([0-9]+(\.[0-9]+)*) ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  local d
  for d in /tmp/kafka_4.1.0 /tmp/kafka_4.0.0 /tmp/kafka_3.9.1; do
    if [[ -d "$d/bin" ]]; then
      echo "${d##*_}"
      return 0
    fi
  done
  echo "unknown"
}

pl_broker_identity_native_path() {
  local ver
  ver="$(pl_broker_identity_native_version)"
  echo "${KAFKA_HOME:-/tmp/kafka_${ver}}"
}

pl_broker_identity_native_running() {
  local pidfile="${KAFKA_PIDFILE:-/tmp/partitionline-kafka.pid}"
  [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

pl_broker_identity_set() {
  PL_BROKER_MODE="${1:?mode}"
  PL_BROKER_ACTUAL="${2:?actual}"
  export PL_BROKER_MODE PL_BROKER_ACTUAL
  printf '%s\n' "$PL_BROKER_ACTUAL" >"$(pl_broker_identity_stamp_file)" 2>/dev/null || true
}

pl_broker_identity_set_docker() {
  local image="${1:?image}"
  pl_broker_identity_set "docker" "docker:${image}"
}

pl_broker_identity_set_native() {
  local ver path
  ver="$(pl_broker_identity_native_version)"
  path="$(pl_broker_identity_native_path)"
  pl_broker_identity_set "native" "native:${ver} path=${path}"
}

pl_broker_identity_set_external() {
  local bootstrap="${1:?bootstrap}"
  pl_broker_identity_set "external" "external:${bootstrap}"
}

# Resolve identity for SKIP_DOCKER / fallback paths.
# Prefer: explicit env → native pid → identity stamp file → docker container → external.
pl_broker_identity_resolve_existing() {
  local bootstrap="${1:-${KAFKA_BOOTSTRAP:-127.0.0.1:9092}}"
  local broker_name="${BROKER_NAME:-pl-ci-kafka}"
  local stamp

  if [[ -n "${PL_BROKER_ACTUAL:-}" && -n "${PL_BROKER_MODE:-}" ]]; then
    return 0
  fi

  if [[ "${PL_ENSURE_BROKER_STARTED:-0}" == "1" ]] || pl_broker_identity_native_running; then
    pl_broker_identity_set_native
    return 0
  fi

  stamp="$(pl_broker_identity_stamp_file)"
  if [[ -f "$stamp" ]]; then
    local actual
    actual="$(tr -d '\r\n' <"$stamp")"
    if [[ "$actual" == docker:* ]]; then
      pl_broker_identity_set "docker" "$actual"
      return 0
    fi
    if [[ "$actual" == native:* ]]; then
      pl_broker_identity_set "native" "$actual"
      return 0
    fi
    if [[ "$actual" == external:* ]]; then
      pl_broker_identity_set "external" "$actual"
      return 0
    fi
  fi

  if command -v docker >/dev/null 2>&1 && docker inspect -f '{{.State.Running}}' "$broker_name" 2>/dev/null | grep -q true; then
    local image
    image="$(docker inspect -f '{{.Config.Image}}' "$broker_name" 2>/dev/null || true)"
    if [[ -n "$image" ]]; then
      pl_broker_identity_set_docker "$image"
      return 0
    fi
  fi

  pl_broker_identity_set_external "$bootstrap"
}

pl_broker_identity_print() {
  local prefix="${1:-broker-identity}"
  if [[ -z "${PL_BROKER_ACTUAL:-}" || -z "${PL_BROKER_MODE:-}" ]]; then
    pl_broker_identity_resolve_existing "${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
  fi
  echo "${prefix}: actual=${PL_BROKER_ACTUAL} mode=${PL_BROKER_MODE}"
}


pl_broker_identity_self_test() {
  local fail=0
  PL_BROKER_MODE= PL_BROKER_ACTUAL=
  pl_broker_identity_set_docker "apache/kafka:4.1.0"
  [[ "$PL_BROKER_ACTUAL" == "docker:apache/kafka:4.1.0" && "$PL_BROKER_MODE" == "docker" ]] || fail=1
  PL_BROKER_MODE= PL_BROKER_ACTUAL=
  KAFKA_VERSION=4.1.0 KAFKA_HOME=/tmp/kafka_4.1.0 pl_broker_identity_set_native
  [[ "$PL_BROKER_ACTUAL" == "native:4.1.0 path=/tmp/kafka_4.1.0" && "$PL_BROKER_MODE" == "native" ]] || fail=1
  PL_BROKER_MODE= PL_BROKER_ACTUAL=
  pl_broker_identity_set_external "127.0.0.1:9092"
  [[ "$PL_BROKER_ACTUAL" == "external:127.0.0.1:9092" && "$PL_BROKER_MODE" == "external" ]] || fail=1
  if [[ "$fail" -ne 0 ]]; then
    echo "pl_broker_identity_self_test: FAIL" >&2
    return 1
  fi
  echo "pl_broker_identity_self_test: ok"
  return 0
}

if [[ "${BASH_SOURCE[0]}" == "$0" && "${1:-}" == "--self-test" ]]; then
  pl_broker_identity_self_test
fi
