#!/usr/bin/env bash
# Real-broker smoke against Apache Kafka (KRaft).
# Used by CI (broker-smoke job) and local verification.
#
# Env:
#   KAFKA_IMAGE      default apache/kafka:3.9.1 (CI also runs apache/kafka:4.1.0)
#   KAFKA_BOOTSTRAP  default 127.0.0.1:9092
#   SKIP_DOCKER=1    use an already-running broker at KAFKA_BOOTSTRAP (no Docker)
#   REQUIRE_SHARE=1  fail if share smoke cannot fetch (default on Kafka 4.x images)
#   REQUIRE_KIP848=1 fail if KIP-848 consumer-protocol smoke cannot fetch (default on 4.x)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.1}"
BROKER_NAME="${BROKER_NAME:-pl-ci-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${KAFKA_TOPIC:-pl-ci-smoke}"
OUT_TOPIC="${KAFKA_OUTPUT_TOPIC:-pl-ci-smoke-out}"

kafka_image_is_4x() {
  [[ "$KAFKA_IMAGE" =~ :4\. ]] || [[ "$KAFKA_IMAGE" =~ /kafka:4 ]]
}

# KIP-932 share needs ShareFetch + finalized share.version=1.
# Default: require on 4.x images; allow soft-skip on 3.x.
if [[ -z "${REQUIRE_SHARE:-}" ]]; then
  if kafka_image_is_4x; then
    REQUIRE_SHARE=1
  else
    REQUIRE_SHARE=0
  fi
fi

# KIP-848 next-gen groups need ConsumerGroupHeartbeat (Kafka 4.x).
if [[ -z "${REQUIRE_KIP848:-}" ]]; then
  if kafka_image_is_4x; then
    REQUIRE_KIP848=1
  else
    REQUIRE_KIP848=0
  fi
fi

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

find_topics_bin() {
  if command -v kafka-topics.sh >/dev/null 2>&1; then
    command -v kafka-topics.sh
    return 0
  fi
  local cand
  for cand in \
    /tmp/kafka_4.1.0/bin/kafka-topics.sh \
    /tmp/kafka_4.0.0/bin/kafka-topics.sh \
    /tmp/kafka_3.9.1/bin/kafka-topics.sh \
    "${KAFKA_HOME:-}/bin/kafka-topics.sh"; do
    if [[ -x "$cand" ]]; then
      echo "$cand"
      return 0
    fi
  done
  return 1
}

# Finalize share.version=1 when the CLI is available (Docker or native).
upgrade_share_feature() {
  local mode="$1" # docker | native
  local log=/tmp/pl-share-feature-smoke.log
  if [[ "$mode" == "docker" ]]; then
    if docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-features.sh \
      --bootstrap-server localhost:9092 upgrade --feature share.version=1 >"$log" 2>&1; then
      echo "ci-broker-smoke: share.version=1"
      return 0
    fi
  else
    local features_bin="" topics_bin
    if topics_bin="$(find_topics_bin)"; then
      features_bin="${topics_bin%/*}/kafka-features.sh"
    fi
    if [[ -x "$features_bin" ]]; then
      if "$features_bin" --bootstrap-server "$BOOTSTRAP" upgrade --feature share.version=1 >"$log" 2>&1; then
        echo "ci-broker-smoke: share.version=1"
        return 0
      fi
    else
      return 0
    fi
  fi
  # Already at level 1, or 3.x without the feature — not fatal here.
  echo "ci-broker-smoke: share.version upgrade skipped (see $log)"
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

  # Build once so timed paths (share/group/eos) are not racing cargo compile.
  echo "== build examples =="
  cargo build --release --examples

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

  # Cooperative-sticky rebalance path (KIP-429).
  echo "== cooperative =="
  cargo run --release --example produce >/dev/null
  export KAFKA_GROUP="pl-ci-coop"
  for attempt in 1 2 3 4 5 6; do
    if run_until_progress "cooperative" '@[0-9]+' cargo run --release --example cooperative; then
      break
    fi
    if [[ "$attempt" -eq 6 ]]; then
      echo "ci-broker-smoke: cooperative example failed after retries" >&2
      exit 1
    fi
    echo "ci-broker-smoke: cooperative not ready yet; retry $attempt"
    sleep 3
  done

  # KIP-848 next-gen consumer groups (ConsumerGroupHeartbeat; Kafka 4.x).
  # Soft-skips on brokers that lack the API unless REQUIRE_KIP848=1.
  echo "== kip848 =="
  cargo run --release --example produce >/dev/null
  export KAFKA_GROUP="pl-ci-kip848"
  kip848_ok=0
  kip848_log="$(mktemp)"
  for attempt in 1 2 3 4 5 6; do
    set +e
    timeout 45s cargo run --release --example kip848 >"$kip848_log" 2>&1
    kip848_rc=$?
    set -e
    if grep -E -- '@[0-9]+' "$kip848_log" >/dev/null; then
      echo "ci-broker-smoke: kip848 ok"
      kip848_ok=1
      break
    fi
    if grep -qiE 'UnsupportedVersion|Unsupported|UNKNOWN_SERVER_ERROR|does not support|ConsumerGroupHeartbeat|InvalidRequest|API version|need [0-9]+ bytes|truncated response' "$kip848_log"; then
      break
    fi
    # Truncated Protocol bodies on 3.9 are terminal when KIP-848 is optional —
    # do not burn six retries before the soft-skip path.
    if [[ "${REQUIRE_KIP848:-0}" != "1" ]] && grep -qiE 'Protocol\(|need [0-9]+ bytes|have 0' "$kip848_log"; then
      break
    fi
    echo "ci-broker-smoke: kip848 not ready yet; retry $attempt"
    sleep 3
  done
  if [[ "$kip848_ok" -eq 1 ]]; then
    rm -f "$kip848_log"
  elif [[ "${REQUIRE_KIP848:-0}" == "1" ]]; then
    echo "ci-broker-smoke: kip848 failed (REQUIRE_KIP848=1, rc=${kip848_rc:-1}); log:" >&2
    cat "$kip848_log" >&2 || true
    rm -f "$kip848_log"
    exit 1
  else
    # Kafka 3.9 often returns empty/truncated bodies (Protocol need-bytes) rather
    # than a clean UnsupportedVersion — soft-skip whenever KIP-848 is optional.
    echo "ci-broker-smoke: kip848 skipped (optional on this broker; use Kafka 4.x + REQUIRE_KIP848=1)"
    rm -f "$kip848_log"
  fi

  # KIP-932 share groups need Kafka 4.x ShareFetch + share.version=1.
  # Default share.auto.offset.reset is latest, so produce while the share
  # member is already polling (not only beforehand).
  echo "== share =="
  export KAFKA_GROUP="pl-ci-share"
  share_log="$(mktemp)"
  set +e
  timeout 45s cargo run --release --example share >"$share_log" 2>&1 &
  share_pid=$!
  set -e
  sleep 4
  cargo run --release --example produce >/dev/null || true
  cargo run --release --example produce >/dev/null || true
  set +e
  wait "$share_pid"
  share_rc=$?
  set -e
  if grep -E -- '@[0-9]+' "$share_log" >/dev/null; then
    echo "ci-broker-smoke: share ok"
  elif [[ "$REQUIRE_SHARE" == "1" ]]; then
    echo "ci-broker-smoke: share failed (REQUIRE_SHARE=1, rc=$share_rc); log:" >&2
    cat "$share_log" >&2 || true
    rm -f "$share_log"
    exit 1
  else
    # Optional on 3.x — truncated Protocol errors are common without ShareFetch.
    echo "ci-broker-smoke: share skipped (optional on this broker; use Kafka 4.x + share.version=1)"
  fi
  rm -f "$share_log"
}

if [[ "${SKIP_DOCKER:-}" == "1" ]]; then
  echo "ci-broker-smoke: SKIP_DOCKER=1; using existing broker at $BOOTSTRAP"
  if ! wait_tcp "$BOOTSTRAP"; then
    echo "ci-broker-smoke: broker not reachable at $BOOTSTRAP" >&2
    exit 1
  fi
  # Topic create via kafka CLI when available (PATH or common native install).
  topics_bin=""
  if topics_bin="$(find_topics_bin)"; then
    "$topics_bin" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$TOPIC" --partitions 1 --replication-factor 1 || true
    "$topics_bin" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$OUT_TOPIC" --partitions 1 --replication-factor 1 || true
  fi
  upgrade_share_feature native || true
  # Native Kafka 4.x share / KIP-848 smoke should be required when features CLI is present.
  if [[ -x /tmp/kafka_4.1.0/bin/kafka-features.sh || -x /tmp/kafka_4.0.0/bin/kafka-features.sh ]]; then
    if [[ "$REQUIRE_SHARE" != "1" ]]; then
      REQUIRE_SHARE=1
    fi
    if [[ "${REQUIRE_KIP848:-0}" != "1" ]]; then
      REQUIRE_KIP848=1
    fi
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
# Do NOT inject partial KAFKA_* env on 4.x — that switches the image into
# env-driven config and fails storage format with missing process.roles
# (seen on apache/kafka:4.1.0 in GHA). Start with image defaults, then
# upgrade share.version=1 after ready (upgrade_share_feature).
docker_run_args=(-d --name "$BROKER_NAME" -p 9092:9092)
if ! docker run "${docker_run_args[@]}" "$KAFKA_IMAGE"; then
  echo "ci-broker-smoke: docker run failed (overlay often broken in nested VMs)." >&2
  # Verifiable fallback: if a broker is already listening, use it (same as SKIP_DOCKER=1).
  if wait_tcp "$BOOTSTRAP"; then
    echo "ci-broker-smoke: $BOOTSTRAP is up — falling back to existing broker" >&2
    trap - EXIT
    cleanup || true
    SKIP_DOCKER=1 exec bash "$0" "$@"
  fi
  echo "ci-broker-smoke: no broker at $BOOTSTRAP; start native Kafka or set SKIP_DOCKER=1" >&2
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

if kafka_image_is_4x; then
  upgrade_share_feature docker || true
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
