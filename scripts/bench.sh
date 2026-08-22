#!/usr/bin/env bash
# Drive Lab A produce windows (acks=all, linger=50, compression=none,
# idempotent=true, 60s warmup + 180s × 3). Does not invent numbers.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
export TOPIC="${TOPIC:-bench}"
export SIZE="${SIZE:-1024}"
export SECONDS_N="${SECONDS_N:-180}"
export WARMUP="${WARMUP:-60}"
export LINGER_MS="${LINGER_MS:-50}"
export INFLIGHT="${INFLIGHT:-100000}"
export BATCH_SIZE="${BATCH_SIZE:-1000000}"
KAFKA_HOME="${KAFKA_HOME:-/tmp/lab-a/kafka_2.13-4.3.1}"
HOST="$(hostname -s)"
DATE="$(date -u +%Y-%m-%d)"
OUT="${OUT:-$ROOT/results/published/${DATE}-${HOST}}"
mkdir -p "$OUT/raw"

{
  echo "captured_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "hostname=$(hostname)"
  uname -a
  echo
  lscpu
  echo
  free -h
} >"$OUT/machine.txt"

cat >"$OUT/pins.toml" <<EOF
kafka = "4.3.1"
kafka_home = "$KAFKA_HOME"
broker = "host process (not docker)"
librdkafka = "2.15.0"
rdkafka_performance = "${RDKAFKA_PERF:-/tmp/lab-a/librdkafka-2.15.0/examples/rdkafka_performance}"
partitionline = "$(cd "$ROOT" && git rev-parse --short HEAD)"
topic = "$TOPIC"
partitions = 6
replication_factor = 1
payload_bytes = $SIZE
linger_ms = $LINGER_MS
acks = "all"
acks_note = "Lab A asked for acks=1 + idempotent=true; librdkafka 2.15.0 requires acks=all when enable.idempotence=true. Both clients used acks=all."
compression = "none"
idempotent = true
batch_size = $BATCH_SIZE
queue_buffering_max_messages = 100000
warmup_s = $WARMUP
measured_s = $SECONDS_N
reps = 3
window_note = "60s warmup + ${SECONDS_N}s × 3 (not 10 min × 3)"
EOF

recreate_topic() {
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" 2>/dev/null || true
  sleep 1
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --topic "$TOPIC" --partitions 6 --replication-factor 1 \
    --config retention.ms=15000 --config segment.bytes=33554432
}

avail_gb() {
  df -BG / | awk 'NR==2 { gsub(/G/, "", $4); print $4 }'
}

recreate_topic

echo "=== librdkafka 2.15.0 C ==="
for rep in 1 2 3; do
  export LATFILE="$OUT/raw/librdkafka-rep${rep}.lat"
  export LOG="$OUT/raw/librdkafka-rep${rep}.log"
  if [[ "$rep" == "1" ]]; then
    WARMUP="$WARMUP" "$ROOT/scripts/bench-librdkafka.sh"
  else
    WARMUP=0 "$ROOT/scripts/bench-librdkafka.sh"
  fi
  python3 "$ROOT/scripts/percentiles.py" "$LATFILE" | tee "$OUT/raw/librdkafka-rep${rep}.pct"
  recreate_topic
  if [[ "$(avail_gb)" -lt 20 ]]; then
    echo "abort: only $(avail_gb)G free; will not invent numbers" >&2
    exit 2
  fi
done

echo "=== partitionline ==="
for rep in 1 2 3; do
  if [[ "$rep" == "1" ]]; then
    w="$WARMUP"
  else
    w=0
  fi
  WARMUP="$w" CSV="$OUT/raw/partitionline-rep${rep}.csv" \
    "$ROOT/scripts/bench-partitionline.sh" | tee "$OUT/raw/partitionline-rep${rep}.log"
  recreate_topic
  if [[ "$(avail_gb)" -lt 20 ]]; then
    echo "abort: only $(avail_gb)G free; will not invent numbers" >&2
    exit 2
  fi
done

echo "raw results in $OUT"
