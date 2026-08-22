#!/usr/bin/env bash
# Lab A: librdkafka C rdkafka_performance. Same knobs as partitionline.
# linger.ms=50 (the example binary defaults to 1000 — we override).
set -euo pipefail

PERF="${RDKAFKA_PERF:-/tmp/lab-a/librdkafka-2.15.0/examples/rdkafka_performance}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-bench}"
SIZE="${SIZE:-1024}"
SECONDS_N="${SECONDS_N:-180}"
WARMUP="${WARMUP:-60}"
LINGER_MS="${LINGER_MS:-50}"
BATCH_SIZE="${BATCH_SIZE:-1000000}"
LATFILE="${LATFILE:-}"
LOG="${LOG:-/dev/stderr}"

if [[ ! -x "$PERF" ]]; then
  echo "rdkafka_performance not found at $PERF" >&2
  exit 2
fi

run_window() {
  local secs="$1"
  local lat="${2:-}"
  local extra=()
  if [[ -n "$lat" ]]; then
    extra+=(-A "$lat")
  fi
  # No -c: run until SIGINT. -l + optional -A for produce→DR latency samples.
  # Do not set linger=0. Do not raise batch.size above partitionline.
  "$PERF" -P \
    -t "$TOPIC" \
    -b "$BOOTSTRAP" \
    -s "$SIZE" \
    -l \
    -a all \
    -z none \
    -X linger.ms="$LINGER_MS" \
    -X enable.idempotence=true \
    -X batch.size="$BATCH_SIZE" \
    -X queue.buffering.max.messages=100000 \
    "${extra[@]}"
}

if [[ "$WARMUP" != "0" ]]; then
  echo "librdkafka warmup ${WARMUP}s (discarded)" >&2
  run_window "$WARMUP" >/tmp/lab-a-rdkafka-warmup.log 2>&1 &
  wp=$!
  sleep "$WARMUP"
  kill -INT "$wp" 2>/dev/null || true
  wait "$wp" || true
  echo "warmup discarded" >&2
fi

echo "librdkafka measured ${SECONDS_N}s size=$SIZE linger.ms=$LINGER_MS acks=all compression=none enable.idempotence=true batch.size=$BATCH_SIZE" >&2
run_window "$SECONDS_N" "$LATFILE" >"$LOG" 2>&1 &
mp=$!
sleep "$SECONDS_N"
kill -INT "$mp" 2>/dev/null || true
wait "$mp" || true
echo "librdkafka measured window ended" >&2
