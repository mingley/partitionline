#!/usr/bin/env bash
# Lab A: librdkafka 2.15.0 C rdkafka_performance.
# Locked knobs (same as partitionline): acks=all, linger.ms=50,
# compression=none, enable.idempotence=true, batch.size=1000000.
# Window: 60s warmup (discarded) + 180s measured. The example binary
# defaults linger.ms=1000 — we override to 50.
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
  local out="$2"
  local lat="${3:-}"
  local extra=()
  if [[ -n "$lat" ]]; then
    extra+=(-A "$lat")
  fi
  # Start the binary itself so SIGINT hits rdkafka_performance (not a bash wrapper).
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
    "${extra[@]}" >"$out" 2>&1 &
  local pid=$!
  sleep "$secs"
  kill -INT "$pid" 2>/dev/null || true
  wait "$pid" || true
}

if [[ "$WARMUP" != "0" ]]; then
  echo "librdkafka warmup ${WARMUP}s (discarded)" >&2
  run_window "$WARMUP" /tmp/lab-a-rdkafka-warmup.log
  echo "warmup discarded" >&2
fi

echo "librdkafka measured ${SECONDS_N}s size=$SIZE linger.ms=$LINGER_MS acks=all compression=none enable.idempotence=true batch.size=$BATCH_SIZE" >&2
run_window "$SECONDS_N" "$LOG" "$LATFILE"
echo "librdkafka measured window ended" >&2
