#!/usr/bin/env bash
# Lab A fetch opponent: librdkafka 2.15.0 C rdkafka_performance -C.
# Consume from earliest. -c COUNT must be the produced record count
# (produce first, completed). Multiple -p for the 6 Lab A partitions.
set -euo pipefail

PERF="${RDKAFKA_PERF:-/tmp/lab-a/librdkafka-2.15.0/examples/rdkafka_performance}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-bench}"
COUNT="${COUNT:-}"
LOG="${LOG:-/dev/stderr}"

if [[ ! -x "$PERF" ]]; then
  echo "rdkafka_performance not found at $PERF" >&2
  exit 2
fi
if [[ -z "$COUNT" ]]; then
  echo "COUNT=produced-record-count is required (produce first)" >&2
  exit 2
fi

echo "librdkafka fetch: -C -t $TOPIC -o beginning -p 0..5 -c $COUNT" >&2
"$PERF" -C \
  -t "$TOPIC" \
  -b "$BOOTSTRAP" \
  -o beginning \
  -p 0 -p 1 -p 2 -p 3 -p 4 -p 5 \
  -c "$COUNT" \
  -X auto.offset.reset=earliest \
  >"$LOG" 2>&1
echo "librdkafka fetch window ended" >&2
