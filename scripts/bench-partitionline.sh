#!/usr/bin/env bash
# Lab A: this crate, --release.
# Locked knobs (same as librdkafka C): acks=all, linger.ms=50,
# compression=none, idempotent=true, batch.size=1000000.
# Window: 60s warmup (discarded) + 180s measured.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-bench}"
SIZE="${SIZE:-1024}"
SECONDS_N="${SECONDS_N:-180}"
WARMUP="${WARMUP:-60}"
LINGER_MS="${LINGER_MS:-50}"
INFLIGHT="${INFLIGHT:-100000}"
CSV="${CSV:-}"

cd "$ROOT"
BIN="${PARTITIONLINE_BENCH:-$ROOT/target/release/bench}"
if [[ ! -x "$BIN" ]]; then
  cargo build --release --bin bench
  BIN="$ROOT/target/release/bench"
fi
# shellcheck disable=SC2086
exec "$BIN" \
  --bootstrap "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --size "$SIZE" \
  --seconds "$SECONDS_N" \
  --warmup "$WARMUP" \
  --linger-ms "$LINGER_MS" \
  --inflight "$INFLIGHT" \
  ${CSV:+--csv "$CSV"}
