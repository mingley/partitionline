#!/usr/bin/env bash
# Lab A fetch: this crate, --release. Produce first (completed), then
# consume from earliest. Not a locked 60s+180s×3 window unless OUT+SECONDS
# are set that way and someone asked for that window.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-bench}"
SIZE="${SIZE:-1024}"
PRODUCE_SECONDS="${PRODUCE_SECONDS:-0}"
LINGER_MS="${LINGER_MS:-50}"
INFLIGHT="${INFLIGHT:-100000}"
CSV="${CSV:-}"

cd "$ROOT"
BIN="${PARTITIONLINE_BENCH_FETCH:-$ROOT/target/release/bench-fetch}"
if [[ ! -x "$BIN" ]]; then
  cargo build --release --bin bench-fetch
  BIN="$ROOT/target/release/bench-fetch"
fi
# shellcheck disable=SC2086
exec "$BIN" \
  --bootstrap "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --size "$SIZE" \
  --produce-seconds "$PRODUCE_SECONDS" \
  --linger-ms "$LINGER_MS" \
  --inflight "$INFLIGHT" \
  ${CSV:+--csv "$CSV"}
