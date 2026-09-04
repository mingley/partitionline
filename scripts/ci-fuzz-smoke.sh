#!/usr/bin/env bash
# Short libFuzzer smoke for CI (WP-2.1). Requires nightly + cargo-fuzz + g++.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SECONDS_PER_TARGET="${FUZZ_SECONDS:-15}"
export CXX="${CXX:-g++}"

if ! command -v g++ >/dev/null 2>&1; then
  echo "ci-fuzz-smoke: g++ required (libfuzzer-sys links C++)" >&2
  exit 1
fi

if ! command -v cargo-fuzz >/dev/null 2>&1; then
  cargo install cargo-fuzz --locked
fi

rustup toolchain install nightly --profile minimal --component rust-src
rustup run nightly cargo fuzz build

targets=(
  decode_fetch_response
  decode_produce_response
  decode_metadata_response
  decode_record_batches
  decode_group_responses
  decode_share_fetch_response
)

for t in "${targets[@]}"; do
  echo "== fuzz $t (${SECONDS_PER_TARGET}s) =="
  rustup run nightly cargo fuzz run "$t" -- \
    -max_total_time="$SECONDS_PER_TARGET" \
    -timeout=5 \
    -rss_limit_mb=2048
done

echo "ci-fuzz-smoke: ok"
