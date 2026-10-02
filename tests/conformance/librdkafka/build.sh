#!/usr/bin/env bash
# Reuse native pin and freeze both behavioral adapters' actual build inputs.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
CASE_BUILD_DIR="${CASE_BUILD_DIR:-$ROOT/work/librdkafka-0125}"
export CASE_BUILD_DIR ROOT
mkdir -p "$CASE_BUILD_DIR"
python3 "$HERE/build-manifest.py" snapshot
bash "$ROOT/benchmarks/peers/librdkafka/build-behavioral.sh"
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$CASE_BUILD_DIR/rust-target" cargo build --locked --offline --manifest-path "$HERE/rust/Cargo.toml" -j"${CASE_BUILD_JOBS:-2}"
python3 "$HERE/build-manifest.py" finish
