#!/usr/bin/env bash
set -euo pipefail

export CARGO_PROFILE_RELEASE_LTO=false
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
export CARGO_TARGET_DIR="$GITHUB_WORKSPACE/perf-candidate-target"
test "$(git -C "$GITHUB_WORKSPACE/baseline" rev-parse HEAD)" = 3f45fe6255e72abc6b0c4488bc13117779c50787
cp examples/qualify_group_conn.rs baseline/examples/qualify_group_conn.rs
candidate_source="$(sha256sum examples/qualify_group_conn.rs | cut -d ' ' -f 1)"
baseline_source="$(sha256sum baseline/examples/qualify_group_conn.rs | cut -d ' ' -f 1)"
test "$candidate_source" = "$baseline_source"
printf 'probe source sha256: %s\n' "$candidate_source"

cargo +stable build --release --locked --example qualify_group_conn
candidate_bin="$CARGO_TARGET_DIR/release/examples/qualify_group_conn"
export CARGO_TARGET_DIR="$GITHUB_WORKSPACE/perf-baseline-target"
(
  cd baseline
  cargo +stable build --release --locked --example qualify_group_conn
)
baseline_bin="$CARGO_TARGET_DIR/release/examples/qualify_group_conn"
candidate_hash="$(sha256sum "$candidate_bin" | cut -d ' ' -f 1)"
baseline_hash="$(sha256sum "$baseline_bin" | cut -d ' ' -f 1)"
printf 'candidate binary sha256: %s\nbaseline binary sha256: %s\n' "$candidate_hash" "$baseline_hash"
test "$candidate_hash" != "$baseline_hash"
test "$(realpath "$candidate_bin")" != "$(realpath "$baseline_bin")"
uname -a
lscpu
rustc +stable -Vv

for trial in 1 2 3 4 5 6 7 8; do
  if [ $((trial % 2)) -eq 1 ]; then
    printf 'trial=%s implementation=baseline\n' "$trial"
    "$baseline_bin"
    printf 'trial=%s implementation=candidate\n' "$trial"
    "$candidate_bin"
  else
    printf 'trial=%s implementation=candidate\n' "$trial"
    "$candidate_bin"
    printf 'trial=%s implementation=baseline\n' "$trial"
    "$baseline_bin"
  fi
done
