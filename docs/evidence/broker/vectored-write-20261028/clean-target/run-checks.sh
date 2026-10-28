#!/usr/bin/env bash
set -u
set -o pipefail
export RUSTUP_HOME=/workspace/work/rustup
export CARGO_HOME=/workspace/work/cargo
export PATH=/workspace/work/cargo/bin:$PATH
export CARGO_TARGET_DIR=/workspace/work/goal-qualification-20261028/vectored-target
export CARGO_BUILD_JOBS=2
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
out=/workspace/work/goal-qualification-20261028/vectored-write-01/clean-target-run
cd /workspace/work/broker-vectored || exit 1
printf '%s\n' 'Initial inspection: /bin/bash: line 1: rustc: command not found (ambient PATH before setting CARGO_HOME/PATH).' > "$out/setup-failure.log"
{ date -u; git rev-parse HEAD; git status --short; rustc +stable -Vv; cargo +stable -Vv; env | LC_ALL=C sort | grep -E '^(CARGO_(BUILD_JOBS|HOME|INCREMENTAL|PROFILE_DEV_DEBUG|PROFILE_TEST_DEBUG|TARGET_DIR)|RUSTUP_HOME)='; } > "$out/environment.log"
tar -czf "$out/pre-run-source.tar.gz" clippy.toml partitionline-broker
find partitionline-broker -type f -print0 | sort -z | xargs -0 sha256sum > "$out/pre-run-source.sha256"
sha256sum clippy.toml >> "$out/pre-run-source.sha256"
runcheck() {
  local name=$1
  shift
  { printf 'Command:'; printf ' %q' "$@"; printf '\n'; date -u; } > "$out/$name.log"
  "$@" >> "$out/$name.log" 2>&1
  local status=$?
  printf 'exit=%d\n' "$status" >> "$out/$name.log"
  printf '%s %d\n' "$name" "$status" | tee -a "$out/results.log"
}
: > "$out/results.log"
runcheck unit cargo +stable test --offline --locked --manifest-path partitionline-broker/Cargo.toml --lib write_prefetch_tests -- --test-threads=1
runcheck transport cargo +stable test --offline --locked --manifest-path partitionline-broker/Cargo.toml --test transport -- --test-threads=1
runcheck transport-all-features cargo +stable test --offline --locked --all-features --manifest-path partitionline-broker/Cargo.toml --test transport -- --test-threads=1
runcheck tls-all-features cargo +stable test --offline --locked --all-features --manifest-path partitionline-broker/Cargo.toml --test tls -- --test-threads=1
runcheck clippy cargo +stable clippy --offline --locked --all-features --manifest-path partitionline-broker/Cargo.toml --lib --test transport --test tls -- -D warnings
find partitionline-broker -type f -print0 | sort -z | xargs -0 sha256sum > "$out/post-run-source.sha256"
sha256sum clippy.toml >> "$out/post-run-source.sha256"
cmp "$out/pre-run-source.sha256" "$out/post-run-source.sha256" > "$out/source-comparison.log" 2>&1
printf 'source_comparison_exit=%d\n' "$?" >> "$out/source-comparison.log"
find "$CARGO_TARGET_DIR/debug/deps" -maxdepth 1 -type f -executable \( -name 'partitionline_broker-*' -o -name 'transport-*' -o -name 'tls-*' \) -print0 | sort -z | xargs -0 -r sha256sum > "$out/post-run-binaries.sha256"
date -u >> "$out/results.log"
