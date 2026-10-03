#!/bin/bash
set -u
export CARGO_HOME=/workspace/work/cargo RUSTUP_HOME=/workspace/work/rustup PATH=/workspace/work/cargo/bin:$PATH CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/workspace/work/broker-sasl/target
session_manifest=/workspace/work/broker-sasl/session-source/partitionline-broker/Cargo.toml
for session_toolchain in stable 1.85.0; do
  for session_features in default sasl all; do
    session_flags=(--no-default-features)
    if [[ "$session_features" == sasl ]]; then session_flags+=(--features sasl); fi
    if [[ "$session_features" == all ]]; then session_flags=(--all-features); fi
    taskset -c 0,1 cargo +"$session_toolchain" test --locked --manifest-path "$session_manifest" "${session_flags[@]}" > "/workspace/work/broker-sasl/credential-logs/session-${session_toolchain}-${session_features}-tests.log" 2>&1 || exit $?
    taskset -c 0,1 cargo +"$session_toolchain" clippy --locked --manifest-path "$session_manifest" "${session_flags[@]}" --all-targets -- -D warnings > "/workspace/work/broker-sasl/credential-logs/session-${session_toolchain}-${session_features}-clippy.log" 2>&1 || exit $?
  done
done
