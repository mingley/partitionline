#!/bin/bash
set -u
export CARGO_HOME=/workspace/work/cargo RUSTUP_HOME=/workspace/work/rustup PATH=/workspace/work/cargo/bin:$PATH CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/workspace/work/broker-sasl/target
for credential_toolchain in stable 1.85.0; do
  taskset -c 0,1 cargo +"$credential_toolchain" test --locked --manifest-path /workspace/work/broker-sasl/foundation-pinned/partitionline-broker/Cargo.toml --no-default-features --features sasl --lib --test sasl --test sasl_credentials > "/workspace/work/broker-sasl/credential-logs/pinned-1b4-${credential_toolchain}-test.log" 2>&1 || exit $?
  taskset -c 0,1 cargo +"$credential_toolchain" clippy --locked --manifest-path /workspace/work/broker-sasl/foundation-pinned/partitionline-broker/Cargo.toml --no-default-features --features sasl --all-targets -- -D warnings > "/workspace/work/broker-sasl/credential-logs/pinned-1b4-${credential_toolchain}-clippy.log" 2>&1 || exit $?
done
