#!/bin/sh
# Short coverage-guided libFuzzer smoke for the broker harness.
# --target keeps the sancov rustflags off host build scripts.
# This is not the KL11-47 24-hour campaign and does not qualify the broker.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
target=x86_64-unknown-linux-gnu
cd "$root/fuzz/broker"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=4 -Cllvm-args=-sanitizer-coverage-inline-8bit-counters -Cllvm-args=-sanitizer-coverage-pc-table"
cargo build --release --locked --target "$target"
for bin in decode_kraft recover_journal controller_respond; do
  echo "smoke $bin"
  "$root/fuzz/broker/target/$target/release/$bin" -max_total_time=15 -timeout=2 -rss_limit_mb=512
done
echo "smoke only; KL11-47 still requires 24h per family"
