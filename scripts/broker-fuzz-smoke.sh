#!/bin/sh
# Short libFuzzer smoke for the broker harness.
# This rustc build does not instrument the broker for coverage, so the run is a
# panic/crash oracle. It is not the KL11-47 24-hour campaign and does not
# qualify the broker.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cargo build --release --locked --manifest-path "$root/fuzz/broker/Cargo.toml"
for bin in decode_kraft recover_journal controller_respond; do
  echo "smoke $bin"
  "$root/fuzz/broker/target/release/$bin" -max_total_time=15 -timeout=2 -rss_limit_mb=512
done
echo "smoke only; KL11-47 still requires 24h per family"
