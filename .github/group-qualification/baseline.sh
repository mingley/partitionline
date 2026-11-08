#!/usr/bin/env bash
set -euo pipefail

export CARGO_TARGET_DIR="$GITHUB_WORKSPACE/target"
names=(
  group::rediscover_stop_tests::replacing_an_idle_coordinator_lets_the_broker_leave
  group::rediscover_stop_tests::replacing_an_idle_offset_coordinator_lets_the_broker_leave
  group::rediscover_stop_tests::replacing_an_idle_legacy_offset_coordinator_lets_the_broker_leave
)
messages=(
  'replacing an idle coordinator left the previous broker blocked in a read'
  'replacing an idle offset coordinator left the previous broker blocked in a read'
  'replacing an idle legacy offset coordinator left the previous broker blocked in a read'
)
for index in 0 1 2; do
  log="$(mktemp)"
  status=0
  cargo +stable test --locked --lib "${names[$index]}" -- --exact --nocapture >"$log" 2>&1 || status=$?
  cat "$log"
  test "$status" -ne 0
  grep -Fq 'test result: FAILED. 0 passed; 1 failed;' "$log"
  grep -Fq "${messages[$index]}" "$log"
  rm "$log"
done
