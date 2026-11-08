#!/usr/bin/env bash
set -euo pipefail

export CARGO_TARGET_DIR="$GITHUB_WORKSPACE/baseline-target"
test "$(realpath -m "$CARGO_TARGET_DIR")" != "$(realpath -m "$GITHUB_WORKSPACE/target")"
test "$(git rev-parse HEAD)" = c49cde3ecf7fc11be8bd0a0e31910f6a1dce6b51
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
  test_bin="$(sed -n 's/.*Running unittests src\/lib.rs (\(.*\))$/\1/p' "$log" | tail -n 1)"
  test -n "$test_bin"
  test_bin="$(realpath "$test_bin")"
  case "$test_bin" in
    "$GITHUB_WORKSPACE"/baseline-target/*) ;;
    *) printf 'unexpected baseline executable: %s\n' "$test_bin" >&2; exit 1 ;;
  esac
  candidate_hash="$(cat "$GITHUB_WORKSPACE/candidate-test.sha256")"
  baseline_hash="$(sha256sum "$test_bin" | cut -d ' ' -f 1)"
  printf 'candidate sha256: %s\nbaseline sha256: %s\n' "$candidate_hash" "$baseline_hash"
  test -n "$candidate_hash"
  test -n "$baseline_hash"
  test "$candidate_hash" != "$baseline_hash"
  grep -Fq 'test result: FAILED. 0 passed; 1 failed;' "$log"
  grep -Fq "${messages[$index]}" "$log"
  rm "$log"
done
