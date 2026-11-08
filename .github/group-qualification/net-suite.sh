#!/usr/bin/env bash
set -euo pipefail
test "$(git -C baseline rev-parse HEAD)" = 3f45fe6255e72abc6b0c4488bc13117779c50787
for impl in baseline candidate; do
  log="$(mktemp)"
  if [ "$impl" = baseline ]; then
    path="$GITHUB_WORKSPACE/baseline"
    target="$GITHUB_WORKSPACE/net-suite-baseline-target"
  else
    path="$GITHUB_WORKSPACE"
    target="$GITHUB_WORKSPACE/net-suite-candidate-target"
  fi
  status=0
  (
    cd "$path"
    CARGO_TARGET_DIR="$target" cargo +stable test --locked --lib net::tests:: -- --test-threads=1
  ) >"$log" 2>&1 || status=$?
  printf 'full_net_suite implementation=%s status=%s\n' "$impl" "$status"
  cat "$log"
  grep -Eq 'test result: (ok|FAILED)\.' "$log"
  if [ "$status" -ne 0 ]; then
    grep -Fq 'net::tests::write_timeout_shuts_the_broker_down' "$log"
  fi
  rm "$log"
done
