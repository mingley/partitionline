#!/usr/bin/env bash
set -euo pipefail

test "$(git -C baseline rev-parse HEAD)" = 7f59ef470340db6027958f439df3716a8dde536c
overall=0
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
  test_bin="$(sed -n 's/.*Running unittests src\/lib.rs (\(.*\))$/\1/p' "$log" | tail -n 1)"
  test -n "$test_bin"
  test_bin="$(realpath "$test_bin")"
  case "$test_bin" in
    "$target"/*) ;;
    *) printf 'unexpected %s executable: %s\n' "$impl" "$test_bin" >&2; exit 1 ;;
  esac
  test_hash="$(sha256sum "$test_bin" | cut -d ' ' -f 1)"
  printf 'full_net_suite implementation=%s executable=%s sha256=%s\n' "$impl" "$test_bin" "$test_hash"
  if [ "$impl" = baseline ]; then
    baseline_hash="$test_hash"
  else
    test "$test_hash" != "$baseline_hash"
  fi
  if [ "$status" -ne 0 ]; then
    overall=1
  fi
  rm "$log"
done
exit "$overall"
