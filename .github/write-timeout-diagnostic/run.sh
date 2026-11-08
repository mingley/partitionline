#!/usr/bin/env bash
set -euo pipefail

overall=0
for variant in baseline-8 candidate-8 baseline-8192 candidate-8192; do
  case "$variant" in
    baseline-*) expected=646fe0269599964b80077b781911282a3318a7ad ;;
    candidate-*) expected=67b753b16433a50cc6b28ae1620d0838ffce9825 ;;
  esac
  case "$variant" in
    *-8) chunk=8 ;;
    *-8192) chunk=8192 ;;
  esac
  test "$(git -C "$variant" rev-parse HEAD)" = "$expected"
  (
    cd "$variant"
    python3 "$GITHUB_WORKSPACE/.github/write-timeout-diagnostic/instrument.py" "$chunk"
  )
  printf 'diagnostic_source variant=%s original_commit=%s net_sha256=' "$variant" "$expected"
  sha256sum "$variant/src/net.rs"
  target="$GITHUB_WORKSPACE/diagnostic-target-$variant"
  log="$(mktemp)"
  status=0
  (
    cd "$variant"
    CARGO_TARGET_DIR="$target" cargo +stable test --locked --lib net::tests:: -- --test-threads=1 --nocapture
  ) >"$log" 2>&1 || status=$?
  printf 'diagnostic_result variant=%s status=%s\n' "$variant" "$status"
  cat "$log"
  grep -Eq 'test result: (ok|FAILED)\.' "$log"
  grep -Fq 'write_timeout_diag phase=terminal' "$log" || grep -Fq 'write_timeout_diag phase=no_terminal' "$log"
  test_bin="$(sed -n 's/.*Running unittests src\/lib.rs (\(.*\))$/\1/p' "$log" | tail -n 1)"
  test -n "$test_bin"
  test_bin="$(realpath "$test_bin")"
  case "$test_bin" in
    "$target"/*) ;;
    *) printf 'unexpected test executable: %s\n' "$test_bin" >&2; exit 1 ;;
  esac
  test_hash="$(sha256sum "$test_bin" | cut -d ' ' -f 1)"
  printf 'diagnostic_binary variant=%s executable=%s sha256=%s\n' "$variant" "$test_bin" "$test_hash"
  if [ "$variant" = baseline-8 ]; then
    baseline_eight_hash="$test_hash"
  elif [ "$variant" = candidate-8 ]; then
    test "$test_hash" != "$baseline_eight_hash"
  elif [ "$variant" = baseline-8192 ]; then
    baseline_large_hash="$test_hash"
  else
    test "$test_hash" != "$baseline_large_hash"
  fi
  if [ "$status" -ne 0 ]; then
    overall=1
  fi
  rm "$log"
done
exit "$overall"
