#!/usr/bin/env bash
set -euo pipefail

variant="$1"
if [ "$variant" = baseline ]; then
  path="$GITHUB_WORKSPACE/baseline"
  target="$GITHUB_WORKSPACE/baseline-target"
  test "$(git -C "$path" rev-parse HEAD)" = c49cde3ecf7fc11be8bd0a0e31910f6a1dce6b51
elif [ "$variant" = candidate ]; then
  path="$GITHUB_WORKSPACE"
  target="$GITHUB_WORKSPACE/target"
  test "$(git -C "$path" rev-parse HEAD)" = "$GITHUB_SHA"
else
  exit 2
fi
log="$(mktemp)"
status=0
(
  cd "$path"
  CARGO_TARGET_DIR="$target" cargo +stable test --locked --lib net::tests:: -- --test-threads=1
) >"$log" 2>&1 || status=$?
printf 'fixture_suite variant=%s status=%s\n' "$variant" "$status"
cat "$log"
grep -Eq 'test result: (ok|FAILED)\.' "$log"
test_bin="$(sed -n 's/.*Running unittests src\/lib.rs (\(.*\))$/\1/p' "$log" | tail -n 1)"
test -n "$test_bin"
test_bin="$(realpath "$test_bin")"
case "$test_bin" in
  "$target"/*) ;;
  *) printf 'unexpected executable: %s\n' "$test_bin" >&2; exit 1 ;;
esac
test_hash="$(sha256sum "$test_bin" | cut -d ' ' -f 1)"
printf 'fixture_binary variant=%s executable=%s sha256=%s\n' "$variant" "$test_bin" "$test_hash"
if [ "$variant" = candidate ]; then
  printf '%s\n' "$test_hash" > "$GITHUB_WORKSPACE/candidate-test.sha256"
else
  test "$test_hash" != "$(cat "$GITHUB_WORKSPACE/candidate-test.sha256")"
fi
rm "$log"
exit "$status"
