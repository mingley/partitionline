#!/usr/bin/env bash
# Cargo invokes this only for the live test executable, never rustc.
set -euo pipefail
case "${PL_STACK_RUNTIME_MIN:-}" in
  2097152|8388608) ;;
  *) echo 'invalid diagnostic test stack value' >&2; exit 2 ;;
esac
[[ $# -ge 1 && "$1" == "$CARGO_TARGET_DIR"/debug/deps/broker_compatibility-* && -x "$1" ]] || {
  echo 'unexpected diagnostic test executable' >&2
  exit 2
}
printf 'PL_STACK_RUNTIME_RUNNER\t%s\n' "$PL_STACK_RUNTIME_MIN" >&2
export RUST_MIN_STACK="$PL_STACK_RUNTIME_MIN"
exec "$@"
