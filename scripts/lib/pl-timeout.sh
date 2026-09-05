#!/usr/bin/env bash
# Portable deadline wrapper for CI smokes.
# Prefer GNU `timeout`, then macOS Homebrew `gtimeout` (coreutils).
# Fail closed with an install hint rather than assuming GNU coreutils on PATH.
#
# Usage:
#   # shellcheck source=scripts/lib/pl-timeout.sh
#   source "$ROOT/scripts/lib/pl-timeout.sh"
#   pl_timeout 45s cargo run --release --example share

pl_timeout() {
  local duration="${1:-}"
  shift || true
  if [[ -z "$duration" || "$#" -lt 1 ]]; then
    echo "pl_timeout: usage: pl_timeout <duration> <command> [args...]" >&2
    return 2
  fi
  if command -v timeout >/dev/null 2>&1; then
    timeout "$duration" "$@"
    return $?
  fi
  if command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$duration" "$@"
    return $?
  fi
  echo "pl_timeout: need GNU timeout or gtimeout (macOS: brew install coreutils)" >&2
  return 127
}
