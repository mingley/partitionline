#!/usr/bin/env bash
# Check local CARGO_REGISTRY_TOKEN availability without contacting a registry.
# Exit 2 when missing, 3 when present (authorization and scope are unverified),
# and 1 on an input or read error. Only an actual publish validates authority.
set -euo pipefail

# shellcheck source=scripts/lib/cargo-registry-token.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/cargo-registry-token.sh"

check_token() {
  local raw="${CARGO_REGISTRY_TOKEN:-}" trimmed
  if ! trimmed="$(printf '%s' "$raw" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"; then
    echo "check-registry-token: failed to normalize token" >&2
    return 1
  fi
  if [[ -z "$trimmed" && -n "${CARGO_REGISTRY_TOKEN_FILE:-}" ]]; then
    if [[ ! -f "$CARGO_REGISTRY_TOKEN_FILE" || ! -r "$CARGO_REGISTRY_TOKEN_FILE" ]]; then
      echo "check-registry-token: token file is not a readable regular file" >&2
      return 1
    fi
    if ! raw="$(tr -d '\r\n' <"$CARGO_REGISTRY_TOKEN_FILE")"; then
      echo "check-registry-token: failed to read token file" >&2
      return 1
    fi
    if ! trimmed="$(printf '%s' "$raw" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"; then
      echo "check-registry-token: failed to normalize token" >&2
      return 1
    fi
  fi
  if [[ -n "$trimmed" ]]; then
    echo "check-registry-token: CARGO_REGISTRY_TOKEN is present; authorization and scope are unverified"
    return 3
  fi
  unset CARGO_REGISTRY_TOKEN || true
  pl_warn_misnamed_cargo_registry_token "check-registry-token"
  echo "check-registry-token: CARGO_REGISTRY_TOKEN is not set"
  return 2
}

if [[ "${1:-}" == "--self-test" && "$#" -eq 1 ]]; then
  # Purely local fixtures; no credentials or network requests.
  test_dir="$(mktemp -d)"
  trap 'rm -rf "$test_dir"' EXIT
  printf ' fixture-token \n' >"$test_dir/token"
  rc=0
  (unset CARGO_REGISTRY_TOKEN CARGO_REGISTRY_TOKEN_FILE; check_token) >/dev/null 2>&1 || rc=$?
  [[ "$rc" -eq 2 ]] || { echo "check-registry-token: self-test missing token failed" >&2; exit 1; }
  rc=0
  (CARGO_REGISTRY_TOKEN=' fixture-token '; unset CARGO_REGISTRY_TOKEN_FILE; check_token) >/dev/null 2>&1 || rc=$?
  [[ "$rc" -eq 3 ]] || { echo "check-registry-token: self-test present token failed" >&2; exit 1; }
  rc=0
  (unset CARGO_REGISTRY_TOKEN; CARGO_REGISTRY_TOKEN_FILE="$test_dir/token"; check_token) >/dev/null 2>&1 || rc=$?
  [[ "$rc" -eq 3 ]] || { echo "check-registry-token: self-test token file failed" >&2; exit 1; }
  rc=0
  (unset CARGO_REGISTRY_TOKEN; CARGO_REGISTRY_TOKEN_FILE="$test_dir/missing"; check_token) >/dev/null 2>&1 || rc=$?
  [[ "$rc" -eq 1 ]] || { echo "check-registry-token: self-test unreadable token file failed" >&2; exit 1; }
  echo "check-registry-token: self-test OK — local availability only; scope unverified"
  exit 0
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: bash scripts/check-registry-token.sh [--self-test]" >&2
  exit 1
fi
rc=0
check_token || rc=$?
exit "$rc"
