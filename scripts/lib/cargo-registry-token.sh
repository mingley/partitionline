#!/usr/bin/env bash
# Shared CARGO_REGISTRY_TOKEN load + misname detection for Installable.
# Source from other scripts:
#   # shellcheck source=scripts/lib/cargo-registry-token.sh
#   source "$ROOT/scripts/lib/cargo-registry-token.sh"
#
# cargo publish and owner-finish only read CARGO_REGISTRY_TOKEN. Cursor Secrets
# typos (CARGO_TOKEN, CRATES_IO_TOKEN, …) leave Installable stuck unless we WARN.
# Optional: CARGO_REGISTRY_TOKEN_FILE=/path loads into the *current* shell so
# subsequent cargo publish sees the value (subshell-only load is not enough).
# Whitespace-only TOKEN is treated as unset (common paste/Secret mistake).

# Load TOKEN_FILE into the current shell when TOKEN is unset. Never print contents.
# Echoes a length-only status line when loaded. An unreadable file is an error.
pl_load_cargo_registry_token_file() {
  local prefix="${1:-cargo-registry-token}"
  if [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
    return 0
  fi
  if [[ -z "${CARGO_REGISTRY_TOKEN_FILE:-}" ]]; then
    return 0
  fi
  if [[ -f "${CARGO_REGISTRY_TOKEN_FILE}" && -r "${CARGO_REGISTRY_TOKEN_FILE}" ]]; then
    # trim trailing CR/LF only — token bytes otherwise preserved
    if ! CARGO_REGISTRY_TOKEN="$(tr -d '\r\n' <"${CARGO_REGISTRY_TOKEN_FILE}")"; then
      echo "${prefix}: failed to read CARGO_REGISTRY_TOKEN_FILE" >&2
      return 1
    fi
    export CARGO_REGISTRY_TOKEN
    echo "${prefix}: loaded CARGO_REGISTRY_TOKEN from CARGO_REGISTRY_TOKEN_FILE (len=${#CARGO_REGISTRY_TOKEN})"
  else
    echo "${prefix}: CARGO_REGISTRY_TOKEN_FILE is not a readable regular file" >&2
    return 1
  fi
  return 0
}

# Treat whitespace-only CARGO_REGISTRY_TOKEN as unset; trim leading/trailing space.
# Never prints token contents. Call after TOKEN_FILE load, before probes/publish.
pl_normalize_cargo_registry_token() {
  local prefix="${1:-cargo-registry-token}"
  if [[ -z "${CARGO_REGISTRY_TOKEN+x}" ]]; then
    return 0
  fi
  local raw="${CARGO_REGISTRY_TOKEN}"
  local trimmed
  if ! trimmed="$(printf '%s' "$raw" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"; then
    echo "${prefix}: failed to normalize CARGO_REGISTRY_TOKEN" >&2
    return 1
  fi
  if [[ -z "$trimmed" ]]; then
    if [[ -n "$raw" ]]; then
      echo "${prefix}: WARN — CARGO_REGISTRY_TOKEN is whitespace-only; treating as unset (re-paste a real crates.io token)." >&2
    fi
    unset CARGO_REGISTRY_TOKEN
    return 0
  fi
  if [[ "$trimmed" != "$raw" ]]; then
    echo "${prefix}: WARN — CARGO_REGISTRY_TOKEN had leading/trailing whitespace; trimmed before probe/publish." >&2
    export CARGO_REGISTRY_TOKEN="$trimmed"
  fi
  return 0
}

# When TOKEN unset, WARN about common misnamed env vars (length only; no values).
pl_warn_misnamed_cargo_registry_token() {
  local prefix="${1:-cargo-registry-token}"
  if [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
    return 0
  fi
  local misnames=()
  local alt val
  for alt in \
    CARGO_TOKEN \
    CRATES_IO_TOKEN \
    CRATES_TOKEN \
    CARGO_REGISTERY_TOKEN \
    CARGO_REGISTY_TOKEN \
    CARGO_REGISTRY_TOKENS \
    REGISTRY_TOKEN \
    CARGO_CRATES_IO_TOKEN
  do
    if [[ -n "${!alt:-}" ]]; then
      val="${!alt}"
      misnames+=("${alt}(len=${#val})")
    fi
  done
  if [[ "${#misnames[@]}" -gt 0 ]]; then
    echo "${prefix}: WARN — found misnamed token env var(s): ${misnames[*]}" >&2
    echo "  Rename to exactly CARGO_REGISTRY_TOKEN (Cursor env Secrets + Actions secret)." >&2
    echo "  cargo publish / owner-finish-installable only read CARGO_REGISTRY_TOKEN." >&2
  fi
  return 0
}

# Normalize, load TOKEN_FILE if needed, normalize again, and warn on misnames.
# Call from the *parent* shell of cargo publish / registry probes (not only a subshell).
pl_prepare_cargo_registry_token() {
  local prefix="${1:-cargo-registry-token}"
  pl_normalize_cargo_registry_token "$prefix" || return 1
  pl_load_cargo_registry_token_file "$prefix" || return 1
  pl_normalize_cargo_registry_token "$prefix" || return 1
  pl_warn_misnamed_cargo_registry_token "$prefix" || return 1
}
