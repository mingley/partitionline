#!/usr/bin/env bash
# Shared crates.io / sparse-index helpers for Installable probes.
# Source from other scripts:  # shellcheck source=scripts/lib/crates-io.sh
#   source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/crates-io.sh"
#
# crates.io's CDN returns HTTP 403 with an empty body when User-Agent is
# missing — always send one. Prefer the API; fall back to the sparse index
# (index.crates.io) when the API is blocked or flaky.

# Sparse-index object key for a crate name (cargo sparse registry layout).
pl_crates_index_key() {
  local name="$1"
  local n=${#name}
  if (( n <= 0 )); then
    echo ""
    return 1
  elif (( n == 1 )); then
    echo "1/${name}"
  elif (( n == 2 )); then
    echo "2/${name}"
  elif (( n == 3 )); then
    echo "3/${name:0:1}/${name}"
  else
    echo "${name:0:2}/${name:2:2}/${name}"
  fi
}

# HTTP GET with a required User-Agent. Prints body to stdout; status on fd 3
# when provided as:  pl_curl_ua URL UA 3>&1 >/tmp/body  (callers use -w normally).
# Simpler: pl_http_code_ua URL UA OUTFILE → echoes status code.
pl_http_code_ua() {
  local url="$1"
  local ua="$2"
  local out="$3"
  curl -sS -A "$ua" -o "$out" -w '%{http_code}' "$url" || true
}

# Return 0 if sparse index lists crate version. Uses newline-delimited JSON.
pl_index_has_version() {
  local name="$1"
  local ver="$2"
  local ua="${3:-partitionline-crates-io/1}"
  local key out code
  key="$(pl_crates_index_key "$name")" || return 1
  out="$(mktemp)"
  code="$(pl_http_code_ua "https://index.crates.io/${key}" "$ua" "$out")"
  if [[ "$code" != "200" ]]; then
    rm -f "$out"
    return 1
  fi
  if command -v python3 >/dev/null 2>&1; then
    NAME="$name" VER="$ver" python3 - "$out" <<'PY'
import json, os, sys
path = sys.argv[1]
want = os.environ["VER"]
try:
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("vers") == want and not obj.get("yanked"):
                raise SystemExit(0)
except OSError:
    raise SystemExit(1)
raise SystemExit(1)
PY
    local rc=$?
    rm -f "$out"
    return "$rc"
  fi
  # grep fallback (less precise; still useful without python3)
  if grep -Eq "\"vers\"[[:space:]]*:[[:space:]]*\"${ver}\"" "$out"; then
    rm -f "$out"
    return 0
  fi
  rm -f "$out"
  return 1
}

# Probe whether crates.io serves name/ver.
# Sets globals (safe under `set -u`; do not capture via $() — that is a subshell):
#   PL_CRATES_PROBE_STATUS  — present | absent | unknown
#   PL_CRATES_PROBE_DETAIL  — short diagnostic (api_http=…, index=…)
# Exit 0 always.
pl_crates_probe_version() {
  local name="$1"
  local ver="$2"
  local ua="${3:-partitionline-crates-io/1}"
  local api_out code key idx_out idx_code
  PL_CRATES_PROBE_STATUS="unknown"
  PL_CRATES_PROBE_DETAIL=""
  api_out="$(mktemp)"
  code="$(pl_http_code_ua "https://crates.io/api/v1/crates/${name}/${ver}" "$ua" "$api_out")"
  PL_CRATES_PROBE_DETAIL="api_http=${code}"
  if [[ "$code" == "200" ]]; then
    rm -f "$api_out"
    # API can lead the sparse index that `cargo` uses. Wait loops / day1 must
    # not treat API-only as Installable-ready or adopter cargo-check flakes.
    if pl_index_has_version "$name" "$ver" "$ua"; then
      PL_CRATES_PROBE_DETAIL="api_http=200,index=present"
      PL_CRATES_PROBE_STATUS="present"
      return 0
    fi
    PL_CRATES_PROBE_DETAIL="api_http=200,index=absent"
    PL_CRATES_PROBE_STATUS="unknown"
    return 0
  fi
  if [[ "$code" == "404" ]]; then
    rm -f "$api_out"
    # Confirm with sparse index (handles rare API/index lag).
    if pl_index_has_version "$name" "$ver" "$ua"; then
      PL_CRATES_PROBE_DETAIL="api_http=404,index=present"
      PL_CRATES_PROBE_STATUS="present"
      return 0
    fi
    PL_CRATES_PROBE_DETAIL="api_http=404,index=absent"
    PL_CRATES_PROBE_STATUS="absent"
    return 0
  fi
  rm -f "$api_out"
  # API blocked/flaky (missing User-Agent → CDN 403): try index.
  if pl_index_has_version "$name" "$ver" "$ua"; then
    PL_CRATES_PROBE_DETAIL="api_http=${code},index=present"
    PL_CRATES_PROBE_STATUS="present"
    return 0
  fi
  key="$(pl_crates_index_key "$name")"
  idx_out="$(mktemp)"
  idx_code="$(pl_http_code_ua "https://index.crates.io/${key}" "$ua" "$idx_out")"
  rm -f "$idx_out"
  PL_CRATES_PROBE_DETAIL="api_http=${code},index_http=${idx_code}"
  if [[ "$idx_code" == "404" ]]; then
    PL_CRATES_PROBE_STATUS="absent"
    return 0
  fi
  PL_CRATES_PROBE_STATUS="unknown"
  return 0
}
