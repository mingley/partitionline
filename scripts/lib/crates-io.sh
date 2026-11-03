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
  local code
  if ! code="$(curl -sS --connect-timeout 5 --max-time 15 -A "$ua" -o "$out" -w '%{http_code}' "$url")"; then
    # A failed transfer may still report a partial 200/404. Do not use it.
    printf '000'
    return 0
  fi
  if [[ "$code" =~ ^[0-9]{3}$ ]]; then
    printf '%s' "$code"
  else
    printf '000'
  fi
}

# Probe the sparse index without confusing an unavailable/malformed response
# with a confirmed missing version. Sets PL_CRATES_INDEX_STATUS to present,
# absent, yanked or unknown and PL_CRATES_INDEX_HTTP to the HTTP status.
pl_index_probe_version() {
  local name="$1"
  local ver="$2"
  local ua="${3:-partitionline-crates-io/1}"
  local key out code status
  PL_CRATES_INDEX_STATUS="unknown"
  PL_CRATES_INDEX_HTTP=""
  key="$(pl_crates_index_key "$name")" || return 0
  out="$(mktemp)"
  code="$(pl_http_code_ua "https://index.crates.io/${key}" "$ua" "$out")"
  PL_CRATES_INDEX_HTTP="$code"
  if [[ "$code" == "404" ]]; then
    rm -f "$out"
    PL_CRATES_INDEX_STATUS="absent"
    return 0
  fi
  if [[ "$code" != "200" ]] || ! command -v python3 >/dev/null 2>&1; then
    rm -f "$out"
    return 0
  fi
  status="$(NAME="$name" VER="$ver" python3 - "$out" <<'PY'
import json, os, re, sys
want = os.environ["VER"]
name = os.environ["NAME"]

def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result

def reject_constant(value):
    raise ValueError("non-JSON numeric constant: " + value)

def valid_version(version):
    match = re.fullmatch(
        r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
        r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
        r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?", version)
    return bool(match) and all(not (part.isdigit() and len(part) > 1 and part[0] == "0")
                               for part in (match.group(1) or "").split("."))

try:
    count = 0
    matches = []
    seen = set()
    with open(sys.argv[1], encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line, object_pairs_hook=unique_pairs, parse_constant=reject_constant)
            if (not isinstance(obj, dict) or obj.get("name") != name
                    or not isinstance(obj.get("vers"), str) or not valid_version(obj["vers"])
                    or type(obj.get("yanked")) is not bool):
                raise ValueError("invalid index record")
            if obj["vers"] in seen:
                raise ValueError("duplicate index version")
            seen.add(obj["vers"])
            count += 1
            if obj["vers"] == want:
                matches.append(obj["yanked"])
    if not count or len(matches) > 1:
        raise ValueError("empty or ambiguous index")
    print("yanked" if matches and matches[0] else "present" if matches else "absent")
except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
    print("unknown")
PY
)" || status="unknown"
  rm -f "$out"
  case "$status" in
    present|absent|yanked) PL_CRATES_INDEX_STATUS="$status" ;;
  esac
}

# Return 0 only if the sparse index lists an unyanked crate version.
pl_index_has_version() {
  pl_index_probe_version "$@"
  [[ "$PL_CRATES_INDEX_STATUS" == "present" ]]
}

# Probe whether crates.io serves name/ver.
# Sets globals (safe under `set -u`; do not capture via $() — that is a subshell):
#   PL_CRATES_PROBE_STATUS  — present | absent | unknown
#   PL_CRATES_PROBE_DETAIL  — short diagnostic (api_http=…, index=…)
#   PL_CRATES_API_STATUS    — present | absent | yanked | unknown
#   PL_CRATES_API_HTTP      — HTTP status, or 000 for a failed transfer
#   PL_CRATES_INDEX_STATUS  — present | absent | yanked | unknown
# Exit 0 always.
pl_crates_probe_version() {
  local name="$1"
  local ver="$2"
  local ua="${3:-partitionline-crates-io/1}"
  local api_out code api_status
  PL_CRATES_PROBE_STATUS="unknown"
  PL_CRATES_PROBE_DETAIL=""
  PL_CRATES_API_STATUS="unknown"
  PL_CRATES_API_HTTP=""
  api_out="$(mktemp)"
  code="$(pl_http_code_ua "https://crates.io/api/v1/crates/${name}/${ver}" "$ua" "$api_out")"
  PL_CRATES_API_HTTP="$code"
  if [[ "$code" == "404" ]]; then
    PL_CRATES_API_STATUS="absent"
  elif [[ "$code" == "200" ]] && command -v python3 >/dev/null 2>&1; then
    api_status="$(NAME="$name" VER="$ver" python3 - "$api_out" <<'PY'
import json, os, sys

def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result

def reject_constant(value):
    raise ValueError("non-JSON numeric constant: " + value)

try:
    with open(sys.argv[1], encoding="utf-8") as f:
        payload = json.load(f, object_pairs_hook=unique_pairs, parse_constant=reject_constant)
    version = payload.get("version") if isinstance(payload, dict) else None
    if (not isinstance(version, dict) or version.get("crate") != os.environ["NAME"]
            or version.get("num") != os.environ["VER"] or type(version.get("yanked")) is not bool):
        raise ValueError("invalid version response")
    print("yanked" if version["yanked"] else "present")
except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
    print("unknown")
PY
)" || api_status="unknown"
    case "$api_status" in
      present|yanked) PL_CRATES_API_STATUS="$api_status" ;;
    esac
  fi
  rm -f "$api_out"
  pl_index_probe_version "$name" "$ver" "$ua"
  PL_CRATES_PROBE_DETAIL="api_http=${code},index=${PL_CRATES_INDEX_STATUS},index_http=${PL_CRATES_INDEX_HTTP},api=${PL_CRATES_API_STATUS}"
  if [[ "$PL_CRATES_INDEX_STATUS" == "present" ]]; then
    PL_CRATES_PROBE_STATUS="present"
  elif [[ "$PL_CRATES_INDEX_STATUS" == "absent" && "$code" == "404" ]]; then
    PL_CRATES_PROBE_STATUS="absent"
  fi
  return 0
}
