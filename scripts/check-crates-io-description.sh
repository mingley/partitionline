#!/usr/bin/env bash
# Compare crates.io published description to Cargo.toml for the current version.
# After Installable, cargo.toml may strengthen identity (no C / no librdkafka)
# before the next cut republishes — surface that as WARN, not Installable BLOCKED.
# Exit 0 always (probe). Prints OK / WARN / SKIP.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
local_desc="$(sed -n 's/^description = "\(.*\)"/\1/p' Cargo.toml | head -1)"

if [[ -z "$ver" || -z "$name" || -z "$local_desc" ]]; then
  echo "check-crates-io-description: SKIP — could not read Cargo.toml name/version/description"
  exit 0
fi

# shellcheck source=scripts/lib/crates-io.sh
source "$ROOT/scripts/lib/crates-io.sh"
ua="partitionline-desc-check/1"
out="$(mktemp)"
code="$(pl_http_code_ua "https://crates.io/api/v1/crates/${name}/${ver}" "$ua" "$out")"
if [[ "$code" != "200" ]]; then
  rm -f "$out"
  echo "check-crates-io-description: SKIP — crates.io HTTP ${code} for ${name} ${ver}"
  exit 0
fi

pub_desc="$(NAME="$name" python3 - "$out" <<'PY'
import json, sys
path = sys.argv[1]
with open(path, encoding="utf-8") as f:
    data = json.load(f)
ver = data.get("version") or {}
desc = ver.get("description") or ""
if not desc:
    crate = data.get("crate") or {}
    desc = crate.get("description") or ""
print(desc)
PY
)"
rm -f "$out"

if [[ -z "$pub_desc" ]]; then
  echo "check-crates-io-description: SKIP — empty published description"
  exit 0
fi

if [[ "$pub_desc" == "$local_desc" ]]; then
  echo "check-crates-io-description: OK — published description matches Cargo.toml"
  exit 0
fi

# Identity markers local wants on crates.io for adoptability.
local_low="$(printf '%s' "$local_desc" | tr '[:upper:]' '[:lower:]')"
pub_low="$(printf '%s' "$pub_desc" | tr '[:upper:]' '[:lower:]')"
local_has_noc=0
pub_has_noc=0
local_has_rdk=0
pub_has_rdk=0
[[ "$local_low" == *"no c"* || "$local_low" == *"pure rust"* || "$local_low" == *"pure-rust"* ]] && local_has_noc=1
[[ "$pub_low" == *"no c"* || "$pub_low" == *"pure rust"* || "$pub_low" == *"pure-rust"* ]] && pub_has_noc=1
[[ "$local_low" == *"librdkafka"* ]] && local_has_rdk=1
[[ "$pub_low" == *"librdkafka"* ]] && pub_has_rdk=1

echo "check-crates-io-description: WARN — published description differs from Cargo.toml"
echo "  published: ${pub_desc}"
echo "  local:     ${local_desc}"
echo "  Next cut (e.g. 0.1.1 / release-plz) republishes Cargo.toml description; do not re-cut 0.1.0."
if [[ "$local_has_rdk" -eq 1 && "$pub_has_rdk" -eq 0 ]]; then
  echo "  identity: local names librdkafka; published page does not yet — adopters on crates.io see weaker no-C signal until next cut."
fi
if [[ "$local_has_noc" -eq 1 && "$pub_has_noc" -eq 0 ]]; then
  echo "  identity: local states pure-Rust/no-C; published description does not."
fi
exit 0
