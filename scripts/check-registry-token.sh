#!/usr/bin/env bash
# Probe CARGO_REGISTRY_TOKEN without printing it.
#
# First cut of a *new* crate needs crates.io scope publish-new (publish-update
# alone cannot create the crate). This script proves the token can authenticate
# for publish-new — it does NOT create a crate.
#
# IMPORTANT:
#   - Do NOT use GET /api/v1/me — cookie / user-info oriented; 403s valid
#     publish-only tokens.
#   - Do NOT PUT /api/v1/crates/new with an empty body — crates.io returns
#     400 "invalid metadata length" *before* auth, so any garbage token looks
#     "OK". That false-accept would greenwash a broken Installable cut.
#
# Probe: PUT a length-prefixed publish body for a nonexistent crate name with
# an empty .crate tarball. Auth runs after metadata parse; empty tarball fails
# afterward without creating a crate:
#   400 / 422 → authenticated (validation / empty tarball)
#   401 / 403 → rejected (bad token, missing publish-new, etc.)
#
# Exit codes:
#   0  — token set and crates.io accepts it for publish-new auth
#   2  — token unset (expected while READY_EXCEPT_TOKEN)
#   1  — token set but rejected / probe failed
#
# Usage:
#   bash scripts/check-registry-token.sh
#   REQUIRE_TOKEN=1 bash scripts/check-registry-token.sh   # treat unset as FAIL
#   bash scripts/check-registry-token.sh --self-test       # fake token must FAIL
set -euo pipefail

REQUIRE_TOKEN="${REQUIRE_TOKEN:-0}"
UA="${CRATES_IO_UA:-partitionline-check-registry-token/1 (https://github.com/mingley/partitionline)}"
# Nonexistent name → PublishNew scope. Must never be a real crate we care about.
PROBE_NAME="${CRATES_IO_PROBE_NAME:-zz-partitionline-auth-probe-do-not-publish}"
PROBE_VERS="${CRATES_IO_PROBE_VERS:-0.0.0-authprobe}"

build_probe_body() {
  # Cargo publish wire format: u32le metadata_len + JSON + u32le crate_len + bytes.
  # Empty crate bytes → fails after auth without creating a crate.
  python3 - "$PROBE_NAME" "$PROBE_VERS" <<'PY'
import json, struct, sys
name, vers = sys.argv[1], sys.argv[2]
meta = {
    "name": name,
    "vers": vers,
    "deps": [],
    "features": {},
    "authors": ["partitionline-auth-probe"],
    "description": "auth probe only — must not publish",
    "documentation": None,
    "homepage": None,
    "readme": None,
    "readme_file": None,
    "keywords": [],
    "categories": [],
    "license": "Apache-2.0",
    "license_file": None,
    "repository": None,
    "links": None,
}
mj = json.dumps(meta, separators=(",", ":")).encode()
sys.stdout.buffer.write(struct.pack("<I", len(mj)) + mj + struct.pack("<I", 0))
PY
}

probe_once() {
  local token="$1"
  local out code body_snip
  out="$(mktemp)"
  local body
  body="$(mktemp)"
  build_probe_body >"$body"
  code="$(curl -sS -A "$UA" -X PUT \
    -H "Authorization: ${token}" \
    -H "Content-Type: application/octet-stream" \
    --data-binary @"$body" \
    -o "$out" -w '%{http_code}' "https://crates.io/api/v1/crates/new" || true)"
  body_snip="$(head -c 240 "$out" | tr '\n' ' ')"
  rm -f "$out" "$body"
  printf '%s\t%s\n' "$code" "$body_snip"
}

interpret_probe() {
  local code="$1"
  local body_snip="$2"
  case "$code" in
    400|422)
      if [[ "$body_snip" == *"invalid metadata length"* ]]; then
        echo "check-registry-token: FAIL — got pre-auth empty-body response (probe body broken)" >&2
        echo "  body: ${body_snip}" >&2
        return 1
      fi
      echo "check-registry-token: OK — crates.io accepted token for publish-new auth (http=${code})"
      echo "  Reminder: first cut of a new crate still needs publish-new on this token."
      return 0
      ;;
    401|403)
      echo "check-registry-token: FAIL — crates.io rejected token (http=${code})" >&2
      echo "  body: ${body_snip}" >&2
      echo "  Recreate at https://crates.io/settings/tokens with publish-new (+ publish-update)." >&2
      echo "  Note: empty-body PUT is not used — it 400s before auth and false-accepts." >&2
      return 1
      ;;
    200)
      echo "check-registry-token: FAIL — probe unexpectedly published (http=200) — investigate immediately" >&2
      echo "  body: ${body_snip}" >&2
      return 1
      ;;
    *)
      echo "check-registry-token: FAIL — unexpected crates.io response (http=${code})" >&2
      echo "  body: ${body_snip}" >&2
      return 1
      ;;
  esac
}

if [[ "${1:-}" == "--self-test" ]]; then
  echo "check-registry-token: self-test — fake token must be rejected"
  # shellcheck disable=SC2034
  IFS=$'\t' read -r code body_snip < <(probe_once "cio_partitionline_self_test_invalid_token")
  if interpret_probe "$code" "$body_snip"; then
    echo "check-registry-token: self-test FAIL — fake token was accepted" >&2
    exit 1
  fi
  echo "check-registry-token: self-test OK — fake token rejected (http=${code})"
  exit 0
fi

if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "check-registry-token: MISSING — CARGO_REGISTRY_TOKEN unset"
  echo "  First cut of a NEW crate: https://crates.io/settings/tokens"
  echo "  Enable scope publish-new (+ publish-update). publish-update alone cannot create the crate."
  if [[ "$REQUIRE_TOKEN" == "1" ]]; then
    exit 1
  fi
  exit 2
fi

# Never echo the token. Length-only is enough for operators to confirm injection.
echo "check-registry-token: probing crates.io PUT /api/v1/crates/new (token len=${#CARGO_REGISTRY_TOKEN}; structured empty-tarball body for ${PROBE_NAME}@${PROBE_VERS})"
IFS=$'\t' read -r code body_snip < <(probe_once "${CARGO_REGISTRY_TOKEN}")
interpret_probe "$code" "$body_snip"
exit $?
