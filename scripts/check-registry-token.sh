#!/usr/bin/env bash
# Probe CARGO_REGISTRY_TOKEN without printing it.
#
# First cut of a *new* crate needs crates.io scope publish-new (publish-update
# alone cannot create the crate). This script only proves the token authenticates;
# scope is enforced by crates.io on publish.
#
# Exit codes:
#   0  — token set and crates.io accepts it (GET /api/v1/me → 200)
#   2  — token unset (expected while READY_EXCEPT_TOKEN)
#   1  — token set but rejected / probe failed
#
# Usage:
#   bash scripts/check-registry-token.sh
#   REQUIRE_TOKEN=1 bash scripts/check-registry-token.sh   # treat unset as FAIL
set -euo pipefail

REQUIRE_TOKEN="${REQUIRE_TOKEN:-0}"
UA="${CRATES_IO_UA:-partitionline-check-registry-token/1 (https://github.com/mingley/partitionline)}"

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
echo "check-registry-token: probing crates.io /api/v1/me (token len=${#CARGO_REGISTRY_TOKEN})"
out="$(mktemp)"
code="$(curl -sS -A "$UA" -H "Authorization: ${CARGO_REGISTRY_TOKEN}" \
  -o "$out" -w '%{http_code}' "https://crates.io/api/v1/me" || true)"

if [[ "$code" == "200" ]]; then
  login="$(python3 - "$out" <<'PY' 2>/dev/null || true
import json,sys
try:
    print(json.load(open(sys.argv[1])).get("user",{}).get("login","?"))
except Exception:
    print("?")
PY
)"
  rm -f "$out"
  echo "check-registry-token: OK — crates.io authenticated as ${login}"
  echo "  Reminder: first cut of a new crate still needs publish-new on this token."
  exit 0
fi

body_snip="$(head -c 160 "$out" | tr '\n' ' ')"
rm -f "$out"
echo "check-registry-token: FAIL — crates.io rejected token (http=${code})" >&2
echo "  body: ${body_snip}" >&2
echo "  Recreate at https://crates.io/settings/tokens with publish-new (+ publish-update)." >&2
exit 1
