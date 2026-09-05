#!/usr/bin/env bash
# KL-01 protocol oracles: Produce/Fetch/Metadata/ListOffsets decoded
# required fields vs pinned Kafka 3.9.1 and 4.1.0.
#
# Default: fixture encode/decode via shipped codecs (no live broker).
# --self-test: fail closed on a missing matrix, empty identity, silent skip,
# or absent test file. Does not talk to a broker.
# REQUIRE_BROKER=1: ensure a broker, stamp requested= vs actual=, and run the
# same semantic checks against decoded live responses. If the broker cannot
# start, fail the live path only after fixtures have already run.
#
# Usage:
#   bash scripts/ci-protocol-oracles.sh
#   bash scripts/ci-protocol-oracles.sh --self-test
#   REQUIRE_BROKER=1 bash scripts/ci-protocol-oracles.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TEST_FILE="tests/protocol_oracles.rs"
MATRIX="tests/fixtures/protocol_oracles/matrix.json"
APIS=(Produce Fetch Metadata ListOffsets)
PINS=(3.9.1 4.1.0)

pl_oracles_fail() {
  echo "ci-protocol-oracles: FAIL — $*" >&2
  exit 1
}

pl_oracles_self_test() {
  [[ -f "$TEST_FILE" ]] || pl_oracles_fail "cargo test file absent ($TEST_FILE)"
  [[ -f scripts/ci-protocol-oracles.sh ]] || pl_oracles_fail "harness script absent"
  [[ -f "$MATRIX" ]] || pl_oracles_fail "advertised matrix missing ($MATRIX)"
  grep -q 'cargo test --test protocol_oracles' scripts/ci-protocol-oracles.sh \
    || pl_oracles_fail "harness missing cargo test --test protocol_oracles"
  grep -q 'decode_produce_response' "$TEST_FILE" \
    || pl_oracles_fail "test does not call shipped decode_produce_response"
  grep -q 'decode_fetch_response' "$TEST_FILE" \
    || pl_oracles_fail "test does not call shipped decode_fetch_response"
  grep -q 'decode_metadata_response' "$TEST_FILE" \
    || pl_oracles_fail "test does not call shipped decode_metadata_response"
  grep -q 'decode_list_offsets_topics_response' "$TEST_FILE" \
    || pl_oracles_fail "test does not call shipped decode_list_offsets_topics_response"
  if grep -qE '"identity"[[:space:]]*:[[:space:]]*""' "$MATRIX"; then
    pl_oracles_fail "matrix cell has empty identity"
  fi
  if grep -qE '"skip"[[:space:]]*:[[:space:]]*true' "$MATRIX"; then
    pl_oracles_fail "unclassified skip in matrix (skip:true)"
  fi
  local api pin ident
  for api in "${APIS[@]}"; do
    for pin in "${PINS[@]}"; do
      ident="fixture:apache/kafka:${pin}"
      grep -q "\"api\": \"${api}\"" "$MATRIX" \
        || pl_oracles_fail "matrix missing api ${api}"
      grep -q "\"pin\": \"${pin}\"" "$MATRIX" \
        || pl_oracles_fail "matrix missing pin ${pin}"
      grep -q "\"identity\": \"${ident}\"" "$MATRIX" \
        || pl_oracles_fail "matrix missing identity ${ident} for ${api}"
    done
  done
  local cell_count
  cell_count="$(grep -c '"api":' "$MATRIX" || true)"
  [[ "$cell_count" -eq 8 ]] || pl_oracles_fail "expected 8 advertised cells, found ${cell_count}"
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$MATRIX" <<'PY'
import json, sys
path = sys.argv[1]
with open(path, encoding="utf-8") as fh:
    data = json.load(fh)
need = {(api, pin) for api in ("Produce", "Fetch", "Metadata", "ListOffsets") for pin in ("3.9.1", "4.1.0")}
got = set()
for cell in data["cells"]:
    api, pin = cell["api"], cell["pin"]
    ident = (cell.get("identity") or "").strip()
    if not ident:
        sys.exit(f"empty identity for {api} {pin}")
    if cell.get("skip") and not cell.get("classified_diffs"):
        sys.exit(f"unclassified skip for {api} {pin}")
    got.add((api, pin))
missing = need - got
if missing:
    sys.exit(f"missing cells: {sorted(missing)}")
if len(got) != 8:
    sys.exit(f"expected 8 cells, got {len(got)}")
print("ci-protocol-oracles: matrix 8 cells ok")
PY
  fi
  echo "ci-protocol-oracles: --self-test ok (fixture matrix; no broker)"
}

pl_oracles_live() {
  # shellcheck source=scripts/lib/pl-timeout.sh
  source "$ROOT/scripts/lib/pl-timeout.sh"
  # shellcheck source=scripts/lib/ensure-broker.sh
  source "$ROOT/scripts/lib/ensure-broker.sh"
  # shellcheck source=scripts/lib/broker-identity.sh
  source "$ROOT/scripts/lib/broker-identity.sh"

  local requested="${KAFKA_IMAGE:-apache/kafka:4.1.0}"
  echo "ci-protocol-oracles: live requested=${requested}"
  if ! pl_ensure_broker "ci-protocol-oracles"; then
    echo "ci-protocol-oracles: live path failed (broker could not start); fixture tests already passed" >&2
    return 1
  fi
  pl_broker_identity_print "ci-protocol-oracles"
  echo "ci-protocol-oracles: requested=${requested} actual=${PL_BROKER_ACTUAL}"
  if [[ -z "${PL_BROKER_ACTUAL:-}" ]]; then
    echo "ci-protocol-oracles: live path failed (empty actual identity)" >&2
    return 1
  fi
  PROTOCOL_ORACLES_LIVE=1 \
    PROTOCOL_ORACLES_IDENTITY="${PL_BROKER_ACTUAL}" \
    PROTOCOL_ORACLES_REQUESTED="${requested}" \
    pl_timeout 120s cargo test --test protocol_oracles -- --nocapture --include-ignored
}

if [[ "${1:-}" == "--self-test" ]]; then
  pl_oracles_self_test
  exit 0
fi

echo "== ci-protocol-oracles: fixture semantic oracles =="
cargo test --test protocol_oracles -- --nocapture

if [[ "${REQUIRE_BROKER:-0}" == "1" ]]; then
  echo "== ci-protocol-oracles: live broker decoded semantics =="
  pl_oracles_live
fi

echo "ci-protocol-oracles: ok"
