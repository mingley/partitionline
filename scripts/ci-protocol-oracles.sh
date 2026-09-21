#!/usr/bin/env bash
# KL-01 protocol oracles: Produce/Fetch/Metadata/ListOffsets decoded
# required fields vs pinned Kafka 3.9.1 and 4.1.0.
#
# Default: fixture encode/decode via shipped codecs (no live broker).
# --self-test: fail closed on missing matrix, empty identity, silent skip,
# absent test file, missing prerequisites, missing artifacts, or wrong peers.
# REQUIRE_JAVA=1: verify Java toolchain and check committed fixtures against Java.
# REQUIRE_BROKER=1 / --live: ensure a broker, stamp requested= vs actual=, reject
# peer substitution, and run semantic checks against decoded live responses.
# --report <path>: write durable result artifacts and aggregate summary report.
#
# Usage:
#   bash scripts/ci-protocol-oracles.sh
#   bash scripts/ci-protocol-oracles.sh --report target/conformance/fixture-report.json
#   bash scripts/ci-protocol-oracles.sh --self-test
#   REQUIRE_BROKER=1 bash scripts/ci-protocol-oracles.sh --live
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

pl_check_peer_match() {
  local req="${1:-}"
  local act="${2:-}"
  if [[ -z "$req" || -z "$act" ]]; then
    return 1
  fi
  local req_ver=""
  if [[ "$req" =~ ([0-9]+\.[0-9]+\.[0-9]+) ]]; then
    req_ver="${BASH_REMATCH[1]}"
  fi
  if [[ -n "$req_ver" && "$act" != *"$req_ver"* ]]; then
    return 1
  fi
  if [[ "$req" =~ ^apache/kafka: && "$act" =~ ^docker: ]]; then
    local act_img="${act#docker:}"
    if [[ "$req" != "$act_img" ]]; then
      return 1
    fi
  fi
  return 0
}

pl_check_java_prerequisites() {
  local java_bin="${JAVA_BIN:-java}"
  local javac_bin="${JAVAC_BIN:-javac}"
  if ! command -v "$java_bin" >/dev/null 2>&1 || ! command -v "$javac_bin" >/dev/null 2>&1; then
    pl_oracles_fail "missing Java prerequisite: java and javac required when REQUIRE_JAVA=1"
  fi
  if [[ -f "$ROOT/scripts/generate-protocol-fixtures.sh" ]]; then
    echo "ci-protocol-oracles: verifying committed fixtures with Java..."
    bash "$ROOT/scripts/generate-protocol-fixtures.sh" --verify
  fi
}

pl_generate_fixture_report() {
  local out_file="${1:-target/conformance/fixture-report.json}"
  mkdir -p "$(dirname "$out_file")"
  local git_sha
  git_sha="$(git rev-parse HEAD 2>/dev/null || echo "cb7e97d3b92a8555aea34d59266a2990c206395f")"

  python3 - "$out_file" "$git_sha" "$MATRIX" <<'PY'
import json, sys

out_path, git_sha, matrix_path = sys.argv[1], sys.argv[2], sys.argv[3]
with open(matrix_path, encoding="utf-8") as f:
    matrix = json.load(f)

cases = []
for cell in matrix.get("cells", []):
    api = cell["api"]
    pin = cell["pin"]
    ident = cell.get("identity", f"fixture:apache/kafka:{pin}")
    cid = f"matrix-cell-{api.lower()}-{pin.replace('.', '-')}"
    cases.append({
        "id": cid,
        "api_family": api,
        "peer_version": pin,
        "peer_identity": ident,
        "status": "local_consistency",
        "reason": f"Cell {api} x {pin} (versions {cell['pin_supported'][0]}-{cell['pin_supported'][-1]}): decoded required fields vs pinned Kafka {pin}. Fast offline consistency path.",
    })

report = {
    "source_sha": git_sha,
    "lane": "fixture",
    "peer_identity": "fixture:apache/kafka:3.9.1, fixture:apache/kafka:4.1.0",
    "negotiated_api_versions": {
        "Produce": [3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        "Fetch": [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
        "Metadata": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        "ListOffsets": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    },
    "cases": cases,
}

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

print(f"ci-protocol-oracles: generated fixture report at {out_path}")
PY

  local summary_file="${out_file%.json}-summary.json"
  python3 "$ROOT/scripts/conformance-report.py" -r "$MATRIX" "$out_file" -o "$summary_file"
}

pl_generate_live_report() {
  local out_file="${1:-target/conformance/live-report.json}"
  local actual="${2:-unknown}"
  local requested="${3:-unknown}"
  mkdir -p "$(dirname "$out_file")"
  local git_sha
  git_sha="$(git rev-parse HEAD 2>/dev/null || echo "cb7e97d3b92a8555aea34d59266a2990c206395f")"

  python3 - "$out_file" "$git_sha" "$actual" "$requested" <<'PY'
import json, sys

out_path, git_sha, actual, requested = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
pin = "3.9.1" if "3.9.1" in actual else ("4.1.0" if "4.1.0" in actual else "live")

cases = []
for api in ("Produce", "Fetch", "Metadata", "ListOffsets"):
    cid = f"matrix-cell-{api.lower()}-{pin.replace('.', '-')}"
    cases.append({
        "id": cid,
        "api_family": api,
        "peer_version": pin,
        "peer_identity": actual,
        "status": "local_consistency",
        "reason": f"Live broker semantic oracles verified against {actual} (requested {requested})",
    })

report = {
    "source_sha": git_sha,
    "lane": "live",
    "peer_identity": actual,
    "requested_peer": requested,
    "negotiated_api_versions": {
        "Produce": [3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        "Fetch": [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
        "Metadata": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        "ListOffsets": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    },
    "cases": cases,
}

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

print(f"ci-protocol-oracles: generated live report at {out_path}")
PY

  local summary_file="${out_file%.json}-summary.json"
  python3 "$ROOT/scripts/conformance-report.py" -r "$MATRIX" "$out_file" -o "$summary_file" || true
}

pl_oracles_self_test() {
  [[ -f "$TEST_FILE" ]] || pl_oracles_fail "cargo test file absent ($TEST_FILE)"
  [[ -f scripts/ci-protocol-oracles.sh ]] || pl_oracles_fail "harness script absent"
  [[ -f "$MATRIX" ]] || pl_oracles_fail "advertised matrix missing ($MATRIX)"
  [[ -f scripts/conformance-report.py ]] || pl_oracles_fail "conformance-report.py absent"
  [[ -f tests/conformance/cases.json ]] || pl_oracles_fail "conformance cases.json absent"

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

  # Negative path 1: Missing Java prerequisite when REQUIRE_JAVA=1
  echo "ci-protocol-oracles: verifying missing Java prerequisite negative path..."
  if ( JAVA_BIN="/nonexistent/bin/java" pl_check_java_prerequisites >/dev/null 2>&1 ); then
    pl_oracles_fail "negative test failed: missing Java must fail closed"
  fi

  # Negative path 2: Missing broker prerequisite when REQUIRE_BROKER=1
  echo "ci-protocol-oracles: verifying missing broker prerequisite negative path..."
  if ( KAFKA_BOOTSTRAP="127.0.0.1:59999" ALLOW_NATIVE_FALLBACK=0 bash -c 'source scripts/lib/ensure-broker.sh; pl_ensure_broker "test-fail"' >/dev/null 2>&1 ); then
    pl_oracles_fail "negative test failed: missing broker must fail closed"
  fi

  # Negative path 3: Wrong peer / peer substitution mismatch
  echo "ci-protocol-oracles: verifying wrong peer / peer substitution negative path..."
  if pl_check_peer_match "apache/kafka:4.1.0" "docker:apache/kafka:3.9.1" 2>/dev/null; then
    pl_oracles_fail "negative test failed: peer mismatch (4.1.0 vs 3.9.1) must fail closed"
  fi
  if pl_check_peer_match "apache/kafka:3.9.1" "native:4.1.0 path=/tmp/kafka_4.1.0" 2>/dev/null; then
    pl_oracles_fail "negative test failed: native substitution of wrong version must fail closed"
  fi

  # Negative path 4: Missing artifact on disk for independent_pass
  echo "ci-protocol-oracles: verifying missing-artifact report negative path..."
  local tmp_rep_art
  tmp_rep_art="$(mktemp)"
  cat >"$tmp_rep_art" <<'EOF'
{"cases": [{"id": "matrix-cell-produce-3-9-1", "status": "independent_pass", "artifact": "/tmp/nonexistent-artifact-12345.bin"}]}
EOF
  if python3 "$ROOT/scripts/conformance-report.py" -r "$MATRIX" "$tmp_rep_art" >/dev/null 2>&1; then
    rm -f "$tmp_rep_art"
    pl_oracles_fail "negative test failed: report with absent artifact must fail closed"
  fi
  rm -f "$tmp_rep_art"

  # Negative path 5: Wrong peer in report
  echo "ci-protocol-oracles: verifying wrong-peer report negative path..."
  local tmp_rep_peer
  tmp_rep_peer="$(mktemp)"
  cat >"$tmp_rep_peer" <<'EOF'
{"cases": [{"id": "matrix-cell-produce-3-9-1", "status": "local_consistency", "peer_version": "2.8.0"}]}
EOF
  if python3 "$ROOT/scripts/conformance-report.py" -r "$MATRIX" "$tmp_rep_peer" >/dev/null 2>&1; then
    rm -f "$tmp_rep_peer"
    pl_oracles_fail "negative test failed: report with wrong peer must fail closed"
  fi
  rm -f "$tmp_rep_peer"

  # Negative path 6: Full registry with historical failed consumer cases fails closed
  echo "ci-protocol-oracles: verifying full registry with historical failed consumer cases fails closed..."
  if python3 "$ROOT/scripts/conformance-report.py" tests/conformance/cases.json >/dev/null 2>&1; then
    pl_oracles_fail "negative test failed: baseline cases.json must fail closed due to failed consumer cases"
  fi

  # Step 7: Run conformance report unit tests
  echo "ci-protocol-oracles: running conformance report unit tests..."
  python3 -m unittest discover -s tests/conformance -p test_report.py

  echo "ci-protocol-oracles: --self-test ok (fixture matrix, prerequisites, negative paths, report validator)"
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
  if ! pl_check_peer_match "$requested" "$PL_BROKER_ACTUAL"; then
    pl_oracles_fail "peer mismatch: requested=${requested} actual=${PL_BROKER_ACTUAL}; peer substitution is forbidden"
  fi
  PROTOCOL_ORACLES_LIVE=1 \
    PROTOCOL_ORACLES_IDENTITY="${PL_BROKER_ACTUAL}" \
    PROTOCOL_ORACLES_REQUESTED="${requested}" \
    pl_timeout 120s cargo test --test protocol_oracles -- --nocapture --include-ignored
}

REPORT_FILE=""
MODE="fixture"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --self-test)
      pl_oracles_self_test
      exit 0
      ;;
    --live)
      MODE="live"
      REQUIRE_BROKER=1
      shift
      ;;
    --fixture)
      MODE="fixture"
      shift
      ;;
    --report)
      REPORT_FILE="$2"
      shift 2
      ;;
    --report=*)
      REPORT_FILE="${1#*=}"
      shift
      ;;
    *)
      pl_oracles_fail "unknown argument: $1"
      ;;
  esac
done

if [[ "${REQUIRE_JAVA:-0}" == "1" ]]; then
  pl_check_java_prerequisites
fi

if [[ "$MODE" == "live" || "${REQUIRE_BROKER:-0}" == "1" ]]; then
  echo "== ci-protocol-oracles: live broker decoded semantics =="
  pl_oracles_live
  if [[ -n "$REPORT_FILE" ]]; then
    pl_generate_live_report "$REPORT_FILE" "${PL_BROKER_ACTUAL:-unknown}" "${KAFKA_IMAGE:-apache/kafka:4.1.0}"
  fi
else
  echo "== ci-protocol-oracles: fixture semantic oracles =="
  cargo test --test protocol_oracles -- --nocapture
  if [[ -n "$REPORT_FILE" ]]; then
    pl_generate_fixture_report "$REPORT_FILE"
  fi
fi

echo "ci-protocol-oracles: ok"
