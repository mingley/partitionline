#!/usr/bin/env bash
# Sustained libFuzzer campaign (KL-01). Distinct from scripts/ci-fuzz-smoke.sh
# (kind=smoke, default FUZZ_SECONDS=15). This path writes campaign metadata
# (kind=campaign, duration_seconds > 15) and retains minimized crashes under
# fuzz/artifacts/minimized/.
#
# Usage:
#   bash scripts/ci-fuzz-campaign.sh --self-test
#   bash scripts/ci-fuzz-campaign.sh --validate fuzz/campaign/metadata.example.json
#   FUZZ_CAMPAIGN_SECONDS=120 bash scripts/ci-fuzz-campaign.sh
#
# FUZZ_CAMPAIGN_SECONDS is the per-target budget and must be > 15. Documented
# real-campaign default is 120 (vs 15s smoke). This script refuses an implicit
# run so a zero-campaign cannot look like a campaign. Nightly + cargo-fuzz + g++
# are required only for a live run; --self-test does not need them.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

EXAMPLE_META="${FUZZ_CAMPAIGN_EXAMPLE_META:-$ROOT/fuzz/campaign/metadata.example.json}"
RUNTIME_META="${FUZZ_CAMPAIGN_METADATA:-$ROOT/fuzz/campaign/metadata.json}"
ARTIFACTS_DIR="$ROOT/fuzz/artifacts/minimized"

# Documented live-campaign budget (not applied unless the caller sets the env).
FUZZ_CAMPAIGN_SECONDS_DEFAULT=120

pl_fuzz_json_has() {
  local file="$1" key="$2"
  grep -qE "\"${key}\"[[:space:]]*:" "$file"
}

pl_fuzz_json_str() {
  local file="$1" key="$2"
  sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p" "$file" | head -n 1
}

pl_fuzz_json_num() {
  local file="$1" key="$2"
  sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\\([0-9][0-9]*\\).*/\\1/p" "$file" | head -n 1
}

pl_fuzz_json_array_nonempty() {
  local file="$1" key="$2"
  local collapsed after inside
  collapsed="$(tr '\n' ' ' <"$file")"
  after="${collapsed#*\"$key\"}"
  if [[ "$after" == "$collapsed" ]]; then
    return 1
  fi
  after="${after#*:}"
  after="${after#*\[}"
  inside="${after%%]*}"
  [[ "$inside" == *\"* ]]
}

# Validate campaign metadata. Smoke / duration<=15 / missing artifacts / absent
# file must fail — a zero-campaign must not look like a campaign.
pl_fuzz_campaign_validate() {
  local meta="${1:-}"
  local root="${2:-$ROOT}"
  local kind duration artifacts_dir art

  if [[ -z "$meta" || ! -f "$meta" ]]; then
    echo "ci-fuzz-campaign: metadata file absent: ${meta:-unset}" >&2
    return 1
  fi

  kind="$(pl_fuzz_json_str "$meta" kind)"
  if [[ "$kind" != "campaign" ]]; then
    echo "ci-fuzz-campaign: kind must be campaign (got '${kind:-missing}'; smoke/zero-campaign cannot look like a campaign)" >&2
    return 1
  fi

  duration="$(pl_fuzz_json_num "$meta" duration_seconds)"
  if [[ -z "$duration" || ! "$duration" =~ ^[0-9]+$ || "$duration" -le 15 ]]; then
    echo "ci-fuzz-campaign: duration_seconds must be > 15 (got '${duration:-missing}'; 15s is CI smoke)" >&2
    return 1
  fi

  if ! pl_fuzz_json_has "$meta" targets || ! pl_fuzz_json_array_nonempty "$meta" targets; then
    echo "ci-fuzz-campaign: targets must be a non-empty list" >&2
    return 1
  fi

  if ! pl_fuzz_json_has "$meta" started_at || ! pl_fuzz_json_has "$meta" finished_at; then
    echo "ci-fuzz-campaign: started_at and finished_at are required" >&2
    return 1
  fi

  if ! pl_fuzz_json_has "$meta" corpus; then
    echo "ci-fuzz-campaign: corpus key is required (path or counts)" >&2
    return 1
  fi

  if ! pl_fuzz_json_has "$meta" coverage; then
    echo "ci-fuzz-campaign: coverage key is required (string/object; may be \"unavailable\")" >&2
    return 1
  fi

  if ! pl_fuzz_json_has "$meta" campaign_id || [[ -z "$(pl_fuzz_json_str "$meta" campaign_id)" ]]; then
    echo "ci-fuzz-campaign: campaign_id is required" >&2
    return 1
  fi

  artifacts_dir="$(pl_fuzz_json_str "$meta" artifacts_dir)"
  if [[ -z "$artifacts_dir" ]]; then
    echo "ci-fuzz-campaign: artifacts_dir missing" >&2
    return 1
  fi
  if [[ "$artifacts_dir" = /* ]]; then
    art="$artifacts_dir"
  else
    art="$root/$artifacts_dir"
  fi
  if [[ ! -d "$art" ]]; then
    echo "ci-fuzz-campaign: artifacts_dir missing: $art" >&2
    return 1
  fi
  return 0
}

# A claimed campaign (non-empty id) is not a campaign without metadata.
pl_fuzz_campaign_claimed() {
  local campaign_id="${1:-}"
  local meta="${2:-}"
  if [[ -z "$meta" || ! -f "$meta" ]]; then
    echo "ci-fuzz-campaign: claimed campaign '${campaign_id:-unknown}' has no metadata" >&2
    return 1
  fi
  pl_fuzz_campaign_validate "$meta"
}

pl_fuzz_expect_fail() {
  local label="$1"
  shift
  echo "ci-fuzz-campaign: self-test — $label must fail"
  if "$@" >/tmp/pl-fuzz-campaign-neg.out 2>/tmp/pl-fuzz-campaign-neg.err; then
    echo "ci-fuzz-campaign: self-test FAIL — $label unexpectedly passed" >&2
    exit 1
  fi
  if ! grep -q 'ci-fuzz-campaign:' /tmp/pl-fuzz-campaign-neg.err; then
    echo "ci-fuzz-campaign: self-test FAIL — $label produced no error message" >&2
    exit 1
  fi
}

pl_fuzz_campaign_self_test() {
  local tmp smoke short zero missing_art missing_dir no_targets claimed
  echo "ci-fuzz-campaign: self-test — schema proof (no nightly/libfuzzer)"

  if [[ ! -f "$EXAMPLE_META" ]]; then
    echo "ci-fuzz-campaign: self-test FAIL — metadata file absent: $EXAMPLE_META" >&2
    exit 1
  fi
  if [[ ! -d "$ARTIFACTS_DIR" ]]; then
    echo "ci-fuzz-campaign: self-test FAIL — artifacts_dir missing: $ARTIFACTS_DIR" >&2
    exit 1
  fi

  echo "ci-fuzz-campaign: self-test — committed example must pass as a campaign"
  if ! pl_fuzz_campaign_validate "$EXAMPLE_META" "$ROOT"; then
    echo "ci-fuzz-campaign: self-test FAIL — example metadata rejected" >&2
    exit 1
  fi
  if [[ "$(pl_fuzz_json_str "$EXAMPLE_META" kind)" != "campaign" ]]; then
    echo "ci-fuzz-campaign: self-test FAIL — example kind is not campaign" >&2
    exit 1
  fi

  tmp="$(mktemp -d "${TMPDIR:-/tmp}/pl-fuzz-campaign.XXXXXX")"
  # shellcheck disable=SC2064
  trap "rm -rf '$tmp'" EXIT

  smoke="$tmp/smoke.json"
  sed 's/"kind": "campaign"/"kind": "smoke"/' "$EXAMPLE_META" >"$smoke"
  pl_fuzz_expect_fail "kind=smoke" pl_fuzz_campaign_validate "$smoke" "$ROOT"

  short="$tmp/short.json"
  sed 's/"duration_seconds": 3600/"duration_seconds": 15/' "$EXAMPLE_META" >"$short"
  pl_fuzz_expect_fail "duration_seconds=15" pl_fuzz_campaign_validate "$short" "$ROOT"

  zero="$tmp/zero.json"
  sed 's/"duration_seconds": 3600/"duration_seconds": 0/' "$EXAMPLE_META" >"$zero"
  pl_fuzz_expect_fail "duration_seconds=0 (zero-campaign)" pl_fuzz_campaign_validate "$zero" "$ROOT"

  missing_art="$tmp/no-artifacts-key.json"
  grep -v '"artifacts_dir"' "$EXAMPLE_META" >"$missing_art"
  pl_fuzz_expect_fail "artifacts_dir key missing" pl_fuzz_campaign_validate "$missing_art" "$ROOT"

  missing_dir="$tmp/missing-dir.json"
  sed 's|"artifacts_dir": "fuzz/artifacts/minimized"|"artifacts_dir": "fuzz/artifacts/does-not-exist"|' \
    "$EXAMPLE_META" >"$missing_dir"
  pl_fuzz_expect_fail "artifacts_dir path missing" pl_fuzz_campaign_validate "$missing_dir" "$ROOT"

  no_targets="$tmp/no-targets.json"
  cat >"$no_targets" <<'EOF'
{
  "kind": "campaign",
  "campaign_id": "empty-targets",
  "duration_seconds": 3600,
  "targets": [],
  "started_at": "2026-09-05T00:00:00Z",
  "finished_at": "2026-09-05T01:00:00Z",
  "corpus": { "path": "fuzz/corpus", "input_count": 0 },
  "coverage": "unavailable",
  "artifacts_dir": "fuzz/artifacts/minimized"
}
EOF
  pl_fuzz_expect_fail "empty targets" pl_fuzz_campaign_validate "$no_targets" "$ROOT"

  pl_fuzz_expect_fail "metadata file absent" pl_fuzz_campaign_validate "$tmp/no-such.json" "$ROOT"

  claimed="$tmp/claimed-missing.json"
  pl_fuzz_expect_fail "claimed campaign has no metadata" \
    pl_fuzz_campaign_claimed "fake-campaign-id" "$claimed"

  echo "ci-fuzz-campaign: self-test OK — committed example is kind=campaign duration>15; smoke/zero-campaign rejected"
}

pl_fuzz_campaign_tools() {
  local missing=()
  if ! command -v g++ >/dev/null 2>&1; then
    missing+=("g++")
  fi
  if ! command -v cargo-fuzz >/dev/null 2>&1; then
    missing+=("cargo-fuzz")
  fi
  if ! command -v rustup >/dev/null 2>&1 || ! rustup toolchain list 2>/dev/null | grep -q nightly; then
    missing+=("nightly")
  fi
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "ci-fuzz-campaign: fail closed: missing ${missing[*]} (not a campaign; not ok). Schema proof: bash scripts/ci-fuzz-campaign.sh --self-test" >&2
    return 1
  fi
  return 0
}

pl_fuzz_campaign_write_metadata() {
  local out="$1"
  local campaign_id="$2"
  local duration="$3"
  local started="$4"
  local finished="$5"
  local corpus_count="$6"
  mkdir -p "$(dirname "$out")"
  cat >"$out" <<EOF
{
  "kind": "campaign",
  "campaign_id": "${campaign_id}",
  "duration_seconds": ${duration},
  "targets": [
    "decode_fetch_response",
    "decode_produce_response",
    "decode_metadata_response",
    "decode_record_batches",
    "decode_group_responses",
    "decode_share_fetch_response",
    "decode_cgheartbeat_responses"
  ],
  "started_at": "${started}",
  "finished_at": "${finished}",
  "corpus": {
    "path": "fuzz/corpus",
    "input_count": ${corpus_count}
  },
  "coverage": "unavailable",
  "artifacts_dir": "fuzz/artifacts/minimized"
}
EOF
}

pl_fuzz_campaign_retain() {
  mkdir -p "$ARTIFACTS_DIR"
  if [[ ! -d "$ROOT/fuzz/artifacts" ]]; then
    return 0
  fi
  find "$ROOT/fuzz/artifacts" -type f \
    \( -name 'crash-*' -o -name 'leak-*' -o -name 'timeout-*' -o -name 'oom-*' \) \
    ! -path '*/minimized/*' \
    -exec cp -n {} "$ARTIFACTS_DIR/" \; 2>/dev/null || true
}

pl_fuzz_campaign_run() {
  local duration started finished campaign_id corpus_count rc sha
  local -a targets

  pl_fuzz_campaign_tools

  if [[ -z "${FUZZ_CAMPAIGN_SECONDS:-}" && -z "${FUZZ_SECONDS:-}" ]]; then
    echo "ci-fuzz-campaign: set FUZZ_CAMPAIGN_SECONDS>15 explicitly (documented campaign budget ${FUZZ_CAMPAIGN_SECONDS_DEFAULT}s/target vs 15s smoke). Refusing implicit run so a zero-campaign cannot look like a campaign." >&2
    exit 1
  fi
  duration="${FUZZ_CAMPAIGN_SECONDS:-$FUZZ_SECONDS}"
  if [[ ! "$duration" =~ ^[0-9]+$ || "$duration" -le 15 ]]; then
    echo "ci-fuzz-campaign: duration_seconds=${duration} is smoke, not a campaign. Use scripts/ci-fuzz-smoke.sh (FUZZ_SECONDS=15)." >&2
    exit 1
  fi

  mkdir -p "$ARTIFACTS_DIR"
  export CXX="${CXX:-g++}"
  targets=(
    decode_fetch_response
    decode_produce_response
    decode_metadata_response
    decode_record_batches
    decode_group_responses
    decode_share_fetch_response
    decode_cgheartbeat_responses
  )

  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sha="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  campaign_id="campaign-$(date -u +%Y%m%dT%H%M%SZ)-${sha}"
  rc=0
  rustup run nightly cargo fuzz build
  for t in "${targets[@]}"; do
    echo "== campaign fuzz $t (${duration}s) =="
    rustup run nightly cargo fuzz run "$t" -- \
      -max_total_time="$duration" \
      -timeout=5 \
      -rss_limit_mb=2048 \
      -artifact_prefix="$ARTIFACTS_DIR/${t}-" \
      || rc=1
  done
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  pl_fuzz_campaign_retain
  corpus_count=0
  if [[ -d "$ROOT/fuzz/corpus" ]]; then
    corpus_count="$(find "$ROOT/fuzz/corpus" -type f | wc -l | tr -d ' ')"
  fi
  pl_fuzz_campaign_write_metadata "$RUNTIME_META" "$campaign_id" "$duration" "$started" "$finished" "$corpus_count"
  pl_fuzz_campaign_validate "$RUNTIME_META" "$ROOT"
  if [[ "$rc" -ne 0 ]]; then
    echo "ci-fuzz-campaign: targets reported failures; minimized artifacts retained; metadata written" >&2
    exit 1
  fi
  echo "ci-fuzz-campaign: ok (kind=campaign duration_seconds=${duration})"
}

case "${1:-}" in
  --self-test)
    pl_fuzz_campaign_self_test
    ;;
  --validate)
    pl_fuzz_campaign_validate "${2:?metadata path}" "$ROOT"
    echo "ci-fuzz-campaign: metadata valid (kind=campaign)"
    ;;
  "")
    pl_fuzz_campaign_run
    ;;
  *)
    echo "ci-fuzz-campaign: unknown argument: $1 (use --self-test, --validate FILE, or FUZZ_CAMPAIGN_SECONDS>15)" >&2
    exit 1
    ;;
esac
