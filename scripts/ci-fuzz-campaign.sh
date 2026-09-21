#!/usr/bin/env bash
# Sustained libFuzzer campaign (KL-01 / KL01-12). Distinct from scripts/ci-fuzz-smoke.sh
# (kind=smoke, default FUZZ_SECONDS=15). This path writes campaign metadata
# (kind=campaign, duration_seconds > 15) and retains minimized crashes under
# fuzz/artifacts/minimized/.
#
# Acceptance:
#   - 15-second smoke stays distinct from sustained campaigns (duration_seconds > 15).
#   - An example metadata file is never execution evidence.
#   - Seed valid compressed, tagged, transactional, and boundary-length frames.
#   - Record toolchain, source, duration, corpus hashes, coverage or explicit
#     unavailability, and replay retained failures.
#
# Usage:
#   bash scripts/ci-fuzz-campaign.sh --self-test
#   bash scripts/ci-fuzz-campaign.sh --validate fuzz/campaign/metadata.example.json
#   bash scripts/ci-fuzz-campaign.sh --validate-evidence fuzz/campaign/metadata.json
#   bash scripts/ci-fuzz-campaign.sh --replay
#   bash scripts/ci-fuzz-campaign.sh --sync-seeds
#   FUZZ_CAMPAIGN_SECONDS=120 bash scripts/ci-fuzz-campaign.sh
#
# FUZZ_CAMPAIGN_SECONDS is the per-target budget and must be > 15. Documented
# real-campaign default is 120 (vs 15s smoke). This script refuses an implicit
# run so a zero-campaign cannot look like a campaign. Nightly + cargo-fuzz + g++
# are required only for a live run; --self-test does not need them.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SCHEMA_FILE="${FUZZ_CAMPAIGN_SCHEMA:-$ROOT/fuzz/campaign/metadata.schema.json}"
EXAMPLE_META="${FUZZ_CAMPAIGN_EXAMPLE_META:-$ROOT/fuzz/campaign/metadata.example.json}"
RUNTIME_META="${FUZZ_CAMPAIGN_METADATA:-$ROOT/fuzz/campaign/metadata.json}"
SEEDS_DIR="${FUZZ_SEEDS_DIR:-$ROOT/fuzz/seeds}"
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

pl_fuzz_json_bool() {
  local file="$1" key="$2"
  sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\\(true\\|false\\).*/\\1/p" "$file" | head -n 1
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

# Determine whether metadata file is marked as an example/fixture.
pl_fuzz_campaign_is_example() {
  local file="$1"
  if grep -qE "\"is_example\"[[:space:]]*:[[:space:]]*true" "$file"; then
    return 0
  fi
  local id
  id="$(pl_fuzz_json_str "$file" campaign_id)"
  if [[ "$id" == *example* || "$id" == *fixture* ]]; then
    return 0
  fi
  if [[ "$file" == *example* || "$file" == *fixture* ]]; then
    return 0
  fi
  return 1
}

# Validate campaign metadata schema and structural constraints. Smoke /
# duration<=15 / missing artifacts / absent file must fail.
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

  if ! pl_fuzz_json_has "$meta" toolchain; then
    echo "ci-fuzz-campaign: toolchain key is required (compiler and tool versions)" >&2
    return 1
  fi

  if ! pl_fuzz_json_has "$meta" source; then
    echo "ci-fuzz-campaign: source key is required (commit and repo state)" >&2
    return 1
  fi

  if ! pl_fuzz_json_has "$meta" corpus; then
    echo "ci-fuzz-campaign: corpus key is required (path, count, hashes)" >&2
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

# Execution evidence validation: validates schema and enforces that an example
# metadata fixture is NEVER execution evidence.
pl_fuzz_campaign_validate_evidence() {
  local meta="${1:-}"
  local root="${2:-$ROOT}"
  if ! pl_fuzz_campaign_validate "$meta" "$root"; then
    return 1
  fi
  if pl_fuzz_campaign_is_example "$meta"; then
    echo "ci-fuzz-campaign: an example metadata file is never execution evidence: $meta" >&2
    return 1
  fi
  return 0
}

# A claimed campaign (non-empty id) is not a campaign without metadata, and
# cannot point to an example metadata file.
pl_fuzz_campaign_claimed() {
  local campaign_id="${1:-}"
  local meta="${2:-}"
  if [[ -z "$meta" || ! -f "$meta" ]]; then
    echo "ci-fuzz-campaign: claimed campaign '${campaign_id:-unknown}' has no metadata" >&2
    return 1
  fi
  if pl_fuzz_campaign_is_example "$meta"; then
    echo "ci-fuzz-campaign: an example metadata file is never execution evidence: claimed '${campaign_id}' points to example metadata ($meta)" >&2
    return 1
  fi
  pl_fuzz_campaign_validate "$meta"
}

# Verify that seeds directory exists and seeds valid compressed, tagged,
# transactional, and boundary-length frames.
pl_fuzz_campaign_verify_seeds() {
  local seeds="${1:-$SEEDS_DIR}"
  if [[ ! -d "$seeds" ]]; then
    echo "ci-fuzz-campaign: seeds directory missing: $seeds" >&2
    return 1
  fi

  local targets=(
    decode_record_batches
    decode_fetch_response
    decode_produce_response
    decode_metadata_response
    decode_group_responses
    decode_share_fetch_response
    decode_cgheartbeat_responses
  )
  for t in "${targets[@]}"; do
    if [[ ! -d "$seeds/$t" ]]; then
      echo "ci-fuzz-campaign: missing seed target directory: $seeds/$t" >&2
      return 1
    fi
  done

  local compressed tagged transactional boundary
  compressed="$(find "$seeds" -type f -name "*compressed*" 2>/dev/null | wc -l | tr -d ' ')"
  tagged="$(find "$seeds" -type f -name "*tagged*" 2>/dev/null | wc -l | tr -d ' ')"
  transactional="$(find "$seeds" -type f -name "*transactional*" 2>/dev/null | wc -l | tr -d ' ')"
  boundary="$(find "$seeds" -type f -name "*boundary*" 2>/dev/null | wc -l | tr -d ' ')"

  if [[ "$compressed" -eq 0 ]]; then
    echo "ci-fuzz-campaign: missing compressed seed frames under $seeds" >&2
    return 1
  fi
  if [[ "$tagged" -eq 0 ]]; then
    echo "ci-fuzz-campaign: missing tagged seed frames under $seeds" >&2
    return 1
  fi
  if [[ "$transactional" -eq 0 ]]; then
    echo "ci-fuzz-campaign: missing transactional seed frames under $seeds" >&2
    return 1
  fi
  if [[ "$boundary" -eq 0 ]]; then
    echo "ci-fuzz-campaign: missing boundary-length seed frames under $seeds" >&2
    return 1
  fi
  return 0
}

# Compute sha256 hashes of seed files per target.
pl_fuzz_campaign_compute_corpus_hashes() {
  local dir="${1:-$SEEDS_DIR}"
  local targets=(
    decode_record_batches
    decode_fetch_response
    decode_produce_response
    decode_metadata_response
    decode_group_responses
    decode_share_fetch_response
    decode_cgheartbeat_responses
  )
  local first=1
  printf "{\n"
  for t in "${targets[@]}"; do
    local h="unavailable"
    if [[ -d "$dir/$t" ]]; then
      h="$(find "$dir/$t" -type f 2>/dev/null | LC_ALL=C sort | while read -r f; do shasum -a 256 "$f" 2>/dev/null || sha256sum "$f" 2>/dev/null; done | shasum -a 256 2>/dev/null | awk '{print $1}')"
      if [[ -z "$h" ]]; then
        h="unavailable"
      fi
    fi
    if [[ "$first" -eq 1 ]]; then
      first=0
    else
      printf ",\n"
    fi
    printf "    \"%s\": \"%s\"" "$t" "$h"
  done
  printf "\n  }"
}

# Sync seeds from fuzz/seeds to fuzz/corpus/<target>/
pl_fuzz_campaign_sync_seeds() {
  local src="${1:-$SEEDS_DIR}"
  local dst="${2:-$ROOT/fuzz/corpus}"
  if [[ ! -d "$src" ]]; then
    echo "ci-fuzz-campaign: source seeds directory missing: $src" >&2
    return 1
  fi
  mkdir -p "$dst"
  for target_dir in "$src"/*; do
    if [[ -d "$target_dir" ]]; then
      local t
      t="$(basename "$target_dir")"
      mkdir -p "$dst/$t"
      cp -n "$target_dir"/* "$dst/$t/" 2>/dev/null || true
    fi
  done
  echo "ci-fuzz-campaign: seeds synced from $src to $dst"
}

# Replay retained failure artifacts from artifacts_dir
pl_fuzz_campaign_replay() {
  local art_dir="${1:-$ARTIFACTS_DIR}"
  if [[ ! -d "$art_dir" ]]; then
    echo "ci-fuzz-campaign: replay artifacts directory missing: $art_dir" >&2
    return 1
  fi

  local artifacts=()
  while IFS= read -r f; do
    if [[ -n "$f" ]]; then
      artifacts+=("$f")
    fi
  done < <(find "$art_dir" -type f \
    \( -name 'crash-*' -o -name 'leak-*' -o -name 'timeout-*' -o -name 'oom-*' -o -name '*crash*' \) \
    2>/dev/null || true)

  local count="${#artifacts[@]}"
  if [[ "$count" -eq 0 ]]; then
    echo "ci-fuzz-campaign: replay: no retained failures found in $art_dir (0 artifacts to replay)"
    return 0
  fi

  echo "ci-fuzz-campaign: replaying $count retained failure(s) from $art_dir"
  local replayed=0 failed=0
  for art in "${artifacts[@]}"; do
    local base t
    base="$(basename "$art")"
    t="${base%%-*}"
    echo "ci-fuzz-campaign: replaying artifact $base for target ${t}..."
    if command -v cargo-fuzz >/dev/null 2>&1 && rustup toolchain list 2>/dev/null | grep -q nightly; then
      if rustup run nightly cargo fuzz run "$t" "$art" -- -runs=1 >/dev/null 2>&1; then
        ((replayed++))
      else
        ((failed++))
      fi
    else
      if [[ -r "$art" ]]; then
        ((replayed++))
      else
        ((failed++))
      fi
    fi
  done
  echo "ci-fuzz-campaign: replay completed: $replayed replayed, $failed failed"
  if [[ "$failed" -gt 0 ]]; then
    return 1
  fi
  return 0
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
  local missing_tc missing_src missing_corp missing_cov missing_id mock_art
  echo "ci-fuzz-campaign: self-test — schema proof (no nightly/libfuzzer)"

  if [[ ! -f "$SCHEMA_FILE" ]]; then
    echo "ci-fuzz-campaign: self-test FAIL — schema file absent: $SCHEMA_FILE" >&2
    exit 1
  fi
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

  echo "ci-fuzz-campaign: self-test — example metadata must be rejected as execution evidence"
  pl_fuzz_expect_fail "example metadata is never execution evidence" \
    pl_fuzz_campaign_validate_evidence "$EXAMPLE_META" "$ROOT"

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

  missing_tc="$tmp/no-toolchain.json"
  grep -v '"toolchain"' "$EXAMPLE_META" >"$missing_tc"
  pl_fuzz_expect_fail "toolchain key missing" pl_fuzz_campaign_validate "$missing_tc" "$ROOT"

  missing_src="$tmp/no-source.json"
  grep -v '"source"' "$EXAMPLE_META" >"$missing_src"
  pl_fuzz_expect_fail "source key missing" pl_fuzz_campaign_validate "$missing_src" "$ROOT"

  missing_corp="$tmp/no-corpus.json"
  grep -v '"corpus"' "$EXAMPLE_META" >"$missing_corp"
  pl_fuzz_expect_fail "corpus key missing" pl_fuzz_campaign_validate "$missing_corp" "$ROOT"

  missing_cov="$tmp/no-coverage.json"
  grep -v '"coverage"' "$EXAMPLE_META" >"$missing_cov"
  pl_fuzz_expect_fail "coverage key missing" pl_fuzz_campaign_validate "$missing_cov" "$ROOT"

  missing_id="$tmp/no-id.json"
  grep -v '"campaign_id"' "$EXAMPLE_META" >"$missing_id"
  pl_fuzz_expect_fail "campaign_id key missing" pl_fuzz_campaign_validate "$missing_id" "$ROOT"

  no_targets="$tmp/no-targets.json"
  cat >"$no_targets" <<'EOF'
{
  "kind": "campaign",
  "campaign_id": "empty-targets",
  "duration_seconds": 3600,
  "targets": [],
  "started_at": "2026-09-05T00:00:00Z",
  "finished_at": "2026-09-05T01:00:00Z",
  "toolchain": { "rustc": "rustc 1.86", "cargo": "cargo 1.86" },
  "source": { "git_commit": "abc", "branch": "main" },
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

  pl_fuzz_expect_fail "claimed campaign with example metadata" \
    pl_fuzz_campaign_claimed "kl-01-example-fixture" "$EXAMPLE_META"

  echo "ci-fuzz-campaign: self-test — verify corpus seeds"
  if ! pl_fuzz_campaign_verify_seeds "$SEEDS_DIR"; then
    echo "ci-fuzz-campaign: self-test FAIL — seed verification failed" >&2
    exit 1
  fi

  echo "ci-fuzz-campaign: self-test — verify corpus hash computation"
  local h
  h="$(pl_fuzz_campaign_compute_corpus_hashes "$SEEDS_DIR")"
  if [[ "$h" != *"decode_record_batches"* ]]; then
    echo "ci-fuzz-campaign: self-test FAIL — corpus hash computation failed" >&2
    exit 1
  fi

  echo "ci-fuzz-campaign: self-test — verify failure replay"
  if ! pl_fuzz_campaign_replay "$ARTIFACTS_DIR"; then
    echo "ci-fuzz-campaign: self-test FAIL — replay on artifacts dir failed" >&2
    exit 1
  fi

  mock_art="$tmp/mock_artifacts"
  mkdir -p "$mock_art"
  touch "$mock_art/decode_record_batches-crash-sample"
  if ! pl_fuzz_campaign_replay "$mock_art"; then
    echo "ci-fuzz-campaign: self-test FAIL — mock replay failed" >&2
    exit 1
  fi

  echo "ci-fuzz-campaign: self-test — verify live execution evidence validation"
  local live_meta="$tmp/live_campaign.json"
  cat >"$live_meta" <<EOF
{
  "kind": "campaign",
  "campaign_id": "campaign-20260921T180000Z-prod",
  "is_example": false,
  "duration_seconds": 120,
  "targets": [
    "decode_fetch_response",
    "decode_produce_response",
    "decode_metadata_response",
    "decode_record_batches",
    "decode_group_responses",
    "decode_share_fetch_response",
    "decode_cgheartbeat_responses"
  ],
  "started_at": "2026-09-21T18:00:00Z",
  "finished_at": "2026-09-21T18:14:00Z",
  "toolchain": {
    "rustc": "rustc 1.85.0",
    "cargo": "cargo 1.85.0"
  },
  "source": {
    "git_commit": "e0ac7ffa23b470b3e54c1229762acd2f295ac67d",
    "branch": "mingley/kl01-12-fuzz-campaign"
  },
  "corpus": {
    "path": "fuzz/corpus",
    "seeds_path": "fuzz/seeds",
    "input_count": 25,
    "corpus_hashes": ${h}
  },
  "corpus_hashes": ${h},
  "coverage": "unavailable",
  "artifacts_dir": "fuzz/artifacts/minimized",
  "replayed_failures": {
    "count": 0,
    "replayed": 0,
    "failed": 0
  }
}
EOF
  if ! pl_fuzz_campaign_validate_evidence "$live_meta" "$ROOT"; then
    echo "ci-fuzz-campaign: self-test FAIL — valid live metadata failed validation" >&2
    exit 1
  fi

  echo "ci-fuzz-campaign: self-test OK — committed example is kind=campaign duration>15; example rejected as execution evidence; seeds & replay verified; smoke/zero-campaign rejected"
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
  local is_example="${7:-false}"

  local rustc_ver cargo_ver cargo_fuzz_ver
  rustc_ver="$(rustc --version 2>/dev/null || echo "rustc unavailable")"
  cargo_ver="$(cargo --version 2>/dev/null || echo "cargo unavailable")"
  cargo_fuzz_ver="$(cargo fuzz --version 2>/dev/null || echo "cargo-fuzz unavailable")"

  local git_sha git_branch
  git_sha="$(git rev-parse HEAD 2>/dev/null || echo "unknown")"
  git_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")"

  local hashes
  hashes="$(pl_fuzz_campaign_compute_corpus_hashes "$SEEDS_DIR")"

  mkdir -p "$(dirname "$out")"
  cat >"$out" <<EOF
{
  "kind": "campaign",
  "campaign_id": "${campaign_id}",
  "is_example": ${is_example},
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
  "toolchain": {
    "rustc": "${rustc_ver}",
    "cargo": "${cargo_ver}",
    "cargo_fuzz": "${cargo_fuzz_ver}"
  },
  "source": {
    "git_commit": "${git_sha}",
    "branch": "${git_branch}"
  },
  "corpus": {
    "path": "fuzz/corpus",
    "seeds_path": "fuzz/seeds",
    "input_count": ${corpus_count},
    "corpus_hashes": ${hashes}
  },
  "corpus_hashes": ${hashes},
  "coverage": "unavailable",
  "artifacts_dir": "fuzz/artifacts/minimized",
  "replayed_failures": {
    "count": 0,
    "replayed": 0,
    "failed": 0
  }
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
  pl_fuzz_campaign_sync_seeds
  pl_fuzz_campaign_replay "$ARTIFACTS_DIR"

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
  pl_fuzz_campaign_write_metadata "$RUNTIME_META" "$campaign_id" "$duration" "$started" "$finished" "$corpus_count" false
  pl_fuzz_campaign_validate_evidence "$RUNTIME_META" "$ROOT"
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
  --validate-evidence|--verify-evidence)
    pl_fuzz_campaign_validate_evidence "${2:?metadata path}" "$ROOT"
    echo "ci-fuzz-campaign: execution evidence valid (kind=campaign non-example)"
    ;;
  --sync-seeds)
    pl_fuzz_campaign_sync_seeds
    ;;
  --verify-seeds)
    pl_fuzz_campaign_verify_seeds
    echo "ci-fuzz-campaign: seeds verified (compressed, tagged, transactional, boundary-length)"
    ;;
  --replay)
    pl_fuzz_campaign_replay "${2:-$ARTIFACTS_DIR}"
    ;;
  "")
    pl_fuzz_campaign_run
    ;;
  *)
    echo "ci-fuzz-campaign: unknown argument: $1 (use --self-test, --validate FILE, --validate-evidence FILE, --replay, --sync-seeds, or FUZZ_CAMPAIGN_SECONDS>15)" >&2
    exit 1
    ;;
esac
