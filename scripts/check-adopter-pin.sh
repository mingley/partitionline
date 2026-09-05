#!/usr/bin/env bash
# Ensure the pre-crates.io git install pin stays honest without empty rc thrash.
#
# While README still recommends a git tag (not crates.io), that tag must:
#   1. Appear identically in README.md, docs/ADOPTION.md, docs/migrate-from-rdkafka.md, docs/guide.md
#   2. Exist as an annotated/lightweight tag in this clone
#   3. Either:
#        a) point at HEAD, or
#        b) lag HEAD only by docs/scripts/meta paths (no src/tests/examples/Cargo.*),
#           so stewardship edits do not force a new rc pin
#      Any library/Cargo change since the pin → FAIL (cut a new rc).
#
# After crates.io publish, README uses `partitionline = "0.x"` and this check
# becomes a no-op success.
#
# Usage:
#   bash scripts/check-adopter-pin.sh
#   ADOPTER_PIN_ALLOW_LAG=1 bash scripts/check-adopter-pin.sh   # warn only on any lag
#   ADOPTER_PIN_STRICT=1 bash scripts/check-adopter-pin.sh      # require tag == HEAD always
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Crates.io install stanza — Installable path already landed.
if grep -qE '^partitionline = "[0-9]' README.md; then
  # Day-1 must also flip ADOPTION off the pre-publish "remaining owner step" story.
  if grep -qiE 'remaining owner step|crates\.io publish is the remaining' docs/ADOPTION.md; then
    echo "check-adopter-pin: FAIL — README is crates.io-shaped but docs/ADOPTION.md still says publish is remaining" >&2
    echo "  Re-run: bash scripts/post-publish-adoption.sh (wired into day1-after-publish)" >&2
    exit 1
  fi
  if ! grep -qE 'partitionline = "[0-9]' docs/ADOPTION.md; then
    echo "check-adopter-pin: FAIL — README is crates.io-shaped but docs/ADOPTION.md lacks a crates.io version dep" >&2
    echo "  Re-run: bash scripts/post-publish-adoption.sh" >&2
    exit 1
  fi
  echo "check-adopter-pin: ok (README + ADOPTION use crates.io version dep)"
  exit 0
fi

extract_tag() {
  local file="$1"
  python3 - "$file" <<'PY'
import re, sys
text = open(sys.argv[1], encoding="utf-8").read()
m = re.search(r'tag\s*=\s*"(v[0-9][^"]*)"', text)
if not m:
    raise SystemExit(f"check-adopter-pin: no git tag pin in {sys.argv[1]}")
print(m.group(1))
PY
}

# Paths that change the crate adopters compile — lag here requires a new rc.
is_library_path() {
  case "$1" in
    src/*|tests/*|examples/*|benches/*|fuzz/*|Cargo.toml|Cargo.lock|deny.toml|clippy.toml|rust-toolchain*|rust-toolchain.toml)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

readme_tag="$(extract_tag README.md)"
adopt_tag="$(extract_tag docs/ADOPTION.md)"
migrate_tag="$(extract_tag docs/migrate-from-rdkafka.md)"
guide_tag="$(extract_tag docs/guide.md)"

if [[ "$readme_tag" != "$adopt_tag" || "$readme_tag" != "$migrate_tag" || "$readme_tag" != "$guide_tag" ]]; then
  echo "check-adopter-pin: FAIL — pin tags disagree" >&2
  echo "  README:  ${readme_tag}" >&2
  echo "  ADOPTION:${adopt_tag}" >&2
  echo "  migrate: ${migrate_tag}" >&2
  echo "  guide:   ${guide_tag}" >&2
  exit 1
fi

# While pre-Installable, migrate/guide must not lead with a live crates.io dep line.
# (Commented after-publish lines are fine.)
if grep -qE '^partitionline = "[0-9]' docs/migrate-from-rdkafka.md; then
  echo "check-adopter-pin: FAIL — docs/migrate-from-rdkafka.md leads with crates.io version while README is still git-shaped" >&2
  echo "  Comment the crates.io line until day1; keep the git tag pin primary." >&2
  exit 1
fi
if grep -qE '^partitionline = \{ version = "[0-9]' docs/guide.md; then
  echo "check-adopter-pin: FAIL — docs/guide.md leads with crates.io version while README is still git-shaped" >&2
  echo "  Use the interim git tag pin (with features) until day1." >&2
  exit 1
fi

tag="$readme_tag"
if ! git rev-parse -q --verify "${tag}^{}" >/dev/null; then
  echo "check-adopter-pin: FAIL — tag ${tag} not in this clone (fetch tags?)" >&2
  exit 1
fi

tag_sha="$(git rev-parse "${tag}^{}")"
head_sha="$(git rev-parse HEAD)"
if [[ "$tag_sha" == "$head_sha" ]]; then
  echo "check-adopter-pin: ok (${tag} == HEAD ${head_sha:0:7})"
  exit 0
fi

ahead="$(git rev-list --count "${tag}..HEAD" 2>/dev/null || echo "?")"
msg="pin ${tag} (${tag_sha:0:7}) lags HEAD (${head_sha:0:7}) by ${ahead} commit(s)"

if [[ "${ADOPTER_PIN_ALLOW_LAG:-}" == "1" ]]; then
  echo "WARN  check-adopter-pin: ${msg} (ADOPTER_PIN_ALLOW_LAG=1)" >&2
  exit 0
fi

if [[ "${ADOPTER_PIN_STRICT:-}" == "1" ]]; then
  echo "FAIL  check-adopter-pin: ${msg} (ADOPTER_PIN_STRICT=1)" >&2
  echo "FAIL  cut a new rc tag at tip and update README/ADOPTION/migrate/guide pins" >&2
  exit 1
fi

# Default: allow lag only when no library/packaging paths changed since the pin.
mapfile -t changed < <(git diff --name-only "${tag}..HEAD")
lib_hits=()
for path in "${changed[@]}"; do
  if is_library_path "$path"; then
    lib_hits+=("$path")
  fi
done

if [[ "${#lib_hits[@]}" -eq 0 ]]; then
  echo "check-adopter-pin: ok (${msg}; docs/scripts-only tip drift)"
  exit 0
fi

echo "FAIL  check-adopter-pin: ${msg}" >&2
echo "FAIL  library/packaging paths changed since ${tag}:" >&2
for path in "${lib_hits[@]}"; do
  echo "  - ${path}" >&2
done
echo "FAIL  cut a new rc tag at tip and update README/ADOPTION/migrate/guide pins" >&2
exit 1
