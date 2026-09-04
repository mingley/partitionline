#!/usr/bin/env bash
# Ensure the pre-crates.io git install pin stays honest.
#
# While README still recommends a git tag (not crates.io), that tag must:
#   1. Appear identically in README.md, docs/ADOPTION.md, docs/migrate-from-rdkafka.md
#   2. Exist as an annotated/lightweight tag in this clone
#   3. Point at HEAD (no silent tip drift — cut a new rc when tip moves)
#
# After crates.io publish, README uses `partitionline = "0.x"` and this check
# becomes a no-op success.
#
# Usage:
#   bash scripts/check-adopter-pin.sh
#   ADOPTER_PIN_ALLOW_LAG=1 bash scripts/check-adopter-pin.sh   # warn only if tag behind tip
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Crates.io install stanza — Installable path already landed.
if grep -qE '^partitionline = "[0-9]' README.md; then
  echo "check-adopter-pin: ok (README uses crates.io version dep)"
  exit 0
fi

extract_tag() {
  local file="$1"
  python3 - "$file" <<'PY'
import re, sys
text = open(sys.argv[1], encoding="utf-8").read()
# Prefer toml git pin with tag = "v…"
m = re.search(r'tag\s*=\s*"(v[0-9][^"]*)"', text)
if not m:
    raise SystemExit(f"check-adopter-pin: no git tag pin in {sys.argv[1]}")
print(m.group(1))
PY
}

readme_tag="$(extract_tag README.md)"
adopt_tag="$(extract_tag docs/ADOPTION.md)"
migrate_tag="$(extract_tag docs/migrate-from-rdkafka.md)"

if [[ "$readme_tag" != "$adopt_tag" || "$readme_tag" != "$migrate_tag" ]]; then
  echo "check-adopter-pin: FAIL — pin tags disagree" >&2
  echo "  README:  ${readme_tag}" >&2
  echo "  ADOPTION:${adopt_tag}" >&2
  echo "  migrate: ${migrate_tag}" >&2
  exit 1
fi

tag="$readme_tag"
if ! git rev-parse -q --verify "${tag}^{}" >/dev/null; then
  echo "check-adopter-pin: FAIL — tag ${tag} not in this clone (fetch tags?)" >&2
  exit 1
fi

tag_sha="$(git rev-parse "${tag}^{}")"
head_sha="$(git rev-parse HEAD)"
if [[ "$tag_sha" != "$head_sha" ]]; then
  ahead="$(git rev-list --count "${tag}..HEAD" 2>/dev/null || echo "?")"
  msg="check-adopter-pin: pin ${tag} (${tag_sha:0:7}) lags HEAD (${head_sha:0:7}) by ${ahead} commit(s)"
  if [[ "${ADOPTER_PIN_ALLOW_LAG:-}" == "1" ]]; then
    echo "WARN  ${msg} (ADOPTER_PIN_ALLOW_LAG=1)" >&2
  else
    echo "FAIL  ${msg}" >&2
    echo "FAIL  cut a new rc tag at tip and update README/ADOPTION/migrate pins" >&2
    exit 1
  fi
else
  echo "check-adopter-pin: ok (${tag} == HEAD ${head_sha:0:7})"
fi
