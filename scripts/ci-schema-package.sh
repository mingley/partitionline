#!/usr/bin/env bash
# Verify the unpublished companion's actual crate archive.
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
report="$root/target/schema-package"
mkdir -p "$report"
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
export CARGO_TARGET_DIR="$work/target"

{
  git rev-parse HEAD
  rustc -Vv
  cargo -V
} > "$report/source.txt"

cargo package --locked --manifest-path partitionline-schema/Cargo.toml \
  2>&1 | tee "$report/package.log"
archives=("$CARGO_TARGET_DIR"/package/partitionline-schema-*.crate)
if [[ ! -f "${archives[0]}" || "${#archives[@]}" != 1 ]]; then
  echo "Expected exactly one partitionline-schema crate archive" >&2
  exit 1
fi
archive="${archives[0]}"
name="${archive##*/}"
package="$work/${name%.crate}"
cp -- "$archive" "$report/$name"
(cd "$report" && sha256sum "$name" > archive.sha256)
tar -tzf "$archive" > "$report/archive-files.txt"
tar -xzf "$archive" -C "$work"

test ! -e "$package/tests/live-kafka"
for license in LICENSE-APACHE LICENSE-MIT; do
  cmp -- "$license" "$package/$license"
done
python3 -B scripts/check-package-docs.py "$package" --report "$report/package-docs.json"

for profile in default no-default all; do
  features=()
  case "$profile" in
    no-default) features=(--no-default-features) ;;
    all) features=(--all-features) ;;
  esac
  cargo test --locked --manifest-path "$package/Cargo.toml" "${features[@]}" \
    2>&1 | tee "$report/$profile.log"
done
