#!/bin/sh
# Check the crate archive and install its operator for a local admin workflow.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$root"
report="$root/target/broker-package"
mkdir -p "$report"
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
CARGO_TARGET_DIR="$work/target"
export CARGO_TARGET_DIR

revision=$(git rev-parse HEAD)
{
  printf '%s\n' "$revision"
  rustc +stable -Vv
  cargo +stable -V
  uname -s -m
} > "$report/source.txt"
cat "$report/source.txt"

printf 'Check the crate archive. Logs are in %s\n' "$report"
cargo +stable package --locked --manifest-path partitionline-broker/Cargo.toml \
  > "$report/package.log" 2>&1
cat "$report/package.log"
set -- "$CARGO_TARGET_DIR"/package/partitionline-broker-*.crate
if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
  printf '%s\n' 'Expected one partitionline-broker crate archive' >&2
  exit 1
fi
archive=$1
name=${archive##*/}
package="$work/${name%.crate}"
cp -- "$archive" "$report/$name"
(cd "$report" && sha256sum "$name" > archive.sha256)
tar -tzf "$archive" > "$report/archive-files.txt"
tar -xzf "$archive" -C "$work"

for file in LICENSE-APACHE LICENSE-MIT NOTICE; do
  cmp "$file" "$package/$file"
done
python3 -B scripts/check-package-docs.py "$package" --report "$report/package-docs.json"
cp -- "$package/.cargo_vcs_info.json" "$report/cargo-vcs-info.json"
python3 -B - "$report/cargo-vcs-info.json" "$revision" <<'PY'
import json
import sys
from pathlib import Path

vcs = json.loads(Path(sys.argv[1]).read_text())
if vcs["git"]["sha1"] != sys.argv[2] or vcs["git"].get("dirty"):
    raise SystemExit("archive source does not match the clean checkout")
PY

for profile in default all-features; do
  set --
  if [ "$profile" = all-features ]; then
    set -- --all-features
  fi
  printf 'Verify %s from the extracted archive\n' "$profile"
  cargo +stable clippy --locked --manifest-path "$package/Cargo.toml" \
    --lib --bins --examples "$@" -- -D warnings > "$report/$profile-clippy.log" 2>&1
  install="$work/install/$profile"
  cargo +stable install --locked --path "$package" --root "$install" --bin operator "$@" \
    > "$report/$profile-install.log" 2>&1
  (cd "$install/bin" && sha256sum operator > "$report/$profile-operator.sha256")

  bin="$install/bin/operator"
  data="$work/data/$profile"
  (
    "$bin" init --data "$data/live" --cluster cluster-1 --voter 1
    "$bin" register-dir --data "$data/live" --log-dir data-a
    "$bin" assign --data "$data/live" --topic orders --partition 0 --replicas 1 --leader 1 --log-dir data-a
    "$bin" feature --data "$data/live" --name metadata.version --level 2 --version 0
    "$bin" feature --data "$data/live" --name metadata.version --level 3 --version 1
    "$bin" backup --data "$data/live" --to "$data/backup/admin.journal"
    if "$bin" feature --data "$data/live" --name metadata.version --level 1 --version 0; then
      printf '%s\n' 'Downgrade was accepted' >&2
      exit 1
    fi
    cmp "$data/live/admin.journal" "$data/backup/admin.journal"
    "$bin" status --data "$data/live" > "$data/live-status"
    "$bin" restore --from "$data/backup/admin.journal" --data "$data/restored"
    cmp "$data/live/admin.journal" "$data/restored/admin.journal"
    "$bin" status --data "$data/restored" > "$data/restored-status"
    cmp "$data/live-status" "$data/restored-status"
    grep -Fx 'feature metadata.version 3' "$data/restored-status"
    grep -Fx 'log_dir orders 0 data-a' "$data/restored-status"
    printf 'Packaged operator lifecycle passed (%s)\n' "$profile"
  ) > "$report/$profile-operator.log" 2>&1
  cat "$report/$profile-operator.log"
done
