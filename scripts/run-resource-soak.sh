#!/usr/bin/env bash
set -euo pipefail
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"
# Resume/check must use the frozen binary and must not rebuild it.
for argument in "$@"; do
  if [[ "$argument" == --resume || "$argument" == --check ]]; then
    exec python3 scripts/resource-soak.py "$@"
  fi
done
cargo +stable build --locked --example resource_soak
metadata_file="$(mktemp)"
trap 'rm -f "$metadata_file"' EXIT
cargo +stable metadata --format-version 1 --no-deps > "$metadata_file"
soak_target="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["target_directory"])' "$metadata_file")"
python3 scripts/resource-soak.py --binary "$soak_target/debug/examples/resource_soak" "$@"
