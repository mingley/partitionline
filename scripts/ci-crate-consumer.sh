#!/usr/bin/env bash
# Compile the real documented surface and shared operator program from an
# extracted .crate. Root defaults currently approve only default and tracing.
# PL_PACKAGE_TOOLCHAINS='current' by default; CI also checks Rust 1.85.0.
# PL_PACKAGE_ALLOW_DIRTY=1 is an explicit local-development opt-in.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
source "${ROOT}/scripts/lib/adopter-consumer-main.sh"
read -r name ver < <(python3 - <<'PY'
import tomllib
from pathlib import Path
p = tomllib.loads(Path('Cargo.toml').read_text())['package']
print(p['name'], p['version'])
PY
)
package_target="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
report_dir="${PL_PACKAGE_REPORT_DIR:-$package_target/package-check}"
mkdir -p "$report_dir"
package_args=()
if [[ "${PL_PACKAGE_ALLOW_DIRTY:-0}" == 1 ]]; then package_args+=(--allow-dirty); fi
cargo package --locked --list "${package_args[@]}" > "$report_dir/package-list.txt"
cargo package --locked --no-verify "${package_args[@]}"
crate="$package_target/package/${name}-${ver}.crate"
[[ -f "$crate" ]] || { echo "ci-crate-consumer: missing exact archive $crate" >&2; exit 1; }
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
python3 - "$crate" "$tmpdir" <<'PY'
import sys,tarfile
with tarfile.open(sys.argv[1]) as packed:
    packed.extractall(sys.argv[2], filter='data')
PY
src="$tmpdir/${name}-${ver}"
python3 -B scripts/check-package-docs.py "$src" --report "$report_dir/package-docs.json"
reports=()
for toolchain in ${PL_PACKAGE_TOOLCHAINS:-current}; do
  [[ "$toolchain" =~ ^[A-Za-z0-9_.-]+$ ]] || { echo 'ci-crate-consumer: invalid toolchain' >&2; exit 1; }
  cargo_command=(cargo)
  doc_args=()
  if [[ "$toolchain" != current ]]; then
    cargo_command+=("+$toolchain")
    doc_args+=(--toolchain "$toolchain")
  fi
  export CARGO_TARGET_DIR="${PL_PACKAGE_CONSUMER_TARGET_DIR:-$package_target/package-consumer-targets}/$toolchain"
  for features in default tracing; do
    cons="$tmpdir/consumer-$toolchain-$features"
    mkdir -p "$cons/src"
    selected='[]'
    if [[ "$features" == tracing ]]; then selected='["tracing"]'; fi
    python3 - "$cons/Cargo.toml" "$name" "$src" "$selected" <<'PY'
import json,sys
from pathlib import Path
manifest,name,package,features = sys.argv[1:]
Path(manifest).write_text(f'''[package]
name = "{name}-crate-consumer"
version = "0.0.0"
edition = "2021"
rust-version = "1.85"
publish = false
[workspace]
[dependencies]
{name} = {{ path = {json.dumps(package)}, features = {features} }}
tokio = {{ version = "1", features = ["rt-multi-thread", "macros"] }}
''')
PY
    pl_write_adopter_consumer_main "$cons/src/main.rs" "$name" "ci-crate-consumer"
    "${cargo_command[@]}" generate-lockfile --manifest-path "$cons/Cargo.toml"
    "${cargo_command[@]}" check --locked --manifest-path "$cons/Cargo.toml"
    python3 -B scripts/check-doc-examples.py --root "$src" --crate "$crate" --features "$features" "${doc_args[@]}" --report "$report_dir/snippets-$toolchain-$features.json"
    cp "$cons/Cargo.lock" "$report_dir/operator-$toolchain-$features.lock"
    reports+=("$report_dir/snippets-$toolchain-$features.json")
  done
done
python3 - "$crate" "$report_dir" "${reports[@]}" <<'PY'
import hashlib,json,sys,tarfile
from pathlib import Path
archive,output = map(Path, sys.argv[1:3])
with tarfile.open(archive) as packed:
    vcs = json.load(packed.extractfile(next(m for m in packed.getmembers() if m.name.endswith('/.cargo_vcs_info.json'))))
report_paths = list(map(Path, sys.argv[3:]))
reports = [json.loads(p.read_text()) for p in report_paths]
package_sha = hashlib.sha256(archive.read_bytes()).hexdigest()
if any(r['package_sha256'] != package_sha or r['package_vcs_info'] != vcs for r in reports):
    raise SystemExit('ci-crate-consumer: stale or mismatched package reports')
if not reports or any(r['compiled_snippets'] == 0 or r['ignored_snippets'] for r in reports):
    raise SystemExit('ci-crate-consumer: missing or skipped actual snippet compilation')
summary = {'package_sha256': package_sha, 'source_vcs_info': vcs,
           'feature_matrix': reports, 'all_features_alias': 'tracing (the only approved optional feature)',
           'operator_lock_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [output / path.name.replace('snippets-', 'operator-').replace('.json', '.lock') for path in report_paths]}}
(output / 'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(f'ci-crate-consumer: {len(reports)} actual package feature/toolchain cells passed; source/package/version and license inventory retained in {output}')
PY
