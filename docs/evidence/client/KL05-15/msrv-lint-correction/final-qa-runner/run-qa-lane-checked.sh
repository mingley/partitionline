#!/usr/bin/env bash
set -euo pipefail
source_root="${1:?immutable source directory required}"
toolchain="${2:?toolchain required}"
features="${3:?default or all required}"
report_root="${4:?report directory required}"
source_receipt="${5:?full source receipt required}"
[[ "$features" == default || "$features" == all ]]
mkdir -p "$report_root"
cd "$source_root"
export CARGO_HOME=/workspace/work/cargo RUSTUP_HOME=/workspace/work/rustup
export PATH=/workspace/work/cargo/bin:$PATH CARGO_TARGET_DIR=/workspace/work/client-share-target
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
flags=()
if [[ "$features" == all ]]; then flags=(--all-features); fi
phase=identity
finish() {
  local result=$?
  python3 - "$report_root" "$phase" "$result" <<'PY'
from pathlib import Path
import json,sys
root=Path(sys.argv[1]); (root/'exit.json').write_text(json.dumps({'phase':sys.argv[2],'exit_code':int(sys.argv[3])},indent=2)+'\n')
PY
  exit "$result"
}
trap finish EXIT
run_checked() {
  local label="$1"
  shift
  phase="$label-before-source"
  taskset -c 0-2,4 python3 /workspace/work/client-share-assessment/verify-existing-source.py "$source_receipt" "$source_root" "$report_root/$label-source-before.json" "$toolchain-$features-$label-before" >"$report_root/$label-source-before.log" 2>&1
  phase="$label"
  set +e
  taskset -c 0-2,4 "$@" >"$report_root/$label.log" 2>&1
  local command_result=$?
  set -e
  python3 - "$report_root/$label-command.json" "$label" "$command_result" "$@" <<'PY'
from pathlib import Path
import json,sys
Path(sys.argv[1]).write_text(json.dumps({'phase':sys.argv[2],'exit_code':int(sys.argv[3]),'argv':sys.argv[4:],'cpuset':'0-2,4'},indent=2)+'\n')
PY
  phase="$label-after-source"
  taskset -c 0-2,4 python3 /workspace/work/client-share-assessment/verify-existing-source.py "$source_receipt" "$source_root" "$report_root/$label-source-after.json" "$toolchain-$features-$label-after" >"$report_root/$label-source-after.log" 2>&1
  phase="$label"
  return "$command_result"
}
run_checked rustc rustc "+$toolchain" -Vv
run_checked cargo cargo "+$toolchain" -V
run_checked feature-metadata cargo "+$toolchain" metadata --offline --locked --format-version=1 --filter-platform x86_64-unknown-linux-gnu "${flags[@]}"
run_checked feature-tree cargo "+$toolchain" tree --offline --locked -e features -p partitionline "${flags[@]}"
python3 - "$report_root" "$features" "$source_root" "$source_receipt" <<'PY'
from pathlib import Path
import hashlib,json,sys
root=Path(sys.argv[1]);profile=sys.argv[2];metadata=json.loads((root/'feature-metadata.log').read_text())
expected_manifest=str(Path(sys.argv[3]).resolve()/'Cargo.toml');source_sha=json.loads(Path(sys.argv[4]).read_text())['source_sha']
package=next(p for p in metadata['packages'] if p['name']=='partitionline' and p['manifest_path']==expected_manifest)
nodes={n['id']:n for n in metadata['resolve']['nodes']};features=nodes[package['id']]['features']
assert set(features)==({'default','zlib-rs','tracing'} if profile=='all' else {'default','zlib-rs'}),features
reachable=set();pending=[package['id']]
while pending:
    current=pending.pop()
    if current in reachable:continue
    reachable.add(current);pending.extend(d['pkg'] for d in nodes[current]['deps'])
packages={p['id']:p for p in metadata['packages']};graph={name:{'name':packages[name]['name'],'version':packages[name]['version'],'features':nodes[name]['features']} for name in sorted(reachable)}
(root/'feature-graph-identity.json').write_text(json.dumps({'schema_version':1,'profile':profile,'package_features':features,'root_package':package['id'],'source_sha':source_sha,'target_platform':'x86_64-unknown-linux-gnu','resolved_reachable_packages':graph,'metadata_sha256':hashlib.sha256((root/'feature-metadata.log').read_bytes()).hexdigest(),'feature_tree_sha256':hashlib.sha256((root/'feature-tree.log').read_bytes()).hexdigest(),'test_scope':'all-targets includes example harnesses; only verifiable_producer/verifiable_consumer standalone binaries are required for contract tests; ignored live/current Docker tests remain ignored and are documented separately'},indent=2)+'\n')
PY
run_checked format cargo "+$toolchain" fmt --all -- --check
run_checked strict-clippy cargo "+$toolchain" clippy --offline --locked --all-targets "${flags[@]}" -- -D warnings
run_checked required-standalone-examples cargo "+$toolchain" build --offline --locked --example verifiable_producer --example verifiable_consumer "${flags[@]}"
run_checked all-target-tests cargo "+$toolchain" test --offline --locked --all-targets "${flags[@]}"
run_checked doc-tests cargo "+$toolchain" test --offline --locked --doc "${flags[@]}"
phase=complete
