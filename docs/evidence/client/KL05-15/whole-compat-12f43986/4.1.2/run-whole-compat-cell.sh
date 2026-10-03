#!/usr/bin/env bash
set -euo pipefail
version="${1:?version required}"
[[ "$version" == 4.1.2 || "$version" == 4.2.1 || "$version" == 4.3.1 ]]
work=/workspace/work/client-share-assessment
source_root=/workspace/work/client-share-compat-12f43986
receipt="$work/final-12f43986/source-verification.json"
report="$work/whole-compat-12f43986/$version"
[[ ! -e "$report" ]]
mkdir -p "$report"
export CARGO_HOME=/workspace/work/cargo RUSTUP_HOME=/workspace/work/rustup
export PATH=/workspace/work/cargo/bin:$PATH CARGO_TARGET_DIR=/workspace/work/client-share-target
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_NET_OFFLINE=true RUSTUP_TOOLCHAIN=stable
export DOCKER_HOST=unix:///var/run/docker.sock
unset DOCKER_CONTEXT DOCKER_TLS DOCKER_TLS_VERIFY DOCKER_CERT_PATH GITHUB_OUTPUT KAFKA_IMAGE
export PL_COMPAT_CPUSET=0,1 PL_COMPAT_PORT=29183 PL_COMPAT_REPORT_DIR="$report"
export PL_COMPAT_RUN_VERIFIABLE=1 PYTHONDONTWRITEBYTECODE=1
[[ "$(type -t cargo)" == file ]]
[[ "$(command -v cargo)" == /workspace/work/cargo/bin/cargo ]]
[[ "$(readlink -f /workspace/work/cargo/bin/cargo)" == /workspace/work/cargo/bin/rustup ]]
cp "$work/run-whole-compat-cell.sh" "$report/"
cp "$work/verify-compat-worktree.py" "$report/"
taskset -c 0,1 python3 "$work/verify-compat-worktree.py" "$receipt" "$source_root" "$report/full-source-before.json" "whole-$version-before" >"$report/full-source-before.log" 2>&1
taskset -c 0,1 python3 - "$source_root" "$report" "$version" <<'PY'
from pathlib import Path
import datetime, hashlib, json, shutil, subprocess, sys
source, report, version = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
cell = next(row for row in json.loads((source / 'tests/conformance/current-broker-cells.json').read_text())['cells'] if row['version'] == version)
image = json.loads(subprocess.check_output(['docker', 'image', 'inspect', cell['reference'], '--format', '{{json .}}'], text=True))
assert 'apache/kafka@' + cell['digest'] in image['RepoDigests']
assert image['Os'] == 'linux' and image['Architecture'] == 'amd64'
free = shutil.disk_usage('/workspace').free
assert free >= 350 * 1024 * 1024 + 400 * 1024 * 1024, free
scripts = ['scripts/ci-broker-compatibility.sh', 'scripts/report-broker-compatibility.py', 'scripts/ci-verifiable-scenario.sh', 'tests/conformance/current-broker-cells.json']
identity = {
    'schema_version': 1, 'source_sha': '12f4398662044947ae653f07923290127457f2ef',
    'version': version, 'original_script_argv': ['bash', 'scripts/ci-broker-compatibility.sh', version],
    'script_sha256': {name: hashlib.sha256((source / name).read_bytes()).hexdigest() for name in scripts},
    'cpuset': '0,1', 'real_cargo_proxy': '/workspace/work/cargo/bin/cargo -> rustup',
    'real_cargo_toolchain': 'stable', 'cargo_net_offline': True,
    'free_bytes_before_launch': free, 'live_floor_bytes': 350 * 1024 * 1024,
    'cached_genuine_image': {'reference': cell['reference'], 'image_id': image['Id'], 'repo_digests': image['RepoDigests'], 'existing_logical_bytes': image['Size'], 'new_image_pull_bytes': 0},
    'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'scope': 'Unchanged original complete runner with real Cargo. Verifiable CLI scenario is additionally enabled on its required Kafka 4.1.2 cell. No native fallback or Cargo shim.',
}
(report / 'launch.json').write_text(json.dumps(identity, indent=2) + '\n')
print('PASS: cached genuine', version, 'launch guard, free', free)
PY
cd "$source_root"
set +e
taskset -c 0,1 python3 "$work/guard-compat-command.py" "$report" "$version" >"$report/external-runtime-guard.log" 2>&1
result=$?
set -e
taskset -c 0,1 python3 "$work/verify-compat-worktree.py" "$receipt" "$source_root" "$report/full-source-after.json" "whole-$version-after" >"$report/full-source-after.log" 2>&1
exit "$result"
