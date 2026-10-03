#!/usr/bin/env bash
set -euo pipefail
source_root="${1:?immutable source directory required}"
toolchain="${2:?toolchain required}"
features="${3:?default or all required}"
report_root="${4:?report directory required}"
source_receipt="${5:-unused}"
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
  taskset -c 0,1 python3 /workspace/work/client-share-assessment/verify-heartbeat-candidate.py "$report_root/$label-source-before.json" "$toolchain-$features-$label-before" >"$report_root/$label-source-before.log" 2>&1
  phase="$label"
  set +e
  taskset -c 0,1 "$@" >"$report_root/$label.log" 2>&1
  local command_result=$?
  set -e
  python3 - "$report_root/$label-command.json" "$label" "$command_result" "$@" <<'PY'
from pathlib import Path
import json,sys
Path(sys.argv[1]).write_text(json.dumps({'phase':sys.argv[2],'exit_code':int(sys.argv[3]),'argv':sys.argv[4:],'cpuset':'0,1'},indent=2)+'\n')
PY
  phase="$label-after-source"
  taskset -c 0,1 python3 /workspace/work/client-share-assessment/verify-heartbeat-candidate.py "$report_root/$label-source-after.json" "$toolchain-$features-$label-after" >"$report_root/$label-source-after.log" 2>&1
  phase="$label"
  return "$command_result"
}
run_checked format cargo "+$toolchain" fmt --all -- --check
run_checked strict-owned-test cargo "+$toolchain" clippy --offline --locked --test full_surface "${flags[@]}" -- -D warnings
run_checked heartbeat-scheduling cargo "+$toolchain" test --offline --locked --test full_surface "${flags[@]}" kip848_broker_heartbeat_interval_scheduling -- --exact --nocapture
phase=complete
