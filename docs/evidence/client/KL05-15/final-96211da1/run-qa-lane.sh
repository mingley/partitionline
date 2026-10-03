#!/usr/bin/env bash
set -euo pipefail
source_root="${1:?immutable source directory required}"
toolchain="${2:?toolchain required}"
features="${3:?default or all required}"
report_root="${4:?report directory required}"
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
  result=$?
  python3 - "$report_root" "$phase" "$result" <<'PY'
from pathlib import Path
import json,sys
root=Path(sys.argv[1]); (root/'exit.json').write_text(json.dumps({'phase':sys.argv[2],'exit_code':int(sys.argv[3])},indent=2)+'\n')
PY
  exit "$result"
}
trap finish EXIT
taskset -c 0-2,4 rustc "+$toolchain" -Vv >"$report_root/rustc.log" 2>&1
taskset -c 0-2,4 cargo "+$toolchain" -V >"$report_root/cargo.log" 2>&1
phase=format
taskset -c 0-2,4 cargo "+$toolchain" fmt --all -- --check >"$report_root/format.log" 2>&1
phase=strict-clippy
taskset -c 0-2,4 cargo "+$toolchain" clippy --offline --locked --all-targets "${flags[@]}" -- -D warnings >"$report_root/clippy.log" 2>&1
phase=required-standalone-examples
taskset -c 0-2,4 cargo "+$toolchain" build --offline --locked --example verifiable_producer --example verifiable_consumer "${flags[@]}" >"$report_root/examples.log" 2>&1
phase=all-target-tests
taskset -c 0-2,4 cargo "+$toolchain" test --offline --locked --all-targets "${flags[@]}" >"$report_root/tests.log" 2>&1
phase=doc-tests
taskset -c 0-2,4 cargo "+$toolchain" test --offline --locked --doc "${flags[@]}" >"$report_root/doctests.log" 2>&1
phase=complete
