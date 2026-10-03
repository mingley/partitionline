#!/usr/bin/env bash
set -euo pipefail
cd /workspace/work/client-share-assessment/preparation-msrv-lint-96211da1
export CARGO_HOME=/workspace/work/cargo RUSTUP_HOME=/workspace/work/rustup
export PATH=/workspace/work/cargo/bin:$PATH CARGO_TARGET_DIR=/workspace/work/client-share-target
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
report=/workspace/work/client-share-assessment/msrv-lint-correction-final-prep
mkdir -p "$report"
phase=start
finish() {
  result=$?
  python3 - "$report" "$phase" "$result" <<'PY'
from pathlib import Path
import json,sys
(Path(sys.argv[1])/'exit.json').write_text(json.dumps({'phase':sys.argv[2],'exit_code':int(sys.argv[3])},indent=2)+'\n')
PY
  exit "$result"
}
trap finish EXIT
checked() {
  local name="$1"
  shift
  phase="$name-before"
  taskset -c 0-2,4 python3 /workspace/work/client-share-assessment/verify-isolated-candidate.py "$report/$name-source-before.json" "$name-before" >"$report/$name-source-before.log" 2>&1
  phase="$name"
  set +e
  taskset -c 0-2,4 "$@" >"$report/$name.log" 2>&1
  local status=$?
  set -e
  python3 - "$report/$name-command.json" "$name" "$status" "$@" <<'PY'
from pathlib import Path
import json,sys
Path(sys.argv[1]).write_text(json.dumps({'phase':sys.argv[2],'exit_code':int(sys.argv[3]),'argv':sys.argv[4:]},indent=2)+'\n')
PY
  phase="$name-after"
  taskset -c 0-2,4 python3 /workspace/work/client-share-assessment/verify-isolated-candidate.py "$report/$name-source-after.json" "$name-after" >"$report/$name-source-after.log" 2>&1
  phase="$name"
  return "$status"
}
checked msrv-default-clippy cargo +1.85.0 clippy --offline --locked --lib --test share_semantics -- -D warnings
checked msrv-all-clippy cargo +1.85.0 clippy --offline --locked --lib --test share_semantics --all-features -- -D warnings
checked stable-default-clippy cargo +stable clippy --offline --locked --lib --test share_semantics -- -D warnings
checked stable-all-clippy cargo +stable clippy --offline --locked --lib --test share_semantics --all-features -- -D warnings
checked stable-sockets cargo +stable test --offline --locked --test share_semantics
checked stable-share-codecs cargo +stable test --offline --locked --lib protocol::share
checked msrv-sockets cargo +1.85.0 test --offline --locked --test share_semantics
checked msrv-share-codecs cargo +1.85.0 test --offline --locked --lib protocol::share
phase=complete
