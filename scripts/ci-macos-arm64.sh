#!/usr/bin/env bash
# Native hosted macOS qualification; Linux broker/perf lanes remain separate.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ "$(uname -s)" != Darwin || "$(uname -m)" != arm64 ]]; then
  echo 'ci-macos-arm64: requires native Darwin arm64' >&2
  exit 1
fi
toolchain="${PL_MACOS_TOOLCHAIN:-stable}"
case "$toolchain" in stable|1.85.0) ;; *) echo 'unqualified Rust toolchain' >&2; exit 1 ;; esac
[[ -z "${CARGO_BUILD_TARGET:-}" ]] || { echo 'unexpected cross target' >&2; exit 1; }
export RUSTUP_TOOLCHAIN="$toolchain"
export CARGO_TERM_COLOR=never
report="${PL_MACOS_REPORT_DIR:-$ROOT/target/macos-arm64/$toolchain}"
mkdir -p "$report"
python3 -B scripts/report-macos-arm64.py capture "$report" "$toolchain"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  python3 - "$report/versions.json" "$GITHUB_OUTPUT" <<'PY'
import json,re,sys
from pathlib import Path
v=json.loads(Path(sys.argv[1]).read_text())
label=f"macos-arm64-{v['requested_toolchain']}-rust{v['rustc_release']}-os{v['macos_version']}-ssl{v['openssl_version'].split()[1]}-py{v['python_version']}-partial-{v['source_sha']}"
with open(sys.argv[2], 'a') as output:
    output.write('artifact_name='+re.sub(r'[^A-Za-z0-9_.-]', '_', label)+'\n')
PY
fi
python3 -B -m unittest discover -s tests/platform -p 'test_*.py' 2>&1 | tee "$report/parser-tests.log"
cargo build --locked --example verifiable_producer --example verifiable_consumer 2>&1 | tee "$report/build-default.log"
cargo test --locked --all-targets 2>&1 | tee "$report/default-tests.log"
cargo build --locked --features tracing --example verifiable_producer --example verifiable_consumer 2>&1 | tee "$report/build-tracing.log"
cargo test --locked --all-targets --features tracing 2>&1 | tee "$report/tracing-tests.log"
if [[ "$toolchain" == stable ]]; then
  cargo clippy --locked --all-targets --all-features -- -D warnings 2>&1 | tee "$report/clippy.log"
fi
bash scripts/ci-docs.sh 2>&1 | tee "$report/docs.log"
PL_PACKAGE_TOOLCHAINS="$toolchain" PL_PACKAGE_REPORT_DIR="$report/package" \
  bash scripts/ci-crate-consumer.sh 2>&1 | tee "$report/package.log"
python3 -B scripts/report-macos-arm64.py finish "$report"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  python3 - "$report/report.json" "$GITHUB_OUTPUT" <<'PY'
import json,sys
from pathlib import Path
report=json.loads(Path(sys.argv[1]).read_text())
with open(sys.argv[2], 'a') as output:
    output.write('artifact_name='+report['artifact_name']+'\n')
PY
fi
