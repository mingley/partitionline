#!/usr/bin/env bash
# Native hosted macOS qualification; Linux broker/perf lanes remain separate.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ "$(uname -s)" != Darwin || "$(uname -m)" != arm64 ]]; then
  echo 'ci-macos-arm64: requires native Darwin arm64' >&2
  exit 1
fi
if (( BASH_VERSINFO[0] < 5 )); then
  echo 'ci-macos-arm64: requires Bash 5+ (Homebrew bash); Apple Bash 3.2 rejects empty arrays under nounset' >&2
  exit 1
fi
toolchain="${PL_MACOS_TOOLCHAIN:-stable}"
[[ "$toolchain" == stable ]] || { echo 'latest stable Rust required' >&2; exit 1; }
[[ -z "${CARGO_BUILD_TARGET:-}" ]] || { echo 'unexpected cross target' >&2; exit 1; }
export RUSTUP_TOOLCHAIN="$toolchain"
export CARGO_TERM_COLOR=never
report="${PL_MACOS_REPORT_DIR:-$ROOT/target/macos-arm64/$toolchain}"
mkdir -p "$report"
run_log() {
  local label="$1"
  shift
  if "$@" 2>&1 | tee "$report/$label.log"; then
    return 0
  else
    local code="$?"
    python3 -B scripts/report-macos-arm64.py diagnose "$report/$label.log" "$label" "$code"
    return "$code"
  fi
}

publish_name() {
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    python3 - "$report/$1" "$GITHUB_OUTPUT" <<'PYNAME'
import json,re,sys
from pathlib import Path
v=json.loads(Path(sys.argv[1]).read_text())
if 'artifact_name' in v:
    label=v['artifact_name']
else:
    label=f"macos-arm64-{v['requested_toolchain']}-rust{v['rustc_release']}-os{v['macos_version']}-ssl{v['openssl_version'].split()[1]}-py{v['python_version']}-bash{v['bash_version'].split('(')[0]}-partial-{v['source_sha']}"
with open(sys.argv[2], 'a') as output:
    output.write('artifact_name='+re.sub(r'[^A-Za-z0-9_.-]', '_', label)+'\n')
PYNAME
  fi
}

run_phase() {
  case "$1" in
    capture)
      python3 -B scripts/report-macos-arm64.py capture "$report" "$toolchain"
      publish_name versions.json
      run_log parser-tests python3 -B -m unittest discover -s tests/platform -p 'test_*.py'
      ;;
    default)
      run_log build-default cargo build --locked --example verifiable_producer --example verifiable_consumer
      run_log default-tests cargo test --locked --all-targets
      ;;
    tracing)
      run_log build-tracing cargo build --locked --features tracing --example verifiable_producer --example verifiable_consumer
      run_log tracing-tests cargo test --locked --all-targets --features tracing
      ;;
    clippy)
      if [[ "$toolchain" == stable ]]; then
        run_log clippy cargo clippy --locked --all-targets --all-features -- -D warnings
      fi
      ;;
    docs) run_log docs bash scripts/ci-docs.sh ;;
    package)
      PL_PACKAGE_TOOLCHAINS="$toolchain" PL_PACKAGE_REPORT_DIR="$report/package" \
        run_log package bash scripts/ci-crate-consumer.sh
      ;;
    report)
      python3 -B scripts/report-macos-arm64.py finish "$report"
      publish_name report.json
      ;;
    *) echo 'unknown macOS qualification phase' >&2; return 1 ;;
  esac
}
if [[ "${1:-all}" == all ]]; then
  for phase in capture default tracing clippy docs package report; do run_phase "$phase"; done
else
  run_phase "$1"
fi
