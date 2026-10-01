#!/usr/bin/env bash
# Native Git Bash / Windows MSVC qualification; no cross-build support claim.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
case "$(uname -s)" in
  MINGW*|MSYS*) ;;
  *) echo 'ci-windows: requires native Windows Git Bash' >&2; exit 1 ;;
esac
(( BASH_VERSINFO[0] >= 5 )) || { echo 'ci-windows: requires Bash 5+' >&2; exit 1; }
toolchain="${PL_WINDOWS_TOOLCHAIN:-stable}"
case "$toolchain" in stable|1.85.0) ;; *) echo 'unqualified Rust toolchain' >&2; exit 1 ;; esac
[[ -z "${CARGO_BUILD_TARGET:-}" ]] || { echo 'unexpected cross target' >&2; exit 1; }
export RUSTUP_TOOLCHAIN="$toolchain" CARGO_TERM_COLOR=never
# setup-python's native Windows executable is named python.exe. Child Bash
# package scripts use python3; do not copy an exe away from its installed stdlib.
python3() { command python "$@"; }
export -f python3
report="${PL_WINDOWS_REPORT_DIR:-$ROOT/target/windows/$toolchain}"
mkdir -p "$report"
run_log() {
  local label="$1"
  shift
  if "$@" 2>&1 | tee "$report/$label.log"; then
    return 0
  else
    local code="$?"
    python3 -B scripts/report-windows.py diagnose "$report/$label.log" "$label" "$code"
    return "$code"
  fi
}
publish_name() {
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    python3 - "$report/$1" "$GITHUB_OUTPUT" <<'PYNAME'
import json,sys
from pathlib import Path
with open(sys.argv[2], 'a') as output:
    output.write('artifact_name='+json.loads(Path(sys.argv[1]).read_text())['artifact_name']+'\n')
PYNAME
  fi
}
run_phase() {
  case "$1" in
    capture)
      python3 -B scripts/report-windows.py capture "$report" "$toolchain"
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
      if [[ "$toolchain" == stable ]]; then run_log clippy cargo clippy --locked --all-targets --all-features -- -D warnings; fi
      ;;
    docs) run_log docs bash scripts/ci-docs.sh ;;
    package)
      # Custom environment variables reach native Python/Cargo without Bash's
      # argv path conversion; provide a drive-qualified forward-slash path.
      PL_PACKAGE_TOOLCHAINS="$toolchain" PL_PACKAGE_REPORT_DIR="$(cygpath -m "$report/package")" \
        run_log package bash scripts/ci-crate-consumer.sh
      ;;
    report)
      python3 -B scripts/report-windows.py finish "$report"
      publish_name report.json
      ;;
    *) echo 'unknown Windows qualification phase' >&2; return 1 ;;
  esac
}
if [[ "${1:-all}" == all ]]; then
  for phase in capture default tracing clippy docs package report; do run_phase "$phase"; done
else
  run_phase "$1"
fi
