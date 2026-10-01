#!/usr/bin/env bash
# Documentation gate: strict rustdoc build and separate doctest execution.
#
# Requirements (KL07-02):
#   1. RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --all-features
#   2. cargo test --locked --doc --all-features
#   3. Deny warnings without allow(rustdoc::...) attributes or phrase matching.
#   4. Self-test / fixture mode proves a private intra-doc link fails the gate,
#      creating and deleting any temporary fixture cleanly.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="${1:-}"
case "$MODE" in
  ""|--self-test|--fixture|--offline) ;;
  *)
    echo "Usage: bash scripts/ci-docs.sh [--self-test|--fixture|--offline]" >&2
    exit 1
    ;;
esac

report="${PL_DOCS_REPORT_DIR:-$ROOT/target/docs-gate}"
mkdir -p "$report"
{
  git rev-parse HEAD
  rustc -Vv
  cargo -V
  uname -sm
} >"$report/identity.log"

run_logged() {
  local phase="$1"
  shift
  local statuses result
  set +e
  "$@" 2>&1 | tee "$report/$phase.log"
  statuses=("${PIPESTATUS[@]}")
  result=${statuses[0]}
  if [[ "$result" -eq 0 && "${statuses[1]}" -ne 0 ]]; then
    result=${statuses[1]}
  fi
  set -e
  if [[ "$result" -ne 0 ]]; then
    python3 - "$report/$phase.log" "$phase" "$result" <<'PY'
from pathlib import Path
import sys
log = Path(sys.argv[1]).read_text(encoding='utf-8', errors='replace')
if len(log) > 14000:
    log = log[:4000] + '\n[full log retained; middle omitted]\n' + log[-10000:]
message = f'{sys.argv[2]} exited {sys.argv[3]}\n' + log
message = message.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
print('::error title=Documentation gate::' + message)
PY
  fi
  return "$result"
}

# Auto-detect if offline can be used when dependencies are cached.
CARGO_OFFLINE_FLAG=()
if [[ "$MODE" == "--offline" || "${OFFLINE:-0}" == "1" ]] || \
   [[ "${OFFLINE:-1}" != "0" && "$(cargo metadata --offline --format-version 1 >/dev/null 2>&1 && echo ok || true)" == "ok" ]]; then
  CARGO_OFFLINE_FLAG=(--offline)
fi

run_docs_build() {
  echo "ci-docs: RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --all-features ${CARGO_OFFLINE_FLAG[*]}"
  run_logged rustdoc env RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --all-features "${CARGO_OFFLINE_FLAG[@]}"
}

run_doctests() {
  echo "ci-docs: cargo test --locked --doc --all-features ${CARGO_OFFLINE_FLAG[*]}"
  run_logged doctests cargo test --locked --doc --all-features "${CARGO_OFFLINE_FLAG[@]}"
}

run_docs_gate() {
  run_docs_build
  run_doctests
  echo "ci-docs: ok (strict rustdoc warnings denied; doctests passed)"
}

run_self_test() {
  echo "ci-docs: self-test (verifying broken private intra-doc link fails the gate)"
  local fixture="src/_fixture_private_intra_doc_link.rs"
  local lib_bak="src/lib.rs.stbak.$$"
  local test_log
  test_log="$(mktemp)"

  cleanup() {
    rm -f "$fixture" "$test_log"
    if [[ -f "$lib_bak" ]]; then
      mv -f "$lib_bak" "src/lib.rs"
    fi
  }
  trap cleanup EXIT INT TERM

  cp "src/lib.rs" "$lib_bak"

  cat > "$fixture" << 'EOF'
// Temporary self-test fixture: private intra-doc link must fail strict rustdoc.
#[allow(dead_code)]
fn _private_target_item() {}

/// Public item linking to private item: [`_private_target_item`].
pub fn _public_fixture_item() {}
EOF

  printf '\n#[path = "_fixture_private_intra_doc_link.rs"]\npub mod _fixture_private_intra_doc_link;\n' >> "src/lib.rs"

  local doc_rc=0
  set +e
  RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --all-features "${CARGO_OFFLINE_FLAG[@]}" >"$test_log" 2>&1 || doc_rc=$?
  set -e

  if [[ "$doc_rc" -eq 0 ]]; then
    echo "ci-docs: self-test FAIL — cargo doc unexpectedly succeeded with private intra-doc link" >&2
    cleanup
    trap - EXIT INT TERM
    exit 1
  fi

  if ! grep -qiE 'links to private item|private_intra_doc_links' "$test_log"; then
    echo "ci-docs: self-test FAIL — cargo doc failed (rc=$doc_rc) but not for private intra-doc link" >&2
    cat "$test_log" >&2
    cleanup
    trap - EXIT INT TERM
    exit 1
  fi

  echo "ci-docs: self-test ok (private intra-doc link correctly rejected: rc=${doc_rc})"

  # Restore tree and remove temporary fixture
  cleanup
  trap - EXIT INT TERM

  # Verify positive pass on clean tree
  echo "ci-docs: self-test — verifying clean tree passes gate"
  run_docs_gate
  echo "ci-docs: self-test ok (broken link rejected, clean crate passed rustdoc and doctests)"
}

if [[ "$MODE" == "--self-test" || "$MODE" == "--fixture" || "${SELF_TEST:-0}" == "1" ]]; then
  run_self_test
else
  run_docs_gate
fi
