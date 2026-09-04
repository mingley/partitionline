#!/usr/bin/env bash
# docs.rs smoke: ensure rustdoc builds (Installable/Operable).
# Unresolved intra-doc links are reported but do not fail the gate yet;
# closing them is tracked separately from the first crates.io cut.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "ci-docs: cargo doc --no-deps"
log="$(mktemp)"
if ! cargo doc --no-deps >"$log" 2>&1; then
  cat "$log" >&2
  echo "ci-docs: cargo doc failed" >&2
  exit 1
fi
n="$(grep -c 'unresolved link' "$log" || true)"
echo "ci-docs: unresolved_intra_doc_links=${n}"
if [[ "$n" -gt 0 ]]; then
  echo "ci-docs: (info) sample unresolved links:" >&2
  grep 'unresolved link' "$log" | sort -u | head -15 >&2 || true
fi
echo "ci-docs: ok (rustdoc builds)"
