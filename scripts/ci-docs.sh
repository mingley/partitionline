#!/usr/bin/env bash
# docs.rs smoke: rustdoc must build with zero unresolved intra-doc links
# (Installable / Operable for crates.io + docs.rs).
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
  echo "ci-docs: unresolved intra-doc links (failing):" >&2
  grep 'unresolved link' "$log" | sort -u | head -40 >&2 || true
  echo "ci-docs: failed (${n} unresolved intra-doc link warning(s))" >&2
  exit 1
fi
echo "ci-docs: ok (rustdoc builds; zero unresolved intra-doc links)"
