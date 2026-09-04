#!/usr/bin/env bash
# Local mirror of the GitHub Actions `branch-lite` job (.github/workflows/ci.yml).
# Use when org runners are starved / tip stays queued — proves the same gate
# locally that Verifiable would run on a free ubuntu-latest.
#
# Usage:
#   bash scripts/ci-branch-lite.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== ci-branch-lite: fmt =="
cargo fmt --all -- --check

echo "== ci-branch-lite: clippy =="
cargo clippy --all-targets --all-features -- -D warnings

echo "== ci-branch-lite: lib tests =="
cargo test --lib

echo "== ci-branch-lite: docs =="
bash scripts/ci-docs.sh

echo "ci-branch-lite: ok (mirrors Actions branch-lite)"
