#!/usr/bin/env bash
# Tip Verifiable gate (fmt / clippy / lib tests / docs).
# Mirrors what used to be the Actions `branch-lite` job. Tip (`dev/**`) pushes
# no longer auto-queue CI while org runners are starved — run this locally
# (also wired into civilization-check / owner-status). Full matrix: open a PR,
# push to main, or workflow_dispatch.
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

echo "ci-branch-lite: ok (tip Verifiable proxy; full matrix via PR/main/dispatch)"
