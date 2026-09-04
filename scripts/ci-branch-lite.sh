#!/usr/bin/env bash
# Tip Verifiable gate (fmt / clippy / lib tests / fuzz decode smoke / docs).
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

# Integration adversarial decode smoke (group/share/txn + hot paths).
# Actions `test` uses --all-targets; tip previously only ran --lib, so this
# WP-2 surface never executed under the local Verifiable proxy.
echo "== ci-branch-lite: fuzz decode smoke =="
cargo test --test fuzz_decode_smoke

echo "== ci-branch-lite: adopter pin =="
bash scripts/check-adopter-pin.sh

echo "== ci-branch-lite: path adopter consumer (pre-crates.io) =="
# Proves day1 registry consumer will compile once 0.1.0 exists (API surface).
MODE=path bash scripts/verify-crates-io-consumer.sh

echo "== ci-branch-lite: workflow YAML =="
bash scripts/check-workflows.sh

echo "== ci-branch-lite: tip-delta classifier (cut/sync trust guard) =="
bash scripts/check-tip-delta.sh

echo "== ci-branch-lite: crates.io token probe self-test =="
bash scripts/check-registry-token.sh --self-test

echo "== ci-branch-lite: post-cut parks refresh DRY_RUN (chain-safe) =="
# tip→Verifiable→SCRAM→lz4→checkout; never merge tip into each park in parallel.
DRY_RUN=1 bash scripts/refresh-post-cut-parks.sh

echo "== ci-branch-lite: post-cut parks stack rehearsal =="
bash scripts/check-post-cut-parks-stack.sh

echo "== ci-branch-lite: day1 after-publish rehearsal (no crates.io wait) =="
DRY_RUN=1 bash scripts/day1-after-publish.sh

echo "== ci-branch-lite: docs =="
bash scripts/ci-docs.sh

echo "ci-branch-lite: ok (tip Verifiable proxy; full matrix via PR/main/dispatch)"
