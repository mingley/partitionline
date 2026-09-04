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

echo "== ci-branch-lite: MSRV (Installable) =="
# Declared rust-version must actually compile/test — not just a Cargo.toml string.
bash scripts/ci-msrv.sh

echo "== ci-branch-lite: deny (Independent) =="
# No C Kafka/OpenSSL/zstd defaults — supply-chain bans, not docs alone.
bash scripts/ci-deny.sh

echo "== ci-branch-lite: path adopter consumer (pre-crates.io) =="
# Proves day1 registry consumer will compile once 0.1.0 exists (API surface).
MODE=path bash scripts/verify-crates-io-consumer.sh

echo "== ci-branch-lite: packed crate consumer (Installable packaging) =="
# Proves the .crate tarball itself is dependable (catches missing public modules).
bash scripts/ci-crate-consumer.sh

echo "== ci-branch-lite: cargo publish --dry-run =="
# Proves the packed crate still uploads-shaped under tip Verifiable, not only cut-path.
# Does not contact crates.io with credentials (dry-run aborts before upload).
cargo publish --dry-run

echo "== ci-branch-lite: crates.io metadata shape =="
# Declared package metadata must stay publish-shaped under tip Verifiable.
bash scripts/check-crate-metadata.sh

echo "== ci-branch-lite: Trusted Publishing workflow shape =="
# OIDC release.yml shape must stay tip-gated; first cut still needs CARGO_REGISTRY_TOKEN.
bash scripts/check-trusted-publishing-ready.sh

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

echo "== ci-branch-lite: Actions hygiene (stale queue surface) =="
# Informational (always exit 0). Tip Verifiable surfaces zombie RC-release /
# stale tip queues that starve runners; cancel remains owner-only.
bash scripts/check-actions-hygiene.sh

echo "== ci-branch-lite: Installable preflight =="
# Tip Verifiable must keep READY_EXCEPT_TOKEN (or READY) visible before PRE_PUBLISH bars.
bash scripts/check-installable-preflight.sh

echo "== ci-branch-lite: merge/tag readiness =="
# Structural merge+tag readiness (version, CHANGELOG, release.yml, tip/main tree).
bash scripts/check-merge-ready.sh

echo "== ci-branch-lite: first-publish Actions alternate (DRY_RUN visibility) =="
# Prove first-publish.yml remains workflow_dispatch-visible on main (Actions
# alternate when token is Actions-secret-only). DRY_RUN=1 does not dispatch.
DRY_RUN=1 bash scripts/owner-dispatch-first-publish.sh

echo "== ci-branch-lite: civilization bars (PRE_PUBLISH) =="
# Prove all bars except Installable credentials before the token cut.
# FULL=0: avoid recursion when FULL=1 audit nests this script.
FULL=0 PRE_PUBLISH=1 bash scripts/audit-civilization-bars.sh

echo "== ci-branch-lite: docs =="
bash scripts/ci-docs.sh

echo "ci-branch-lite: ok (tip Verifiable proxy; full matrix via PR/main/dispatch)"
