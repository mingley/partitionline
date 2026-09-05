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

echo "== ci-branch-lite: Trusted Publishing enable rehearsal (DRY_RUN) =="
# Post-Installable OIDC UI checklist + shape; DRY_RUN allows absent crate.
DRY_RUN=1 bash scripts/owner-enable-trusted-publishing.sh

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
# Absent crate + DRY_RUN exits PARTIAL/2 by design — capture so set -e cannot
# abort tip Verifiable (or greenwash) while Installable waits on the token.
day1_rc=0
DRY_RUN=1 bash scripts/day1-after-publish.sh || day1_rc=$?
if [[ "$day1_rc" -eq 2 ]]; then
  echo "ci-branch-lite: PARTIAL — day1 DRY_RUN not yet Installable (expected pre-token; rehearsal held)"
elif [[ "$day1_rc" -ne 0 ]]; then
  echo "ci-branch-lite: FAIL — day1 DRY_RUN rc=${day1_rc}" >&2
  exit "$day1_rc"
fi

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
# Already-Installable → PARTIAL/2 (refuse re-dispatch soft-OK); capture like day1.
dispatch_rc=0
DRY_RUN=1 bash scripts/owner-dispatch-first-publish.sh || dispatch_rc=$?
if [[ "$dispatch_rc" -eq 2 ]]; then
  echo "ci-branch-lite: PARTIAL — first-publish DRY_RUN already Installable (re-dispatch refused; handoff re-entry)"
elif [[ "$dispatch_rc" -ne 0 ]]; then
  echo "ci-branch-lite: FAIL — first-publish DRY_RUN rc=${dispatch_rc}" >&2
  exit "$dispatch_rc"
fi

echo "== ci-branch-lite: post-Installable handoff rehearsal (DRY_RUN) =="
# Same parks-on-main / day1 honesty as cut-path. HANDOFF_FROM_BARS=1 skips nested
# bars (this proxy already runs PRE_PUBLISH bars below). Already-Installable +
# parks-off-main → PARTIAL/2; pre-token parks/day1 pending also PARTIAL/2.
handoff_rc=0
HANDOFF_FROM_BARS=1 DRY_RUN=1 bash scripts/owner-post-installable-handoff.sh || handoff_rc=$?
if [[ "$handoff_rc" -eq 2 ]]; then
  echo "ci-branch-lite: PARTIAL — handoff DRY_RUN soft-failed (parks-on-main / day1 / TP; handoff re-entry)"
elif [[ "$handoff_rc" -ne 0 ]]; then
  echo "ci-branch-lite: FAIL — handoff DRY_RUN rc=${handoff_rc}" >&2
  exit "$handoff_rc"
fi

echo "== ci-branch-lite: tip Verifiable PARTIAL exit self-test =="
# Prove finalize exit codes (ok=0 / PARTIAL=2 / soft PARTIAL=0) before live broker.
bash scripts/ci-tip-verifiable-broker.sh --self-test

echo "== ci-branch-lite: tip live-broker Verifiable =="
# Tip pushes skip Actions. Without a live broker chain, tip "Verifiable" was
# fmt/clippy/lib-only. Soft-skips honestly when no broker/tooling; never greenwashes
# (`ok` only if broker+auth+integrity all pass; early SKIP exit 0; PARTIAL exit 2
# fails this set -e proxy unless TIP_VERIFIABLE_SOFT=1).
bash scripts/ci-tip-verifiable-broker.sh

echo "== ci-branch-lite: civilization bars (PRE_PUBLISH) =="
# Prove all bars except Installable credentials before the token cut.
# FULL=0: avoid recursion when FULL=1 audit nests this script.
FULL=0 PRE_PUBLISH=1 bash scripts/audit-civilization-bars.sh

echo "== ci-branch-lite: docs =="
bash scripts/ci-docs.sh

if [[ "${day1_rc:-0}" -eq 2 || "${dispatch_rc:-0}" -eq 2 || "${handoff_rc:-0}" -eq 2 ]]; then
  if bash scripts/check-installable.sh >/dev/null 2>&1; then
    echo "ci-branch-lite: ok with PARTIAL — tip Verifiable proxy held; Installable already met — post-cut re-entry (parks/day1/dispatch), not a token blocker"
  else
    echo "ci-branch-lite: ok with PARTIAL — tip Verifiable proxy held; Installable still blocked on CARGO_REGISTRY_TOKEN (pre-token rehearsal)"
  fi
  exit 0
fi
echo "ci-branch-lite: ok (tip Verifiable proxy; full matrix via PR/main/dispatch)"
