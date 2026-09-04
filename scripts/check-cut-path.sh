#!/usr/bin/env bash
# One-shot cut-path readiness: preflight + tip-delta + post-cut parks stack.
# Does not publish. Expect READY_EXCEPT_TOKEN until CARGO_REGISTRY_TOKEN is set.
#
# Usage:
#   bash scripts/check-cut-path.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== check-cut-path: Installable preflight =="
bash scripts/check-installable-preflight.sh

echo
echo "== check-cut-path: registry token probe self-test =="
bash scripts/check-registry-token.sh --self-test

echo
echo "== check-cut-path: registry token probe =="
tok_rc=0
bash scripts/check-registry-token.sh || tok_rc=$?
if [[ "$tok_rc" -eq 1 ]]; then
  echo "check-cut-path: FAIL — token present but rejected by crates.io" >&2
  exit 1
fi
# tok_rc 0 (ok) or 2 (missing) are fine for rehearsal

echo
echo "== check-cut-path: tip-delta (docs/scripts-only vs main) =="
bash scripts/check-tip-delta.sh

echo
echo "== check-cut-path: MSRV (Installable) =="
bash scripts/ci-msrv.sh

echo
echo "== check-cut-path: deny (Independent) =="
bash scripts/ci-deny.sh

echo
echo "== check-cut-path: post-cut parks refresh DRY_RUN (chain-safe) =="
# Proves tip→Verifiable→SCRAM→lz4→checkout refresh stays idempotent before cut.
# Do not merge tip into each park in parallel — that forks the tip⊆… chain.
DRY_RUN=1 bash scripts/refresh-post-cut-parks.sh

echo
echo "== check-cut-path: cargo publish --dry-run =="
# Proves the packed crate still uploads-shaped before the token arrives.
# Does not contact crates.io with credentials (dry-run aborts before upload).
cargo publish --dry-run

echo
echo "== check-cut-path: crates.io metadata shape =="
bash scripts/check-crate-metadata.sh

echo
echo "== check-cut-path: path adopter consumer (pre-crates.io) =="
# Day1 registry consumer rehearsal against this workspace (API surface).
MODE=path bash scripts/verify-crates-io-consumer.sh

echo
echo "== check-cut-path: packed crate consumer (Installable packaging) =="
bash scripts/ci-crate-consumer.sh

echo
echo "== check-cut-path: post-cut parks stack =="
bash scripts/check-post-cut-parks-stack.sh

echo
echo "== check-cut-path: Trusted Publishing workflow shape =="
bash scripts/check-trusted-publishing-ready.sh

echo
echo "== check-cut-path: Trusted Publishing enable rehearsal (DRY_RUN) =="
# Finish chains owner-enable-trusted-publishing after Installable. Rehearse the
# OIDC UI checklist + workflow shape now (crate may be absent under DRY_RUN).
DRY_RUN=1 bash scripts/owner-enable-trusted-publishing.sh

echo
echo "== check-cut-path: civilization bars (PRE_PUBLISH) =="
# Prove five bars green (Installable credentials may BLOCKED) before the cut.
# FULL=0: keep this rehearsal leaf even if caller exported FULL=1.
FULL=0 PRE_PUBLISH=1 bash scripts/audit-civilization-bars.sh

echo
echo "== check-cut-path: merge/tag readiness =="
bash scripts/check-merge-ready.sh

echo
echo "== check-cut-path: first-publish Actions alternate (DRY_RUN visibility) =="
# GitHub only lists workflow_dispatch from the default branch. Prove
# first-publish.yml stays visible on main so the Actions-secret alternate
# path remains owner-dispatchable once CARGO_REGISTRY_TOKEN is an Actions secret.
# DRY_RUN=1 does not dispatch (agents often 403 on workflow_dispatch anyway).
DRY_RUN=1 bash scripts/owner-dispatch-first-publish.sh

echo
echo "== check-cut-path: day1 after-publish rehearsal (no crates.io wait) =="
# Finish chains day1 after the cut; rehearse README flip + consumer path now so
# day1 cannot fail on tip drift once crates.io 0.1.0 exists.
DRY_RUN=1 bash scripts/day1-after-publish.sh

echo
echo "== check-cut-path: Actions hygiene (stale queue surface) =="
# Informational (always exit 0). Surfaces zombie RC-release / stale tip queues
# that starve runners before the owner cut; cancel remains owner-only (403 to agents).
bash scripts/check-actions-hygiene.sh

echo
echo "== check-cut-path: finish DRY_RUN (tip-aware parks, hard-fail) =="
DRY_RUN=1 bash scripts/owner-finish-installable.sh

echo
echo "check-cut-path: OK — cut path rehearsed; blocked only on CARGO_REGISTRY_TOKEN if preflight said READY_EXCEPT_TOKEN"
