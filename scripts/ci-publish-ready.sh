#!/usr/bin/env bash
# Pre-publish readiness gate (WP-0.5). Does not publish.
# Run before tagging v0.1.0 once CARGO_REGISTRY_TOKEN is available.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== fmt =="
cargo fmt --all -- --check

echo "== clippy =="
cargo clippy --all-targets --all-features -- -D warnings

echo "== test =="
cargo test --all-targets

echo "== deny =="
bash scripts/ci-deny.sh

echo "== msrv =="
bash scripts/ci-msrv.sh

echo "== docs =="
bash scripts/ci-docs.sh

echo "== package =="
cargo package

echo "== crate metadata (crates.io shape) =="
bash scripts/check-crate-metadata.sh

echo "== crate consumer =="
bash scripts/ci-crate-consumer.sh

echo "== adopter crates.io consumer rehearsal (path) =="
MODE=path bash scripts/verify-crates-io-consumer.sh

echo "== publish dry-run =="
cargo publish --dry-run

echo "== day-1 README flip preflight =="
DRY_RUN=1 bash scripts/post-publish-readme.sh >/tmp/pl-readme-flip-dry.log
tail -2 /tmp/pl-readme-flip-dry.log

echo "== day-1 after-publish rehearsal (no crates.io wait) =="
DRY_RUN=1 bash scripts/day1-after-publish.sh

echo "== adopter pin =="
bash scripts/check-adopter-pin.sh

echo "== workflow YAML =="
bash scripts/check-workflows.sh

echo "== tip-delta classifier (cut/sync trust guard) =="
bash scripts/check-tip-delta.sh

echo "== post-cut parks stack rehearsal =="
bash scripts/check-post-cut-parks-stack.sh

echo "== merge/tag readiness =="
bash scripts/check-merge-ready.sh

echo "== civilization bars (pre-publish) =="
PRE_PUBLISH=1 bash scripts/audit-civilization-bars.sh

echo "== civilization check =="
REQUIRE_BROKER="${REQUIRE_BROKER:-0}" bash scripts/ci-civilization-check.sh

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
echo
echo "ci-publish-ready: ok for partitionline ${ver}"
echo
bash scripts/owner-status.sh || true
echo
echo "Next (owner):"
echo "  1. Ensure CARGO_REGISTRY_TOKEN is set (Cloud Agent env + Actions secret)"
echo "  2. Preferred (FF-merge tip → main + local publish; bypasses starved Actions):"
echo "       bash scripts/owner-finish-installable.sh"
echo "  3. Or: merge civilization → main, then bash scripts/owner-cut-release.sh"
echo "  4. Confirm https://crates.io/crates/partitionline/${ver}"
echo "  5. README crates.io line (day1) + Trusted Publishing for release.yml"
