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

echo "== crate consumer =="
bash scripts/ci-crate-consumer.sh

echo "== publish dry-run =="
cargo publish --dry-run

echo "== day-1 README flip preflight =="
DRY_RUN=1 bash scripts/post-publish-readme.sh >/tmp/pl-readme-flip-dry.log
tail -2 /tmp/pl-readme-flip-dry.log

echo "== civilization check =="
REQUIRE_BROKER="${REQUIRE_BROKER:-0}" bash scripts/ci-civilization-check.sh

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
echo
echo "ci-publish-ready: ok for partitionline ${ver}"
echo
bash scripts/owner-status.sh || true
echo
echo "Next (owner):"
echo "  1. Ensure CARGO_REGISTRY_TOKEN is set (env + GitHub Actions secret)"
echo "  2. Merge to main"
echo "  3. git tag v${ver} && git push origin v${ver}"
echo "  4. Confirm https://crates.io/crates/partitionline"
echo "  5. README: partitionline = \"${ver%.*}\"  # e.g. 0.1"
