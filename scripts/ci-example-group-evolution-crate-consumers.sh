#!/usr/bin/env bash
# KL-07 Partial: compile cooperative / kip848 / share examples as an *external*
# package consumer of the packed .crate (not an in-tree path dependency).
#
# Group-evolution recipes (KIP-429 cooperative-sticky, KIP-848 consumer
# protocol, KIP-932 share groups) adopters copy from the guide must type-check
# against a published-shaped tarball. Compile-only (no live broker). Complements
# classic group/ops and other external-consumer Partials when those land. Does
# not close full KL-07 (two-user diagnosis remains). Does not lift Suite HOLD.
#
# Usage:
#   bash scripts/ci-example-group-evolution-crate-consumers.sh
#   bash scripts/ci-example-group-evolution-crate-consumers.sh --self-test
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SELF_TEST=0
if [[ "${1:-}" == "--self-test" ]]; then
  SELF_TEST=1
fi

EXAMPLES=(cooperative kip848 share)
# Bars gate these paths literally (keep in sync with EXAMPLES):
#   examples/cooperative.rs
#   examples/kip848.rs
#   examples/share.rs

for ex in "${EXAMPLES[@]}"; do
  if [[ ! -f "examples/${ex}.rs" ]]; then
    echo "ci-example-group-evolution-crate-consumers: FAIL — missing examples/${ex}.rs" >&2
    exit 1
  fi
done

if [[ ! -f scripts/ci-crate-consumer.sh ]]; then
  echo "ci-example-group-evolution-crate-consumers: FAIL — sibling ci-crate-consumer.sh missing" >&2
  exit 1
fi

if [[ "$SELF_TEST" -eq 1 ]]; then
  echo "ci-example-group-evolution-crate-consumers: self-test OK (examples cooperative/kip848/share present)"
  exit 0
fi

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
crate="target/package/${name}-${ver}.crate"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

echo "ci-example-group-evolution-crate-consumers: packaging ${name} ${ver}"
cargo package --allow-dirty --no-verify --quiet
if [[ ! -f "$crate" ]]; then
  crate="$(ls -1 target/package/"${name}"-*.crate | head -1)"
fi
echo "ci-example-group-evolution-crate-consumers: extracting $crate"
tar -xzf "$crate" -C "$tmpdir"
src="$(echo "$tmpdir"/"${name}"-*)"

cons="$tmpdir/group-evolution-example-consumer"
mkdir -p "$cons/src/bin"
cat >"$cons/Cargo.toml" <<EOF
[package]
name = "${name}-group-evolution-example-crate-consumers"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
${name} = { path = "$src" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
EOF

for ex in "${EXAMPLES[@]}"; do
  cp "examples/${ex}.rs" "$cons/src/bin/${ex}.rs"
done

echo "ci-example-group-evolution-crate-consumers: cargo check --bins (cooperative/kip848/share as external consumer)"
(cd "$cons" && cargo check --bins --quiet)
echo "ci-example-group-evolution-crate-consumers: ok (cooperative/kip848/share compile against packed crate)"
