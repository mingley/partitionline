#!/usr/bin/env bash
# KL-07 Partial: compile produce / consume / txn examples as an *external*
# package consumer of the packed .crate (not an in-tree path dependency).
#
# Proves adopters can copy the guide examples into their own package and
# `cargo check` against a published-shaped tarball. Does not require a live
# broker (compile-only). Does not close full KL-07 (no two-user diagnosis
# exercise; metric cookbooks may land separately). Does not lift Suite HOLD.
#
# Usage:
#   bash scripts/ci-example-crate-consumers.sh
#   bash scripts/ci-example-crate-consumers.sh --self-test   # structure only
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SELF_TEST=0
if [[ "${1:-}" == "--self-test" ]]; then
  SELF_TEST=1
fi

EXAMPLES=(produce consume txn)
# Bars gate these paths literally (keep in sync with EXAMPLES):
#   examples/produce.rs
#   examples/consume.rs
#   examples/txn.rs

for ex in "${EXAMPLES[@]}"; do
  if [[ ! -f "examples/${ex}.rs" ]]; then
    echo "ci-example-crate-consumers: FAIL — missing examples/${ex}.rs" >&2
    exit 1
  fi
done

if [[ ! -f scripts/ci-crate-consumer.sh ]]; then
  echo "ci-example-crate-consumers: FAIL — sibling ci-crate-consumer.sh missing" >&2
  exit 1
fi

if [[ "$SELF_TEST" -eq 1 ]]; then
  echo "ci-example-crate-consumers: self-test OK (examples produce/consume/txn present)"
  exit 0
fi

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
crate="target/package/${name}-${ver}.crate"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

echo "ci-example-crate-consumers: packaging ${name} ${ver}"
cargo package --allow-dirty --no-verify --quiet
if [[ ! -f "$crate" ]]; then
  crate="$(ls -1 target/package/"${name}"-*.crate | head -1)"
fi
echo "ci-example-crate-consumers: extracting $crate"
tar -xzf "$crate" -C "$tmpdir"
src="$(echo "$tmpdir"/"${name}"-*)"

cons="$tmpdir/example-consumer"
mkdir -p "$cons/src/bin"
cat >"$cons/Cargo.toml" <<EOF
[package]
name = "${name}-example-crate-consumers"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
${name} = { path = "$src" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
EOF

# Bins-only package: src/bin/<example>.rs (no lib/main).
for ex in "${EXAMPLES[@]}"; do
  cp "examples/${ex}.rs" "$cons/src/bin/${ex}.rs"
done

echo "ci-example-crate-consumers: cargo check --bins (produce/consume/txn as external consumer)"
(cd "$cons" && cargo check --bins --quiet)
echo "ci-example-crate-consumers: ok (guide examples compile against packed crate)"
