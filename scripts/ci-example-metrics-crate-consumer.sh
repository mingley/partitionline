#!/usr/bin/env bash
# KL-07 Partial: compile examples/metrics.rs as an *external* package consumer
# of the packed .crate (not an in-tree path dependency).
#
# Proves adopters can copy the metrics / Prometheus-text example into their own
# package and `cargo check` against a published-shaped tarball. Compile-only
# (no live broker). Does not close full KL-07 (produce/consume/txn consumers and
# two-user diagnosis remain separate). Does not lift Suite HOLD.
#
# Usage:
#   bash scripts/ci-example-metrics-crate-consumer.sh
#   bash scripts/ci-example-metrics-crate-consumer.sh --self-test
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SELF_TEST=0
if [[ "${1:-}" == "--self-test" ]]; then
  SELF_TEST=1
fi

EXAMPLE="metrics"
# Bars gate this path literally:
#   examples/metrics.rs

if [[ ! -f "examples/${EXAMPLE}.rs" ]]; then
  echo "ci-example-metrics-crate-consumer: FAIL — missing examples/${EXAMPLE}.rs" >&2
  exit 1
fi

if [[ ! -f scripts/ci-crate-consumer.sh ]]; then
  echo "ci-example-metrics-crate-consumer: FAIL — sibling ci-crate-consumer.sh missing" >&2
  exit 1
fi

if [[ "$SELF_TEST" -eq 1 ]]; then
  echo "ci-example-metrics-crate-consumer: self-test OK (examples/metrics.rs present)"
  exit 0
fi

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
crate="target/package/${name}-${ver}.crate"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

echo "ci-example-metrics-crate-consumer: packaging ${name} ${ver}"
cargo package --allow-dirty --no-verify --quiet
if [[ ! -f "$crate" ]]; then
  crate="$(ls -1 target/package/"${name}"-*.crate | head -1)"
fi
echo "ci-example-metrics-crate-consumer: extracting $crate"
tar -xzf "$crate" -C "$tmpdir"
src="$(echo "$tmpdir"/"${name}"-*)"

cons="$tmpdir/metrics-example-consumer"
mkdir -p "$cons/src/bin"
cat >"$cons/Cargo.toml" <<EOF
[package]
name = "${name}-metrics-example-crate-consumer"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
${name} = { path = "$src" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
EOF

cp "examples/${EXAMPLE}.rs" "$cons/src/bin/${EXAMPLE}.rs"

echo "ci-example-metrics-crate-consumer: cargo check --bins (metrics as external consumer)"
(cd "$cons" && cargo check --bins --quiet)
echo "ci-example-metrics-crate-consumer: ok (metrics example compiles against packed crate)"
