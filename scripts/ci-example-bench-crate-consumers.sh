#!/usr/bin/env bash
# KL-07 Partial: compile bench_produce / bench_fetch / bench_latency examples as
# an *external* package consumer of the packed .crate (not an in-tree path
# dependency).
#
# Locked throughput/latency recipes adopters copy from the integrity/benchmark
# guide section must type-check against a published-shaped tarball. Compile-only
# (no live broker; does not claim Suite HOLD / Lab A wins). Complements other
# external-consumer Partials when those land. Does not close full KL-07
# (two-user diagnosis remains). Does not lift Suite HOLD.
#
# Usage:
#   bash scripts/ci-example-bench-crate-consumers.sh
#   bash scripts/ci-example-bench-crate-consumers.sh --self-test
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SELF_TEST=0
if [[ "${1:-}" == "--self-test" ]]; then
  SELF_TEST=1
fi

EXAMPLES=(bench_produce bench_fetch bench_latency)
# Bars gate these paths literally (keep in sync with EXAMPLES):
#   examples/bench_produce.rs
#   examples/bench_fetch.rs
#   examples/bench_latency.rs

for ex in "${EXAMPLES[@]}"; do
  if [[ ! -f "examples/${ex}.rs" ]]; then
    echo "ci-example-bench-crate-consumers: FAIL — missing examples/${ex}.rs" >&2
    exit 1
  fi
done

if [[ ! -f scripts/ci-crate-consumer.sh ]]; then
  echo "ci-example-bench-crate-consumers: FAIL — sibling ci-crate-consumer.sh missing" >&2
  exit 1
fi

if [[ "$SELF_TEST" -eq 1 ]]; then
  echo "ci-example-bench-crate-consumers: self-test OK (examples bench_produce/bench_fetch/bench_latency present)"
  exit 0
fi

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
crate="target/package/${name}-${ver}.crate"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

echo "ci-example-bench-crate-consumers: packaging ${name} ${ver}"
cargo package --allow-dirty --no-verify --quiet
if [[ ! -f "$crate" ]]; then
  crate="$(ls -1 target/package/"${name}"-*.crate | head -1)"
fi
echo "ci-example-bench-crate-consumers: extracting $crate"
tar -xzf "$crate" -C "$tmpdir"
src="$(echo "$tmpdir"/"${name}"-*)"

cons="$tmpdir/bench-example-consumer"
mkdir -p "$cons/src/bin"
# bytes: bench_produce/bench_latency; fs: optional TLS_CA_PEM reads in produce/fetch
cat >"$cons/Cargo.toml" <<EOF
[package]
name = "${name}-bench-example-crate-consumers"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
${name} = { path = "$src" }
bytes = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time", "fs"] }
EOF

for ex in "${EXAMPLES[@]}"; do
  cp "examples/${ex}.rs" "$cons/src/bin/${ex}.rs"
done

echo "ci-example-bench-crate-consumers: cargo check --bins (bench_produce/fetch/latency as external consumer)"
(cd "$cons" && cargo check --bins --quiet)
echo "ci-example-bench-crate-consumers: ok (bench_produce/bench_fetch/bench_latency compile against packed crate)"
