#!/usr/bin/env bash
# KL-07 Partial: compile roundtrip / intercept / consume_intercept examples as
# an *external* package consumer of the packed .crate (not an in-tree path
# dependency).
#
# Migrators start with `examples/roundtrip.rs` (migrate checklist #1). Interceptor
# examples are the diagnosis-friendly produce/fetch hooks adopters copy next.
# Compile-only (no live broker). Complements produce/consume/txn, ops, metrics,
# and auth external-consumer Partials when those land. Does not close full
# KL-07 (two-user diagnosis remains). Does not lift Suite HOLD.
#
# Usage:
#   bash scripts/ci-example-roundtrip-intercept-crate-consumers.sh
#   bash scripts/ci-example-roundtrip-intercept-crate-consumers.sh --self-test
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SELF_TEST=0
if [[ "${1:-}" == "--self-test" ]]; then
  SELF_TEST=1
fi

EXAMPLES=(roundtrip intercept consume_intercept)
# Bars gate these paths literally (keep in sync with EXAMPLES):
#   examples/roundtrip.rs
#   examples/intercept.rs
#   examples/consume_intercept.rs

for ex in "${EXAMPLES[@]}"; do
  if [[ ! -f "examples/${ex}.rs" ]]; then
    echo "ci-example-roundtrip-intercept-crate-consumers: FAIL — missing examples/${ex}.rs" >&2
    exit 1
  fi
done

if [[ ! -f scripts/ci-crate-consumer.sh ]]; then
  echo "ci-example-roundtrip-intercept-crate-consumers: FAIL — sibling ci-crate-consumer.sh missing" >&2
  exit 1
fi

if [[ "$SELF_TEST" -eq 1 ]]; then
  echo "ci-example-roundtrip-intercept-crate-consumers: self-test OK (examples roundtrip/intercept/consume_intercept present)"
  exit 0
fi

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
crate="target/package/${name}-${ver}.crate"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

echo "ci-example-roundtrip-intercept-crate-consumers: packaging ${name} ${ver}"
cargo package --allow-dirty --no-verify --quiet
if [[ ! -f "$crate" ]]; then
  crate="$(ls -1 target/package/"${name}"-*.crate | head -1)"
fi
echo "ci-example-roundtrip-intercept-crate-consumers: extracting $crate"
tar -xzf "$crate" -C "$tmpdir"
src="$(echo "$tmpdir"/"${name}"-*)"

cons="$tmpdir/roundtrip-intercept-example-consumer"
mkdir -p "$cons/src/bin"
cat >"$cons/Cargo.toml" <<EOF
[package]
name = "${name}-roundtrip-intercept-example-crate-consumers"
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

echo "ci-example-roundtrip-intercept-crate-consumers: cargo check --bins (roundtrip/intercept/consume_intercept as external consumer)"
(cd "$cons" && cargo check --bins --quiet)
echo "ci-example-roundtrip-intercept-crate-consumers: ok (roundtrip/intercept/consume_intercept compile against packed crate)"
