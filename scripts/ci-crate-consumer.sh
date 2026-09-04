#!/usr/bin/env bash
# Consume partitionline from a packed .crate as a downstream crate would
# (Installable packaging proof without crates.io). Does not publish.
#
# Links the operator surface (produce / consume / group / share / admin /
# SASL+TLS config types) so a crates.io tarball cannot silently drop a
# civilization-critical public module.
#
# Shares the consumer main.rs with scripts/verify-crates-io-consumer.sh via
# scripts/lib/adopter-consumer-main.sh so the two proofs cannot drift.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=lib/adopter-consumer-main.sh
source "${ROOT}/scripts/lib/adopter-consumer-main.sh"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
crate="target/package/${name}-${ver}.crate"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

echo "ci-crate-consumer: packaging ${name} ${ver}"
cargo package --allow-dirty --no-verify --quiet
if [[ ! -f "$crate" ]]; then
  crate="$(ls -1 target/package/"${name}"-*.crate | head -1)"
fi
echo "ci-crate-consumer: extracting $crate"
tar -xzf "$crate" -C "$tmpdir"
src="$(echo "$tmpdir"/"${name}"-*)"

cons="$tmpdir/consumer"
mkdir -p "$cons/src"
cat >"$cons/Cargo.toml" <<EOF
[package]
name = "${name}-crate-consumer"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
${name} = { path = "$src" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
EOF

pl_write_adopter_consumer_main "$cons/src/main.rs" "$name" "ci-crate-consumer"

echo "ci-crate-consumer: cargo check downstream (operator surface)"
(cd "$cons" && cargo check --quiet)
echo "ci-crate-consumer: ok (packed crate is dependable)"
