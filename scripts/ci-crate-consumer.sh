#!/usr/bin/env bash
# Consume partitionline from a packed .crate as a downstream crate would
# (Installable packaging proof without crates.io). Does not publish.
#
# Links the operator surface (produce / consume / group / share / admin /
# SASL+TLS config types) so a crates.io tarball cannot silently drop a
# civilization-critical public module.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

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

# Keep type names in sync with src/lib.rs re-exports (operator surface).
cat >"$cons/src/main.rs" <<EOF
use ${name}::{
    Admin, AdminConfig, Consumer, ConsumerConfig, ConsumerGroup, ProduceRecord, Producer,
    ProducerConfig, Sasl, ShareGroup, TlsConfig,
};

#[tokio::main]
async fn main() {
    // Compile-only smoke: construct configs / records without connecting.
    let _ = ProducerConfig::bootstrap(["127.0.0.1:9092"]);
    let _ = ConsumerConfig::bootstrap(["127.0.0.1:9092"]);
    let _ = AdminConfig::bootstrap(["127.0.0.1:9092"]);
    let _ = ProduceRecord::to("ci-crate-consumer").value(&b"x"[..]);
    let _ = Sasl::plain("ci", "ci");
    let _ = TlsConfig::default();
    // Keep operator types referenced so the packed crate cannot drop them.
    let _ = std::any::type_name::<Producer>();
    let _ = std::any::type_name::<Consumer>();
    let _ = std::any::type_name::<ConsumerGroup>();
    let _ = std::any::type_name::<ShareGroup>();
    let _ = std::any::type_name::<Admin>();
    println!("ci-crate-consumer: ok");
}
EOF

echo "ci-crate-consumer: cargo check downstream (operator surface)"
(cd "$cons" && cargo check --quiet)
echo "ci-crate-consumer: ok (packed crate is dependable)"
