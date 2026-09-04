#!/usr/bin/env bash
# Consume partitionline from a packed .crate as a downstream crate would
# (Installable packaging proof without crates.io). Does not publish.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
crate="target/package/partitionline-${ver}.crate"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

echo "ci-crate-consumer: packaging"
cargo package --allow-dirty --no-verify --quiet
if [[ ! -f "$crate" ]]; then
  # cargo package may write under target/package with different allow-dirty naming
  crate="$(ls -1 target/package/partitionline-*.crate | head -1)"
fi
echo "ci-crate-consumer: extracting $crate"
tar -xzf "$crate" -C "$tmpdir"
src="$(echo "$tmpdir"/partitionline-*)"

cons="$tmpdir/consumer"
mkdir -p "$cons/src"
cat >"$cons/Cargo.toml" <<EOF
[package]
name = "partitionline-crate-consumer"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
partitionline = { path = "$src" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
EOF

cat >"$cons/src/main.rs" <<'EOF'
use partitionline::{ProduceRecord, Producer, ProducerConfig};

#[tokio::main]
async fn main() {
    // Compile-only smoke: construct config without connecting.
    let _ = ProducerConfig::bootstrap(["127.0.0.1:9092"]);
    let _ = ProduceRecord::to("ci-crate-consumer").value(&b"x"[..]);
    // Keep Producer type referenced so the public API links.
    let _ = std::any::type_name::<Producer>();
    println!("ci-crate-consumer: ok");
}
EOF

echo "ci-crate-consumer: cargo check downstream"
(cd "$cons" && cargo check --quiet)
echo "ci-crate-consumer: ok (packed crate is dependable)"
