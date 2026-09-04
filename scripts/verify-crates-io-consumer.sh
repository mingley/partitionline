#!/usr/bin/env bash
# Prove Installable the way an adopter experiences it: depend on partitionline
# and cargo-check the operator surface (produce/consume/group/share/admin +
# SASL/TLS config types). Does not publish.
#
# Modes:
#   MODE=registry (default) — require crates.io ${ver}, depend via registry
#   MODE=path               — pre-publish rehearsal against this workspace
#                             (catches API drift before the first cut)
#
# Registry mode is wired into day1-after-publish / owner-finish-installable.
# Path mode is wired into ci-publish-ready so day1 cannot fail on type drift.
#
# Usage:
#   bash scripts/verify-crates-io-consumer.sh
#   MODE=path bash scripts/verify-crates-io-consumer.sh
#   VER=0.1.0 bash scripts/verify-crates-io-consumer.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="${VER:-$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)}"
mode="${MODE:-registry}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

case "$mode" in
  registry|path) ;;
  *)
    echo "verify-crates-io-consumer: MODE must be registry or path (got '$mode')" >&2
    exit 1
    ;;
esac

if [[ "$mode" == "registry" ]]; then
  echo "verify-crates-io-consumer: expecting crates.io ${name} ${ver}"
  if ! bash scripts/check-installable.sh >/tmp/pl-verify-installable.log 2>&1; then
    cat /tmp/pl-verify-installable.log >&2 || true
    echo "verify-crates-io-consumer: FAIL — ${name} ${ver} not Installable yet" >&2
    exit 1
  fi
  dep_line="${name} = \"=${ver}\""
else
  echo "verify-crates-io-consumer: MODE=path pre-publish rehearsal against ${ROOT}"
  dep_line="${name} = { path = \"${ROOT}\" }"
fi

cons="$tmpdir/consumer"
mkdir -p "$cons/src"
cat >"$cons/Cargo.toml" <<EOF
[package]
name = "${name}-crates-io-consumer"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
${dep_line}
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
EOF

# Keep type names in sync with src/lib.rs re-exports / ci-crate-consumer.sh.
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
    let _ = ProduceRecord::to("crates-io-consumer").value(&b"x"[..]);
    let _ = Sasl::plain("ci", "ci");
    let _ = TlsConfig::default();
    let _ = std::any::type_name::<Producer>();
    let _ = std::any::type_name::<Consumer>();
    let _ = std::any::type_name::<ConsumerGroup>();
    let _ = std::any::type_name::<ShareGroup>();
    let _ = std::any::type_name::<Admin>();
    println!("verify-crates-io-consumer: ok");
}
EOF

echo "verify-crates-io-consumer: cargo check (${mode}) for ${name} ${ver}"
(cd "$cons" && cargo check --quiet)

if [[ "$mode" == "path" ]]; then
  echo "verify-crates-io-consumer: ok (path rehearsal — day1 registry consumer will compile)"
  exit 0
fi

if ! grep -q "^name = \"${name}\"$" "$cons/Cargo.lock"; then
  echo "verify-crates-io-consumer: FAIL — ${name} missing from consumer Cargo.lock" >&2
  exit 1
fi
if ! grep -A2 "^name = \"${name}\"$" "$cons/Cargo.lock" | grep -q "version = \"${ver}\""; then
  echo "verify-crates-io-consumer: FAIL — lock did not select ${name} ${ver}" >&2
  grep -A5 "^name = \"${name}\"$" "$cons/Cargo.lock" >&2 || true
  exit 1
fi
if grep -A6 "^name = \"${name}\"$" "$cons/Cargo.lock" | grep -q '^source = "registry+'; then
  echo "verify-crates-io-consumer: registry source confirmed"
else
  echo "verify-crates-io-consumer: FAIL — ${name} was not resolved from crates.io registry" >&2
  grep -A8 "^name = \"${name}\"$" "$cons/Cargo.lock" >&2 || true
  exit 1
fi

echo "verify-crates-io-consumer: ok (adopter can cargo-depend on crates.io ${name} ${ver})"
