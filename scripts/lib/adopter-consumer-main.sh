#!/usr/bin/env bash
# Shared adopter operator-surface consumer main.rs emitter.
# Sourced by ci-crate-consumer.sh and verify-crates-io-consumer.sh so the
# packed-crate proof and the crates.io (or path) proof cannot drift apart.
#
# Usage (from a consumer script):
#   # shellcheck source=scripts/lib/adopter-consumer-main.sh
#   source "$ROOT/scripts/lib/adopter-consumer-main.sh"
#   pl_write_adopter_consumer_main "$cons/src/main.rs" "$name" "ci-crate-consumer"

pl_write_adopter_consumer_main() {
  local out="$1"
  local crate_name="$2"
  local label="${3:-adopter-consumer}"
  cat >"$out" <<EOF
use ${crate_name}::{
    Admin, AdminConfig, Consumer, ConsumerConfig, ConsumerGroup, ProduceRecord, Producer,
    ProducerConfig, Sasl, ShareGroup, TlsConfig,
};

#[tokio::main]
async fn main() {
    // Compile-only smoke: construct configs / records without connecting.
    let _ = ProducerConfig::bootstrap(["127.0.0.1:9092"]);
    let _ = ConsumerConfig::bootstrap(["127.0.0.1:9092"]);
    let _ = AdminConfig::bootstrap(["127.0.0.1:9092"]);
    let _ = ProduceRecord::to("${label}").value(&b"x"[..]);
    let _ = Sasl::plain("ci", "ci");
    let _ = TlsConfig::default();
    // Keep operator types referenced so a published crate cannot drop them.
    let _ = std::any::type_name::<Producer>();
    let _ = std::any::type_name::<Consumer>();
    let _ = std::any::type_name::<ConsumerGroup>();
    let _ = std::any::type_name::<ShareGroup>();
    let _ = std::any::type_name::<Admin>();
    println!("${label}: ok");
}
EOF
}
