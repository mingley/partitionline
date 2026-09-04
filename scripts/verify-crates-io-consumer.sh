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
# Shares the consumer main.rs with scripts/ci-crate-consumer.sh via
# scripts/lib/adopter-consumer-main.sh so the two proofs cannot drift.
#
# Usage:
#   bash scripts/verify-crates-io-consumer.sh
#   MODE=path bash scripts/verify-crates-io-consumer.sh
#   VER=0.1.0 bash scripts/verify-crates-io-consumer.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=lib/adopter-consumer-main.sh
source "${ROOT}/scripts/lib/adopter-consumer-main.sh"

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

pl_write_adopter_consumer_main "$cons/src/main.rs" "$name" "crates-io-consumer"

echo "verify-crates-io-consumer: cargo check (${mode}) for ${name} ${ver}"
# Sparse-index lag: registry mode may need a few cargo retries after API shows the crate.
cargo_attempts="${CARGO_CHECK_ATTEMPTS:-12}"
cargo_sleep="${CARGO_CHECK_SLEEP_SECS:-5}"
if [[ "$mode" != "registry" ]]; then
  cargo_attempts=1
fi
cargo_ok=0
for cargo_i in $(seq 1 "$cargo_attempts"); do
  if (cd "$cons" && cargo check --quiet); then
    cargo_ok=1
    break
  fi
  if [[ "$cargo_i" -lt "$cargo_attempts" ]]; then
    echo "verify-crates-io-consumer: cargo check not ready (${cargo_i}/${cargo_attempts}); retrying in ${cargo_sleep}s..."
    sleep "$cargo_sleep"
  fi
done
if [[ "$cargo_ok" != "1" ]]; then
  echo "verify-crates-io-consumer: FAIL — cargo check did not succeed for ${name} ${ver}" >&2
  exit 1
fi

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
