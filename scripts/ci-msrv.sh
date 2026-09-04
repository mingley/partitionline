#!/usr/bin/env bash
# Compile/test on the declared MSRV toolchain (Cargo.toml rust-version).
# Used by civilization-check and publish-ready so Installable is not only a
# string match in Cargo.toml.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

msrv="$(sed -n 's/^rust-version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
if [[ -z "$msrv" ]]; then
  echo "ci-msrv: rust-version missing from Cargo.toml" >&2
  exit 1
fi

if ! rustup run "$msrv" rustc -vV >/dev/null 2>&1; then
  echo "ci-msrv: installing toolchain $msrv"
  rustup toolchain install "$msrv" --profile minimal
fi

echo "ci-msrv: cargo +${msrv} check --all-targets"
rustup run "$msrv" cargo check --all-targets
echo "ci-msrv: cargo +${msrv} test --lib"
rustup run "$msrv" cargo test --lib --quiet
echo "ci-msrv: ok ($msrv)"
