#!/usr/bin/env bash
# Compile and test with the supported stable toolchain.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if ! rustup run stable rustc -vV >/dev/null 2>&1; then
  rustup toolchain install stable --profile minimal
fi
rustup run stable rustc -vV
rustup run stable cargo check --locked --all-targets
rustup run stable cargo test --locked --lib --quiet
