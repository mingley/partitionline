#!/usr/bin/env bash
# Supply-chain gate: advisories, licenses, bans (no C Kafka / OpenSSL / zstd-sys), sources.
# Prefer a preinstalled cargo-deny (CI: EmbarkStudios/cargo-deny-action). Locally, install
# with rustc >= 1.88 — advisory DB uses CVSS 4.0 which older deny cannot parse.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! command -v cargo-deny >/dev/null 2>&1; then
  echo "installing cargo-deny (needs rustc >= 1.88)…" >&2
  if rustup toolchain list | grep -q '^1\.88'; then
    cargo +1.88 install cargo-deny --locked
  else
    rustup toolchain install 1.88 --profile minimal
    cargo +1.88 install cargo-deny --locked
  fi
fi

cargo deny check advisories licenses bans sources
