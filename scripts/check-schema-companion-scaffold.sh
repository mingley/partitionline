#!/usr/bin/env bash
# Prove partitionline-schema scaffold stays buildable, pure-Rust defaults, and
# excluded from the core crates.io package. Does not publish the companion.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "check-schema-companion-scaffold: cargo test (wire framing)"
# Capture lib unit-test summary (not doc-tests). Quiet mode hides the unit line.
out="$(cargo test --manifest-path partitionline-schema/Cargo.toml --lib -- --nocapture 2>&1)"
echo "$out" | tail -20
if ! echo "$out" | grep -Eq 'test result: ok\. [1-9][0-9]* passed'; then
  echo "check-schema-companion-scaffold: FAIL — expected passing unit tests" >&2
  exit 1
fi

echo "check-schema-companion-scaffold: forbid unsafe_code + missing_docs in lib"
grep -qF '#![forbid(unsafe_code)]' partitionline-schema/src/lib.rs
grep -qF '#![deny(missing_docs)]' partitionline-schema/src/lib.rs

echo "check-schema-companion-scaffold: publish = false + workspace exclude"
grep -qE '^publish = false' partitionline-schema/Cargo.toml
grep -qF 'partitionline-schema' Cargo.toml
grep -qE 'exclude = \[.*partitionline-schema' Cargo.toml \
  || grep -A2 '^\[workspace\]' Cargo.toml | grep -qF 'partitionline-schema'

echo "check-schema-companion-scaffold: core package must not ship companion sources"
if cargo package --list --allow-dirty 2>/dev/null | grep -q 'partitionline-schema/'; then
  echo "check-schema-companion-scaffold: FAIL — core package lists partitionline-schema/" >&2
  cargo package --list --allow-dirty | grep 'partitionline-schema/' >&2 || true
  exit 1
fi

echo "check-schema-companion-scaffold: no C/OpenSSL defaults in companion deps"
# Only scan dependency table keys — comments may mention OpenSSL as a non-goal.
if awk '
  BEGIN { dep=0 }
  /^\[.*dependencies/ { dep=1; next }
  /^\[/ { dep=0 }
  dep && $0 ~ /^[[:space:]]*#/ { next }
  dep && tolower($0) ~ /(openssl|librdkafka|zstd-sys|bindgen|^cc[[:space:]]*=)/ { found=1 }
  END { exit found ? 0 : 1 }
' partitionline-schema/Cargo.toml; then
  echo "check-schema-companion-scaffold: FAIL — C/native deps in companion defaults" >&2
  exit 1
fi

echo "check-schema-companion-scaffold: OK"
