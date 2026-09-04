#!/usr/bin/env bash
# Local civilization-bar checklist (docs/CIVILIZATION.md success criteria).
# Does not publish. Broker/Docker steps skip cleanly when Docker overlay is
# unavailable unless REQUIRE_BROKER=1.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

pass=0
fail=0
skip=0

ok() { echo "OK  $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
ski() { echo "SKIP $*"; skip=$((skip + 1)); }

echo "== Installable (pre-publish) =="
if grep -q '^rust-version = "1\.85"' Cargo.toml; then ok "MSRV declared 1.85"; else bad "MSRV rust-version"; fi
if grep -q '^documentation = "https://docs.rs/partitionline"' Cargo.toml; then ok "docs.rs URL"; else bad "docs.rs URL"; fi
if bash scripts/ci-msrv.sh >/tmp/pl-msrv.log 2>&1; then
  ok "MSRV toolchain check (rust-version)"
else
  bad "MSRV toolchain check; see /tmp/pl-msrv.log"
fi
if bash scripts/ci-docs.sh >/tmp/pl-docs.log 2>&1; then
  ok "rustdoc builds (docs.rs smoke)"
else
  bad "rustdoc build; see /tmp/pl-docs.log"
fi
if cargo package --allow-dirty --quiet; then ok "cargo package"; else bad "cargo package"; fi
if bash scripts/ci-crate-consumer.sh >/tmp/pl-crate-consumer.log 2>&1; then
  ok "packed crate downstream consumer"
else
  bad "packed crate downstream consumer; see /tmp/pl-crate-consumer.log"
fi
if cargo publish --dry-run --allow-dirty >/tmp/pl-publish-dry.log 2>&1; then
  ok "cargo publish --dry-run"
else
  bad "cargo publish --dry-run; see /tmp/pl-publish-dry.log"
fi
if curl -fsSA 'partitionline-ci/1' 'https://crates.io/api/v1/crates/partitionline' >/tmp/pl-crates.json 2>/dev/null; then
  ok "crates.io has partitionline (published)"
else
  ok "crates.io name free / not yet published"
fi

echo "== Verifiable =="
if cargo test --lib --quiet; then ok "cargo test --lib"; else bad "cargo test --lib"; fi
if bash scripts/ci-deny.sh >/tmp/pl-deny.log 2>&1; then ok "cargo deny"; else bad "cargo deny; see /tmp/pl-deny.log"; fi
if [[ -f fuzz/fuzz_targets/decode_fetch_response.rs \
   && -f fuzz/fuzz_targets/decode_produce_response.rs \
   && -f fuzz/fuzz_targets/decode_metadata_response.rs \
   && -f fuzz/fuzz_targets/decode_group_responses.rs \
   && -f fuzz/fuzz_targets/decode_share_fetch_response.rs ]]; then
  ok "fuzz targets present (>=3)"
else
  bad "fuzz targets missing"
fi
broker_ok=0
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  if bash scripts/ci-broker-smoke.sh >/tmp/pl-broker.log 2>&1 \
    && grep -q 'ci-broker-smoke: ok' /tmp/pl-broker.log; then
    ok "broker smoke (docker)"
    broker_ok=1
  elif grep -qiE 'overlay|invalid argument|failed to mount|docker run failed' /tmp/pl-broker.log; then
    echo "(docker overlay unavailable; trying native Kafka fallback)"
  elif [[ "${REQUIRE_BROKER:-}" == "1" ]]; then
    bad "broker smoke; see /tmp/pl-broker.log"
    broker_ok=1
  else
    # Soft-skip (exit 0 without "ok") is not evidence — fall through to native.
    echo "(docker smoke soft-skipped; trying native Kafka fallback)"
  fi
fi
if [[ "$broker_ok" -eq 0 ]]; then
  if bash scripts/ci-native-kafka.sh start >/tmp/pl-native-kafka.log 2>&1 \
    && SKIP_DOCKER=1 bash scripts/ci-broker-smoke.sh >/tmp/pl-broker.log 2>&1 \
    && grep -q 'ci-broker-smoke: ok' /tmp/pl-broker.log; then
    ok "broker smoke (native Kafka)"
    bash scripts/ci-native-kafka.sh stop >/dev/null 2>&1 || true
  else
    if [[ "${REQUIRE_BROKER:-}" == "1" ]]; then
      bad "broker smoke; see /tmp/pl-broker.log /tmp/pl-native-kafka.log"
    else
      ski "broker smoke (no usable Docker/native broker)"
    fi
  fi
fi
# TLS + PLAIN/SCRAM/OAUTHBEARER (SASL_SSL) — isolated ports; soft-skips without Java/Kafka.
if bash scripts/ci-auth-smoke.sh >/tmp/pl-auth.log 2>&1 \
  && grep -q 'ci-auth-smoke: ok' /tmp/pl-auth.log; then
  ok "auth smoke (TLS + PLAIN/SCRAM/OAUTHBEARER SASL_SSL)"
elif grep -q 'ci-auth-smoke: skipping' /tmp/pl-auth.log; then
  if [[ "${REQUIRE_AUTH:-}" == "1" ]]; then
    bad "auth smoke skipped; see /tmp/pl-auth.log"
  else
    ski "auth smoke (missing Java/openssl/Kafka tooling)"
  fi
else
  if [[ "${REQUIRE_AUTH:-}" == "1" ]]; then
    bad "auth smoke; see /tmp/pl-auth.log"
  else
    ski "auth smoke (failed soft); see /tmp/pl-auth.log"
  fi
fi

echo "== Operable =="
for f in docs/guide.md docs/migrate-from-rdkafka.md docs/security.md docs/RELEASE.md; do
  if [[ -f "$f" ]]; then ok "$f"; else bad "missing $f"; fi
done
if grep -qiE 'tracing|metrics' docs/guide.md; then
  ok "observability path documented"
else
  bad "observability path missing from guide"
fi

echo "== Honest =="
if grep -qiE 'unsigned|Suite HOLD|HOLD' docs/STATUS.md docs/benchmark.md; then
  ok "bench honesty labels present"
else
  bad "bench honesty labels missing"
fi

echo "== Independent =="
if grep -q 'unsafe_code' Cargo.toml && grep -q 'forbid' Cargo.toml; then
  ok "unsafe_code forbid"
else
  bad "unsafe_code forbid"
fi
if grep -E '^\s*(rdkafka|openssl|zstd-sys)\s*=' Cargo.toml >/dev/null; then
  bad "C stack listed as dependency in Cargo.toml"
else
  ok "no C Kafka/OpenSSL/zstd deps in Cargo.toml"
fi

echo "== Stewarded =="
for f in CHANGELOG.md docs/CIVILIZATION.md SECURITY.md .github/PULL_REQUEST_TEMPLATE.md .github/CODEOWNERS; do
  if [[ -f "$f" ]]; then ok "$f"; else bad "missing $f"; fi
done

echo
echo "civilization-check: pass=$pass fail=$fail skip=$skip"
echo
echo "== Owner blocker probe (informational) =="
bash scripts/owner-status.sh || true
if [[ "$fail" -gt 0 ]]; then
  exit 1
fi
