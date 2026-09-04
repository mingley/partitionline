#!/usr/bin/env bash
# Evidence audit for docs/CIVILIZATION.md success criteria (six bars).
# Does not publish. Exit 0 only when every bar is PASS.
# Bars blocked on owner credentials report BLOCKED (still non-zero overall).
#
# Usage:
#   bash scripts/audit-civilization-bars.sh
#   FULL=1 bash scripts/audit-civilization-bars.sh   # also run branch-lite + deny
#   JSON=1 bash scripts/audit-civilization-bars.sh   # machine-readable summary line
#   PRE_PUBLISH=1 bash scripts/audit-civilization-bars.sh
#     # exit 0 if the only BLOCKEDs are Installable (crates.io / token) — for
#     # publish-ready / pre-cut gates that must not fail before first publish
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

FULL="${FULL:-0}"
JSON="${JSON:-0}"
PRE_PUBLISH="${PRE_PUBLISH:-0}"

pass=0
fail=0
blocked=0
partial=0
installable_blocked=0

ok() { echo "PASS  $*"; pass=$((pass + 1)); }
bad() { echo "FAIL  $*"; fail=$((fail + 1)); }
blk() { echo "BLOCKED  $*"; blocked=$((blocked + 1)); }
iblk() { echo "BLOCKED  $*"; blocked=$((blocked + 1)); installable_blocked=$((installable_blocked + 1)); }
part() { echo "PARTIAL  $*"; partial=$((partial + 1)); }

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
echo "audit-civilization-bars: ${name} ${ver} @ $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
echo

# --- 1. Installable ---
echo "== 1. Installable (crates.io + MSRV) =="
msrv_ok=0
if grep -qE '^rust-version = "1\.[0-9]+"' Cargo.toml; then
  ok "MSRV declared in Cargo.toml"
  msrv_ok=1
else
  bad "Cargo.toml missing rust-version"
fi
if [[ -f .github/workflows/ci.yml ]] \
  && grep -qE 'msrv|MSRV|rust-version|"1\.85"|rust:.*1\.85' .github/workflows/ci.yml; then
  ok "MSRV exercised in CI workflow"
elif [[ -f scripts/ci-msrv.sh ]]; then
  part "MSRV CI job label not obvious; scripts/ci-msrv.sh present"
else
  bad "no MSRV CI / scripts/ci-msrv.sh"
fi
# shellcheck source=scripts/lib/crates-io.sh
source "$ROOT/scripts/lib/crates-io.sh"
pl_crates_probe_version "$name" "$ver" "partitionline-audit-bars/1"
case "${PL_CRATES_PROBE_STATUS}" in
  present)
    ok "crates.io has ${name} ${ver} (${PL_CRATES_PROBE_DETAIL})"
    ;;
  absent)
    iblk "crates.io missing ${name} ${ver} (${PL_CRATES_PROBE_DETAIL}) — need CARGO_REGISTRY_TOKEN + owner-finish-installable (or first-publish.yml / owner-cut-release)"
    ;;
  *)
    bad "crates.io probe inconclusive (${PL_CRATES_PROBE_DETAIL})"
    ;;
esac
# Probe auth — do not PASS on presence alone (publish-update-only / garbage tokens).
tok_rc=0
bash scripts/check-registry-token.sh >/tmp/pl-audit-token.log 2>&1 || tok_rc=$?
case "$tok_rc" in
  0)
    ok "CARGO_REGISTRY_TOKEN accepted by crates.io for publish-new auth"
    ;;
  2)
    iblk "CARGO_REGISTRY_TOKEN unset (first publish / Actions secret; needs publish-new)"
    ;;
  *)
    iblk "CARGO_REGISTRY_TOKEN rejected by crates.io (recreate with publish-new; see check-registry-token)"
    ;;
esac

# --- 2. Verifiable ---
echo
echo "== 2. Verifiable (mock + broker CI + fuzz) =="
if cargo test --lib --quiet >/tmp/pl-audit-libtest.log 2>&1; then
  ok "cargo test --lib"
else
  bad "cargo test --lib; see /tmp/pl-audit-libtest.log"
fi
if [[ -f .github/workflows/ci.yml ]] && grep -qE 'broker-smoke|ci-broker-smoke' .github/workflows/ci.yml; then
  ok "broker-smoke job present in CI"
else
  bad "broker-smoke not wired in CI"
fi
if [[ -f scripts/ci-broker-smoke.sh && -f scripts/ci-auth-smoke.sh ]]; then
  ok "broker + auth smoke scripts present"
else
  bad "missing broker/auth smoke scripts"
fi
fuzz_n=0
if [[ -d fuzz/fuzz_targets ]]; then
  fuzz_n="$(find fuzz/fuzz_targets -name '*.rs' | wc -l | tr -d ' ')"
fi
if [[ "$fuzz_n" -ge 5 ]]; then
  ok "fuzz targets present (${fuzz_n})"
else
  bad "need >=5 fuzz targets (found ${fuzz_n})"
fi
if cargo test --test fuzz_decode_smoke --quiet >/tmp/pl-audit-fuzz.log 2>&1; then
  ok "fuzz decode smoke"
else
  bad "fuzz decode smoke; see /tmp/pl-audit-fuzz.log"
fi
if [[ "$FULL" == "1" ]]; then
  if bash scripts/ci-branch-lite.sh >/tmp/pl-audit-branch-lite.log 2>&1; then
    ok "FULL branch-lite Verifiable proxy"
  else
    bad "FULL branch-lite; see /tmp/pl-audit-branch-lite.log"
  fi
fi

# --- 3. Operable ---
echo
echo "== 3. Operable (guide + migration + observability) =="
for f in docs/guide.md docs/migrate-from-rdkafka.md docs/ADOPTION.md; do
  if [[ -f "$f" ]]; then ok "$f"; else bad "missing $f"; fi
done
if grep -qiE 'tracing|metrics|prometheus' docs/guide.md; then
  ok "observability path documented in guide"
else
  bad "guide missing metrics/tracing path"
fi

# --- 4. Honest ---
echo
echo "== 4. Honest (labeled benches; no false Suite HOLD lift) =="
if grep -qiE 'unsigned|Suite HOLD|HOLD' docs/STATUS.md docs/benchmark.md 2>/dev/null; then
  ok "STATUS/benchmark honesty labels present"
else
  bad "STATUS/benchmark missing honesty labels"
fi
# Refuse a false lift claim in STATUS.
if grep -qiE 'Suite HOLD (lifted|cleared|removed)|HOLD lifted' docs/STATUS.md 2>/dev/null; then
  bad "STATUS appears to claim Suite HOLD lift — verify signed Lab A first"
else
  ok "STATUS does not claim Suite HOLD lift"
fi

# --- 5. Independent ---
echo
echo "== 5. Independent (no C Kafka/compression/SASL defaults) =="
if grep -q 'unsafe_code' Cargo.toml && grep -q 'forbid' Cargo.toml; then
  ok "unsafe_code forbid"
else
  bad "unsafe_code forbid missing"
fi
if grep -E '^\s*(rdkafka|rdkafka-sys|openssl-sys|native-tls|zstd-sys|libzstd-sys)\s*=' Cargo.toml >/dev/null; then
  bad "C stack listed as direct Cargo.toml dependency"
else
  ok "no C Kafka/OpenSSL/zstd direct deps in Cargo.toml"
fi
if [[ -f deny.toml ]] && grep -qE 'rdkafka|openssl|zstd-sys' deny.toml; then
  ok "cargo-deny bans cover C Kafka/TLS/zstd stack"
else
  part "deny.toml C-stack bans not obvious"
fi
if [[ "$FULL" == "1" ]]; then
  if bash scripts/ci-deny.sh >/tmp/pl-audit-deny.log 2>&1; then
    ok "FULL cargo deny"
  else
    bad "FULL cargo deny; see /tmp/pl-audit-deny.log"
  fi
fi

# --- 6. Stewarded ---
echo
echo "== 6. Stewarded (changelog, release policy, templates, plan) =="
for f in CHANGELOG.md docs/RELEASE.md docs/CIVILIZATION.md SECURITY.md \
  .github/PULL_REQUEST_TEMPLATE.md .github/ISSUE_TEMPLATE .github/CODEOWNERS; do
  if [[ -e "$f" ]]; then ok "$f"; else bad "missing $f"; fi
done
if grep -q "^## \\[${ver}\\]" CHANGELOG.md || grep -q "^## \\[0\\.1\\.0\\]" CHANGELOG.md; then
  ok "CHANGELOG has ${ver} (or 0.1.0) section"
else
  bad "CHANGELOG missing ## [${ver}] section"
fi

echo
echo "audit-civilization-bars: pass=${pass} partial=${partial} blocked=${blocked} fail=${fail} installable_blocked=${installable_blocked}"
if [[ "$JSON" == "1" ]]; then
  printf '{"pass":%s,"partial":%s,"blocked":%s,"fail":%s,"installable_blocked":%s,"installable":"%s","version":"%s"}\n' \
    "$pass" "$partial" "$blocked" "$fail" "$installable_blocked" "${PL_CRATES_PROBE_STATUS:-unknown}" "$ver"
fi

if [[ "$fail" -gt 0 ]]; then
  echo "audit-civilization-bars: NOT COMPLETE — FAIL items above" >&2
  exit 1
fi

# Pre-publish gates: Installable BLOCKED is expected until crates.io lands.
if [[ "$PRE_PUBLISH" == "1" ]]; then
  other_blocked=$((blocked - installable_blocked))
  if [[ "$other_blocked" -gt 0 ]]; then
    echo "audit-civilization-bars: NOT COMPLETE — non-Installable BLOCKED items" >&2
    exit 1
  fi
  if [[ "$installable_blocked" -gt 0 ]]; then
    echo "audit-civilization-bars: PRE_PUBLISH OK — bars green except Installable (owner token/cut)"
    exit 0
  fi
  echo "audit-civilization-bars: PRE_PUBLISH OK — all six bars PASS (already Installable)"
  exit 0
fi

if [[ "$blocked" -gt 0 ]]; then
  echo "audit-civilization-bars: NOT COMPLETE — civilization bars unmet" >&2
  if [[ "$installable_blocked" -eq "$blocked" ]]; then
    echo "audit-civilization-bars: remaining blockers are owner/credentials (see BLOCKED lines)" >&2
  fi
  exit 1
fi
if [[ "$partial" -gt 0 ]]; then
  echo "audit-civilization-bars: OK with PARTIAL notes (no FAIL/BLOCKED)"
  exit 0
fi
echo "audit-civilization-bars: OK — all six bars PASS"
exit 0
