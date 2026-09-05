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
    # After crates.io has this version, Installable is proven; token is for future cuts only.
    if [[ "${PL_CRATES_PROBE_STATUS}" == "present" ]]; then
      ok "CARGO_REGISTRY_TOKEN unset (Installable already proven on crates.io; token only for future cuts)"
    else
      iblk "CARGO_REGISTRY_TOKEN unset (first publish / Actions secret; needs publish-new)"
    fi
    ;;
  *)
    if [[ "${PL_CRATES_PROBE_STATUS}" == "present" ]]; then
      part "CARGO_REGISTRY_TOKEN rejected (Installable already proven; recreate before next cut)"
    else
      iblk "CARGO_REGISTRY_TOKEN rejected by crates.io (recreate with publish-new; see check-registry-token)"
    fi
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

# KL-01 recovery slice: actual broker identity + portable timeout (no silent matrix drift).
if [[ -f scripts/lib/pl-timeout.sh && -f scripts/lib/broker-identity.sh ]] \
  && grep -q 'pl_timeout' scripts/ci-broker-smoke.sh \
  && grep -q 'pl_timeout' scripts/ci-auth-smoke.sh \
  && grep -q 'actual=\${PL_BROKER_ACTUAL}' scripts/ci-broker-smoke.sh \
  && grep -q 'pl_broker_identity_set_docker' scripts/ci-broker-smoke.sh \
  && grep -q 'pl_broker_identity_set_native' scripts/ci-native-kafka.sh \
  && bash scripts/lib/broker-identity.sh --self-test >/tmp/pl-broker-identity-self-test.log 2>&1; then
  ok "KL-01 broker identity + portable timeout (actual= stamp; pl_timeout; native/docker identity self-test)"
else
  bad "KL-01 broker identity/timeout missing or self-test failed; see /tmp/pl-broker-identity-self-test.log"
fi

# KL-01 recovery slice: Produce/Fetch/Metadata/ListOffsets semantic oracles vs 3.9.1 and 4.1.0.
if [[ -f scripts/ci-protocol-oracles.sh ]] \
  && grep -q 'cargo test --test protocol_oracles' scripts/ci-protocol-oracles.sh \
  && bash scripts/ci-protocol-oracles.sh --self-test >/tmp/pl-protocol-oracles-self-test.log 2>&1; then
  ok "KL-01 protocol oracles (Produce/Fetch/Metadata/ListOffsets vs 3.9.1/4.1.0; fixture --self-test)"
else
  bad "KL-01 protocol oracles missing or self-test failed; see /tmp/pl-protocol-oracles-self-test.log"
fi

# KL-08: serialized publish path — release-plz PR-only; cut/release exact-SHA + consumer.
if grep -qF 'command: release-pr' .github/workflows/release-plz.yml \
  && ! grep -qE 'if:.*CARGO_REGISTRY_TOKEN' .github/workflows/release-plz.yml \
  && grep -qF 'check-main-ci.sh' scripts/owner-cut-release.sh \
  && grep -qF 'check-main-ci.sh' scripts/owner-publish.sh \
  && grep -qF 'check-main-ci.sh' scripts/ci-publish-ready.sh \
  && grep -qF 'ci-crate-consumer.sh' .github/workflows/release.yml \
  && grep -qF 'CHECK_SHA=' .github/workflows/release.yml \
  && bash scripts/owner-cut-release.sh --self-test >/tmp/pl-kl08-cut-self-test.log 2>&1 \
  && bash scripts/rehearse-partial-release.sh --self-test >/tmp/pl-kl08-partial-rehearse.log 2>&1; then
  ok "KL-08 release serialize (release-plz PR-only; exact-SHA CI on cut/publish/release.yml; crate consumer; partial-release recovery rehearsal)"
else
  bad "KL-08 release serialize missing/failed; see /tmp/pl-kl08-cut-self-test.log /tmp/pl-kl08-partial-rehearse.log"
fi

# KL-02 slice: produce cancellation contract (docs + mock tests + durable close).
if [[ -f tests/produce_cancel.rs && -f docs/guide.md ]] \
  && grep -qF 'Produce cancellation and shutdown' docs/guide.md \
  && grep -qF 'dropping_send_future_while_buffered_is_ambiguous_but_still_delivers' tests/produce_cancel.rs \
  && grep -qF 'send_after_close_returns_closed' tests/produce_cancel.rs \
  && grep -qF 'closed: AtomicBool' src/producer.rs \
  && grep -qF 'dropping this future does **not** dequeue' src/producer.rs; then
  ok "KL-02 produce cancel contract (guide table; produce_cancel tests; durable close flag)"
else
  bad "KL-02 produce cancel contract missing"
fi


# KL-02 slice: consumer leave/close must not auto-commit unprocessed offsets.
if [[ -f tests/consumer_close_commit.rs && -f docs/guide.md ]] \
  && grep -qF 'Consumer leave/close and auto-commit' docs/guide.md \
  && grep -qF 'auto_commit_on_leave_with_long_interval_does_not_commit' tests/consumer_close_commit.rs \
  && grep -qF 'KL-02: never auto-commit positions on leave/close' src/group.rs \
  && grep -qF 'KL-02: do not auto-commit on unsubscribe' src/group.rs; then
  ok "KL-02 consumer close-commit honesty (no leave/unsubscribe auto-commit; poll-interval kept)"
else
  bad "KL-02 consumer close-commit honesty missing"
fi

# KL-02 slice: buffer ownership + mock overload soak (not a 2x/RSS close).
if [[ -f tests/buffer_ownership.rs && -f docs/guide.md ]] \
  && grep -qF 'Buffer ownership and overload (mock)' docs/guide.md \
  && grep -qF 'saturating_try_send_never_exceeds_buffer_memory' tests/buffer_ownership.rs \
  && grep -qF 'queue_full_under_overload_releases_no_orphan_bytes' tests/buffer_ownership.rs \
  && grep -qF 'send_timeout_when_buffer_full_and_max_block_expires' tests/buffer_ownership.rs \
  && grep -qF 'bytes_buffered' src/metrics.rs \
  && grep -qF 'fn try_reserve_buffer' src/producer.rs; then
  ok "KL-02 buffer ownership + mock overload soak (bytes_buffered ≤ buffer_memory; drain on flush)"
else
  bad "KL-02 buffer ownership + mock overload soak missing"
fi

# KL-06 slice: credential Debug redaction (passwords / client_secret / key PEM).
if [[ -f tests/credential_redact.rs && -f docs/security.md ]] \
  && grep -qF 'Credential redaction' docs/security.md \
  && grep -qF 'sasl_plain_debug_redacts_password' tests/credential_redact.rs \
  && grep -qF 'oidc_config_debug_redacts_client_secret' tests/credential_redact.rs \
  && grep -qF 'tls_config_debug_redacts_client_key_pem' tests/credential_redact.rs \
  && grep -qF 'producer_config_debug_redacts_embedded_secrets' tests/credential_redact.rs \
  && grep -qF 'field("password", &"<redacted>")' src/config.rs \
  && grep -qF 'field("client_secret", &"<redacted>")' src/protocol/oidc.rs \
  && grep -qF 'client_key_pem' src/net.rs \
  && grep -qF '"<redacted>"' src/net.rs; then
  ok "KL-06 credential Debug redaction (Sasl/Oidc/Tls + config cascade; security.md)"
else
  bad "KL-06 credential Debug redaction missing"
fi

# KL-06 slice: auth Error body hygiene (OIDC / OAUTHBEARER omit response bodies).
if grep -qF 'oidc token endpoint HTTP {status}' src/protocol/oidc.rs \
  && ! grep -qF 'oidc token endpoint HTTP {status}: {text}' src/protocol/oidc.rs \
  && grep -qF 'oauthbearer: authentication failed' src/protocol/sasl.rs \
  && ! grep -qF 'oauthbearer: {err}' src/protocol/sasl.rs \
  && grep -qF 'oidc_http_error_display_omits_response_body' tests/credential_redact.rs \
  && grep -qF 'OIDC token-endpoint and OAUTHBEARER' docs/security.md; then
  ok "KL-06 auth Error body hygiene (OIDC/OAUTHBEARER; security.md)"
else
  bad "KL-06 auth Error body hygiene missing"
fi

# KL-06 slice: metrics/span redaction honesty (counters+topic only; instruments skip self).
if grep -qF 'metrics_debug_excludes_credential_material' tests/credential_redact.rs \
  && grep -qF 'tracing_instruments_skip_self_holding_configs' tests/credential_redact.rs \
  && grep -qF 'Metrics snapshots' docs/security.md \
  && grep -qF 'skip(self)' docs/security.md \
  && ! grep -n 'tracing::instrument' src/producer.rs src/consumer.rs src/group.rs \
       | grep -v 'skip(self'; then
  ok "KL-06 metrics/span redaction honesty (metrics snapshots + tracing skip(self); security.md)"
else
  bad "KL-06 metrics/span redaction honesty missing"
fi

# KL-06 slice: OIDC bounded transient retry + outage fail-closed.
if grep -qF '## Auth recovery (current behavior)' docs/security.md \
  && grep -qF 'bounded' docs/security.md \
  && grep -qF 'OIDC_FETCH_ATTEMPTS' src/protocol/oidc.rs \
  && grep -qF 'fetch_token_rejects_http_503_fail_closed' src/protocol/oidc.rs \
  && grep -qF 'fetch_token_hang_times_out_fail_closed' src/protocol/oidc.rs \
  && grep -qF 'fetch_token_retries_transient_503_then_succeeds' src/protocol/oidc.rs \
  && grep -qF 'fetch_token_does_not_retry_http_401' src/protocol/oidc.rs \
  && grep -qF 'Mid-connection refresh / rotation / outage soak still open' docs/security.md; then
  ok "KL-06 OIDC bounded transient retry + outage fail-closed (503/401/timeout tests; security.md)"
else
  bad "KL-06 OIDC bounded transient retry / outage fail-closed missing"
fi


# KL-08 slice: support matrix honesty (CI-backed brokers/MSRV; not a 1.0 contract).
if [[ -f docs/support.md && -f docs/RELEASE.md && -f docs/ADOPTION.md && -f docs/api-stability.md ]] \
  && grep -qF 'Support matrix' docs/support.md \
  && grep -qF 'apache/kafka:3.9.1' docs/support.md \
  && grep -qF 'apache/kafka:4.1.0' docs/support.md \
  && grep -qF 'rust-version' docs/support.md \
  && grep -qF 'Does **not** close KL-08' docs/support.md \
  && grep -qF 'support.md' docs/RELEASE.md \
  && grep -qF 'support.md' docs/ADOPTION.md \
  && grep -qF 'support.md' docs/api-stability.md; then
  ok "KL-08 support matrix honesty (docs/support.md + RELEASE/ADOPTION/api-stability links)"
else
  bad "KL-08 support matrix honesty missing"
fi

# KL-08 slice: adopter 24h/7d exercise template (UNFILLED — not evidence).
if [[ -f docs/adopter-exercise.md ]] \
  && grep -qF 'UNFILLED — not evidence' docs/adopter-exercise.md \
  && grep -qF 'Does **not** close KL-08' docs/adopter-exercise.md \
  && grep -qF '24-hour' docs/adopter-exercise.md \
  && grep -qF '7-day' docs/adopter-exercise.md \
  && grep -qF 'adopter-exercise.md' docs/support.md \
  && grep -qF 'adopter-exercise.md' docs/ADOPTION.md; then
  ok "KL-08 adopter exercise template (24h/7d UNFILLED; support+ADOPTION links)"
else
  bad "KL-08 adopter exercise template missing"
fi
if [[ -f scripts/ci-integrity-smoke.sh && -f scripts/ci-latency-gate.sh \
   && -f scripts/lib/ensure-broker.sh && -f scripts/ci-tip-verifiable-broker.sh ]]; then
  ok "tip live-broker Verifiable scripts present (integrity/latency/ensure-broker/tip-verifiable)"
else
  bad "missing tip live-broker Verifiable scripts (ci-integrity-smoke/ci-latency-gate/ensure-broker/ci-tip-verifiable-broker)"
fi
if [[ -f scripts/ci-branch-lite.sh ]] && grep -q 'ci-tip-verifiable-broker' scripts/ci-branch-lite.sh \
  && [[ -f scripts/check-cut-path.sh ]] && grep -q 'ci-tip-verifiable-broker' scripts/check-cut-path.sh; then
  ok "tip Verifiable proxy wires ci-tip-verifiable-broker (branch-lite + cut-path)"
else
  bad "ci-tip-verifiable-broker not wired into ci-branch-lite / check-cut-path"
fi
# Soft-skip must not claim tip Verifiable `ok` after mid-chain skips (PARTIAL).
# PARTIAL must exit 2 by default so set -e tip proxies cannot greenwash.
# Prefer executable --self-test over grep-only (finalize exit codes cannot drift).
# Cut path (`owner-finish-installable`) + Installable preflight must also run both honesty self-tests.
if [[ -f scripts/ci-tip-verifiable-broker.sh ]] \
  && grep -q 'pl_tip_verifiable_finalize' scripts/ci-tip-verifiable-broker.sh \
  && grep -q 'pl_tip_verifiable_interpret_integrity' scripts/ci-tip-verifiable-broker.sh \
  && grep -qF 'latency gate failed (soft)' scripts/ci-tip-verifiable-broker.sh \
  && grep -q 'pl_tip_verifiable_quiet_retry_integrity' scripts/ci-tip-verifiable-broker.sh \
  && grep -q 'TIP_VERIFIABLE_QUIET_RETRIES' scripts/ci-tip-verifiable-broker.sh \
  && grep -qF -- '--self-test' scripts/ci-tip-verifiable-broker.sh \
  && grep -q 'TIP_VERIFIABLE_SOFT' scripts/ci-tip-verifiable-broker.sh \
  && grep -q 'pl_tip_verifiable_tooling_ready' scripts/ci-tip-verifiable-broker.sh \
  && bash scripts/ci-tip-verifiable-broker.sh --self-test >/tmp/pl-tip-verifiable-self-test.log 2>&1 \
  && grep -q 'self-test OK' /tmp/pl-tip-verifiable-self-test.log \
  && grep -q 'soft latency honesty' /tmp/pl-tip-verifiable-self-test.log \
  && grep -q 'quiet retry' /tmp/pl-tip-verifiable-self-test.log \
  && [[ -f scripts/ci-branch-lite.sh ]] && grep -qF -- 'ci-tip-verifiable-broker.sh --self-test' scripts/ci-branch-lite.sh \
  && [[ -f scripts/check-cut-path.sh ]] && grep -qF -- 'ci-tip-verifiable-broker.sh --self-test' scripts/check-cut-path.sh \
  && [[ -f scripts/owner-finish-installable.sh ]] \
  && grep -qF -- 'check-registry-token.sh --self-test' scripts/owner-finish-installable.sh \
  && grep -qF -- 'ci-tip-verifiable-broker.sh --self-test' scripts/owner-finish-installable.sh \
  && [[ -f scripts/check-installable-preflight.sh ]] \
  && grep -qF -- 'check-registry-token.sh --self-test' scripts/check-installable-preflight.sh \
  && grep -qF -- 'ci-tip-verifiable-broker.sh --self-test' scripts/check-installable-preflight.sh \
  && grep -qF -- 'pl_prepare_cargo_registry_token' scripts/check-installable-preflight.sh \
  && grep -qF -- 'check-installable-preflight.sh --self-test' scripts/check-installable-preflight.sh \
  && bash scripts/check-installable-preflight.sh --self-test >/tmp/pl-preflight-self-test.log 2>&1 \
  && grep -q 'self-test OK' /tmp/pl-preflight-self-test.log; then
  ok "tip Verifiable soft-skip honesty (--self-test PARTIAL exit 2 + soft latency + quiet retry; wired into branch-lite/cut-path/finish/preflight)"
  ok "Installable preflight TOKEN prepare honesty (whitespace/misname/TOKEN_FILE before READY_EXCEPT_TOKEN; --self-test)"
else
  bad "ci-tip-verifiable-broker soft-skip honesty missing (--self-test / finalize / soft latency / quiet retry / tip proxy+finish+preflight wiring); see /tmp/pl-tip-verifiable-self-test.log"
  bad "Installable preflight TOKEN prepare honesty missing (pl_prepare / --self-test); see /tmp/pl-preflight-self-test.log"
fi
# Integrity leaf must not print final `ok` after soft latency — civilization-check / tip
# proxies must see PARTIAL/exit 2 (REQUIRE_INTEGRITY=1 → hard-fail). Soft branch must
# exit before the final `ok` line in source order.
if [[ -f scripts/ci-integrity-smoke.sh ]] \
  && grep -qF 'latency_soft=1' scripts/ci-integrity-smoke.sh \
  && grep -qF 'ci-integrity-smoke: PARTIAL' scripts/ci-integrity-smoke.sh \
  && awk '
      /latency_soft=1/ { soft=NR }
      /ci-integrity-smoke: PARTIAL/ { partial=NR }
      /exit 2/ && soft && NR >= soft { e2=NR }
      /ci-integrity-smoke: ok \(unsigned/ { ok=NR }
      END { exit (soft && partial && e2 && ok && partial < ok && e2 < ok) ? 0 : 1 }
    ' scripts/ci-integrity-smoke.sh \
  && [[ -f scripts/ci-civilization-check.sh ]] \
  && grep -qF 'ci-integrity-smoke: PARTIAL' scripts/ci-civilization-check.sh \
  && grep -qF 'latency soft-miss' scripts/ci-civilization-check.sh; then
  ok "integrity-smoke soft-latency honesty (PARTIAL/exit 2 before final ok; civilization-check ski)"
else
  bad "integrity-smoke soft-latency honesty missing (must PARTIAL/exit 2 before final ok; never greenwash soft)"
fi

# CI integrity job: Lab A HW only; nested latency skipped (latency-gate job owns GHA ceiling).
# Auth + integrity checkout pins match parks (v7).
if grep -A20 '^  integrity-smoke:' .github/workflows/ci.yml | grep -qF 'SKIP_LATENCY_GATE: "1"' \
  && grep -A20 '^  integrity-smoke:' .github/workflows/ci.yml | grep -qF 'actions/checkout@v7' \
  && grep -A12 '^  auth-smoke:' .github/workflows/ci.yml | grep -qF 'actions/checkout@v7' \
  && ! grep -A12 '^  auth-smoke:' .github/workflows/ci.yml | grep -qF 'actions/checkout@v5' \
  && ! grep -A20 '^  integrity-smoke:' .github/workflows/ci.yml | grep -qF 'actions/checkout@v5'; then
  ok "CI integrity/auth: checkout v7 + SKIP_LATENCY_GATE on integrity (latency-gate owns GHA latency)"
else
  bad "CI integrity/auth must use checkout v7; integrity-smoke must SKIP_LATENCY_GATE (avoid REQUIRE_INTEGRITY latency flake)"
fi

# KL-01/KL-04: shared-runner vs local vs controlled-host latency budgets (do not raise GHA 5000).
if [[ -f docs/latency-ci-policy.json ]] \
  && bash scripts/ci-latency-gate.sh --self-test >/tmp/pl-latency-gate-self-test.log 2>&1; then
  ok "KL-01/KL-04 latency CI policy (self-test + docs/latency-ci-policy.json; 1344/750 historical; Suite HOLD)"
else
  bad "KL-01/KL-04 latency policy self-test failed or docs/latency-ci-policy.json missing; see /tmp/pl-latency-gate-self-test.log"
fi

# KL-04 slice: controlled-host latency exercise template (UNFILLED — not evidence).
if [[ -f docs/controlled-host-latency-exercise.md ]] \
  && grep -qF 'UNFILLED — not evidence' docs/controlled-host-latency-exercise.md \
  && grep -qF 'Does **not** close KL-04' docs/controlled-host-latency-exercise.md \
  && grep -qF 'controlled-host' docs/controlled-host-latency-exercise.md \
  && grep -qF 'Suite HOLD' docs/controlled-host-latency-exercise.md \
  && grep -qF 'controlled-host-latency-exercise.md' docs/guide.md \
  && grep -qF 'controlled-host-latency-exercise.md' docs/ROADMAP.md; then
  ok "KL-04 controlled-host latency exercise template (UNFILLED; guide+ROADMAP links)"
else
  bad "KL-04 controlled-host latency exercise template missing"
fi

# Post-Installable: MISSING token copy must not pretend Installable is still blocked.
if grep -qF 'crates.io already has this crate/version (Installable met)' scripts/check-registry-token.sh \
  && grep -qF 'token only needed for future cuts' scripts/check-registry-token.sh; then
  ok "registry-token MISSING copy is post-Installable honest (future cuts, not Installable blocker)"
else
  bad "registry-token MISSING copy must note Installable-met → token only for future cuts"
fi

# owner-status must not label MISSING token as Installable-BLOCKED after crates.io has the version.
if grep -qF 'OK  CARGO_REGISTRY_TOKEN unset (Installable met; token only for future cuts / Actions)' scripts/owner-status.sh \
  && grep -qF 'token only needed for future cuts' scripts/owner-status.sh \
  && grep -qF 'bars: skipped (fast path; Installable met' scripts/owner-status.sh \
  && grep -qF 'TOKEN unset while Installable waits' scripts/owner-status.sh; then
  ok "owner-status post-Installable token honesty (OK not BLOCKED; bars skip copy distinguishes Installable-met)"
else
  bad "owner-status must OK missing token when Installable met; bars skip must not blame token post-Installable"
fi

# owner-unblock must not steer first-cut token ask when Installable is already met.
if grep -qF 'Tracking #86 is closed' scripts/owner-unblock.sh \
  && grep -qF 'expect ALREADY_INSTALLABLE' scripts/owner-unblock.sh \
  && grep -qF 'Trusted Publishing UI' scripts/owner-unblock.sh \
  && grep -qF 'expect READY_EXCEPT_TOKEN' scripts/owner-unblock.sh; then
  ok "owner-unblock post-Installable path (ALREADY_INSTALLABLE vs READY_EXCEPT_TOKEN; #86 closed)"
else
  bad "owner-unblock must branch ALREADY_INSTALLABLE (do not re-cut) vs READY_EXCEPT_TOKEN first-cut"
fi

# Post-Installable: crates.io description can lag Cargo.toml until the next cut — surface WARN, do not re-cut 0.1.0.
if [[ -x scripts/check-crates-io-description.sh ]] \
  && grep -qF 'check-crates-io-description.sh' scripts/owner-status.sh \
  && grep -qF 'do not re-cut 0.1.0' scripts/check-crates-io-description.sh \
  && grep -qF 'weaker no-C signal until next cut' scripts/check-crates-io-description.sh; then
  ok "crates.io description drift probe (WARN when published lags Cargo.toml identity; no re-cut)"
else
  bad "need check-crates-io-description wired into owner-status (WARN on desc drift; never re-cut 0.1.0)"
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
if [[ -f scripts/ci-fuzz-campaign.sh ]] \
  && bash scripts/ci-fuzz-campaign.sh --self-test >/tmp/pl-fuzz-campaign-self-test.log 2>&1 \
  && grep -q 'self-test OK' /tmp/pl-fuzz-campaign-self-test.log \
  && grep -q '"kind": "campaign"' fuzz/campaign/metadata.example.json \
  && ! grep -q '"kind": "smoke"' fuzz/campaign/metadata.example.json; then
  ok "KL-01 fuzz campaign metadata (self-test; kind=campaign; distinct from 15s smoke)"
else
  bad "KL-01 fuzz campaign metadata missing or self-test failed; see /tmp/pl-fuzz-campaign-self-test.log"
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
# Documented git pin must cargo-check while Installable waits (Adoptable before crates.io).
# After Installable, MODE=git SKIPs once README is crates.io-shaped — that alone is not
# Operable proof. Prefer MODE=registry against live crates.io when Installable is met.
if grep -qF 'MODE=git' scripts/check-cut-path.sh \
  && grep -qF 'MODE=git' scripts/check-installable-preflight.sh \
  && grep -qF 'MODE=git' scripts/owner-finish-installable.sh \
  && grep -qE 'MODE=git|mode.*git' scripts/verify-crates-io-consumer.sh; then
  if MODE=git bash scripts/verify-crates-io-consumer.sh >/tmp/pl-git-adopter.log 2>&1; then
    ok "git-tag adopter consumer (documented pin cargo-checks; wired into cut-path + preflight + finish)"
  else
    bad "git-tag adopter consumer failed; see /tmp/pl-git-adopter.log"
  fi
else
  bad "git-tag adopter consumer not wired into cut-path / preflight / finish / verify-crates-io-consumer"
fi
if bash scripts/check-installable.sh >/dev/null 2>&1; then
  if grep -qF 'MODE=registry' scripts/check-cut-path.sh \
    && grep -qF 'MODE=registry' scripts/ci-branch-lite.sh \
    && grep -qF 'MODE=registry' scripts/owner-finish-installable.sh \
    && grep -qF 'MODE=registry' scripts/verify-crates-io-consumer.sh \
    && grep -qF 'ci-crate-consumer.sh' .github/workflows/ci.yml; then
    if MODE=registry bash scripts/verify-crates-io-consumer.sh >/tmp/pl-registry-adopter.log 2>&1; then
      ok "registry adopter consumer (crates.io pin cargo-checks; wired into cut-path + branch-lite + finish + package CI)"
    else
      bad "registry adopter consumer failed; see /tmp/pl-registry-adopter.log"
    fi
  else
    bad "registry adopter consumer not wired into cut-path / branch-lite / finish / verify-crates-io-consumer / ci.yml package"
  fi
else
  ok "registry adopter consumer deferred (Installable not met yet — git/path proofs cover pre-cut)"
fi

# Schema companion scaffold (WP-6.3): wire framing in-tree, excluded from core package.
if [[ -x scripts/check-schema-companion-scaffold.sh ]] \
  && grep -qF 'partitionline-schema' Cargo.toml \
  && bash scripts/check-schema-companion-scaffold.sh >/tmp/pl-schema-scaffold.log 2>&1; then
  ok "schema companion scaffold (wire framing tests; publish=false; excluded from core package)"
else
  bad "schema companion scaffold missing/failed; see /tmp/pl-schema-scaffold.log"
fi

# Adopter-pin honesty: pre-Installable = git tag parity (no crates.io lead);
# post-Installable = four-file crates.io shape (day1 flipped).
if [[ -x scripts/check-adopter-pin.sh ]] \
  && grep -qF 'docs/guide.md' scripts/check-adopter-pin.sh \
  && grep -qF 'leads with crates.io version while README is still git-shaped' scripts/check-adopter-pin.sh \
  && bash scripts/check-adopter-pin.sh >/tmp/pl-adopter-pin.log 2>&1; then
  if bash scripts/check-installable.sh >/dev/null 2>&1; then
    if grep -qE '^partitionline = "[0-9]' README.md \
      && grep -qE 'partitionline = "[0-9]' docs/ADOPTION.md \
      && grep -qE 'partitionline = \{ version = "[0-9]' docs/guide.md \
      && grep -qE '^partitionline = "[0-9]' docs/migrate-from-rdkafka.md \
      && ! grep -qE '^partitionline = \{ git =' docs/guide.md \
      && ! grep -qE '^partitionline = \{ git =' docs/migrate-from-rdkafka.md; then
      ok "adopter-pin honesty (post-Installable crates.io four-file shape; check-adopter-pin ok)"
    else
      bad "adopter-pin honesty failed post-Installable — docs not fully crates.io-shaped; see /tmp/pl-adopter-pin.log"
    fi
  else
    if grep -qF 'tag = "v0.1.0-rc.6"' docs/guide.md \
      && ! grep -qE '^partitionline = "[0-9]' docs/migrate-from-rdkafka.md \
      && ! grep -qE '^partitionline = \{ version = "[0-9]' docs/guide.md; then
      ok "adopter-pin honesty (README/ADOPTION/migrate/guide tag parity; no live crates.io lead pre-Installable)"
    else
      bad "adopter-pin honesty missing/failed pre-Installable; see /tmp/pl-adopter-pin.log"
    fi
  fi
else
  bad "adopter-pin honesty missing/failed; see /tmp/pl-adopter-pin.log"
fi

# Day1 must flip guide + migrate (not only README/ADOPTION), rehearsed in publish-ready.
if grep -qF 'post-publish-guide.sh' scripts/day1-after-publish.sh \
  && grep -qF 'post-publish-migrate.sh' scripts/day1-after-publish.sh \
  && grep -qF 'post-publish-guide.sh' scripts/ci-publish-ready.sh \
  && grep -qF 'post-publish-migrate.sh' scripts/ci-publish-ready.sh \
  && grep -qF 'docs/guide.md' scripts/lib/preserve-day1-docs.sh \
  && grep -qF 'docs/migrate-from-rdkafka.md' scripts/lib/preserve-day1-docs.sh \
  && grep -qF 'README/ADOPTION/guide/migrate' scripts/lib/preserve-day1-docs.sh \
  && DRY_RUN=1 bash scripts/post-publish-guide.sh >/tmp/pl-guide-flip-bars.log 2>&1 \
  && DRY_RUN=1 bash scripts/post-publish-migrate.sh >/tmp/pl-migrate-flip-bars.log 2>&1 \
  && grep -q 'DRY_RUN ok' /tmp/pl-guide-flip-bars.log \
  && grep -q 'DRY_RUN ok' /tmp/pl-migrate-flip-bars.log; then
  ok "day1 guide+migrate crates.io flip (DRY_RUN rehearsed; wired into day1 + publish-ready + preserve-day1)"
else
  bad "day1 guide+migrate flip missing/failed; see /tmp/pl-guide-flip-bars.log /tmp/pl-migrate-flip-bars.log"
fi
# Parks auto-refresh must restore main/caller before cut/publish (token-day footgun).
if [[ -x scripts/check-parks-refresh-cut-guards.sh ]] \
  && bash scripts/check-parks-refresh-cut-guards.sh >/tmp/pl-parks-refresh-guards.log 2>&1 \
  && grep -qF -- 'check-parks-refresh-cut-guards.sh' scripts/owner-finish-installable.sh \
  && grep -qF -- 'check-parks-refresh-cut-guards.sh' scripts/check-cut-path.sh \
  && grep -qF -- 'check-parks-refresh-cut-guards.sh' scripts/check-installable-preflight.sh; then
  ok "parks-refresh cut guards (restore main/caller; wired into finish + cut-path + preflight)"
else
  bad "parks-refresh cut guards missing or unwired; see /tmp/pl-parks-refresh-guards.log"
fi
# day1 crates.io README/ADOPTION flips must survive parks land (stash pop can fail).
# Live parks land now runs inside handoff (LAND_PARKS=1) as well as finish — both must preserve.
if [[ -x scripts/lib/preserve-day1-docs.sh ]] \
  && bash scripts/lib/preserve-day1-docs.sh --self-test >/tmp/pl-day1-docs-preserve.log 2>&1 \
  && grep -qF -- 'preserve-day1-docs.sh' scripts/owner-finish-installable.sh \
  && grep -qF -- 'preserve-day1-docs.sh' scripts/check-installable-preflight.sh \
  && grep -qF -- 'preserve-day1-docs.sh' scripts/check-cut-path.sh \
  && grep -qF -- 'preserve-day1-docs.sh' scripts/owner-post-installable-handoff.sh \
  && grep -qF -- 'pl_day1_docs_begin' scripts/owner-post-installable-handoff.sh \
  && grep -qF -- 'pl_day1_docs_end' scripts/owner-post-installable-handoff.sh; then
  ok "day1 docs preserve across parks (stash+backup; wired into finish + cut-path + preflight + handoff)"
else
  bad "day1 docs preserve missing or unwired (finish/preflight/cut-path/handoff); see /tmp/pl-day1-docs-preserve.log"
fi
# Post-Installable handoff must exist for TP/parks re-entry (Actions-alternate + soft-fails).
if [[ -x scripts/owner-post-installable-handoff.sh ]] \
  && grep -qF -- 'owner-post-installable-handoff.sh' scripts/owner-finish-installable.sh \
  && grep -qF -- 'owner-post-installable-handoff.sh' scripts/check-cut-path.sh \
  && grep -qF -- 'owner-post-installable-handoff.sh' scripts/day1-after-publish.sh \
  && grep -qF -- 'owner-post-installable-handoff.sh' scripts/owner-dispatch-first-publish.sh \
  && grep -qF -- 'LAND_PARKS=' scripts/owner-finish-installable.sh \
  && grep -qF -- 'Post-Installable handoff' scripts/owner-finish-installable.sh \
  && grep -qF -- 'would run owner-post-installable-handoff' scripts/owner-finish-installable.sh \
  && grep -qF -- 'owner-post-installable-handoff' scripts/owner-cut-release.sh \
  && grep -qF -- 'LAND_PARKS' scripts/owner-cut-release.sh \
  && grep -qF -- 'SKIP_HANDOFF' scripts/owner-cut-release.sh \
  && grep -qF -- 'SKIP_HANDOFF=1' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — already-Installable DRY_RUN soft-failed' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — not-yet-Installable DRY_RUN soft-failed' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — Installable OK but Actions secret not synced' scripts/owner-finish-installable.sh \
  && grep -qF 'secret_rc' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — Installable OK but Actions secret not synced' scripts/owner-cut-release.sh \
  && grep -qF 'secret_rc' scripts/owner-cut-release.sh \
  && grep -qF 'parks_rc' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — not yet Installable' scripts/day1-after-publish.sh \
  && grep -qF 'day1_rc' scripts/check-cut-path.sh \
  && grep -qF 'day1_rc' scripts/ci-branch-lite.sh \
  && grep -qF 'day1_rc' scripts/ci-publish-ready.sh \
  && grep -qF 'finish_rc' scripts/check-cut-path.sh \
  && grep -qF 'dispatch_rc' scripts/check-cut-path.sh \
  && grep -qF 'dispatch_rc' scripts/ci-branch-lite.sh \
  && grep -qF 'dispatch_rc' scripts/ci-publish-ready.sh \
  && grep -qF 'handoff_rc' scripts/check-cut-path.sh \
  && grep -qF 'handoff_rc' scripts/ci-branch-lite.sh \
  && grep -qF 'handoff_rc' scripts/ci-publish-ready.sh \
  && grep -qF 'PARTIAL — handoff DRY_RUN soft-failed' scripts/check-cut-path.sh \
  && grep -qF 'PARTIAL — handoff DRY_RUN soft-failed' scripts/ci-branch-lite.sh \
  && grep -qF 'PARTIAL — handoff DRY_RUN soft-failed' scripts/ci-publish-ready.sh \
  && grep -qF 'PARTIAL — already Installable' scripts/owner-dispatch-first-publish.sh \
  && bash scripts/owner-dispatch-first-publish.sh --self-test >/tmp/pl-dispatch-self-test.log 2>&1 \
  && grep -q 'self-test OK' /tmp/pl-dispatch-self-test.log \
  && grep -qF 'check-installable-preflight.sh' scripts/owner-request-registry-token.sh \
  && grep -qF 'READY_EXCEPT_TOKEN' scripts/owner-request-registry-token.sh \
  && grep -qF 'parks stack stale' scripts/owner-request-registry-token.sh \
  && grep -qF 'owner-request-registry-token: PARTIAL — parks not on main yet' scripts/owner-request-registry-token.sh \
  && grep -qF 'exit 2' scripts/owner-request-registry-token.sh \
  && grep -qF 'no README/ADOPTION/guide/migrate commit performed' scripts/day1-after-publish.sh \
  && grep -qF 'commit README + docs/ADOPTION.md + docs/guide.md + docs/migrate-from-rdkafka.md if day1 changed them' scripts/owner-unblock.sh \
  && grep -qF -- '--self-test' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'PARTIAL — Installable OK but parks land failed' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'day1-after-publish.sh' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'PARTIAL — Installable OK but adopter docs still git-shaped' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'SKIP_DAY1=1' scripts/owner-finish-installable.sh \
  && grep -qF 'PARTIAL — Installable OK but parks not on main' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'REQUIRE_PARKS=1' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'DRY_RUN: parks on main' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'PARTIAL — parks not on main (DRY_RUN' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'PARTIAL — Installable OK but adopter docs still git-shaped (DRY_RUN' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'adopter-docs-shaped.sh' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'pl_adopter_docs_crates_io_shaped' scripts/lib/adopter-docs-shaped.sh \
  && grep -qF 'docs/guide.md' scripts/lib/adopter-docs-shaped.sh \
  && grep -qF 'docs/migrate-from-rdkafka.md' scripts/lib/adopter-docs-shaped.sh \
  && grep -qF 'README + ADOPTION + guide + migrate' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'docs/guide.md + docs/migrate-from-rdkafka.md' scripts/owner-finish-installable.sh \
  && grep -qF 'README + ADOPTION + guide + migrate' scripts/owner-cut-release.sh \
  && grep -qF 'docs/guide.md docs/migrate-from-rdkafka.md' scripts/owner-publish.sh \
  && grep -qF 'README/ADOPTION/guide/migrate crates.io lines (day1)' scripts/ci-publish-ready.sh \
  && grep -qF 'day1 README/ADOPTION/guide/migrate flip preflight' scripts/ci-civilization-check.sh \
  && grep -qF 'post-publish-guide.sh' scripts/ci-civilization-check.sh \
  && grep -qF 'post-publish-migrate.sh' scripts/ci-civilization-check.sh \
  && grep -qF 'pl_day1_docs_paths' scripts/lib/preserve-day1-docs.sh \
  && grep -qF '_pl_day1_reset' scripts/lib/preserve-day1-docs.sh \
  && grep -qF 'Installable already met; post-cut re-entry' scripts/ci-publish-ready.sh \
  && grep -qF 'ci-publish-ready: PARTIAL for partitionline' scripts/ci-publish-ready.sh \
  && grep -qF 'exit 2' scripts/ci-publish-ready.sh \
  && grep -qF 'pre-token rehearsal; Installable still blocked' scripts/ci-publish-ready.sh \
  && grep -qF 'check-parks-on-main.sh' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'check-parks-on-main.sh' scripts/check-installable-preflight.sh \
  && grep -qF 'check-parks-on-main.sh' scripts/owner-status.sh \
  && grep -qF 'PARTIAL — Installable OK but parks not on main' scripts/check-installable-preflight.sh \
  && grep -qF 'OWNER_STATUS_FULL' scripts/owner-status.sh \
  && grep -qF 'expected pre-Installable' scripts/owner-status.sh \
  && grep -qF 'expected pre-Installable' scripts/check-installable-preflight.sh \
  && grep -qF 'expected pre-Installable' docs/CIVILIZATION.md \
  && grep -qF 'expected pre-Installable' docs/RELEASE.md \
  && grep -qF 'expected pre-Installable' docs/ADOPTION.md \
  && grep -qF 'Installable is met' docs/CIVILIZATION.md \
  && grep -qF '0.1.0 published' docs/CIVILIZATION.md \
  && grep -qF 'Installable is met' docs/ADOPTION.md \
  && ! grep -qF 'not published yet' docs/CIVILIZATION.md \
  && ! grep -qF 'blocked only on credentials' docs/ADOPTION.md \
  && grep -qF 'branch-lite (local Actions mirror): PARTIAL' scripts/owner-status.sh \
  && grep -qF 'Installable already met — post-cut re-entry' scripts/ci-branch-lite.sh \
  && grep -qF 'ci-branch-lite: PARTIAL — tip Verifiable proxy held; Installable already met' scripts/ci-branch-lite.sh \
  && grep -qF 'PARTIAL (exit 2; Installable met, post-cut re-entry' scripts/owner-status.sh \
  && grep -qF 'Installable already met — post-cut re-entry' scripts/check-cut-path.sh \
  && grep -qF 'check-cut-path: PARTIAL — cut path rehearsed; Installable already met' scripts/check-cut-path.sh \
  && grep -qF 'pre-token rehearsal' scripts/ci-branch-lite.sh \
  && grep -qF 'pre-token rehearsal' scripts/check-cut-path.sh \
  && ! grep -qF 'pre-token holds exit 0 with a PARTIAL note' scripts/owner-finish-installable.sh \
  && ! grep -qF 'pre-token holds exit 0 with a PARTIAL note' scripts/check-cut-path.sh \
  && ! grep -qF 'pre-token holds exit 0 with a PARTIAL note' docs/STATUS.md \
  && grep -qF 'crate absent (expected pre-Installable' scripts/check-trusted-publishing-ready.sh \
  && grep -qF 'INFO — workflow shape OK; crates.io Trusted Publishing UI still owner' scripts/owner-enable-trusted-publishing.sh \
  && grep -qF 'stay off main until after crates.io' scripts/owner-request-registry-token.sh \
  && grep -qF 'pre-cut pending is expected' scripts/owner-unblock.sh \
  && grep -qF 'tip⊆parks stack' scripts/owner-unblock.sh \
  && bash scripts/check-parks-on-main.sh --self-test >/tmp/pl-parks-on-main-self-test.log 2>&1 \
  && grep -q 'self-test OK' /tmp/pl-parks-on-main-self-test.log \
  && grep -qF 'parks not on main' scripts/owner-status.sh \
  && grep -qF 'parks not on main' scripts/owner-unblock.sh \
  && bash scripts/owner-post-installable-handoff.sh --self-test >/tmp/pl-handoff-self-test.log 2>&1 \
  && grep -q 'self-test OK' /tmp/pl-handoff-self-test.log \
  && grep -q 'fail-closed PARTIAL' /tmp/pl-handoff-self-test.log \
  && HANDOFF_FROM_BARS=1 DRY_RUN=1 bash scripts/owner-post-installable-handoff.sh >/tmp/pl-handoff-dry.log 2>&1; handoff_dry_rc=$? \
  && { \
       # Pre/post-Installable parks pending → PARTIAL/2 (fail-closed). \
       { [[ "$handoff_dry_rc" -eq 2 ]] \
         && { grep -q 'PARTIAL — parks not on main (DRY_RUN; expected pre-token' /tmp/pl-handoff-dry.log \
              || grep -q 'PARTIAL — parks not on main (DRY_RUN; already Installable' /tmp/pl-handoff-dry.log; }; } \
       || \
       # Parks already on main after land → DRY_RUN complete exit 0. \
       { [[ "$handoff_dry_rc" -eq 0 ]] \
         && grep -q 'DRY_RUN complete' /tmp/pl-handoff-dry.log \
         && grep -qE 'parks on main: OK|check-parks-on-main: OK' /tmp/pl-handoff-dry.log; } \
     }; then
  ok "post-Installable handoff (DRY_RUN fail-closed PARTIAL/2 while parks pending, or complete when parks on main; finish+cut-release chain + cut-path + day1 + first-publish)"
else
  bad "post-Installable handoff missing/unwired; see /tmp/pl-handoff-dry.log /tmp/pl-handoff-self-test.log"
fi

# Handoff must PARTIAL (not hard-fail) when tip STATUS stamps advance ahead of park heads.
if grep -qF 'tip not ancestor of park heads (DRY_RUN)' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'parks on main but tip not ancestor of park heads' scripts/owner-post-installable-handoff.sh \
  && grep -qF 'refresh-post-cut-parks.sh' scripts/owner-post-installable-handoff.sh; then
  ok "handoff tip-ahead-of-parks honesty (PARTIAL/2 + refresh; parks-on-main still OK)"
else
  bad "handoff must PARTIAL when tip ahead of parks while parks already on main"
fi

# Cut-release bare must auto PUBLISH_LOCAL=1 when token is in-env (token-day footgun).
if bash scripts/owner-cut-release.sh --self-test >/tmp/pl-cut-publish-local-auto.log 2>&1 \
  && grep -q 'handoff chained' /tmp/pl-cut-publish-local-auto.log \
  && grep -q 'DRY_RUN reaches handoff' /tmp/pl-cut-publish-local-auto.log \
  && grep -q 'Actions secret PARTIAL gated' /tmp/pl-cut-publish-local-auto.log; then
  ok "cut-release PUBLISH_LOCAL auto-default + handoff chain + DRY_RUN reaches handoff + SKIP_HANDOFF for finish + Actions secret PARTIAL"
else
  bad "cut-release PUBLISH_LOCAL auto-default / handoff chain / Actions secret PARTIAL missing/broken; see /tmp/pl-cut-publish-local-auto.log"
fi

# Cut DRY_RUN must not claim Installable when crate absent; capture dry_handoff_rc.
if grep -qF 'PARTIAL — handoff soft-failed (not yet Installable' scripts/owner-cut-release.sh \
  && grep -qF 'dry_handoff_rc' scripts/owner-cut-release.sh \
  && grep -qF 'is Installable on crates.io but handoff soft-failed' scripts/owner-cut-release.sh; then
  ok "cut-release handoff PARTIAL honesty (Installable-gated copy + dry_handoff_rc; no pre-token Installable lie)"
else
  bad "cut-release handoff PARTIAL honesty missing (must not claim Installable when crate absent)"
fi

# ALREADY_INSTALLABLE surfaces must probe four-file day1 shape (shared lib).
if grep -qF 'adopter-docs-shaped.sh' scripts/check-installable-preflight.sh \
  && grep -qF 'adopter docs still git-shaped' scripts/check-installable-preflight.sh \
  && grep -qF 'adopter-docs-shaped.sh' scripts/owner-request-registry-token.sh \
  && grep -qF 'adopter docs still git-shaped' scripts/owner-request-registry-token.sh \
  && grep -qF 'Commit day1 crates.io lines if still dirty: README + docs/ADOPTION.md + docs/guide.md + docs/migrate-from-rdkafka.md' scripts/owner-status.sh \
  && bash scripts/lib/adopter-docs-shaped.sh --self-test >/tmp/pl-adopter-docs-shaped-self-test.log 2>&1 \
  && grep -q 'self-test OK' /tmp/pl-adopter-docs-shaped-self-test.log; then
  ok "ALREADY_INSTALLABLE day1 four-file honesty (preflight + token-ask + status; shared adopter-docs-shaped lib)"
else
  bad "ALREADY_INSTALLABLE day1 four-file honesty missing; see /tmp/pl-adopter-docs-shaped-self-test.log"
fi


# Owner checklists must match cut-release auto PUBLISH_LOCAL (no bare → Actions steer).
bare_actions="$(grep -nE 'owner-cut-release\.sh.*tag → Actions' scripts/owner-unblock.sh scripts/owner-status.sh 2>/dev/null | grep -v 'PUBLISH_LOCAL=0' || true)"
if grep -qF 'token in-env → local publish (auto)' scripts/owner-unblock.sh \
  && grep -qF 'token in-env → local publish (auto)' scripts/owner-status.sh \
  && [[ -z "$bare_actions" ]]; then
  ok "cut-release owner-helper comments match PUBLISH_LOCAL auto-default"
else
  bad "owner-unblock/status still steers bare cut-release to Actions (or missing auto-local comment)"
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
# Open Dependabot cargo/Actions bumps must map to post-cut parks (tip stays docs/scripts-only).
if [[ ! -x scripts/check-dependabot-parks-coverage.sh ]]; then
  bad "missing scripts/check-dependabot-parks-coverage.sh"
elif ! bash scripts/check-dependabot-parks-coverage.sh --self-test >/tmp/pl-dep-parks-self.log 2>&1; then
  bad "Dependabot parks coverage --self-test failed; see /tmp/pl-dep-parks-self.log"
else
  dep_rc=0
  bash scripts/check-dependabot-parks-coverage.sh >/tmp/pl-dep-parks.log 2>&1 || dep_rc=$?
  if [[ "$dep_rc" -eq 0 ]]; then
    ok "Dependabot ↔ post-cut parks coverage (open bumps mapped; tip-delta safe)"
  elif [[ "$dep_rc" -eq 2 ]]; then
    ok "Dependabot ↔ post-cut parks coverage soft-skipped (gh/API)"
  else
    bad "Dependabot parks coverage missing/unmapped; see /tmp/pl-dep-parks.log"
  fi
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
# Soft structural PARTIALs (e.g. MSRV label / deny.toml wording) stay allowed
# under PRE_PUBLISH so token-day rehearsal is not blocked by doc-shape notes.
# Full (non-PRE_PUBLISH) bars refuse final OK when any PARTIAL remains.
if [[ "$PRE_PUBLISH" == "1" ]]; then
  other_blocked=$((blocked - installable_blocked))
  if [[ "$other_blocked" -gt 0 ]]; then
    echo "audit-civilization-bars: NOT COMPLETE — non-Installable BLOCKED items" >&2
    exit 1
  fi
  if [[ "$installable_blocked" -gt 0 ]]; then
    if [[ "$partial" -gt 0 ]]; then
      echo "audit-civilization-bars: PRE_PUBLISH OK — bars green except Installable (owner token/cut); PARTIAL notes above"
    else
      echo "audit-civilization-bars: PRE_PUBLISH OK — bars green except Installable (owner token/cut)"
    fi
    exit 0
  fi
  if [[ "$partial" -gt 0 ]]; then
    echo "audit-civilization-bars: PRE_PUBLISH OK — Installable proven; PARTIAL notes above (full bars would exit 2)"
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
  echo "audit-civilization-bars: PARTIAL — soft notes remain (no FAIL/BLOCKED; exit 2)" >&2
  exit 2
fi
echo "audit-civilization-bars: OK — all six bars PASS"
exit 0