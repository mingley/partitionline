#!/usr/bin/env bash
# Pre-publish readiness gate (WP-0.5). Does not publish.
# Run before tagging v0.1.0 once CARGO_REGISTRY_TOKEN is available.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== partial-release recovery rehearsal (KL-08; no publish) =="
# Cheap grep gate; --self-test must stay 0 (never publishes or re-cuts 0.1.0).
bash scripts/rehearse-partial-release.sh --self-test

echo "== fmt =="
cargo fmt --all -- --check

echo "== clippy =="
cargo clippy --all-targets --all-features -- -D warnings

echo "== test =="
cargo test --all-targets

echo "== deny =="
bash scripts/ci-deny.sh

echo "== msrv =="
bash scripts/ci-msrv.sh

echo "== docs =="
bash scripts/ci-docs.sh

echo "== package =="
cargo package

echo "== crate metadata (crates.io shape) =="
bash scripts/check-crate-metadata.sh

echo "== crate consumer =="
bash scripts/ci-crate-consumer.sh

echo "== consumer-control examples as packed-crate consumers =="
bash scripts/ci-example-consumer-control-crate-consumers.sh

echo "== adopter crates.io consumer rehearsal (path) =="
MODE=path bash scripts/verify-crates-io-consumer.sh

echo "== exact-SHA main CI probe (KL-08; soft unless REQUIRE_MAIN_CI=1) =="
ci_rc=0
CHECK_SHA="$(git rev-parse HEAD)" bash scripts/check-main-ci.sh || ci_rc=$?
if [[ "$ci_rc" -eq 1 ]]; then
  echo "ci-publish-ready: FAIL — CI red for this SHA" >&2
  exit 1
fi
if [[ "$ci_rc" -eq 2 && "${REQUIRE_MAIN_CI:-0}" == "1" ]]; then
  echo "ci-publish-ready: FAIL — CI inconclusive and REQUIRE_MAIN_CI=1" >&2
  exit 1
fi
if [[ "$ci_rc" -eq 2 ]]; then
  echo "ci-publish-ready: WARN — CI inconclusive for this SHA (continuing; set REQUIRE_MAIN_CI=1 to hard-fail)"
fi

echo "== publish dry-run =="
cargo publish --dry-run

echo "== day-1 README/ADOPTION/guide/migrate flip preflight =="
DRY_RUN=1 bash scripts/post-publish-readme.sh >/tmp/pl-readme-flip-dry.log
tail -2 /tmp/pl-readme-flip-dry.log

echo "== ADOPTION crates.io flip rehearsal =="
DRY_RUN=1 bash scripts/post-publish-adoption.sh >/tmp/pl-adoption-flip-dry.log
tail -2 /tmp/pl-adoption-flip-dry.log

echo "== guide crates.io flip rehearsal =="
DRY_RUN=1 bash scripts/post-publish-guide.sh >/tmp/pl-guide-flip-dry.log
tail -2 /tmp/pl-guide-flip-dry.log

echo "== migrate crates.io flip rehearsal =="
DRY_RUN=1 bash scripts/post-publish-migrate.sh >/tmp/pl-migrate-flip-dry.log
tail -2 /tmp/pl-migrate-flip-dry.log

echo "== day-1 after-publish rehearsal (no crates.io wait) =="
# Live cut runs this *before* publish. Absent crate + DRY_RUN is PARTIAL/2 —
# capture so token-day owner-cut-release cannot abort on the expected miss.
day1_rc=0
DRY_RUN=1 bash scripts/day1-after-publish.sh || day1_rc=$?
if [[ "$day1_rc" -eq 2 ]]; then
  echo "ci-publish-ready: PARTIAL — day1 DRY_RUN not yet Installable (expected pre-publish; rehearsal held)"
elif [[ "$day1_rc" -ne 0 ]]; then
  echo "ci-publish-ready: FAIL — day1 DRY_RUN rc=${day1_rc}" >&2
  exit "$day1_rc"
fi

echo "== first-publish Actions alternate (DRY_RUN visibility) =="
# Same already-Installable refuse as cut-path + branch-lite — token-day
# publish-ready must not soft-OK a re-dispatch rehearsal.
dispatch_rc=0
DRY_RUN=1 bash scripts/owner-dispatch-first-publish.sh || dispatch_rc=$?
if [[ "$dispatch_rc" -eq 2 ]]; then
  echo "ci-publish-ready: PARTIAL — first-publish DRY_RUN already Installable (re-dispatch refused; handoff re-entry)"
elif [[ "$dispatch_rc" -ne 0 ]]; then
  echo "ci-publish-ready: FAIL — first-publish DRY_RUN rc=${dispatch_rc}" >&2
  exit "$dispatch_rc"
fi

echo "== post-Installable handoff rehearsal (DRY_RUN) =="
# Token-day publish-ready must surface parks-on-main / day1 handoff honesty
# (same as cut-path + branch-lite). Nested bars skipped — PRE_PUBLISH bars run below.
handoff_rc=0
HANDOFF_FROM_BARS=1 DRY_RUN=1 bash scripts/owner-post-installable-handoff.sh || handoff_rc=$?
if [[ "$handoff_rc" -eq 2 ]]; then
  echo "ci-publish-ready: PARTIAL — handoff DRY_RUN soft-failed (parks-on-main / day1 / TP; handoff re-entry)"
elif [[ "$handoff_rc" -ne 0 ]]; then
  echo "ci-publish-ready: FAIL — handoff DRY_RUN rc=${handoff_rc}" >&2
  exit "$handoff_rc"
fi

echo "== adopter pin =="
bash scripts/check-adopter-pin.sh

echo "== workflow YAML =="
bash scripts/check-workflows.sh

echo "== tip-delta classifier (cut/sync trust guard) =="
bash scripts/check-tip-delta.sh

echo "== post-cut parks stack rehearsal =="
parks_rc=0
bash scripts/check-post-cut-parks-stack.sh || parks_rc=$?
if [[ "$parks_rc" -ne 0 ]]; then
  if [[ "${AUTO_REFRESH_PARKS:-0}" == "1" ]]; then
    echo "ci-publish-ready: parks lag tip — AUTO_REFRESH_PARKS=1 refreshing chain"
    caller_branch="$(git rev-parse --abbrev-ref HEAD)"
    bash scripts/refresh-post-cut-parks.sh
    bash scripts/check-post-cut-parks-stack.sh
    # refresh ends on civilization tip; restore caller (usually main) for publish.
    if [[ -n "$caller_branch" && "$caller_branch" != "HEAD" \
        && "$(git rev-parse --abbrev-ref HEAD)" != "$caller_branch" ]]; then
      echo "ci-publish-ready: restoring branch ${caller_branch} after parks refresh"
      git checkout "$caller_branch"
    fi
  else
    echo "ci-publish-ready: parks stack failed (set AUTO_REFRESH_PARKS=1 to refresh, or: bash scripts/refresh-post-cut-parks.sh)" >&2
    exit "$parks_rc"
  fi
fi

echo "== Trusted Publishing workflow shape =="
bash scripts/check-trusted-publishing-ready.sh

echo "== merge/tag readiness =="
bash scripts/check-merge-ready.sh

echo "== civilization bars (pre-publish) =="
PRE_PUBLISH=1 bash scripts/audit-civilization-bars.sh

echo "== civilization check =="
REQUIRE_BROKER="${REQUIRE_BROKER:-0}" bash scripts/ci-civilization-check.sh

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
echo
if [[ "${day1_rc:-0}" -eq 2 || "${dispatch_rc:-0}" -eq 2 || "${handoff_rc:-0}" -eq 2 ]]; then
  if bash scripts/check-installable.sh >/dev/null 2>&1; then
    # Installable proven — post-cut PARTIAL must not soft-green publish-ready.
    echo "ci-publish-ready: PARTIAL for partitionline ${ver} — Installable already met; post-cut re-entry (day1/dispatch/handoff)" >&2
    exit 2
  fi
  echo "ci-publish-ready: ok with PARTIAL for partitionline ${ver} — pre-token rehearsal; Installable still blocked on CARGO_REGISTRY_TOKEN (cut still publishes first)"
else
  echo "ci-publish-ready: ok for partitionline ${ver}"
fi
echo
bash scripts/owner-status.sh || true
echo
echo "Next (owner):"
echo "  1. Ensure CARGO_REGISTRY_TOKEN is set (Cloud Agent env + Actions secret)"
echo "  2. Preferred (FF-merge tip → main + local publish; bypasses starved Actions):"
echo "       bash scripts/owner-finish-installable.sh"
echo "  3. Or: merge civilization → main, then bash scripts/owner-cut-release.sh"
echo "       # token in-env → local publish (auto); PUBLISH_LOCAL=0 → tag → Actions"
echo "  4. Confirm https://crates.io/crates/partitionline/${ver}"
echo "  5. README/ADOPTION/guide/migrate crates.io lines (day1) + bash scripts/owner-post-installable-handoff.sh"
