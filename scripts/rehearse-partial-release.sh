#!/usr/bin/env bash
# Rehearse idempotent partial-release recovery. Does not publish and does not
# re-cut 0.1.0. --self-test greps the serialized path plus post-publish
# day1/handoff recovery (publish succeeded, day1 pending).
#
# Usage:
#   bash scripts/rehearse-partial-release.sh --self-test
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

case "${1:-}" in
  ""|--self-test) ;;
  *)
    echo "usage: bash scripts/rehearse-partial-release.sh --self-test" >&2
    echo "rehearse-partial-release: refuses other args (never publishes)" >&2
    exit 1
    ;;
esac

fail() {
  echo "rehearse-partial-release --self-test: FAIL — $*" >&2
  exit 1
}

# 1. Cargo.toml is 0.1.0 — recovery must not bump/re-cut it.
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
[[ "$ver" == "0.1.0" ]] || fail "Cargo.toml version is ${ver}; recovery must not bump/re-cut 0.1.0"

# 2. release-plz is PR-only: command: release-pr, no token-gated publish / command: release.
plz=".github/workflows/release-plz.yml"
[[ -f "$plz" ]] || fail "missing ${plz}"
grep -qF 'command: release-pr' "$plz" \
  || fail "release-plz.yml missing command: release-pr"
if grep -qE 'command:[[:space:]]*"?release"?[[:space:]]*$' "$plz"; then
  fail "release-plz.yml has command: release (would publish)"
fi
if grep -qE 'if:.*CARGO_REGISTRY_TOKEN' "$plz"; then
  fail "release-plz.yml has token-gated job (would publish)"
fi
if grep -qF 'CARGO_REGISTRY_TOKEN' "$plz"; then
  fail "release-plz.yml still mentions CARGO_REGISTRY_TOKEN (would publish)"
fi
if grep -qE 'cargo[[:space:]]+publish' "$plz"; then
  fail "release-plz.yml contains cargo publish (would publish)"
fi

# 3. Canonical publisher: release.yml (cargo publish) + owner-cut-release.
rel=".github/workflows/release.yml"
cut="scripts/owner-cut-release.sh"
ready="scripts/ci-publish-ready.sh"
[[ -f "$rel" ]] || fail "missing ${rel}"
[[ -f "$cut" ]] || fail "missing ${cut}"
[[ -f "$ready" ]] || fail "missing ${ready}"
grep -qE 'cargo[[:space:]]+publish' "$rel" \
  || fail "release.yml missing cargo publish (canonical publisher)"
grep -qF 'owner-publish.sh' "$cut" \
  || fail "owner-cut-release.sh missing owner-publish.sh (canonical publisher)"

# 4. Exact-SHA CI + crate-consumer required on release.yml / cut / publish-ready.
for f in "$rel" "$cut" "$ready"; do
  grep -qF 'check-main-ci.sh' "$f" \
    || fail "${f} missing check-main-ci.sh (exact-SHA CI required)"
done
grep -qF 'ci-crate-consumer.sh' "$rel" \
  || fail "release.yml missing ci-crate-consumer.sh"
grep -qF 'ci-crate-consumer.sh' "$ready" \
  || fail "ci-publish-ready.sh missing ci-crate-consumer.sh"
# cut-release consumer evidence is the publish-ready gate (includes ci-crate-consumer).
grep -qF 'ci-publish-ready.sh' "$cut" \
  || fail "owner-cut-release.sh missing ci-publish-ready.sh (crate-consumer gate)"
grep -qF 'CHECK_SHA=' "$rel" \
  || fail "release.yml missing CHECK_SHA= (exact-SHA CI)"

# 5. Already-on-crates.io skip language in release.yml.
grep -qF 'skip=1' "$rel" \
  || fail "release.yml missing skip=1 (already-on-crates.io skip)"
grep -qi 'already on crates.io' "$rel" \
  || fail "release.yml missing already-on-crates.io skip language"

# 6. Simulated "publish succeeded, day1 pending" → day1/handoff DRY_RUN, not another publish.
day1="scripts/day1-after-publish.sh"
handoff="scripts/owner-post-installable-handoff.sh"
[[ -f "$day1" ]] || fail "missing ${day1}"
[[ -f "$handoff" ]] || fail "missing ${handoff}"
grep -qF 'day1-after-publish.sh' "$cut" \
  || fail "owner-cut-release missing day1-after-publish (recovery would not call day1)"
grep -qF 'owner-post-installable-handoff' "$cut" \
  || fail "owner-cut-release missing handoff (recovery would not call handoff)"
grep -qF 'DRY_RUN' "$day1" \
  || fail "day1-after-publish missing DRY_RUN"
grep -qF 'DRY_RUN' "$handoff" \
  || fail "owner-post-installable-handoff missing DRY_RUN"
if grep -qE 'cargo[[:space:]]+publish' "$day1"; then
  fail "day1-after-publish contains cargo publish (would publish)"
fi
if grep -qE 'cargo[[:space:]]+publish' "$handoff"; then
  fail "handoff contains cargo publish (would publish)"
fi
# After crates.io wait, cut must reach day1 + handoff — not another publish.
if ! awk '
  /wait for crates.io/ { wait=NR }
  /day1-after-publish\.sh/ { if (wait && NR > wait) day1=NR }
  /pl_cut_run_handoff/ { if (wait && NR > wait) hand=NR }
  /cargo publish/ { if (wait && NR > wait) pub=NR }
  END { exit (wait && day1 && hand && !pub) ? 0 : 1 }
' "$cut"; then
  fail "publish-succeeded recovery is not day1/handoff (would publish or skip recovery)"
fi

echo "rehearse-partial-release: simulate publish-succeeded, day1 pending"
echo "  would: DRY_RUN=1 bash scripts/day1-after-publish.sh"
echo "  would: DRY_RUN=1 bash scripts/owner-post-installable-handoff.sh"
echo "  would not: cargo publish / version bump / re-cut 0.1.0"

echo "rehearse-partial-release: --self-test OK — 0.1.0 stays; release-plz PR-only; canonical release.yml + owner-cut-release; exact-SHA CI + crate-consumer; crates.io skip; day1/handoff DRY_RUN not another publish"
exit 0
