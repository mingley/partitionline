#!/usr/bin/env bash
# Pre-merge / pre-tag readiness for civilization → main → vX.Y.Z.
# Does not publish and does not require CARGO_REGISTRY_TOKEN.
#
# Exit 0 only when tip is structurally ready to merge and tag.
# Optional FULL=1 also runs ci-branch-lite (tip Verifiable proxy).
#
# Usage:
#   bash scripts/check-merge-ready.sh
#   FULL=1 bash scripts/check-merge-ready.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail=0
ok() { echo "OK  $*"; }
bad() { echo "FAIL  $*" >&2; fail=1; }
warn() { echo "WARN  $*"; }

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
msrv="$(sed -n 's/^rust-version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
head_sha="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

echo "check-merge-ready: ${name} ${ver} @ ${head_sha} (${branch})"
echo

if [[ -n "$(git status --porcelain 2>/dev/null || true)" ]]; then
  bad "working tree is dirty — commit or stash before merge/tag"
else
  ok "working tree clean"
fi

if [[ "$ver" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  ok "Cargo.toml version is final ${ver}"
else
  bad "Cargo.toml version '${ver}' is not final X.Y.Z"
fi

if [[ -n "$msrv" ]]; then
  ok "MSRV declared rust-version=${msrv}"
else
  bad "Cargo.toml missing rust-version (MSRV)"
fi

if grep -q "^## \\[${ver}\\]" CHANGELOG.md; then
  ok "CHANGELOG has ## [${ver}] section"
else
  bad "CHANGELOG.md missing ## [${ver}] section for this cut"
fi

if [[ -f .github/workflows/release.yml ]]; then
  if grep -q 'crates-io-auth-action' .github/workflows/release.yml \
    && grep -q 'CARGO_REGISTRY_TOKEN' .github/workflows/release.yml \
    && grep -q 'id-token: write' .github/workflows/release.yml; then
    ok "release.yml has OIDC trusted publishing + CARGO_REGISTRY_TOKEN fallback"
  else
    bad "release.yml missing OIDC auth action and/or CARGO_REGISTRY_TOKEN fallback"
  fi
  if grep -q 'v\[0-9\]+\\.\[0-9\]+\\.\[0-9\]+' .github/workflows/release.yml \
    || grep -q 'v\[0-9\]+\.\[0-9\]+\.\[0-9\]+' .github/workflows/release.yml; then
    ok "release.yml tag filter is final vX.Y.Z glob"
  else
    warn "could not confirm final-only tag glob in release.yml (inspect manually)"
  fi
else
  bad ".github/workflows/release.yml missing"
fi

if bash scripts/check-workflows.sh >/tmp/pl-merge-workflows.log 2>&1; then
  ok "workflow YAML parses ($(tail -1 /tmp/pl-merge-workflows.log))"
else
  bad "workflow YAML check failed (see /tmp/pl-merge-workflows.log)"
  cat /tmp/pl-merge-workflows.log >&2 || true
fi

if bash scripts/check-adopter-pin.sh >/tmp/pl-merge-adopter.log 2>&1; then
  ok "adopter pin: $(tail -1 /tmp/pl-merge-adopter.log)"
else
  bad "adopter pin failed (see /tmp/pl-merge-adopter.log)"
  cat /tmp/pl-merge-adopter.log >&2 || true
fi

# Tip should be civilization plan (or main itself when already merged).
case "$branch" in
  main|dev/civilization-plan-b686)
    ok "branch ${branch} is an expected publish path"
    ;;
  *)
    warn "branch ${branch} is not main / civilization tip — confirm before tagging"
    ;;
esac

if git rev-parse --verify origin/main >/dev/null 2>&1; then
  ahead="$(git rev-list --count origin/main..HEAD 2>/dev/null || echo 0)"
  behind="$(git rev-list --count HEAD..origin/main 2>/dev/null || echo 0)"
  if [[ "$behind" != "0" ]]; then
    bad "tip is behind origin/main by ${behind} commit(s) — rebase/merge main first"
  else
    ok "tip is not behind origin/main (ahead by ${ahead})"
  fi
else
  warn "origin/main not available locally — fetch before merge"
fi

if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  warn "CARGO_REGISTRY_TOKEN unset (needed for first crates.io publish / Actions secret)"
else
  ok "CARGO_REGISTRY_TOKEN is set in this environment"
fi

code="$(curl -sS -A 'partitionline-check-merge-ready/1' -o /tmp/pl-merge-crates.json -w '%{http_code}' \
  "https://crates.io/api/v1/crates/${name}/${ver}" || true)"
if [[ "$code" == "200" ]]; then
  warn "crates.io already has ${name} ${ver} — tagging again would be a no-op / yank path"
elif [[ "$code" == "404" ]]; then
  ok "crates.io does not yet have ${name} ${ver} (first cut still needed)"
else
  warn "crates.io HTTP ${code} while probing ${name}/${ver}"
fi

if [[ "${FULL:-0}" == "1" ]]; then
  echo
  echo "== FULL=1: tip Verifiable proxy (ci-branch-lite) =="
  if bash scripts/ci-branch-lite.sh >/tmp/pl-merge-branch-lite.log 2>&1; then
    ok "ci-branch-lite"
  else
    bad "ci-branch-lite failed (see /tmp/pl-merge-branch-lite.log)"
    tail -40 /tmp/pl-merge-branch-lite.log >&2 || true
  fi
fi

echo
if [[ "$fail" != "0" ]]; then
  echo "check-merge-ready: FAILED — fix items above before merge/tag" >&2
  exit 1
fi

echo "check-merge-ready: OK — tip is structurally ready to merge and tag v${ver}"
echo
echo "Owner next (after CARGO_REGISTRY_TOKEN + healthy Actions):"
echo "  1. Merge ${branch} → main (PR or fast-forward)"
echo "  2. git fetch origin main && git checkout main && git pull origin main"
echo "  3. git tag -a v${ver} -m '${name} ${ver}'"
echo "  4. git push origin v${ver}"
echo "  5. bash scripts/day1-after-publish.sh && bash scripts/check-installable.sh"
echo "  6. crates.io → Trusted Publishing → GitHub workflow release.yml"
exit 0
