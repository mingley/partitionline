#!/usr/bin/env bash
# Preserve day1 README/ADOPTION edits across post-cut parks land.
#
# owner-finish-installable flips those docs to crates.io shape, then lands parks
# which need a clean tree. Stash alone can fail to pop after park merges; keep a
# filesystem backup and restore from it if stash pop fails.
#
# Usage (from owner-finish-installable):
#   # shellcheck source=scripts/lib/preserve-day1-docs.sh
#   source "$ROOT/scripts/lib/preserve-day1-docs.sh"
#   pl_day1_docs_begin
#   bash scripts/owner-land-post-cut-parks.sh || parks_rc=$?
#   pl_day1_docs_end
#
# Self-test:
#   bash scripts/lib/preserve-day1-docs.sh --self-test
set -euo pipefail

PL_DAY1_DOCS_BACKUP_DIR="${PL_DAY1_DOCS_BACKUP_DIR:-}"
PL_DAY1_DOCS_STASHED="${PL_DAY1_DOCS_STASHED:-0}"

pl_day1_docs_paths() {
  printf '%s\n' README.md docs/ADOPTION.md
}

pl_day1_docs_dirty() {
  local paths=()
  mapfile -t paths < <(pl_day1_docs_paths)
  [[ -n "$(git status --porcelain -- "${paths[@]}" 2>/dev/null || true)" ]]
}

pl_day1_docs_begin() {
  PL_DAY1_DOCS_BACKUP_DIR=""
  PL_DAY1_DOCS_STASHED=0
  if ! pl_day1_docs_dirty; then
    echo "preserve-day1-docs: no dirty README/ADOPTION — nothing to preserve"
    return 0
  fi
  PL_DAY1_DOCS_BACKUP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pl-day1-docs.XXXXXX")"
  local p
  while IFS= read -r p; do
    if [[ -f "$p" ]]; then
      mkdir -p "$PL_DAY1_DOCS_BACKUP_DIR/$(dirname "$p")"
      cp -a "$p" "$PL_DAY1_DOCS_BACKUP_DIR/$p"
    fi
  done < <(pl_day1_docs_paths)
  echo "preserve-day1-docs: backed up day1 docs to ${PL_DAY1_DOCS_BACKUP_DIR}"
  git stash push -m "pl-finish-day1-docs" -- README.md docs/ADOPTION.md
  PL_DAY1_DOCS_STASHED=1
  echo "preserve-day1-docs: stashed day1 README/ADOPTION before parks land"
}

pl_day1_docs_restore_from_backup() {
  [[ -n "${PL_DAY1_DOCS_BACKUP_DIR}" && -d "${PL_DAY1_DOCS_BACKUP_DIR}" ]] || return 1
  local p
  while IFS= read -r p; do
    if [[ -f "$PL_DAY1_DOCS_BACKUP_DIR/$p" ]]; then
      mkdir -p "$(dirname "$p")"
      cp -a "$PL_DAY1_DOCS_BACKUP_DIR/$p" "$p"
    fi
  done < <(pl_day1_docs_paths)
  echo "preserve-day1-docs: restored day1 README/ADOPTION from filesystem backup"
  return 0
}

pl_day1_docs_end() {
  local restored=0
  if [[ "${PL_DAY1_DOCS_STASHED}" == "1" ]]; then
    if git stash pop --quiet; then
      echo "preserve-day1-docs: restored day1 README/ADOPTION via stash pop"
      restored=1
    else
      echo "preserve-day1-docs: WARN — stash pop failed; trying filesystem backup" >&2
      git reset --quiet -- README.md docs/ADOPTION.md 2>/dev/null || true
      if pl_day1_docs_restore_from_backup; then
        restored=1
      fi
    fi
  elif [[ -n "${PL_DAY1_DOCS_BACKUP_DIR}" && -d "${PL_DAY1_DOCS_BACKUP_DIR}" ]]; then
    if pl_day1_docs_restore_from_backup; then
      restored=1
    fi
  fi
  if [[ -n "${PL_DAY1_DOCS_BACKUP_DIR}" && -d "${PL_DAY1_DOCS_BACKUP_DIR}" ]]; then
    rm -rf "${PL_DAY1_DOCS_BACKUP_DIR}"
  fi
  PL_DAY1_DOCS_BACKUP_DIR=""
  PL_DAY1_DOCS_STASHED=0
  if [[ "$restored" -eq 0 ]]; then
    echo "preserve-day1-docs: nothing to restore (day1 docs were clean before parks)"
  fi
}

pl_day1_docs_self_test() {
  local root="$1"
  echo "preserve-day1-docs: self-test — backup restores after stash is dropped"
  local tmp
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/pl-day1-docs-selftest.XXXXXX")"
  # shellcheck disable=SC2064
  trap "rm -rf '$tmp'" EXIT
  (
    set -euo pipefail
    cd "$tmp"
    git init -q
    git config user.email "partitionline-selftest@example.com"
    git config user.name "partitionline-selftest"
    mkdir -p docs
    printf 'README pin git\n' >README.md
    printf 'ADOPTION pin git\n' >docs/ADOPTION.md
    git add README.md docs/ADOPTION.md
    git commit -q -m "base"
    # Simulate day1 crates.io flip (dirty tree).
    printf 'README crates.io 0.1\n' >README.md
    printf 'ADOPTION crates.io 0.1\n' >docs/ADOPTION.md
    # shellcheck source=/dev/null
    source "$root/scripts/lib/preserve-day1-docs.sh"
    pl_day1_docs_begin
    # Simulate parks rewriting the files on a clean tree.
    printf 'README after parks\n' >README.md
    printf 'ADOPTION after parks\n' >docs/ADOPTION.md
    git add README.md docs/ADOPTION.md
    git commit -q -m "parks"
    # Force the backup path: drop the stash without applying it.
    git stash drop --quiet
    PL_DAY1_DOCS_STASHED=1
    pl_day1_docs_end
    grep -qx 'README crates.io 0.1' README.md
    grep -qx 'ADOPTION crates.io 0.1' docs/ADOPTION.md
  )
  echo "preserve-day1-docs: self-test OK"
}

# CLI driver only when executed as main — not when sourced (including when the
# --self-test subshell re-sources this file via an absolute path that makes
# BASH_SOURCE[0] == $0 and would otherwise fall through to usage/exit 2).
if ! (return 0 2>/dev/null); then
  ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  if [[ "${1:-}" == "--self-test" ]]; then
    pl_day1_docs_self_test "$ROOT"
    exit 0
  fi
  echo "usage: bash scripts/lib/preserve-day1-docs.sh --self-test" >&2
  exit 2
fi
