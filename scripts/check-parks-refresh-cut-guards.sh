#!/usr/bin/env bash
# Guard: parks auto-refresh must not leave the Installable cut on non-main.
#
# refresh-post-cut-parks ends on the civilization tip. owner-finish-installable
# and ci-publish-ready must restore main/caller before cut/publish, or
# owner-cut-release / owner-publish refuse the branch.
#
# Usage:
#   bash scripts/check-parks-refresh-cut-guards.sh
#   bash scripts/check-parks-refresh-cut-guards.sh --self-test   # same (always a unit)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail() { echo "check-parks-refresh-cut-guards: FAIL — $*" >&2; exit 1; }
ok() { echo "check-parks-refresh-cut-guards: ok — $*"; }

finish="$ROOT/scripts/owner-finish-installable.sh"
ready="$ROOT/scripts/ci-publish-ready.sh"
[[ -f "$finish" ]] || fail "missing $finish"
[[ -f "$ready" ]] || fail "missing $ready"

# Finish: refresh then restore main (order matters).
awk '
  /refresh-post-cut-parks\.sh/ { refresh=NR }
  /restoring main after parks refresh/ { restore=NR }
  END {
    if (!refresh) { print "missing refresh-post-cut-parks in owner-finish-installable"; exit 2 }
    if (!restore) { print "missing restore-main after parks refresh in owner-finish-installable"; exit 3 }
    if (restore < refresh) { print "restore-main appears before refresh in owner-finish-installable"; exit 4 }
  }
' "$finish" || fail "finish script parks→main restore order broken"

# Finish live cut must pass AUTO_REFRESH_PARKS into cut-release.
grep -qF 'AUTO_REFRESH_PARKS=1' "$finish" \
  || fail "owner-finish-installable must set AUTO_REFRESH_PARKS=1 for cut"

# ci-publish-ready: capture caller, refresh, restore caller.
awk '
  /caller_branch=.*rev-parse --abbrev-ref HEAD/ { capture=NR }
  /refresh-post-cut-parks\.sh/ { if (!refresh) refresh=NR }
  /restoring branch .* after parks refresh/ { restore=NR }
  END {
    if (!capture) { print "missing caller_branch capture in ci-publish-ready"; exit 2 }
    if (!refresh) { print "missing refresh-post-cut-parks in ci-publish-ready"; exit 3 }
    if (!restore) { print "missing restore caller_branch in ci-publish-ready"; exit 4 }
    if (!(capture < refresh && refresh < restore)) {
      print "ci-publish-ready order must be capture → refresh → restore"
      exit 5
    }
  }
' "$ready" || fail "ci-publish-ready parks→caller restore order broken"

ok "finish restores main after parks refresh; publish-ready restores caller"
exit 0
