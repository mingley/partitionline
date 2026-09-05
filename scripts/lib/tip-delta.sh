#!/usr/bin/env bash
# Shared tip↔main delta classifier for civilization cut/sync guards.
#
# Docs/scripts-only tip drift is allowed (and tip→main thrash is refused) while
# Installable waits on CARGO_REGISTRY_TOKEN. Code/library tip drift must not be
# published via PUBLISH_LOCAL until main Verifiable CI has covered that tip SHA.
#
# Source from other scripts:
#   # shellcheck source=scripts/lib/tip-delta.sh
#   source "$ROOT/scripts/lib/tip-delta.sh"
#
# pl_tip_delta_is_docs_only BASE_SHA TIP_SHA
#   Returns 0 when every changed path is docs/scripts/changelog/readme/templates.
#   Returns 1 when BASE==TIP (empty delta) or any non-docs path changed.
#   Callers should treat equal SHAs separately (nothing to classify).

pl_tip_path_is_docs_only() {
  case "$1" in
    docs/*|scripts/*|CHANGELOG.md|README.md|.github/PULL_REQUEST_TEMPLATE.md|.github/ISSUE_TEMPLATE/*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

pl_tip_delta_is_docs_only() {
  local base_sha="$1"
  local tip_sha="$2"
  local f
  if [[ "$base_sha" == "$tip_sha" ]]; then
    return 1
  fi
  while IFS= read -r f; do
    [[ -z "$f" ]] && continue
    if ! pl_tip_path_is_docs_only "$f"; then
      return 1
    fi
  done < <(git diff --name-only "$base_sha" "$tip_sha")
  return 0
}
