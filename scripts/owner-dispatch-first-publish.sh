#!/usr/bin/env bash
# The first-publish workflow and its dispatch interface are retired.
set -euo pipefail

if [[ "${1:-}" == "--self-test" ]]; then
  for workflow in first-publish.yml 'First publish (crates.io) [RETIRED]' 12345 release.yml; do
    rc=0
    WORKFLOW="$workflow" bash "$0" >/dev/null 2>&1 || rc=$?
    if [[ "$rc" -ne 2 ]]; then
      echo "owner-dispatch-first-publish: self-test FAIL — retired dispatcher accepted a workflow override" >&2
      exit 1
    fi
  done
  echo "owner-dispatch-first-publish: self-test OK — retired dispatcher refuses all workflow overrides"
  exit 0
fi

echo "owner-dispatch-first-publish: PARTIAL — first-publish dispatch is retired; follow docs/RELEASE.md" >&2
echo "  For an already published version, use scripts/owner-post-installable-handoff.sh after registry and consumer verification." >&2
exit 2
