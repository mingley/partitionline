#!/usr/bin/env bash
# One-shot cut-path readiness: preflight + tip-delta + post-cut parks stack.
# Does not publish. Expect READY_EXCEPT_TOKEN until CARGO_REGISTRY_TOKEN is set.
#
# Usage:
#   bash scripts/check-cut-path.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== check-cut-path: Installable preflight =="
bash scripts/check-installable-preflight.sh

echo
echo "== check-cut-path: tip-delta (docs/scripts-only vs main) =="
bash scripts/check-tip-delta.sh

echo
echo "== check-cut-path: post-cut parks stack =="
bash scripts/check-post-cut-parks-stack.sh

echo
echo "== check-cut-path: Trusted Publishing workflow shape =="
bash scripts/check-trusted-publishing-ready.sh

echo "== check-cut-path: finish DRY_RUN (tip-aware parks, hard-fail) =="
DRY_RUN=1 bash scripts/owner-finish-installable.sh

echo
echo "check-cut-path: OK — cut path rehearsed; blocked only on CARGO_REGISTRY_TOKEN if preflight said READY_EXCEPT_TOKEN"
