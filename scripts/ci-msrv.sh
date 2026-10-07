#!/usr/bin/env bash
# Compatibility entry point for existing callers; validation now uses stable.
set -euo pipefail
exec bash "$(dirname "${BASH_SOURCE[0]}")/ci-stable.sh" "$@"
