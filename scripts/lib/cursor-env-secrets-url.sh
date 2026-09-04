#!/usr/bin/env bash
# Shared Cursor Cloud Agent env Secrets URL for Installable token injection.
# Override with PARTITIONLINE_CURSOR_ENV_SECRETS_URL when the env moves.
#
# Usage (from other scripts):
#   # shellcheck source=scripts/lib/cursor-env-secrets-url.sh
#   source "$ROOT/scripts/lib/cursor-env-secrets-url.sh"
#   echo "$PARTITIONLINE_CURSOR_ENV_SECRETS_URL"
#
# Or print helper:
#   bash scripts/lib/cursor-env-secrets-url.sh
: "${PARTITIONLINE_CURSOR_ENV_SECRETS_URL:=https://cursor.com/dashboard/cloud-agents/environments/e/55ff85be-9e3a-11f1-a7d1-d6b4613131ce}"
export PARTITIONLINE_CURSOR_ENV_SECRETS_URL

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  printf '%s\n' "$PARTITIONLINE_CURSOR_ENV_SECRETS_URL"
fi
