#!/usr/bin/env bash
# Shared day1 adopter-docs crates.io-shape probe (README + ADOPTION + guide + migrate).
#
# Sourced by handoff / preflight / token-ask so ALREADY_INSTALLABLE surfaces cannot
# soft-green while guide/migrate still lead with the git rc pin.
#
# Usage:
#   # shellcheck source=scripts/lib/adopter-docs-shaped.sh
#   source "$ROOT/scripts/lib/adopter-docs-shaped.sh"
#   pl_adopter_docs_crates_io_shaped || echo git-shaped
#
# Self-test:
#   bash scripts/lib/adopter-docs-shaped.sh --self-test

pl_adopter_docs_crates_io_shaped() {
  grep -qE '^partitionline = "[0-9]' README.md \
    && grep -qE 'partitionline = "[0-9]' docs/ADOPTION.md \
    && grep -qE 'partitionline = \{ version = "[0-9]' docs/guide.md \
    && grep -qE '^partitionline = "[0-9]' docs/migrate-from-rdkafka.md \
    && ! grep -qE '^partitionline = \{ git =' docs/guide.md \
    && ! grep -qE '^partitionline = \{ git =' docs/migrate-from-rdkafka.md
}

if ! (return 0 2>/dev/null); then
  ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  cd "$ROOT"
  if [[ "${1:-}" == "--self-test" ]]; then
    # shellcheck source=/dev/null
    source "$ROOT/scripts/lib/adopter-docs-shaped.sh"
    if ! grep -qF 'pl_adopter_docs_crates_io_shaped' "$ROOT/scripts/lib/adopter-docs-shaped.sh"; then
      echo "adopter-docs-shaped: self-test FAIL — helper missing" >&2
      exit 1
    fi
    # Align expectation with crates.io: absent → git-shaped; present → crates.io-shaped.
    if bash scripts/check-installable.sh >/dev/null 2>&1; then
      if ! pl_adopter_docs_crates_io_shaped; then
        echo "adopter-docs-shaped: self-test FAIL — crates.io Installable but docs still git-shaped" >&2
        exit 1
      fi
      echo "adopter-docs-shaped: self-test OK — helper present; tip crates.io-shaped (Installable)"
      exit 0
    fi
    if pl_adopter_docs_crates_io_shaped; then
      echo "adopter-docs-shaped: self-test FAIL — tip docs crates.io-shaped while crate absent" >&2
      exit 1
    fi
    echo "adopter-docs-shaped: self-test OK — helper present; tip git-shaped (expected pre-Installable)"
    exit 0
  fi
  echo "usage: bash scripts/lib/adopter-docs-shaped.sh --self-test" >&2
  exit 2
fi
