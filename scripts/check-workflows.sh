#!/usr/bin/env bash
# Fail if any GitHub Actions workflow is not parseable YAML.
#
# Catches the class of bug that made release.yml empty-job-fail on every
# branch push: a flush-left multiline shell string under `run: |` breaks
# YAML, and GitHub records conclusion=failure with zero jobs.
#
# Usage:
#   bash scripts/check-workflows.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "check-workflows: SKIP (python3 not available)" >&2
  exit 0
fi

python3 - <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("check-workflows: SKIP (PyYAML not installed)", file=sys.stderr)
    raise SystemExit(0)

root = Path(".github/workflows")
if not root.is_dir():
    print("check-workflows: FAIL — .github/workflows missing", file=sys.stderr)
    raise SystemExit(1)

files = sorted(root.glob("*.yml")) + sorted(root.glob("*.yaml"))
if not files:
    print("check-workflows: FAIL — no workflow files", file=sys.stderr)
    raise SystemExit(1)


def flush_left_in_run_blocks(path: Path, text: str) -> bool:
    """Return True if a flush-left line appears inside a run: |/> block."""
    lines = text.splitlines()
    i = 0
    bad = False
    while i < len(lines):
        stripped = lines[i].lstrip(" ")
        indent = len(lines[i]) - len(stripped)
        if stripped.startswith("run:") and ("|" in stripped or ">" in stripped):
            base = indent
            saw_content = False
            i += 1
            while i < len(lines):
                line = lines[i]
                if not line.strip():
                    i += 1
                    continue
                cur = len(line) - len(line.lstrip(" "))
                # Flush-left after we have already seen indented content is the
                # day-1 Release-notes failure mode (invalid YAML / empty jobs).
                if cur == 0 and saw_content:
                    print(
                        f"check-workflows: FAIL {path}:{i + 1}: "
                        f"flush-left line inside run block",
                        file=sys.stderr,
                    )
                    bad = True
                    break
                if cur <= base:
                    break
                saw_content = True
                i += 1
            continue
        i += 1
    return bad


failed = 0
for path in files:
    text = path.read_text(encoding="utf-8")
    try:
        yaml.safe_load(text)
    except Exception as exc:  # noqa: BLE001 — surface any parse error
        print(f"check-workflows: FAIL {path}: {exc}", file=sys.stderr)
        failed = 1
        # Still scan for the flush-left pattern to name the line.
        if flush_left_in_run_blocks(path, text):
            pass
        continue
    if flush_left_in_run_blocks(path, text):
        failed = 1
        continue
    print(f"check-workflows: ok {path}")

raise SystemExit(failed)
PY
