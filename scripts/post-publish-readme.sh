#!/usr/bin/env bash
# Flip README install stanza from git to crates.io after a successful publish.
# Safe to re-run. Does not commit (owner reviews the diff).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
major_minor="${ver%.*}"

python3 - <<'PY' "$major_minor"
import pathlib, sys, re
mm = sys.argv[1]
path = pathlib.Path("README.md")
text = path.read_text()
new_block = f'''```toml
[dependencies]
partitionline = "{mm}"
```'''
# Replace the install toml fence (first dependencies block in README).
pat = re.compile(r"```toml\n\[dependencies\]\n.*?\n```", re.S)
if not pat.search(text):
    raise SystemExit("post-publish-readme: could not find README install toml block")
text2, n = pat.subn(new_block, text, count=1)
if n != 1:
    raise SystemExit("post-publish-readme: expected one install block replacement")
path.write_text(text2)
print(f"post-publish-readme: README now shows partitionline = \"{mm}\"")
PY
