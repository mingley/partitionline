#!/usr/bin/env bash
# Flip README install stanza from git to crates.io after a successful publish.
# Also rewrites the WP-0.5 status blurb so it does not still say "waits on publish".
# Safe to re-run. Does not commit (owner reviews the diff).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
major_minor="${ver%.*}"

python3 - <<'PY' "$major_minor" "$ver"
import pathlib, sys, re
mm, ver = sys.argv[1], sys.argv[2]
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

# Flip any pre-publish status blurb to a crates.io-confirmed line.
status = (
    f"**Status:** partitionline {ver} is on "
    f"[crates.io](https://crates.io/crates/partitionline) "
    f"(`partitionline = \"{mm}\"`). Probe: `bash scripts/check-installable.sh`."
)
# Match from **Status…** through the end of the paragraph (blank line or heading).
status_pat = re.compile(
    r"\*\*Status(?:\s*\([^)]+\))?:\*\*[^\n]*(?:\n(?!\n|#)[^\n]*)*",
)
if status_pat.search(text2):
    text2, sn = status_pat.subn(status, text2, count=1)
    if sn != 1:
        raise SystemExit("post-publish-readme: status blurb replacement failed")
else:
    # Insert after the install fence if no status line exists.
    text2 = text2.replace(new_block, new_block + "\n\n" + status, 1)

path.write_text(text2)
print(f'post-publish-readme: README now shows partitionline = "{mm}" ({ver} on crates.io)')
PY
