#!/usr/bin/env bash
# Flip docs/guide.md tracing install pin from interim git rc to crates.io after publish.
# Safe to re-run. Does not commit.
#
# DRY_RUN=1 writes to a temp file and asserts the crates.io version+features line.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
mm="${ver%.*}"
dry="${DRY_RUN:-0}"
name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"

OUT="$(python3 - <<'PY' "$name" "$mm" "$ver" "$dry"
import pathlib, re, sys, tempfile, os

name, mm, ver, dry = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "1"
path = pathlib.Path("docs/guide.md")
text = path.read_text()

new_block = f'''```toml
{name} = {{ version = "{mm}", features = ["tracing"] }}
```'''

# Prefer replacing a git-pin tracing stanza; fall back to any version= tracing stanza.
patterns = [
    re.compile(
        rf"```toml\n(?:#.*\n)*{re.escape(name)} = \{{ git = \"[^\"]+\", tag = \"[^\"]+\", features = \[\"tracing\"\] \}}\n(?:#.*\n)*```",
        re.M,
    ),
    re.compile(
        rf"```toml\n(?:#.*\n)*{re.escape(name)} = \{{ version = \"[^\"]+\", features = \[\"tracing\"\] \}}\n(?:#.*\n)*```",
        re.M,
    ),
]
text2 = None
for pat in patterns:
    if pat.search(text):
        text2, n = pat.subn(new_block, text, count=1)
        if n != 1:
            raise SystemExit("post-publish-guide: tracing pin replacement count != 1")
        break
if text2 is None:
    raise SystemExit("post-publish-guide: could not find tracing install toml block in docs/guide.md")

needle = f'{name} = {{ version = "{mm}", features = ["tracing"] }}'
if needle not in text2:
    raise SystemExit(f"post-publish-guide: flipped guide missing {needle!r}")
if f'tag = "v' in text2 and 'features = ["tracing"]' in text2:
    # Still have a live git tracing pin somewhere — refuse soft-flip.
    live_git = re.search(
        rf'^{re.escape(name)} = \{{ git = .*features = \["tracing"\]',
        text2,
        re.M,
    )
    if live_git:
        raise SystemExit("post-publish-guide: live git tracing pin remains after flip")

if dry:
    fd, tmp = tempfile.mkstemp(prefix="pl-guide-flip-", suffix=".md")
    os.close(fd)
    pathlib.Path(tmp).write_text(text2)
    print(f"post-publish-guide: DRY_RUN ok ({needle}; wrote {tmp})")
    print(tmp)
else:
    path.write_text(text2)
    print(f'post-publish-guide: guide now shows {needle} ({ver} on crates.io)')
PY
)"

printf '%s\n' "$OUT"
