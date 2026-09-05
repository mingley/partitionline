#!/usr/bin/env bash
# Flip docs/migrate-from-rdkafka.md install pin from interim git rc to crates.io after publish.
# Safe to re-run. Does not commit.
#
# DRY_RUN=1 writes to a temp file and asserts crates.io is the primary after-line.
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
path = pathlib.Path("docs/migrate-from-rdkafka.md")
text = path.read_text()

new_block = f'''```toml
# before
rdkafka = {{ version = "0.39", features = ["cmake-build"] }}

# after (crates.io {ver})
{name} = "{mm}"
```'''

pat = re.compile(r"```toml\n# before\nrdkafka = \{.*?\n```", re.S)
if not pat.search(text):
    raise SystemExit("post-publish-migrate: could not find before/after toml block")
text2, n = pat.subn(new_block, text, count=1)
if n != 1:
    raise SystemExit("post-publish-migrate: expected one dependency block replacement")

needle = f'{name} = "{mm}"'
if needle not in text2:
    raise SystemExit(f"post-publish-migrate: flipped migrate missing {needle!r}")
# Refuse leaving an uncommented git pin as the primary after-line.
if re.search(rf'^{re.escape(name)} = \{{ git =', text2, re.M):
    raise SystemExit("post-publish-migrate: live git pin remains after flip")

if dry:
    fd, tmp = tempfile.mkstemp(prefix="pl-migrate-flip-", suffix=".md")
    os.close(fd)
    pathlib.Path(tmp).write_text(text2)
    print(f"post-publish-migrate: DRY_RUN ok ({needle}; wrote {tmp})")
    print(tmp)
else:
    path.write_text(text2)
    print(f'post-publish-migrate: migrate now shows {needle} ({ver} on crates.io)')
PY
)"

printf '%s\n' "$OUT"
