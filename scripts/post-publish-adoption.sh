#!/usr/bin/env bash
# Flip docs/ADOPTION.md Install section from git-pin to crates.io after publish.
# Also marks the crates.io adoption-gap row as done. Safe to re-run. No commit.
#
# DRY_RUN=1 writes to a temp file and asserts crates.io is the primary install.
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
path = pathlib.Path("docs/ADOPTION.md")
text = path.read_text()

install = f"""## Install

`{name}` **{ver}** is on [crates.io](https://crates.io/crates/{name}):

```toml
[dependencies]
{name} = "{mm}"
```

Git tags remain available for bisects; new adopters should prefer crates.io.

"""

# Replace ## Install … (any subtitle) through the next ## heading (exclusive).
install_pat = re.compile(r"## Install[^\n]*\n.*?(?=\n## )", re.S)
if not install_pat.search(text):
    raise SystemExit("post-publish-adoption: could not find ## Install section")
text2, n = install_pat.subn(install, text, count=1)
if n != 1:
    raise SystemExit("post-publish-adoption: expected one Install section replacement")

# Flip the crates.io adoption-gap row if present.
gap_pat = re.compile(r"\| crates\.io release \|[^\n]*\|")
gap_row = f"| crates.io release | **{ver} published** — `{name} = \"{mm}\"` |"
if gap_pat.search(text2):
    text2, gn = gap_pat.subn(gap_row, text2, count=1)
    if gn != 1:
        raise SystemExit("post-publish-adoption: crates.io gap row replacement failed")

install_body = re.search(r"## Install.*?(?=\n## )", text2, re.S)
if install_body and re.search(
    r"remaining owner step|crates\.io publish is the remaining",
    install_body.group(0),
    re.I,
):
    raise SystemExit("post-publish-adoption: Install still says publish is remaining")

needle = f'{name} = "{mm}"'
if needle not in text2:
    raise SystemExit(f"post-publish-adoption: flipped ADOPTION missing {needle!r}")
if f"crates.io/crates/{name}" not in text2:
    raise SystemExit("post-publish-adoption: flipped ADOPTION missing crates.io link")

if dry:
    fd, tmp = tempfile.mkstemp(prefix="pl-adoption-flip-", suffix=".md")
    os.close(fd)
    pathlib.Path(tmp).write_text(text2)
    print(f"post-publish-adoption: DRY_RUN ok ({needle}; wrote {tmp})")
    print(tmp)
else:
    path.write_text(text2)
    print(
        f'post-publish-adoption: ADOPTION now shows {name} = "{mm}" '
        f"({ver} on crates.io)"
    )
PY
)"

printf '%s\n' "$OUT"
