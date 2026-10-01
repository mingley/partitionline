# Markdown example gate

`python3 scripts/check-doc-examples.py` (Python 3.12+) packages the crate, extracts every
Rust fence in each document in `registry.json`, and compiles the exact bodies
with rustdoc through a fresh downstream package. The downstream dependency is
the extracted `.crate`, outside the checkout; it has its own resolved lockfile.
Hidden `# ` setup lines follow rustdoc conventions. Plain `rust` and
`rust,no_run` blocks are both compiled without execution, so the gate needs no
broker. All three registered public documents are checked; adding a fence to
one automatically adds a case. The JSON report records each fence's location,
body hash, ignored reason, local-link count and package checksum.

An intentionally schematic fence uses `rust,ignore` with an adjacent
`<!-- doc-example-ignore: explanation -->` comment. These blocks are reported
and never counted as compiled. `compile_fail` and unknown flags are rejected.
The parser supports backtick/tilde fences with zero to three leading spaces.
Link checks cover inline links/images and reference definitions outside code,
local files, ATX heading anchors (including duplicate/Unicode headings) and
explicit HTML anchor IDs. External URLs are not fetched.

Run the checker's positive and negative tests with:

```bash
python3 -B -m unittest discover -s tests/docs -p 'test_*.py'
```

A local development tree can use `--allow-dirty`; CI packages the clean source.
Use `--crate path/to/partitionline-0.1.0.crate` to inspect an existing package,
`--links-only` for navigation checks, or `--report path.json` to retain evidence.
