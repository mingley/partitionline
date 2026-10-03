The first additive live-probe example was checked on the complete pushed
`2dc90b471b384e88741a3d8db97963530a754d97` source plus only that example overlay.
Its source SHA4890873ef6cb5efcb218d689f65e675c1f0eb7d29e9331d4d011123851ff3651
is preserved in rejected-example.rs. Three commands passed: stable default
check/strict Clippy and stable all-features check. Stable all-features strict
Clippy failed101 because OIDC Limits fields were assigned after Default. There
were no behavior tests or live connections. Eight complete70,818-file byte/full
permission-mode checks preserve the unchanged inputs around all four attempts.

Every current generated ELF was losslessly retained immediately after every
command, including the failed command, before further compilation. Compiler
toolchain provenance and safe disk samples are recorded; no failing example
executable was run. The raw source audit is sealed0600 scratch, with lossless
gzip roundtrip/SHA/mode/restore mapping in validation.json.

The field assignments were replaced with an equivalent struct initializer in
the separate corrected attempt. This failed cohort remains retained and is not
part of that fresh10-command passing qualification.
