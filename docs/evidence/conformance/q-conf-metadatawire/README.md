# Metadata v13 Apache wire comparison

This is a finite body-codec comparison, not a live Kafka test or complete
Metadata conformance. The registered KL01-20 cohort remains open.

Five request/response pairs cover null topics, empty topics, a UTF-8 topic
name, an ID-only request and unknown tags at request, broker, topic and
partition levels. Every response uses top-level error 31, one broker, one
topic and one partition. The Rust decoder checks the represented fields and
consumes the complete body. Rust then re-encodes the known fields, and the
Apache SDK parses and checks them. Unknown tags are intentionally discarded
by the Rust codec. A zero top-level error, historical peers, other versions,
more nested shapes and live-broker behavior remain open. A separate
[boundary comparison](boundaries-20261110.md) covers selected v0–v12 bodies
against five SDKs; v0 is a raw wire case only.

## Previous runs

At source commit
[`32b4096`](https://github.com/mingley/partitionline/commit/32b4096d0bc6b83e31a7f914ef576db600ddbb3c),
the [first run](https://github.com/mingley/partitionline/actions/runs/42702021230)
passed the body comparisons for three current SDKs and both Rust feature
profiles, including the two deliberately corrupted top-level error controls.
The workflow failed its formatting and Clippy checks.

At source commit
[`9f78ecd`](https://github.com/mingley/partitionline/commit/9f78ecd7355b22ab2b967d7e44aa0cb59ba664ca),
the [second run](https://github.com/mingley/partitionline/actions/runs/42702609048)
passed the same body comparisons, formatting and strict test-target Clippy.
It used Rust 1.99.1, Temurin 21.0.13, and SHA-256-pinned Apache 4.1.2, 4.2.1
and 4.3.1 client JARs. For each SDK and each default/all-features profile,
Rust decoded five Java pairs and Java parsed five Rust re-encoded pairs.
The 4.1.2 default profile also rejected deliberate changes from error 31 to 30
in each direction. The run logs contain source and fixture hashes; the
uploaded artifact was not independently inspected.

The [third run](https://github.com/mingley/partitionline/actions/runs/42703525892)
on `ccf5361b255f3ab86a2fc895b87af13d67b7eddb` passed the comparison,
formatting and Clippy, plus a timeout control where a descendant ignored TERM
after its parent exited. Its process group was gone after KILL.

The [fourth run](https://github.com/mingley/partitionline/actions/runs/42704216370)
on `3c462e2b2355c7001ee3eeb1938bb4e0ffce1bbe` also passed and printed the
bounded synthetic corpora. The five [Apache pairs](../../../../tests/fixtures/metadata_wire/apache-pairs.txt) and five
[Rust pairs](../../../../tests/fixtures/metadata_wire/rust-pairs.txt) were extracted from those logs; their independently
recomputed SHA-256 values are respectively
`0f0e18942643b10ef0cd3512c9e97e8b237921cca64906710d8f509ce54cacc9`
and `3a1ef65a8406a184b59112c704a0377d1cea3bf5fd9d514ff8bd81cdbb9e45c4`.
These match the per-peer and per-profile run receipts. The uploaded artifacts
were not independently inspected.

The [fifth run](https://github.com/mingley/partitionline/actions/runs/42705108869)
on `e0868e875fc70cd7fe87972ed48a10606cd13b5a` passed the peer comparison,
byte-for-byte checks against both retained corpora, formatting and strict
Clippy. Runtime-timeout and readiness-timeout controls both observed an
already-exited parent with a TERM-resistant descendant, escalated to KILL and
observed the process group gone. The readiness handshake had a separate
bounded deadline.

The [sixth run](https://github.com/mingley/partitionline/actions/runs/42705755662)
on `9c267881c7e02cd7699e684db8651b90d903d80c` passed the peer matrix, both
timeout controls, formatting and strict test-target Clippy. The ordinary
offline test passed once in each of the default and all-feature profiles in
the checkout and in the extracted crate. This run packaged with
`--no-verify --allow-dirty`. The workflow now requires a clean checkout and
uses `cargo package --locked --no-verify`, then runs only those two targeted
extracted-crate tests.

The [clean-checkout run](https://github.com/mingley/partitionline/actions/runs/42706549956)
on `e3b06f651d78aed3ac39b3ed6f8d0408193a94e8` passed the peer matrix, both timeout controls,
byte-for-byte corpus checks, formatting and strict test-target Clippy. The
offline test passed once in each default and all-feature profile, both in the
checkout and in the extracted crate. Packaging used `--locked --no-verify`
from a clean checkout. These targeted checks are not a full package or release
gate.

The runner checks its owned process groups without reading `/proc` and
escalates from TERM to KILL on timeout. A process that escapes its group,
permission errors, kernel failures or abrupt termination of the runner can
prevent cleanup from being proved.
