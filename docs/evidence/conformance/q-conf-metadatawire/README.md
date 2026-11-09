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
more nested shapes and live-broker behavior remain open.

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

The later runner adds a bounded timeout-cleanup control and the workflow adds
path-filtered main and pull request triggers plus manual dispatch. Results for
that changed source must be recorded separately. The runner checks that each
owned process group is gone without reading `/proc`, and on a timeout
escalates from TERM to KILL. A process that escapes its group, permission
errors, kernel failures or abrupt termination of the runner can prevent
cleanup from being proved.
