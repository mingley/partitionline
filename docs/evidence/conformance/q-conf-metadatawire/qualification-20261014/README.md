# KL01-20 qualification 2026-10-14

Pinned Maven `kafka-clients` jars built 432 Metadata request/response
bodies and one 3.9.1 unsupported receipt. Two independent generations
per peer matched byte-for-byte. Rust decoded every body offline, checked
version-gated fields, rejected every truncated prefix, and re-encoded
empty all-topics requests plus untagged responses.

When the same jars are present, Rust-encoded error-topic bodies for
every crate-spoken ∩ pin-supported version parse with each peer's
`MetadataRequestData` / `MetadataResponseData` readers (64 parses).

3.9.1 v13 remains `UnsupportedVersionException`. Original
registered-case dispositions are unchanged. No `cases.json` rows were
added. `records.rs` and `buf.rs` were not edited.

Do not regenerate this snapshot in place. Re-run
`tests/conformance/java/generate_metadata_wire.py` into a new directory
if the generator or pins change.
