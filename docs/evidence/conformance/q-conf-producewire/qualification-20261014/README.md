# KL01-18 qualification 2026-10-14

Pinned Maven `kafka-clients` jars built 364 Produce request/response
bodies and two 3.9.1 unsupported receipts. Two independent generations
per peer matched byte-for-byte. Rust decoded every body offline, checked
version-gated fields, rejected every truncated prefix, and re-encoded
empty requests plus untagged responses that the public helpers can emit.

When the same jars are present, Rust-encoded transactional bodies for
every crate-spoken ∩ pin-supported version parse with each peer's
`ProduceRequestData` / `ProduceResponseData` readers (53 parses).

3.9.1 v12 and v13 remain `UnsupportedVersionException`. Original
registered-case dispositions are unchanged. No `cases.json` rows were
added. `records.rs` and `buf.rs` were not edited.

Do not regenerate this snapshot in place. Re-run
`tests/conformance/java/generate_produce_wire.py` into a new directory
if the generator or pins change.
