# KL01-21 qualification 2026-10-14

Pinned Maven `kafka-clients` jars built 358 ListOffsets request/response
bodies and four historical unsupported receipts (3.9.1 v10/v11,
4.1.0/4.1.2 v11). Two independent generations per peer matched
byte-for-byte. Rust decoded every body offline, checked version-gated
fields, rejected every truncated prefix, and re-encoded empty and
untagged cells.

When the same jars are present, Rust-encoded error-topic bodies for
every crate-spoken ∩ pin-supported version parse with each peer's
`ListOffsetsRequestData` / `ListOffsetsResponseData` readers (51 parses).

3.9.1 v10/v11 and 4.1.x v11 remain `UnsupportedVersionException`.
Original registered-case dispositions are unchanged, including
`proto-unsupported-list-offsets-v11-client-cap`. No `cases.json` rows
were added. `records.rs` and `buf.rs` were not edited.

Do not regenerate this snapshot in place. Re-run
`tests/conformance/java/generate_list_offsets_wire.py` into a new
directory if the generator or pins change.
