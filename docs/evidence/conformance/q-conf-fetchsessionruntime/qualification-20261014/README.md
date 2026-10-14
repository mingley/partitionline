# KL01-24 qualification 2026-10-14

Pinned Maven `kafka-clients` jars built 50 Fetch v12 session bodies
(10 per peer, 0 unsupported) for topic `pl-session`. Two independent
generations per peer matched byte-for-byte. Rust decoded every body
leftover-empty and rejected every truncated prefix.

Named cells follow hashed `FetchSessionHandler.handleResponse` and
`FetchMetadata`: full INITIAL `(0,0)`, incremental `(91,1)` with an
empty topic list, forgotten partition 1 at `(91,2)`,
`FETCH_SESSION_ID_NOT_FOUND` (70) reset to INITIAL, and
`INVALID_FETCH_SESSION_EPOCH` (71) close-existing-attempt-new `(91,0)`.

A loopback broker served the Apache Fetch response bytes and advertised
FETCH max v12. Public Consumer replay on the 4.1.0 and 4.3.1 peers sent
INITIAL (not LEGACY), then incremental session 91 epoch 1, then INITIAL
again after NOT_FOUND.

When the same jars are present, Apache parsers reverse-read four named
cells per peer (20 parses).

The registered `feature-fetch-incremental-session-runtime` row stays
`not_run` at `cb7e97d3b92a8555aea34d59266a2990c206395f`. No `cases.json`
rows were added. `records.rs` and `buf.rs` were not edited.

Do not regenerate this snapshot in place. Re-run
`tests/conformance/java/generate_fetch_session_runtime.py` into a new
directory if the generator or pins change.
