# KL01-22 qualification 2026-10-14

Pinned Maven `kafka-clients` jars built 75 Fetch v12 and
OffsetCommit/OffsetFetch v8 bodies (15 per peer, 0 unsupported). Two
independent generations per peer matched byte-for-byte. Rust decoded
every body offline, rejected every truncated Fetch prefix, leftover-empty
decoded OffsetCommit/OffsetFetch v8, and re-encoded those offset bodies.

A loopback broker served the Apache Fetch response bytes and advertised
FETCH max v12. Public Consumer APIs replayed A01–A05 on the 4.1.0 and
4.3.1 peers: offset filter `[2]`, read-committed `[2]`, partial retry
`[(0,0),(1,0)]` with a p1-only second body, `OFFSET_OUT_OF_RANGE` at
position 0, and `max_poll_records=1` with `position()` 1 /
`fetch_cursor()` 3.

When the same jars are present, Apache parsers reverse-read four named
cells per peer (20 parses).

The five `audit-regression-a0*` rows stay `failed` at
`cb7e97d3b92a8555aea34d59266a2990c206395f`. No `cases.json` rows were
added. `records.rs`, `buf.rs`, and `consumer.rs` were not edited.

Do not regenerate this snapshot in place. Re-run
`tests/conformance/java/generate_fetch_delivery_regressions.py` into a
new directory if the generator or pins change.
