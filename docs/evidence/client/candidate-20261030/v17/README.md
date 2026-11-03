# Client candidate v17

V17 keeps the v16 Rust source catalogs, patches, deadline fixture, cases, controls and timing. It updates the qualification helper to display a bounded numeric timeout phase for one retained-backing test when the expected line is present and unambiguous. The v16 failure artifact was unavailable, so its timeout phase is unknown.

Libtest output is unframed, so the phase attribution is best effort. The display does not redact secrets. The candidate has not been qualified by a v17 native run. The pending-queue benchmark remains inactive until its v17 bindings and full same-job correctness gate are reviewed.

The previous candidate and its archived evidence remain in the sibling v16 directory.
