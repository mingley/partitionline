# Controller shutdown join candidate

This directory preserves a source-only candidate for a repeated-shutdown failure in `ControllerHandler`. On the pinned source, an actor panic can leave an already completed join handle in the handler. A later shutdown can then poll that handle again and panic. This is a source-derived finding; it has not been reproduced by running Rust tests.

The candidate keeps a pending join handle owned across cancellation, records the completed result once, and reports a stored actor failure to later shutdown callers. It releases the join mutex before dropping the task's panic payload. If that payload's destructor panics, the first caller can still panic; the recorded failure remains available to later callers. Panic-abort terminates the process.

The archive contains the exact baseline and candidate source files, patches, five synthetic controls, and the author's receipt and verifier. The separate independent review checks those source artifacts and identifies the native work still needed. The baseline and fixed test outcomes are predictions: neither source was compiled or executed here. The synthetic controls do not exercise the real controller, WAL, transport, or the separate queue-capacity design. They establish no production, release, or performance claim.

The source preimage is `partitionline-broker/src/raft/protocol.rs` at commit `6fa8cdb3588d884580d01331e44df657c02f7670`, SHA-256 `47ae96c84080813e4b81cfbfcdbe8bd45e48389c13c8562c0963dfc743137127`. The candidate patch inside the archive has SHA-256 `879595b42050f6ba7a5995d341263e6a163e64ca2e35420fd10cf0eb910beaef`. Hosted baseline/fixed tests and the existing broker validation gates remain necessary before adoption.
