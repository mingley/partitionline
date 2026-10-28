# Transaction marker recovery candidate

This candidate keeps the existing marker event format and history semantics.
It checks each event against the configured recovery fetch budget before
writing it, and limits recovery batches to the configured journal index cap.
An event that would be too large to fetch returns `InvalidLimits` before it is
written; a smaller event can still be accepted under the same configuration.

`log_end_offset`, `last_stable_offset`, and `read_committed` now return
`Result` and report `Poisoned` after an uncertain append. Reopen the log to
recover. A trailing-marker index avoids rescanning the entire history for each
marker and replayed event. Its logical entries are bounded by retained journal
records and the journal index cap; allocated capacity and total process memory
are not bounded by that count. No performance claim follows without measurement.

Independent source review identified the poisoned-read and recovery-limit
failures, the prior history scan's quadratic replay risk, and the constraints on
compacting this log. Its `read_committed(from)` contract exposes committed
history from arbitrary offsets, and marker retries return their original
offsets. Bounded deletion therefore requires an explicit retention boundary,
out-of-range reads, absolute offsets, durable marker decision state, and a
policy for producer epochs. Unresolved transactions can pin the stable boundary.
This marker component does not itself fence producers or coordinate transaction
timeouts and marker propagation. The [event compaction design](capture/event-compaction-design.md)
describes the remaining work; this change does not implement retention or
remove the finite-history limit.

On Linux x86_64 with stable Rust, 8 selected marker unit tests passed with
default and all features. Strict Clippy passed for the all-features broker
library and fault-campaign target; the long fault campaign was not run. Tests
cover exact 28-, 34-, and 35-byte event fetch limits, a small journal index,
interleaved retries and recovery, and injected errors reported after a
successfully synchronized data or marker append. After an injected ambiguous
failure, reads and writes fail until reopen; the tests check the durable record
and its visibility on recovery. Injected errors do not simulate power loss or
certify filesystem or hardware durability.

The checks ran in a sparse worktree based on `d0f185f12` with uncommitted
changes to the marker log, a test-only journal failure hook, the fault-campaign
caller and broker README. The [patch](capture/candidate.diff) records the
changes. Receipts include commands, compiler, source hashes, sparse worktree
status, and matching pre/post inputs; all captured broker source hashes were
checked against all receipts. They do not exhaustively identify external tools,
OS or cached dependency bytes. Both test executables were copied from the build
target after checking their receipt hashes; the runner itself retained only
hashes at execution time.

The patch was then integrated into the main worktree and the selected tests
were repeated: 8 passed under default features, 8 under all features, and
strict all-features library/fault-campaign Clippy passed. All three integrated
receipts report stable inputs at base commit
`1e2c3506dcb9b3b76acb27f95130de8e28b6c9bd`; they record a dirty worktree and
therefore do not identify a clean commit. The integrated marker, journal and
fault-campaign inputs in `integrated-source/` match all three receipt hashes.
The merged broker README was captured after the runs; the runner did not hash
that file during the tests. Both integrated executables were copied later after
checking their receipt hashes. The fault campaign was linted, not run.

These are focused checks, not a full broker suite, sustained fault campaign,
independent compatibility run, performance comparison or production
qualification. Run the broader gates against a committed integrated source.

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/marker-recovery-20261028
```
