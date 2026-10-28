# Group checkpoint candidate: failure handling

This revised candidate replaces finite consumer, share and streams group
snapshot histories with a synchronized, atomically renamed checkpoint before
their index, file or snapshot-count limit blocks another update. The temporary
adjacent pathname is reserved; rollover can temporarily require storage for
both the old journal and one replacement. Checkpoint recovery validates the
state and resumes normal transition checks for subsequent records.

The candidate changes these experimental coordinators' `describe` methods from
`Option<_>` to `Result<Option<_>, Error>`. After a storage failure, both reads
and mutations return `Poisoned` until the coordinator is reopened. Stale temp
cleanup reserves the temp inode before unlinking it, so an already-open journal
reachable by a hard link is not removed; a temporary symlink is refused.

With latest stable Rust on Linux x86_64, the default-feature checkpoint filter
passed 16 tests and strict Clippy passed for broker library/tests. The tests
include 40 heartbeats under each of three limits for each coordinator, repeated
reopen and expiry checks, invalid or misplaced checkpoints, hard-link ownership,
and subprocess exits before/after rename and after directory sync. Injected
failures immediately before and after rename checked that reads and further
heartbeats return `Poisoned` and reopen selects the complete old or new state.
These process exits and injected errors do not simulate power loss or certify
filesystem or hardware durability.

The candidate was uncommitted on base `99868cad0`. The test and Clippy receipts
record their commands, compiler, environment and pre/post hashes for the scoped
inputs; both report stable inputs. The four changed Rust source files match both
receipts. The executed test binary was hashed immediately after the test and a
later copy matched that hash. The broker README is a post-run snapshot and its
changes must be merged with later main-branch documentation. The receipts do
not exhaustively identify external tools, operating system or cached dependency
bytes. Run full broker suites, all-feature checks and independent peer/fault
histories on a committed integrated candidate before promotion.

The earlier 12-test candidate and all initial setup/lint failures remain in
[the earlier archive](../group-checkpoint-20261028/README.md). Full-state
serialization on heartbeat remains; classic, offsets and lifecycle journals
have distinct finite-history limits tracked separately. No throughput or
production-readiness claim follows from these tests.

Verify stored and reconstructed bytes from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/group-checkpoint-final-20261028
```
