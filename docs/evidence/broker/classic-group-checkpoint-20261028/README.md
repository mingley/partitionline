# Classic group checkpoint candidate

This candidate rolls the experimental classic group's full-state journal to a
synchronized, atomically renamed checkpoint before a configured history,
index, or file limit prevents an update. A checkpoint uses a separate marker,
can only be the first journal record, and is validated before transition replay
continues. The adjacent `.checkpoint.tmp` name is reserved. Rollover can require
space for the old journal and its replacement at the same time.

`epoch`, `generation`, and `assignment` now return `Result`; a persistence
failure poisons reads and writes until the coordinator is reopened. Rejected
fresh joins no longer consume member IDs, and counter exhaustion fails before
mutation. Recovery rejects duplicate member records and checks coordinator,
generation, membership, leader, and assignment invariants; malformed legacy
records accepted by the older, less strict parser can now be rejected. Normal
legacy records retain their format. Snapshot length is checked before reserving
an output buffer, which adds a map traversal; this is not a total process memory
bound and has not been benchmarked.

On Linux x86_64 with stable Rust, the 17 selected classic-group unit tests
passed with default features and with all features. Strict Clippy passed for the
all-features broker library. Tests cover multi-group and assignment recovery
under history and file limits, repeated reopen, rejected joins and member-ID
exhaustion, malformed and misplaced checkpoints, oversized snapshots, and
injected errors before and after checkpoint rename. Injected failures do not
simulate power loss or certify filesystem or hardware durability. Lifecycle and
transactional journals retain their own finite-history limitations. In
particular, lifecycle's current `owned` and `commit` read paths do not check
poison after an uncertain write and need separate work.

The tests ran in a sparse worktree based on commit
`cce150a91` with uncommitted changes to the classic coordinator and broker README.
The [patch](capture/candidate.diff) records those changes. The receipts list the
commands, compiler, source hashes, sparse worktree status, and matching
pre/post inputs; all captured broker source hashes were checked against all
three receipts. The receipts do not exhaustively identify external tools,
operating system, or cached dependency bytes. Both test executables were copied
from the build target after their receipt hashes were checked; their compressed
copies are in `executables/`. The runner itself retained hashes only at
execution time.

These focused checks do not constitute the full broker suite, an independent
compatibility run, a performance comparison, or production qualification. Run
the broader gates against the integrated source.

Verify stored and reconstructed bytes from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/classic-group-checkpoint-20261028
```
