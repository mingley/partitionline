# Group lifecycle checkpoint candidate

This candidate rolls the experimental group lifecycle's full-state journal to
a synchronized, atomically renamed checkpoint before a configured history,
index, or file limit prevents an update. A checkpoint has a separate marker,
can only be the first journal record, and is validated before ordinary
transition replay continues. The adjacent `.checkpoint.tmp` name is reserved;
rollover can require space for both the old journal and its replacement.

`owned` now returns `Result`, and both `owned` and `commit` reject reads after
a persistence failure until the lifecycle store is reopened. The candidate
also fixes recovery of a static member replacement made with an older clock:
the existing rebalance deadline is retained and accepted when the stored
heartbeat is unchanged. Checkpoint and legacy recovery reject duplicate static
instances and deadlines earlier than the stored heartbeat; malformed legacy
records accepted by the older parser can therefore now be rejected. Normal
legacy records retain their format. Snapshot length is checked before reserving
an output buffer, adding a map traversal; this is not a total process memory
bound and has not been benchmarked.

On Linux x86_64 with stable Rust, 13 selected lifecycle unit tests passed with
default features and all features. One cooperative group transition history
also passed. Strict Clippy passed for the all-features broker library and that
history test. The tests cover multiple members, ownership and delivered state,
static replacement and deadlines, repeated reopen under history and file
limits, malformed and misplaced checkpoints, oversized snapshots, and injected
errors before and after checkpoint rename for delivery and revocation. Injected
errors do not simulate power loss or certify filesystem or hardware durability.
Transactional journals retain separate finite-history limitations.

The tests ran in a sparse worktree based on commit
`3d94df75a16b7acd1167dcdb55906da9ae4e9425` with uncommitted changes to the
lifecycle coordinator, broker README, and cooperative history test. The
[patch](capture/candidate.diff) records those changes. The receipts list the
commands, compiler, source hashes, sparse worktree status, and matching
pre/post inputs; all captured broker source hashes were checked against all
four receipts. The receipts do not exhaustively identify external tools,
operating system, or cached dependency bytes. The three test executables were
copied from the build target after their receipt hashes were checked; their
compressed copies are in `executables/`. The runner itself retained hashes only
at execution time.

These focused checks do not constitute the full broker suite, an independent
compatibility run, a performance comparison, or production qualification. Run
the broader gates against the integrated source.

Verify stored and reconstructed bytes from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/lifecycle-checkpoint-20261028
```
