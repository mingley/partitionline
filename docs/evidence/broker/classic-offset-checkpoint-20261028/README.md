# Classic group offset checkpoint candidate

This candidate rolls the experimental classic group offset store's snapshot
journal over to a synchronized, atomically renamed checkpoint before its
configured history, index, or file limit blocks another update. Recovery accepts
an initial checkpoint after validating the snapshot and continues validating
ordinary transitions. The adjacent `.checkpoint.tmp` name is reserved. A
rollover temporarily needs space for both journal files.

The store now rejects group and per-group offset limits above 65,535, the
maximum its existing snapshot format can encode. Its `list`, `describe`, and
`fetch` methods return `Result` so an unconfirmed in-memory mutation is not
reported after a persistence error. Reopen a poisoned store to recover the
complete old or new state. The candidate checks encoded size before reserving
its output buffer; this does not bound total process memory or allocator
overhead. The size check adds a map traversal and has not been benchmarked.

On Linux x86_64 with stable Rust, 11 selected offset unit tests and one
cooperative group transition history passed. The unit tests exercise repeated
reopen under history and file limits, malformed and misplaced checkpoints,
pre-rename and post-rename injected errors, retries after those errors, and
snapshot size and format limits. Strict Clippy passed for the all-features
broker library and the history test. These injected failures do not simulate
power loss or certify filesystem or hardware durability. Other classic and
transactional journals retain separate finite-history limitations.

The tests ran in a sparse worktree based on commit
`aa35a02b40b1e2a7dd1ae8d71fbc8a8a8806f354`, with uncommitted changes to the
broker offset implementation, its README, and the shared group history test.
The [patch](capture/candidate.diff) records those changes. The receipts list
the commands, compiler, source hashes, sparse worktree status, and matching
pre/post inputs. They do not exhaustively identify external tools, operating
system, or cached dependency bytes. Both test executables were copied from the
build target after their receipt hashes were checked; their compressed copies
are in `executables/`. The receipts themselves note that the runner retained
only hashes at execution time.

These are focused candidate checks, not the full broker test suite, an
independent compatibility run, a performance comparison, or a production
qualification. Run the broader gates against the integrated source.

Verify stored and reconstructed bytes from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/classic-offset-checkpoint-20261028
```
