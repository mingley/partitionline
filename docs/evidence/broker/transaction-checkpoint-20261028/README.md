# Transaction coordinator checkpoint candidate

This candidate checkpoints the experimental coordinator's current producer
and transaction state before a history, index, or file limit blocks an event.
Each checkpoint contains the post-transition state, so a successful write
includes the decision it acknowledges. The candidate checks the prospective
state against the entry, fetch, and complete-file budgets before every event;
this leaves room to prepare, finish, advance the coordinator epoch, or fence an
admitted transaction when those operations do not grow its snapshot. The
adjacent `.checkpoint.tmp` name is reserved, and rollover can require space for
both journal files.

Independent source review found that an earlier pre-state-plus-event candidate
could admit two 151-byte IDs into a 512-byte entry and later fail to prepare an
ongoing transaction because its checkpoint included the large event again. It
also found a deterministic file-budget refusal that could unnecessarily poison
reads, and an event-only compatibility failure for state larger than a single
entry. The retained earlier candidate patches were not tested; the final
candidate uses a post-state image, preflights the full 24-byte journal header
and 32-byte entry header, and preserves event-only access for oversized legacy
histories. See the [event compaction design](capture/event-compaction-design.md)
for the separate marker and transactional-offset requirements.

`epoch` and `state` now return `Result` and report `Poisoned` after an ambiguous
persistence failure. Normal old event records remain readable. Legacy state
that cannot fit a checkpoint remains readable and may append events while its
original history, index and file budgets permit; it still cannot roll over
until the state fits. Continued legacy growth can exceed the journal's 64 MiB
entry maximum, after which raising limits cannot enable this single-entry
format. A multi-entry checkpoint is not implemented. Once a new checkpoint is
written, older binaries cannot read the journal.

On Linux x86_64 with stable Rust, the 20 selected coordinator unit tests passed
under default and all features. Strict Clippy passed for the all-features broker
library. The tests include compacted versus uncompacted histories, producer and
coordinator fencing, six transaction states and participant sets, exact
entry/file/fetch limits, the 512-byte closure case, full and growing legacy
histories, and child-process exits before rename, after rename, and after
parent-directory synchronization. These exits and injected failures do not
simulate power loss or certify filesystem or device durability.

The checkpoint builds a full serialized buffer and clones the affected
transaction, not the whole transaction map. Recovery and tree/string allocations
are not bounded as total process RSS; no memory, CPU, or throughput claim follows
from these tests. Transaction marker propagation and fencing integration are not
implemented: initializing an ongoing transaction currently clears its stored
participant set. The marker and transactional-offset event logs retain separate
finite-history, producer-fencing and visibility requirements. They are not
fixed by this coordinator change.

The checks ran in a sparse worktree based on `9a3e65651` with uncommitted
changes to the coordinator, journal helper, and broker README. The
[final patch](capture/candidate.diff) records the changes. The receipts include
commands, compiler, source hashes, sparse worktree status, and matching pre/post
inputs; all captured broker source hashes were checked against all receipts.
They do not exhaustively identify external tools, OS, or cached dependency bytes.
Both test executables were copied from the build target after checking their
receipt hashes; the runner itself retained only hashes at execution time.

These focused checks do not constitute the full broker suite, independent
compatibility testing, a performance comparison, or production qualification.
Run the broader gates against integrated source.

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/transaction-checkpoint-20261028
```
