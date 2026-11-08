# Broker journal ownership evidence

The repair keeps process-local pathname and inode reservations while a Journal, an owned reader, or a staged sync still uses that generation. On Unix, a private descriptor pins the inode. Replacement retains the old inode until its engine closes and keeps distinct leases for any old staged tickets. The original quorum `Journal(AlreadyOpen)` failure remains unattributed: its log did not record the path, inode or owner.

Source [a5a562f](https://github.com/mingley/partitionline/commit/a5a562f8945af8128f32d30ed593caf0542c7669) is based on main `ba8d72f51c23b4bfbd1057e61bbd483da831aa91`. Its broker source SHA-256 values are:

| File | SHA-256 |
| --- | --- |
| `src/journal.rs` | `130adb705e0e3150508c7381cb1c572d293881bd6a9f3416a2df1fe21e42b549` |
| `src/partition.rs` | `e2006a15994329fecdb424f16af7d186cb2c5e769be8103fa32f9f21e37cd48e` |
| `src/produce.rs` | `fa3bff1c088ff391ed34fdb8ec211da5f757b08c807a2c21d0ba1cd648391a56` |
| `src/segments.rs` | `dc437576653833f172aa8363e9ad21adbb3ee39df142e6036c05560b5cd61dc5` |

[Run 42449957807](https://github.com/mingley/partitionline/actions/runs/42449957807), workflow head `ed6c4a06ec5d4a02e34bfa6fffb21832fb34d27e`, tested that source on x86_64 Linux with stable rustc 1.99.1:

- The default broker all-target suite passed 747 tests and the all-feature suite passed 895; each ignored three descriptor diagnostics that passed when run separately.
- Checkpoint and epoch tickets retained the old hard-linked inode and original pathname across replacement and Journal drop. A two-generation test verified each inode is released after its own last ticket and the path after the final ticket. A separate control kept the retired inode reserved until the old engine closed.
- Journal, CheckedCursor, SparseCursor and rolling-directory controls covered live exclusion, unlinked identities and reopening. Under a 64-descriptor limit, failed pin and sync descriptor clones returned errors; guards were released and reopening succeeded.
- Strict formatting, all-feature/all-target Clippy, rustdoc and both doctest profiles passed. The pinned Apache API inventory passed along with 55 inventory negative controls.
- The same three staged-ticket behavioral tests failed at their ownership assertions on baseline `7f59ef470340db6027958f439df3716a8dde536c`; no retries or weakened assertions were used.

The [README-only revision 4e965c2](https://github.com/mingley/partitionline/commit/4e965c2de6810f687b1c3656f1d379036b4a0e60) moves the descriptor section to the end without changing source or inventory. Its separate [run 42450213366](https://github.com/mingley/partitionline/actions/runs/42450213366) was still running when this note was written.

The original [investigation note](../../../qualification/journal-ownership-2026-11-08.md) records three independently forced stale-inode windows, two contaminated diagnostic suites, and the instrumented suite's four Raft `Node(NotLeader)` failures. The first candidate's [run 42448577937](https://github.com/mingley/partitionline/actions/runs/42448577937) failed formatting. [Run 42449313188](https://github.com/mingley/partitionline/actions/runs/42449313188) failed a shallow-checkout source-parent check and reported stale whole-source inventory pins. These runs remain available.

## Descriptor and operation costs

On Unix, one idle flat store keeps two descriptors and one idle rolling store keeps three, excluding catalog, sockets and transient work. The extra inode pin is one descriptor per identified inode; an old and new pin can coexist during replacement. A staged partition already required a cloned sync descriptor before this change. Each staged ticket now also clones and later drops two reference-counted leases, without taking the registry mutex on the normal path while the Journal remains live. Final ownership removal takes up to two short registry locks instead of one. Each path and inode lease has an `Arc` allocation; `Arc::new` retains its standard allocation-failure behavior.

In [run 42449728745](https://github.com/mingley/partitionline/actions/runs/42449728745), with the catalog already open and the test running alone, two flat stores changed the descriptor count 8→12; eight grouped requests with two staged partitions each increased it to 28, then completion returned it to 12. Two rolling stores changed 8→14. The actor can group up to eight requests bounded by `max_partitions`; `max_stores` and `max_sync_concurrency` do not enforce a process-wide descriptor budget.

These are operation and descriptor counts, not elapsed-time, throughput or competitive benchmark results. Cross-process locking, Windows inode semantics, other filesystems, sustained load and power-loss behavior remain outside this qualification.
