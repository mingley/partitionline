# Journal ownership investigation, 2026-11-08

The broker test failure in [run 42444695386](https://github.com/mingley/partitionline/actions/runs/42444695386) reported only `Journal(AlreadyOpen)` in `metadata_quorum::tests::new_leader_repairs_uncommitted_metadata_without_creating_a_ghost_topic`. It did not record the path, inode, owner, or acquisition phase. The cause of that failure remains unknown.

The following experiments use base commit `7f59ef470340db6027958f439df3716a8dde536c`; before instrumentation, SHA-256 was `87269cb14a1d623cd352ba6ac1dfd0997aec3e99ae17a1c88131bd660d97315a` for `partitionline-broker/src/journal.rs` and `ae66e8fdbe9d06e4e81e0685cd42da779585cce804971047d2ea9c87d23f3573` for `partitionline-broker/src/metadata_quorum.rs`. The host was x86_64 Linux with stable rustc 1.99.1. The workflow at commit `29ab02251e08f21fee991d129bcb5bfc83d9738c` injects diagnostics into the runner checkout; its source describes the exact controls.

In [run 42447970657](https://github.com/mingley/partitionline/actions/runs/42447970657), three separately executed witnesses each passed:

- A live Journal rejected a hard-linked alias. After unlinking and closing its file while deliberately retaining its ownership guard, the first new file reused the same inode and was rejected as `AlreadyOpen`. It opened after dropping the stale guard.
- The same sequence reproduced for CheckedCursor, including the live hard-link control and successful open after dropping the guard.
- A directory guard rejected a second live owner. An explicitly held directory descriptor prevented reuse across 512 creations after removal. Once that descriptor was closed while the guard stayed registered, the first new directory reused the inode and was rejected. It was accepted after dropping the guard.

These tests force lifetime boundaries; they establish an underlying false-collision window on this host, not the cause of the original quorum failure. They do not establish behavior on other operating systems or filesystems.

The earlier [run 42447522101](https://github.com/mingley/partitionline/actions/runs/42447522101) executed injected inode-reuse witnesses concurrently with its instrumented default suite. Although that run reported a passing suite, the witnesses intentionally held stale registry entries and could interfere with other tests, so it is not clean suite evidence. The subsequent [run 42447618995](https://github.com/mingley/partitionline/actions/runs/42447618995) also mixed the witnesses into the suite; that suite failed two diagnostic controls and is likewise contaminated.

The final run executed the three witnesses separately and skipped them during the instrumented default suite. The suite failed with 448 passed and four Raft TCP tests reporting `Node(NotLeader)`; the original quorum test passed, and no target collision provenance was captured. Instrumentation replaced the test process's registry set with a map from keys to initially reserved paths, so this was not an unmodified production-code qualification. The logged owner would be the initial reservation path, which can differ from the final path after a replacement.

A source repair and its qualification are separate work. No production-readiness, performance, or root-cause claim follows from these diagnostics.
