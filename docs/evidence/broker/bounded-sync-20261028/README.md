# Bounded partition sync candidate

The candidate limits concurrent partition syncs per storage actor to a configurable
1–64 (default 4), including the actor. It attempts at most limit minus one scoped
helper threads at a time. A thread creation failure falls back to synchronous
`sync_data`; all started workers are joined and errors or unwinding sync operations
mark the affected partition failed. In grouped Produce, each partition is synced
once and a failure is propagated to all its staged batches before any reply. The
single-partition path runs inline. These are per-actor limits, not a process-wide
cap across multiple routers. `operator serve --sync-concurrency` selects the limit.

The candidate adds a field to the public `produce::Config` struct. The broker is
unpublished, but existing external struct literals must add this field. The default
keeps constructors working; throughput and latency effects are not measured. A
persistent worker pool could reduce thread creation but requires its own lifecycle,
bounded-queue and failure tests and comparative measurements.

Latest stable Rust 1.99.1 on the x86 workspace ran 15 Produce unit tests, nine
Produce integration tests, one operator process test, and strict Clippy on the
selected lib, operator and integration targets; all passed. The initial sparse
checkout command failed before compilation and is retained as `capture/lib.log`.
`capture/lib-02.log` is an earlier fixed-cap iteration and is not the final
configuration. `capture/lib-03.log`, `capture/integration.log`, and
`capture/clippy.log` are the final scoped checks. Full broker matrices and
independent live peers have not run on this candidate.

The [receipt](capture/receipt.json) pins the uncommitted sparse-worktree base and
patch plus 5,921 broker-package file hashes recorded **after** the runs. The source
archive and its per-file index are post-run snapshots; they do not prove every
build input was unchanged during execution. Test binaries and operator are also
post-run snapshots: no binary hash was recorded at execution time. The result is
not an exact committed-tree qualification, durability certification or a
comparative performance claim. Retest the integrated committed tree.

Verify stored and reconstructed artifacts from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/bounded-sync-20261028
```
