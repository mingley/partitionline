# General homogeneous-route nudge scan

This is a design review, not an implemented or qualified producer change. The
unapplied candidate
[`1af801af08e89931f53b811da49be896783e1ec4`](https://github.com/mingley/partitionline/commit/1af801af08e89931f53b811da49be896783e1ec4)
shortens scans only below 8,192 records. Its combined behavioral run remains
failed; see the
[qualification record](../send-all-combined-qualification-20261108/README.md).

One possible larger-input design would track whether records share one
explicit topic and partition while the existing collection loop consumes
them. It would retain the collection loop's 8,192-record yields. For each
later nudge it would inspect one representative under the cluster lock per
original 8,192-record chunk, retain the union of leaders seen across chunks,
and keep the original full-chunk yields and final emission point. No extra
pre-admission await would be needed.

The original nudge checks freshness for every record while holding the
cluster lock. For identical topics and partitions, with the same locked
metadata snapshot and a monotonic freshness clock, one representative per
chunk produces the same slot union: once a fresh record adds a slot, a later
stale record cannot remove it. That is a conditional observation, not a
guarantee that the optimized and original operations see the same snapshots.

Shorter lock occupancy can change metadata interleavings, worker retirement,
and whether a full connect queue has room when nudges are sent. Faster
admission can change when existing buffer or channel waits become pending and
when per-record deadlines start. Preserving explicit yield locations therefore
does not establish equivalent cancellation, metadata, queue, deadline or
delivery behavior.

A further source-derived concern is that repeated `HashSet::insert` calls may
change table capacity even when inserting a duplicate, depending on the
pinned standard-library implementation. If multiple chunks observe different
leaders, one insertion per chunk could leave a different table and iteration
order. With a bounded connect queue, a different iteration order could affect
which nudge is accepted. This has not been demonstrated in a public producer
or broker history, and slot-union equality alone would not rule it out.

Retaining every existing nudge yield also limits scalability. For an
uninterrupted batch of `n` homogeneous records and chunk size 8,192, the
suffix scans contain approximately `n² / (2 * 8192)` full chunks and yields.
Reducing work inside each chunk helps, but this remains quadratic in chunk
count for very large batches.

Before applying any such design, controls would need to exercise iterator
ownership and cancellation at the original yields, 31/32 and 8,191/8,192/8,193
boundaries, long topics and late mismatches, metadata expiry and leader
changes at successive chunk snapshots, worker replacement, a full and draining
connect queue, interceptors and sticky routing, and exact ordered receipts,
records and accounting. Matched x86 measurements must include the worst poll
and full completion, and any changed scheduler or batching behavior must be
reported. A different yield policy or coalesced nudges would require its own
explicit semantics and qualification.
