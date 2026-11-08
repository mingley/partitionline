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

A standalone
[control](https://github.com/mingley/partitionline/actions/runs/42475049099)
confirms an in-memory ordering difference on stable Rust 1.99.1 / LLVM 23.1.1
for x86_64. With a fixed hasher and the three keys `(1,0), (2,0), (4,0)`,
inserting each key 8,192 times produced capacity 7 and iteration order
`[(1,0), (2,0), (4,0)]`. Inserting each once produced capacity 3 and order
`[(4,0), (1,0), (2,0)]`. A cloned `RandomState` instance also produced
different first keys in its bounded search. A fixed-hasher one-key control
kept capacity 3 and the same order.

The witness models a one-free-slot queue by taking the first iterated key; its
two sets therefore select different keys. It does **not** use a Tokio channel,
the producer or Kafka, and does not prove that three changing leaders and a
nearly full connect queue coincide in a public history. That history is a
source-derived possibility, not an observed producer failure. Slot-union
equality alone does not establish equivalent nudge order.

The [witness source](https://github.com/mingley/partitionline/blob/3fa5a8e3652d60033fb9818c59229a897b21789f/docs/evidence/client/send-all-hashset-order-20261108/witness.rs)
has blob `e93cdbc72075d4ea69aa69d6348887d4dbb190a8` and SHA-256
`9d816603470d2d3a984328606a0614f9cec08324dce011e88c6b1cb4f4f5735c`.
The executed binary SHA-256 was
`7d3907236f2b3b98653bab0a1a86b8eb39984d4c983c9bd433577221f3743d1a`;
the binary was not retained. The
[logs and provenance](https://github.com/mingley/partitionline/actions/runs/42475049099/artifacts/13615470363)
were retained. The selected keys and order are observations of this pinned
toolchain and hasher, not a promised `HashSet` iteration order.

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
