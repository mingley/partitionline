# Deferred producer failure cleanup: design sketch

Status: not implemented or qualified. The isolated
[cleanup candidate](https://github.com/mingley/partitionline/commit/dca26dfaf47989db15e3d27b0cddafc5483a9d84)
retains one failure batch across cancellation and passed local fixture controls
in [run 42447458557](https://github.com/mingley/partitionline/actions/runs/42447458557).
Its cancellation Drop runs every remaining error interceptor synchronously.
A blocking callback can block the thread dropping the future, including a
current-thread runtime; a 16,385-record debug fixture took about 14 ms before
Drop returned. This is not a worst-case bound. Keep the candidate isolated
until that behavior is resolved or deliberately accepted.

The existing helper can lose a record result and retain its byte reservation
when canceled. Moving cleanup to an independent owner can address both
ownership and executor isolation, but it requires changes beyond one helper.

## Ownership and capacity

- Every accepted record needs one non-cloneable completion owner, its resource
  lease, and a bounded cleanup-capacity reservation established before the
  actual worker handoff. Retry and worker retirement transfer that same owner.
  Rejected sends and futures dropped before admission have no ticket. The bound
  must count zero-byte records and apply when byte buffering is unlimited.
- A canceled batch must transfer all uncompleted owners to a cleanup service
  without an await and without running user interceptors or user-provided
  wakers on the dropping thread. Queue submission must have an explicit,
  tested full/closed policy; losing a batch or waiting for a queue slot during
  Drop is not acceptable. Simply choosing a bounded channel is insufficient:
  prove admission reservations cover every simultaneously queued envelope.
  Any lock, allocation, copy or queue contention in Drop needs its own work
  bound; a record-count bound alone is not a hard wall-clock guarantee.
- Split completion data from the submitter and producer queues. A queued batch
  must not retain a strong reference back to its own sender or to a producer
  object that owns that sender. Prove that shutdown keeps the receiver alive
  until every admitted owner has either transferred or completed; a weak
  reference alone cannot guarantee cleanup after the producer is dropped.
- Retire sticky cohorts and publish transaction failure before a retry or later
  admission can treat them as live. Deferring the existing sticky retirement
  without an earlier cancellation marker changes this ordering. Preserve
  outcome, byte, sticky, metric, callback and completion-ticket ordering for
  normal completion, cancellation and shutdown.

A dedicated, bounded callback executor can keep a blocked interceptor from
occupying a Tokio runtime thread. It cannot forcibly stop arbitrary synchronous
user code: all callback workers can still block, and callbacks that wait for a
flush covering their own record can form a cycle. Specify the callback
restrictions and what timed close returns while cleanup is still owned. A
worker must not be joined synchronously on the runtime thread after its
deadline. Do not claim a hard wall-clock bound against blocked user code,
process suspension, or `panic=abort`.

Flush needs its own completion/error barrier; a queued cleanup job is not
complete. Its ticket must stay outstanding until the outcome and required
cleanup are settled, and a canceled flush must not consume a failure.
Transaction-generation fencing remains separate from that cursor. See the
[independent flush review](../producer-flush-review-20261108/README.md).

## Required controls

Test unpolled and partially polled cancellation, every handoff and retry,
retired workers, zero-byte records, queue saturation and closure, shutdown
without a runtime, callback/waker panic, blocked and reentrant callbacks, and
sticky cohorts spanning a failure. Exercise concurrent submitters against
reserved capacity and a cleanup worker that stops unexpectedly. Verify every
record outcome and exactly-once resource release. Model the ownership and
shutdown transitions, then run real-broker tests and matched x86 measurements
for normal throughput, cancellation work, memory, and scheduler latency.
