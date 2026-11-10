# Producer grouping cancellation: design boundary

This is a source-based design, not a fix or a test result. It addresses
ownership while a producer batch is being selected and grouped. It does not
qualify delivery, retries, transactions, shutdown, or a live Kafka history.
The reviewed source is
[`8570b01`](https://github.com/mingley/partitionline/blob/8570b01da6693acbcaa66120c861c5fd689628c4/src/producer.rs),
with producer blob `426dbdb5c5e27fdf655930a76682c5c18fb07e89`.
The [assembly review](../producer-assembly-drop-review-20261110/README.md)
records the existing synthetic dropped-receipt witness.

## Ownership starts before assembly

[`drain_pending`](https://github.com/mingley/partitionline/blob/8570b01da6693acbcaa66120c861c5fd689628c4/src/producer.rs#L7680)
moves records from the worker queue into a local batch, then can yield while
updating the estimate and recomputing the earliest deadline. Dropping that
future at such a yield drops its local batch before it reaches
`group_pending`. This is a source observation; no new public producer
history was run here.

[`group_pending`](https://github.com/mingley/partitionline/blob/8570b01da6693acbcaa66120c861c5fd689628c4/src/producer.rs#L9720)
can then suspend with records in its local batch, one-topic groups, mixed
buckets or assembly output. An iterator produced by `Vec::drain` can own
unconsumed records across a yield. A guard around only the assembly output
therefore misses earlier and active owners. Once grouping returns, later
stages have their own ownership and cancellation boundaries; fixing grouping
would not by itself repair them.

The [worker destructor](https://github.com/mingley/partitionline/blob/8570b01da6693acbcaa66120c861c5fd689628c4/src/producer.rs#L7564)
visits its pending queue and in-flight requests, but the reviewed grouping
locals are not in either. Requeueing a recovered batch through the existing
path can also meet a full or closed retry queue and enter terminal cleanup.
It must not be described as proof that every receipt is delivered.

## Proposed owned state

A candidate should establish a worker-owned `GroupingWork` before moving
the first accepted record out of `pending` and before any subsequent
await. It would hold the remaining input, partial output, active topic and
partition state, and all buckets containing records. Scratch allocations
without records may remain separate. A mutable borrow of this state can
cross an await; destroying the grouping future leaves the owners in the
worker. No borrowed drain iterator may own records across a suspension.

At every suspension, each accepted record must be in exactly one durable
local owner: the worker pending queue, the grouping state, a later request
owner, or a terminal-completion owner. Transfers must be synchronous between
those owners, and terminal state must be committed at most once. The grouped
record order must remain stable within each topic-partition. The design must
also reconcile the worker's pending estimate and earliest deadline when an
unfinished grouping is resumed or recovered. This is a proposed invariant,
not a proof of the existing code.

On normal completion, transfer the finished groups to the next owner before
clearing the grouping state. If only the grouping future is dropped and the
worker remains alive, the worker must resume or recover that state before
accepting later work in an order that violates a partition's ordering. If the
worker itself is dropped, it must hand the unfinished state to a surviving
owner or complete it locally. The path for a full or closed recovery queue
must retain an explicit outcome; a best-effort `try_send` alone is not that
guarantee.

Recovery must not require allocating a new container during unwinding.
Capacity and ownership for the handoff must be established before accepted
records can reach it. This needs a separate record-count bound or another
proven reservation: [`rec_bytes`](https://github.com/mingley/partitionline/blob/8570b01da6693acbcaa66120c861c5fd689628c4/src/producer.rs#L1796)
charges visible key, value and header bytes, and
[`try_reserve_buffer(0)`](https://github.com/mingley/partitionline/blob/8570b01da6693acbcaa66120c861c5fd689628c4/src/producer.rs#L1766)
does not increase the byte count. The payload-byte budget by itself cannot
bound the count of zero-payload records. Other admission limits may constrain
that count; this observation does not establish an unbounded live history.

## Outcome, accounting and notifications

A record may be a retry of an earlier ambiguous attempt. A local abort must
not assert that the broker did not append it. The current public errors do
not contain a dedicated delivery-outcome-unknown variant. A design must
specify the public error and whether retry is safe separately; an ambiguous
outcome is not automatically safe to retry without duplicates. A counter
such as `retry` must not be treated as proof of a known broker outcome
without an independently established history.

For each terminal record, settle the ledger, charged bytes, slot permit and
metrics exactly once and make a truthful result available. Receipts and
interceptor notifications are distinct obligations: `try_send` has no
per-record receiver, and a dropped receiver does not cancel its accepted
record. No user callback should run under internal locks.

The existing [synchronous failure helper](https://github.com/mingley/partitionline/blob/8570b01da6693acbcaa66120c861c5fd689628c4/src/producer.rs#L11562)
releases accounting before iterating records, but
[`fail_one_pending`](https://github.com/mingley/partitionline/blob/8570b01da6693acbcaa66120c861c5fd689628c4/src/producer.rs#L11540)
calls the interceptor before sending that record's receipt. Interceptors are
[synchronous user code](https://github.com/mingley/partitionline/blob/8570b01da6693acbcaa66120c861c5fd689628c4/src/interceptor.rs#L11);
one can block or panic. A wake invoked by receipt publication can also run
arbitrary waker code. Merely moving the callback after the send does not
ensure later receipts or callbacks progress if an earlier wake blocks.

A viable completion design needs to separate recording terminal state and
resource accounting from invoking user code, then define who drives bounded
notification work when the runtime is absent, shutting down, or saturated.
The current oneshot API does not expose a separate store-without-wake step.
A pre-reserved completion cell and a bounded dispatcher are possibilities,
not an implemented solution. If user callbacks are allowed to block forever,
a finite dispatcher cannot promise that all later callbacks run. The public
contract must choose and document its liveness and panic behavior rather than
claiming unconditional exactly-once callback completion. `catch_unwind`
alone does not address blocking or `panic=abort`.

## Checks before integration

- Poll and cancel at each batch-selection, one-topic and mixed-topic yield.
  Include 8,191, 8,192 and 8,193 records, multiple partitions in an active
  topic, several later topics, and a batch larger than the cooperative step.
  Verify the union of owners contains every unique record exactly once and
  preserves each partition's order.
- Exercise both a dropped grouping future with a live worker and worker
  destruction. Check recovered estimates and deadlines, no runtime,
  retry-queue full/closed, a dropped receipt receiver and `try_send`.
- Verify terminal result, sticky ledger, byte charge, permit and metrics
  individually and exactly once for each record. Include a record whose
  earlier delivery outcome is unknown. A private fixture alone is not a
  real-broker delivery history.
- Exercise reentrant, panicking and blocking interceptors and wakers with
  bounded test deadlines. State the guarantee retained when a callback
  never returns, the runtime is gone, or the process aborts.
- Compare normal-path grouping and cancellation cost against the exact
  baseline using separate builds, realistic small batches and large
  multi-topic batches. Retain raw samples; neither a local guard nor a
  synthetic speed result qualifies a Kafka ranking.

No implementation is proposed for main until ownership, truthful outcomes,
notification semantics and bounded recovery capacity have compatible
contracts and these controls pass.
