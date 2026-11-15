# Producer flush: independent source review

This is a source review of the proposed flush design, not a tested repair.
It refers to producer blob `ca894b515e0e65e8a451cf811346513240ab6238`
at [commit c49cde3](https://github.com/mingley/partitionline/blob/c49cde3ecf7fc11be8bd0a0e31910f6a1dce6b51/src/producer.rs).
The existing [generation design](../producer-flush-design-20261103/DESIGN.md)
is a draft; its model does not cover the complete producer lifecycle.

A bounded, temporary gate on all new admissions is useful as a correctness
reference but changes concurrent-send behavior. A callback for a covered
record can synchronously wait for another thread's same-producer send; that
send waits for the gate, and the gate waits for the callback. Running callbacks
outside locks does not break this cycle. A callback that blocks can also stop a
current-thread runtime from polling a flush deadline. The ordinary design
should let later sends use a new generation, with a carefully bounded pause
reserved for capacity recovery.

The following are additional implementation and test requirements from source
analysis. They have not yet been verified by new controls or a broker test:

- Seal or split a sticky cohort at a generation cutoff. Later records must not
  keep extending a cohort containing covered records. Sticky admission commits
  membership around the handoff, while a worker waits for cohort membership
  before draining ([admission](https://github.com/mingley/partitionline/blob/c49cde3ecf7fc11be8bd0a0e31910f6a1dce6b51/src/producer.rs#L2849),
  [worker](https://github.com/mingley/partitionline/blob/c49cde3ecf7fc11be8bd0a0e31910f6a1dce6b51/src/producer.rs#L7598)).
  Test a sender paused between handoff and policy commit.
- A flush wake by itself is insufficient if a registered sender enqueues only
  after a worker consumed that wake. Inspect the record generation on dequeue,
  including retries and newly polled workers. Also bound worker work and
  prioritize covered work: [`pull_ready`](https://github.com/mingley/partitionline/blob/c49cde3ecf7fc11be8bd0a0e31910f6a1dce6b51/src/producer.rs#L7477)
  drains until the channel is empty, and the
  [biased selection](https://github.com/mingley/partitionline/blob/c49cde3ecf7fc11be8bd0a0e31910f6a1dce6b51/src/producer.rs#L7931)
  can favor data over flush. Test continuous later admissions.
- Give every fallback seal a distinct incarnation, not just a generation
  identity. If a forced recovery reopens generation G and another flush
  reseals G, a stale waiter's drop must not reopen the new seal. Exhaustion
  must fail closed. Overlapping waiters need immutable completion and error
  snapshots so a waiter on an old cutoff cannot wait for new records or lose
  an error after another waiter reopens admission.
- Transfer the same completion and resource owner through retries, retired
  workers, detached batches and cleanup; zero-byte records still need a
  bounded accepted-record count. Construct the owner before returning a future
  that can be dropped unpolled. The
  [cancellation witness](https://github.com/mingley/partitionline/actions/runs/42444842648)
  showed a receiver closing and a byte reservation remaining in one local
  cleanup fixture; it does not prove other paths safe.
- Preserve an error until a flush actually returns it. Internally distinguish a
  fully drained delivery timeout from the caller's drain deadline, and keep
  transaction error state separate from the public flush cursor. Current
  [close](https://github.com/mingley/partitionline/blob/c49cde3ecf7fc11be8bd0a0e31910f6a1dce6b51/src/producer.rs#L4078)
  awaits shutdown after flush and maps non-timeout delivery errors to success.
- Define the boundary for in-progress sends and partial `send_all`: the
  current proposed cutoff covers records whose admission committed before
  flush was first polled, not necessarily send futures created earlier.
  Document and test whichever API contract is chosen.

Qualification needs deterministic handoff/seal, sticky cohort, starvation,
overlapping waiter, forced reopen, cancellation and callback controls, plus
real broker coverage and matched x86 throughput and tail-latency measurements
under concurrent sends, batching, retries and repeated flushes. This review
makes no performance, completeness or production-readiness claim.
