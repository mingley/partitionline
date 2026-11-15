# Bounded cleanup service: design review

This is a design review, not an implemented producer repair. The proposed
service would reserve a bounded cleanup slot before a worker detaches an
accepted batch. A guard would transfer the batch and its sequence journal to
an independent service if the owning future were canceled. The service would
settle records without running callbacks or user wakers on the dropping
runtime thread. The [public sequence witness][witness] shows why this ownership
boundary matters, but does not qualify this proposal.

The design must resolve the following before it can be treated as safe:

- **Admission and writes.** A fence checked only at the start of `send` is
  insufficient: an already-reserved channel permit can publish after the
  fence. Every final admission, including sticky and `try_send`, must
  linearize with it. A worker that obtained a write claim before the fence can
  still write; those claims must be tracked and drained or fenced before the
  producer treats recovery as complete. Transaction state and identity changes
  need the same generation-aware ordering.
- **Receiver and permit lifetime.** A reserved Tokio channel permit guarantees
  capacity, not that its receiver remains alive. [Control 42449432639][permit]
  showed an owned permit can be used after receiver close or drop, without
  returning the envelope as a send error. Timed close must leave an independently
  rooted service alive until every outstanding permit, queued envelope and
  active cleanup resolves. A rejected or failed handoff must not drop the
  envelope on the caller's runtime thread.
- **All ownership boundaries.** A cleanup credit must be acquired before an
  accepted batch enters any cancellable detached state; waiting for one after
  grouping merely moves the loss window. One possible bound charges each
  simultaneous recovery envelope to a distinct accepted-record credit, with
  all split and retry siblings moving their credits with their owned records.
  This is a proposed accounting proof, not an implementation. Guarding only
  `assign_sequences` leaves
  detached records in grouping, AddPartitions, encoding, socket writes,
  response parsing, terminal callbacks and retry handoff. The owner must exist
  before any cancellable future can take the batch and must move through each
  transfer. A never-written range, a partially stamped range and a possibly
  written range require distinct journal states. Re-entering current sequence
  assignment after a partial stamp reserves a second range while retaining
  old prefix stamps.
- **Callbacks, receipts and unwinding.** Cleanup must preserve the intended
  callback, receipt, error and byte-accounting order, including when a callback
  blocks, panics or reenters the producer. Tokio's oneshot sender can publish
  its value before invoking the receiver's waker; a panic in that waker is not
  proof that publication failed. Retrying the send after an unwind can deliver
  twice. A rooted stage cursor must retain sibling records and record which
  callback or publication was attempted, while acknowledging that a panic
  cannot prove exactly-once external callback side effects.
- **Bounded work and shutdown.** One blocked callback can occupy a cleanup
  thread indefinitely. A thread per producer is only a per-producer bound:
  repeated timed-out closes with blocked callbacks can retain unbounded threads
  process-wide unless the service has a shared bound or an explicit wider
  resource policy. Service slots, worker admission and callback execution
  must have explicit bounds and backpressure; a close deadline must not
  synchronously wait for user code. The service and its envelopes must survive
  a timed close, while runtime shutdown, service failure and receiver loss must
  have defined outcomes. A Tokio task alone does not survive runtime teardown.
- **Lifetime graph.** A suspended task, queue, notification job or core that
  strongly retains a receipt waker or interceptor can form a cycle through a
  retained `Producer` clone. Splitting a completion core from worker state is
  not sufficient by itself. The design must prove the complete ownership graph,
  including the final waker and payload destruction, and state the behavior of
  user-created interceptor cycles.

Qualification needs public producer histories for cancellation at each detach,
a partial sequence stamp, a possibly written batch, retry handoff, overlapping
flushes and close deadlines. It also needs blocked and panicking callbacks,
receipt wakers that panic after publication, byte and sticky accounting,
transaction-generation changes, bounded service saturation, runtime shutdown,
and independent broker readback. Any hot-path slot, lock or allocation cost
needs matched release-mode measurement. None of those requirements is
established by this source review, and the current draft remains isolated.

[witness]: ../producer-sequence-public-20261108/README.md
[permit]: https://github.com/mingley/partitionline/actions/runs/42449432639
