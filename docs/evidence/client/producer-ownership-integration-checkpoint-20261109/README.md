# Producer ownership integration checkpoint

This is source review and an implementation checkpoint, not a producer fix.
No producer behavior changed and no new tests ran. The reviewed main commit is
[`731eb97a08445a18da0b043986ae192397ba176a`](https://github.com/mingley/partitionline/tree/731eb97a08445a18da0b043986ae192397ba176a),
`src/producer.rs` blob `426dbdb5c5e27fdf655930a76682c5c18fb07e89`.
The existing [public regression controls](../producer-ownership-regression-controls-20261108/README.md)
and [guard experiment](../producer-sequence-guard-20261109/README.md)
remain open; their recorded results are from their stated earlier commits.

## Existing retry task is not the cleanup owner

The nearest existing service is
[`retry_loop`](https://github.com/mingley/partitionline/blob/731eb97a08445a18da0b043986ae192397ba176a/src/producer.rs#L6814)
with `RetryReceiver`, `RetryBatchGuard` and `RetryGuard`. It cannot
provide a bounded cancellation-safe batch lease without a redesign:

- Its channel holds individual `Pending` records. A retry producer uses
  `try_send` on each member; a full or closed channel fails that member
  synchronously, so a group can be partially transferred or split.
- It receives records only after worker handoff. In
  `drain_pending`, `group_pending`, expiry partitioning, sequence
  stamping, and acknowledgement processing, actual records can be in
  future-local containers across an await. Worker `Drop` cannot recover
  those containers. A lease around only the final Produce request therefore
  misses accepted records.
- Shutdown aborts the retry task before it aborts all published workers.
  Its receiver and guards synchronously fail retained records, invoking
  interceptors on the dropping thread. A blocked or panicking callback can
  delay or interrupt cleanup. Neither abort nor `Drop` can await a full
  channel or a stopped runtime.
- Channel capacity counts currently queued records, not all accepted and
  in-flight work and not complete retry batches. `buffer_memory(0)`
  disables the byte cap; records with zero accounted bytes do not consume it
  even when the cap is nonzero. Thus current byte credits alone cannot
  reserve a finite cleanup slot for every admitted record. A bounded channel
  offered from `Drop` can be full precisely when its contents are needed
  for cleanup.

Normal completion also needs the owner: both acknowledgement and failure
paths can release buffer credits before later yielding while callbacks and
receipts remain unpublished. `flush_until` currently checks worker flush
messages and `retries_out`, rather than a persistent admitted-record cutoff.
An independent request or record ledger that leaves payloads and callbacks in
local vectors would not fix these failures.

## Required integrated ownership

One viable route is an owner shared by admission, batching, retry, response
handling, flush and close. Its concrete representation is still a design
choice; it must establish these invariants together:

1. **Admission and capacity.** Reserve payload bytes and a record/root slot
   before accepting a record, and commit the actual payload, receipt,
   callback state, sticky permit and admission ticket to the root as one
   operation with worker admission and the close gate. Canceled pre-commit
   admissions release their reservations exactly once. Channels carry
   revocable claims or identifiers, not the sole copies of the records.
   `send`, `try_send`, sticky admission and `send_all` all need the
   same commit point.
2. **Lease and cancellation.** A worker may mutate records only under a
   unique, generation-checked lease. The root continues to hold actual
   records while grouping, encoding, waiting, retrying and completing.
   Every yielding transition updates root-owned state before yielding.
   Dropping a lease synchronously marks it reclaimable and wakes a separately
   supervised owner; it must not allocate, wait, invoke user code or depend
   on a fallible queue offer. The supervisor waits for the prior worker lease
   to end before reclaiming. Completion, resource destruction and callbacks
   run outside owner locks, with explicit panic and reentrancy policy.
3. **Batch and reservation identity.** Seal each partition batch once with a
   root-scoped, nonreused batch key and immutable ordered members, topic
   incarnation, producer identity and sequence reservation. Allocate
   checked logical reservation/ticket identifiers and fail closed on
   exhaustion; numerical Kafka sequence values alone cannot identify an
   owner. Record reservation before cooperative stamping. Retries keep the
   whole original batch and its timestamps; member timeouts cannot
   silently detach a partial retry. Preserve FIFO ordering across old and new
   leader workers. A multi-partition request must journal all its batch
   offers atomically or fail without exposing a partial request.
4. **Write and identity fencing.** Distinguish definitely-unwritten,
   offered/possibly-written and broker-confirmed attempts. Serialize
   identity transitions against actual write admission: an already granted
   write offer cannot be allowed to start after the old identity has been
   retired. Wait for or revoke and join outstanding offers before publishing
   a replacement identity, while retaining ambiguous batches until a
   justified broker settlement or explicit terminal policy. Do not rewind an
   ambiguous attempt or silently assign it to a new identity. Stale
   completions must not mutate the current generation. The protocol for
   ambiguous outcomes and transaction fencing still needs implementation.
5. **Completion and barriers.** Record broker settlement separately from
   individual delivery timeout and callback/receipt publication. Release
   credits exactly once after the chosen terminal publication and resource
   ownership rules are met. Flush captures an admission cutoff and waits for
   its included work and callback policy, including overlapping and canceled
   waiters. Close atomically seals admission with a cutoff, then waits or
   times out without discarding root ownership. Multiple close observers
   need a defined rejoin policy. A blocked interceptor can prevent a timely
   close; a timeout must not claim that socket I/O or callbacks have stopped.

Kafka wire sequences wrap from `i32::MAX` to zero. A checked logical
reservation identity is separate from the wire sequence; proper arithmetic
and wrapped batch membership must be verified alongside retry and rollback.
The current saturating reservation and stamp do not do that.

A finite owner needs explicit record, byte, batch, request and completion
capacity, including byte-empty records and scratch/encoding leases. Capacity
must be reserved before admission, so cleanup needs no additional queue slot.
Existing `ProducerConfig` is an exhaustive public type and its zero byte
limit is documented as unlimited. A separate bounded constructor or a
deliberately documented breaking API change are possible; retaining the
legacy unlimited mode cannot establish a finite global memory bound. Owner
references and task handles must also avoid cycles, and runtime destruction
needs an explicit fallback contract rather than an assumed live supervisor.

## Controls needed for the integrated implementation

Run the retained public ownership and retry-split histories as positive
controls on the actual implementation, with released-gate controls and
instrumentation described. Add cancellation at channel commit, each
cooperative drain/group/stamp/encode/response/completion boundary, retry
handoff and callback dispatch, including multiple partitions and replaced
workers. Check every accepted receipt, aggregate callback counts and byte
credits, with finite test limits and no inference of permanent loss from a
single timeout.

Add full/closed cleanup-capacity cases, zero-byte records, concurrent
admission and close, overlapping/canceled flush waiters, callback blocking and
panic, and runtime/producer shutdown. Add a transport control that closes
before a write and another that closes after accepting a prefix; assert
only the first is eligible for unwritten rollback. Delay an old-generation
response across identity changes and prevent a paused old write offer from
starting after publication. Reproduce the ambiguous original-write/whole
retry and unchanged mock-log case without splitting on flush.

Exercise sequences around `i32::MAX` with an independent modulo oracle,
checked logical exhaustion, equal numeric sequences from distinct
reservations, partial stamping, and stale rollback. Label any injected
producer or mock state as synthetic; it is not a natural Kafka history.
Add live-Kafka retry, fencing and transaction controls where feasible, then
paired benchmarks on the exact qualified source.

No integrated owner or new positive public control was produced in this
checkpoint. The scope above crosses admission, worker and retry data types,
encoding, broker settlement, flush and shutdown. A record-only cleanup guard,
a bounded retry queue or a generation-only guard would leave critical
ownership and write races, so none is proposed for main.
