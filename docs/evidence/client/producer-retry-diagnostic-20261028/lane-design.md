# Producer retry ownership and order

Status: design only. The candidate patch has not been compiled or tested and
must not be integrated as a complete solution. The clean matrix failures and
source pins remain in `docs/evidence/client/clean-default-validation-20261028`.

## Observed paths

- `BrokerConn` can return `Closed` when the peer closes. Produce read/write paths
  call `Error::is_retriable`, which intentionally excludes `Closed`; local
  producer shutdown also uses `Closed`. Retry only at the transport boundary
  while the producer is open. Do not change the public error category.
- A retry actor can select a worker just before that worker's receiver closes.
  A closed reservation currently completes the accepted record with `Closed`,
  even though a replacement can be dialed. Any removal must compare worker
  identity so an old observation cannot evict a replacement.
- A worker removes its own cached handle before awaiting retry transfer.
  Dropping the last `WorkerHandle` aborts its task; if it aborts while records
  are held in an async transfer, their response channels can close without the
  normal completion path. The isolated candidate holds the retired handle
  while it transfers and retires only the observed slot.
- Retry/failure paths move pending vectors or chunks across awaits. Dropping
  those futures can drop accepted records without completing their callbacks or
  releasing the corresponding accounting through the intended path. Existing
  buffered and in-flight guards do not protect every moved chunk.
- A replacement worker may accept later records while an earlier sent sequence
  is waiting in the retry actor. Keeping its sequence number is not enough to
  prove ordered writes or correct idempotent recovery.

## Required ownership model

Every accepted record must have exactly one owner from channel admission until
completion. Introduce a bounded retry-transfer guard whose owned collection
remains reachable across each await and whose drop hands any untransferred work
to a bounded cleanup owner. The guard must distinguish accepted records from a
currently unadmitted reservation. Successful transfer moves the record and its
existing permit, deadline, sequence and callback exactly once; cancellation or
shutdown must complete the rest exactly once with an appropriate terminal
ambiguous outcome. The cleanup owner must itself have bounded admission and a
shutdown path; an unbounded detached task or a synchronous destructor over an
arbitrarily large batch would trade correctness for unbounded resource/latency.
Any queued retry count must also transfer/decrement exactly once.

Use a partition-scoped strand or equivalent bounded ledger keyed by topic
identity, partition and producer identity/epoch. Install the barrier before a
failed worker becomes unavailable for new admissions. A replacement must not
write a later sequence until all earlier ambiguous batches have been safely
replayed in order or a terminal epoch/transaction decision fences them. Merely
serializing the retry actor is insufficient because initial admissions have a
separate path. Waiting must not hold cluster, sticky-policy or transport locks.
A reconnect must negotiate on its own socket. On delivery deadline expiry, do
not silently reset a sequence or report success: preserve Kafka's ambiguous
outcome and explicit epoch/transaction handling. Topic recreation, metadata
movement and producer-epoch bump invalidate or migrate the strand only with a
well-defined identity check.

## Required deterministic histories

1. Lost acknowledgement after one and several appended batches, with a new
   admission racing the redial; assert same producer/epoch/sequence on retries,
   wire order, offsets and exactly-once log contents using a broker fixture that
   rejects invalid sequences. Include unchanged original failed histories.
2. Pause retry transfer after moving one chunk, cancel/close the owning worker
   and retry task in turn, then observe every callback, buffered-byte and retry
   counter exactly once. Include channel saturation, mixed partitions and both
   sticky and direct routing. Exercise cancellation before and after admission.
3. Replace the worker between stale-handle observation and cleanup: the live
   replacement and sibling slots must remain intact, and a remote socket close
   must not be confused with a locally closed producer.
4. Delay retries across the delivery deadline and inject partial writes, lost
   acks, epoch bump, transactional abort/fencing, topic recreation and leader
   movement. Assert no success for an ambiguous unacknowledged record and no
   unsupported version on a newly negotiated socket.
5. Exercise large batches under bounded memory and scheduling checks with
   unchanged deadlines; verify cleanup and barrier state do not leak on close.

The current test harness and candidate are not evidence of these guarantees.
The patch and this plan require independent review, source-pinned runs and a
full client matrix; performance claims require separate measurements.

## Additional independent review: close versus admission

Closing the Tokio receiver and draining it is insufficient by itself: a sender
that reserved capacity before `Receiver::close` can still send after the last
`try_recv`. Use a per-worker synchronous admission gate shared by every
`WorkerHandle` and the worker. A sender obtains channel capacity outside the
gate, then takes the gate briefly, checks `accepting`, and sends through its
permit synchronously. On retirement, the worker takes the same gate, marks the
worker non-accepting, and closes the receiver before publishing a replacement;
then it drops the gate and drains all accepted records. A sender holding an old
permit must reacquire the gate and reroute when it sees retirement. This applies
to direct, sticky, try-send and retry paths. Never wait, invoke callbacks, or
perform I/O while holding the gate. Retiring a worker must compare identity so
an old failure cannot replace a newer owner.

A per-partition lane must be installed from the first issue, not only on error:
metadata can route a later record to a new worker before the old worker detects
failure. The lane owns its producer ID, producer epoch, ordered batches and
acknowledgment state. One live socket may pipeline up to the negotiated limit;
on leader or socket change the lane freezes new issue, retains the previous
sequence history, and releases or fences it only after earlier outcomes are
resolved according to Kafka semantics. Memory and lane counts require explicit
admission limits; removing idle lanes must not race another lookup. A keyed
mutex per record that serializes all successful requests would defeat batching
and is not an acceptable unmeasured substitute.

The receiver-retirement harness must hold an old channel permit across closure,
then verify it cannot enqueue after the linearization point, and must verify a
send already accepted under the gate is drained exactly once. Exercise async,
try-send, sticky, retry, cancellation, close and replacement-worker identity.

## Sequence allocation, wrapping and cancellation

Kafka Java 4.3.1's `DefaultRecordBatch.incrementSequence` uses nonnegative
sequence numbers modulo 2^31. The pinned local oracle records `MAX+1=0`,
`(MAX-1)+2=0`, and `(MAX-1)+3=1`; the matching decrement oracle records
`0-1=MAX` and `0-2=MAX-1`. The Rust record codec already implements that
arithmetic, but the producer's sequence allocation, per-record stamping and
rewind currently saturate instead. The oracle sources, outputs, jar hash,
launcher/JDK version and input hash checks are in
`/workspace/work/producer-sequence-java/`. Its first run failed due to the
Kafka 4.3 internal package path; that output is retained.

Changing arithmetic alone is insufficient. `assign_sequences` advances the
shared counter before cooperative stamping completes. A task can be canceled
while only part of its batch is stamped, and other workers can reserve later
sequences before a rewind. After wrap, comparing only `current == base+count`
also has an ABA ambiguity. The lane must allocate a unique generation-bound
issue/reservation lease and retain batch ownership while stamping and encoding.
Cancel before any bytes are sent may roll back only if the lease proves there
was no later reservation. Once a partial or full write is possible, the lane
must preserve the original batch sequence and record count for retry, or
explicitly fence/renew the producer identity according to Kafka semantics.
Do not detach an idempotent subset or reset an ambiguous batch to sequence zero.

Boundary histories must cover a multi-record batch crossing MAX, a subsequent
batch, successful and failed pre-send rollback before/after another reservation,
cancellation at cooperative stamp/encode yields, ambiguous partial writes,
retry after wrap, epoch bump, and topic identity or leader change. Wire-level
checks must compare producer ID, epoch, base sequence, batch count, and broker
contents against independent Kafka peers. Shared state is bounded by distinct
live or historical partition identities; a lane with no inflight records cannot
be discarded while its sequence under the current producer epoch remains needed.

A concrete lane state should contain the admitted topic UUID or a documented
name-scoped identity, partition, broker-returned producer ID and epoch, a
non-reused issue generation, the next modulo-2^31 sequence, the current socket
generation, and an ordered bounded queue of complete batch descriptors. Each
descriptor owns its immutable encoded records, base sequence, record count,
deadline, callbacks, permits, and attempt/ack state. Retaining a batch as the
unit of retry preserves broker deduplication; removing one member and retrying a
subset is not equivalent.

Before reserving, validate that batch count fits Kafka's signed wire count.
Reserve a unique issue token while holding the lane's short synchronous state
lock; attach a cancellation guard before yielding to cooperative stamping. An
issue lease must exclude another reservation on that lane until the first batch's
frame has been fully written or definitively failed before writing. Different
lanes can progress concurrently. A full write releases the issue lease but the
lane retains up to the configured five ordered unresolved batches on that same
socket. A partial or uncertain write freezes the lane and moves its entire
ordered unresolved prefix into recovery; it never rewinds the sequence merely
because the socket closed.

For a definite pre-write failure, rollback is permitted only when the producer
identity/epoch and unique issue token still match the lane's latest reservation,
no byte of that batch could have been sent, and no later reservation exists.
Comparing modular base/count against the current sequence value alone cannot
prove those facts after a full sequence wrap. Cancellation during stamping or
encoding follows this same rule. The guard must own the complete batch and a
reserved bounded cleanup handoff before the first yield, so a dropped future
cannot drop callbacks, byte reservations, or retry counters. A callback panic or
blocking interceptor must not prevent accounting and completion of the other
records; cleanup needs a defined panic and shutdown policy.

Connection replacement and leader change must freeze the existing lane before
another socket can issue a newer batch. Reconcile older attempts in their
original order with the same producer identity and batch bytes; only then may a
new issue proceed. An acknowledgment must match the socket generation, producer
identity and issue token before it can release a lane or barrier. A
broker-authorized producer epoch transition invalidates all old-generation lane
issues; transaction state must preserve the broker's commit/abort and fencing
rules. A change of topic UUID must not silently attach old retained records to
the recreated topic. When a broker does not expose UUIDs, use explicit
name-scoped semantics rather than inventing an identity.

Group requests spanning multiple partitions must acquire issue rights in a
stable key order or use a central scheduler that does not hold one lane while
waiting on another. Partition lane maps and deferred work need bounded admission
and idle retention rules: sequence state under the current producer epoch cannot
be evicted just because no records are currently inflight. A ready/deferred
queue should wake only affected partitions instead of scanning the entire map.
Performance must be measured: neither a global lock across encoding/network I/O
nor per-record locking that prevents batching is an acceptable unmeasured fix.
