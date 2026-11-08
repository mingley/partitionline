# Producer completion ownership

The producer does not yet have a cancellation-safe completion barrier. The
standalone [completion model][model] and an [integrated draft][draft] are
experiments, not production fixes. The draft stays off main.

## Evidence

- The [sequence reservation witness][sequence] passed two focused controls
  with default features and two with all features. Dropping an artificial owner
  during the real `assign_sequences` helper closed 8,193 receipts while the
  sequence map remained advanced. The fixture did not run a worker, account
  bytes, send to Kafka, or demonstrate a broker-side sequence failure.
- The [handoff-waker witness][waker] observed a queued record and a double
  byte release after an injected receiver waker panicked. It did not establish
  a public API trigger or that an ordinary Tokio worker waker panics.
- A separate [cleanup experiment][cleanup] passed its scoped controls but
  ran the remaining interceptor callbacks synchronously on cancellation. A
  blocking callback delayed an unrelated timer in its controlled current-thread
  test. That synchronous-drop candidate remains unapplied.
- In [draft run 42460005132][draft-run], both feature modes passed 11 focused
  controls and 23 producer-cancellation tests. Both then failed
  `cancelled_channel_admission_releases_only_the_unenqueued_reservation`:
  default returned `Timeout`; all features observed 4,096 buffered bytes
  versus 4,097 captured accepted records. Each arm had 7 passed, 1 failed and
  1 ignored in that test file; later retry and format checks were skipped.
  These results do not establish whether the draft, fixture timing, or another
  cause produced the failures.

## Source review

The following are interleavings derived from code inspection, not reproduced
public producer or broker failures:

- [Sequence assignment][assignment] advances the partition counter before a
  yielding stamp operation. Cancellation can detach the unsent batch from the
  worker that would otherwise own it. A completion count alone cannot restore
  the reserved Kafka sequence range.
- [Response handling][response] removes an in-flight batch from its drop guard
  before several yielding steps. Grouping, retry and terminal helpers also
  hold moved records or sibling groups across yields. On cancellation, the
  worker's remaining fields do not necessarily contain those records.
- A retiring worker can finish a failed record's draft ticket before it calls
  `note_fail`. A concurrent flush that no longer sees that worker can return
  before its recorded error is published.
- The draft captures a flush cutoff before close stops new sends. A later
  admission can remain Pending after shutdown starts; the draft's Lost-only
  post-shutdown check can still return success. An earlier non-timeout worker
  error can also make `drain_before_abort` skip the ticket check and proceed
  toward `EndTxn`.
- The draft's completion token finishes only after `Pending` is dropped.
  This closes one accounting-order window but still runs record destructors
  and wakes synchronously on the task performing the drop. Its polling
  fallback can miss a short deadline after completion.

## Requirements for a repair

1. Give each accepted record a bounded, independently rooted owner for its
   immutable payload, receipt, byte reservation, sticky permit and completion
   sequence. Queue and batch state should carry generation-checked keys.
   Worker and retry owners must transfer a key atomically; stale queued
   descriptors must not deliver twice. Pre-admission caller inputs remain
   caller-owned.
2. Root worker receivers, batch vectors and scratch state outside cancellable
   futures. Cancellation should mark a worker or batch for recovery without
   draining records, running callbacks or invoking user wakers on the dropping
   runtime thread. The supervisor must wait until the old task has stopped
   before taking over its in-flight work. Use bounded batch jobs and a bounded
   callback executor, not a separately spawned task per record.
3. Journal each ordered Kafka batch with its producer identity, epoch,
   sequence range and write state. Journal the range and advance the sequence
   map in the same non-yielding critical section. A never-written tail can be
   rolled back only when safe; a possibly written batch needs idempotent retry
   rules or a producer/transaction fence. Preserve partition and sticky-cohort
   order.
4. Capture the flush cutoff at first poll under the admission lock. Wait for a
   cumulative completed prefix, including earlier records even if another
   overlapping flush is canceled. Limit records, publisher operations and
   waiters; reject counter reuse on exhaustion. A persistent cutoff signal and
   bounded worker intake are needed so later traffic cannot starve the drain.
5. Seal admission and take the close cutoff together. Keep accepted retries
   distinct from new sends, and retain unfinished cleanup after a close
   deadline. Keep the rooted completion core free of strong edges back to
   `Producer` or `Shared`; a receipt waker or interceptor may itself retain
   a producer clone. Explicit close is still needed for user-created cycles.
6. Preserve callback and receipt order, release each reservation exactly once,
   and publish applicable worker/transaction errors before advancing the
   completion horizon. Use typed internal outcomes for completion, caller
   deadline, closed transport and lost ownership. Transaction commit and abort
   need a separate transaction-generation admission fence; acknowledging a
   flush error must not clear transaction failure state.

No implementation can guarantee that a callback which blocks forever finishes
before a strict close deadline. The service must retain its unfinished work
without blocking the caller, and the API must define callback execution context
and panic behavior. Dropping an unpolled public send can still drop its
caller-provided record or iterator; the cleanup guarantee must distinguish that
case from records already accepted by the producer.

Qualification needs gated cancellation at each detach and handoff, overlapping
flush and partial `send_all` histories, blocking and panicking callbacks,
receipt and byte-accounting controls, real public producer retry/close and
transaction histories, and a paired x86 performance comparison with pinned
source, binaries and settings. None of those remaining checks is satisfied by
the standalone model or the private helper witnesses.

[model]: https://github.com/mingley/partitionline/blob/dce2ec64c0366a102568b2b15ca9d23f61a82ba7/docs/evidence/client/producer-completion-model-20261108/README.md
[draft]: https://github.com/mingley/partitionline/blob/bc45ba4f65f126c406ff88a57fcfe08007d35b29/docs/evidence/client/producer-completion-draft-20261108/README.md
[sequence]: ../producer-sequence-cancel-20261108/README.md
[waker]: ../producer-waker-handoff-20261108/README.md
[cleanup]: ../producer-failure-cleanup-20261108/DEFERRED.md
[draft-run]: https://github.com/mingley/partitionline/actions/runs/42460005132
[assignment]: https://github.com/mingley/partitionline/blob/1036a393eb85f23032b935bf86862b7e98e36e14/src/producer.rs#L10429
[response]: https://github.com/mingley/partitionline/blob/1036a393eb85f23032b935bf86862b7e98e36e14/src/producer.rs#L8670
