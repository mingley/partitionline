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
- In [public-history run 42460321562][public-run], the draft was exercised
  through public producer calls against a gated mock broker, with test-only
  producer observers and mock gates. Five positive histories passed in each
  feature mode. In a sixth, after one flush waiter was canceled, the surviving
  waiter returned Ok even though delivery and callback observed a broker error.
  The job deliberately recorded this expected negative;
  its successful job status does not qualify the draft or prove behavior with
  a real Kafka broker.
- The first [draft run 42459658935][compile-run] failed both feature modes at
  compilation because a test fixture lacked a `CompletionDomain` import; no
  controls ran in that attempt.
- In [draft run 42460005132][draft-run], both feature modes passed 11 focused
  controls and 23 producer-cancellation tests. Both then failed
  `cancelled_channel_admission_releases_only_the_unenqueued_reservation`:
  default returned `Timeout`; all features observed 4,096 buffered bytes
  versus 4,097 captured accepted records. Each arm had 7 passed, 1 failed and
  1 ignored in that test file; later retry and format checks were skipped.
  These results do not establish whether the draft, fixture timing, or another
  cause produced the failures.

- In [gated shutdown run 42461750015][shutdown-run], three mock histories
  passed in each source/feature arm. In the b7 draft, a second send waiting
  for buffer capacity remained pending for 200 ms after the close seal; after
  the held response was released it returned Closed, and final buffered bytes
  were zero. The earlier bc45 draft admitted that waiter after the release.
  This instrumented mock test does not qualify either draft. The waiter had not
  reserved its own bytes, so it does not reproduce the separate, source-derived
  concern about an already-reserved pre-admission future.
- A later [close-sealing draft run 42460603408][sealing-run] passed 14 focused
  controls and 23 producer-cancellation tests in each feature mode. Both
  modes then failed the same retained-backing test, with 4,096 buffered bytes
  versus 4,097 captured accepted records; each had 7 passed, 1 failed and 1
  ignored. Later retry and format checks were skipped. The snapshot assertion
  alone does not prove a leaked reservation or identify the cause.

A [matched retained-backing diagnostic][paired-run] ran the same single
fixture three times per arm, with separate target directories. The base was
`4409633fa989effe1083e623f6e59fcc0d179eee`, the draft was
`bc45ba4f65f126c406ff88a57fcfe08007d35b29`, and both used fixture SHA-256
`ecc74d9d33d1ff5544002b027fe907fa8542f11052224ee1aa45a74c7475e110`.
Every invocation failed the 4,096-versus-4,097 snapshot assertion:

| Source | Features | Failed runs | Elapsed seconds, in run order |
| --- | --- | --- | --- |
| Base `4409633` | Default | 3 of 3 | 24.028, 23.705, 23.671 |
| Draft `bc45ba4` | Default | 3 of 3 | 23.762, 23.775, 23.817 |
| Base `4409633` | All | 3 of 3 | 23.951, 23.887, 23.710 |
| Draft `bc45ba4` | All | 3 of 3 | 23.868, 23.944, 23.829 |

The job succeeded because it collected all observations. The runner reported
`/sys/fs/cgroup/cpu.max` unavailable; the CPU quota is unknown. These failed
test durations are not a performance comparison and do not establish the cause
of the assertion or prove a reservation leak. The diagnostic does not explain
the earlier default-feature `Timeout`.

## Short mock timing

[Run 42461275246][timing-run] compared base `4409633` with the bc45 draft
using default-feature release builds, one partition and 4,096 eight-byte
records on one hosted AMD EPYC 9V74 x86_64 machine. It alternated eight
samples per arm over approximately 0.15 seconds in total. Each sample checked
the complete ordered mock log, aggregate acknowledgement callback count,
error count and final byte accounting.

| Measurement | Base median | bc45 median |
| --- | --- | --- |
| Admit 4,096 records | 1.555 ms | 1.954 ms |
| Flush | 3.975 ms | 4.257 ms |

The producer source SHA-256 values were
`3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f` for
the base and
`0ec7876bd7a2954847694d497f9c2a4275d23f6a48b2e7fbf89d86dea447b6ea` for
bc45. Separate build directories produced different binaries, whose SHA-256
values were
`714a558c7df6afde6fa5e27cacf69c51d19eb105f74531d908c9a685941c68f0` and
`16aacaca991d64bf676c6eddd90c6eb2176ba373da622a586d4b35e5b3a1ed2d`.
The timing driver SHA-256 was
`51b5b5d5b595841e9179501bd59e9c1047a41d54a2f2f2f3304e94064b983076`,
the mock fixture was
`b3761d62c090d87251c1825e64ec468c5e092d22ca27fd4a690b46b7631fa5b1`,
and both lockfiles were
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
The binaries were not retained. The [sample artifact][timing-artifact] has ID
`13609867189` and SHA-256
`e84e1bcc680662220575cbbbc4d2fb5380594feea7d15d244f6170f9f39b3aa5`.

The runner reported `cpu.max` unavailable; its CPU quota is unknown. The
mock uses this repository's codec, and this short diagnostic neither measures
the later b7 draft nor compares Kafka implementations or establishes production
readiness.

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
- The earlier [bc45 draft][draft] captures a flush cutoff before close stops new
  sends. A later admission can remain Pending after shutdown starts while its
  Lost-only post-shutdown check returns success. In that version an earlier
  non-timeout worker error can also make `drain_before_abort` skip the ticket
  check and proceed toward `EndTxn`.
- The later [b7 draft][sealing-draft] seals admission first. A send that reserved
  resources before the seal can nevertheless remain blocked waiting for worker
  capacity until another event or its timeout; close can finish while an
  unpolled pre-admission future still holds its byte reservation. The revised
  abort check also has no transaction-generation fence against a concurrent
  admission after its check and before `EndTxn`.
- The draft's completion token finishes only after `Pending` is dropped.
  This closes one accounting-order window but still runs record destructors
  and wakes synchronously on the task performing the drop. Its polling
  fallback can miss a short deadline after completion.

The baseline [close and close_timeout][close-source] return Ok for non-Timeout
flush errors after shutdown. Propagating lost ownership from close would be an
explicit public contract choice; it needs compatibility documentation and tests.

## Requirements for a repair

The following slot and supervisor structure is one proposed design. Other
implementations may satisfy the same ownership and lifecycle requirements.

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

Another possible design keeps the existing `Pending` values in independently
rooted worker and batch state instead of allocating one slot per record. It
would avoid making per-record slots a prerequisite, but every currently
detached vector, iterator and scratch group would need a rooted, resumable
cursor. Transfers between a worker, retry actor and replacement worker would
still need exclusive ownership, stale-descriptor protection and a batch
sequence journal. The supervisor would wait for the old task to stop before
recovering its state, without holding locks across user callbacks. Neither
design has been implemented or measured.

The worker-root alternative has another source-derived lifetime risk. If a
suspended worker task or its join handle strongly retains a root containing a
receipt sender, the receiver's registered waker can retain a `Producer` clone.
A possible chain is `Shared.nodes` → worker handle and task → worker root →
receipt channel and waker → `Producer` → `Inner` → `Shared`. That retained
`Inner` can prevent its normal drop-triggered shutdown. A worker completing or
an explicit close may break particular histories; no public leak has been
reproduced by this review. See the current [pending][cycle-pending],
[worker task][cycle-task], [worker][cycle-worker] and [Inner drop][cycle-inner]
source.

A separate completion core does not by itself break this chain if a producer or
suspended task can strongly reach the core and its wakers or callbacks can
strongly reach the producer. Possible approaches include weak-only per-poll
leases that retain no such root while suspended, or separating user-retaining
receipt state with a proven acyclic ownership graph. For example, a
receiver-held receipt cell and a weak sender reference might break the direct
sender-to-waker edge, but that is only a proposal: cancellation, publication,
wakeup, receiver loss, shutdown and batch recovery still need implementation
and tests. User-provided interceptors may themselves retain producer clones;
explicit close and the callback lifetime contract still matter. Neither
alternative is qualified.

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
[public-run]: https://github.com/mingley/partitionline/actions/runs/42460321562
[compile-run]: https://github.com/mingley/partitionline/actions/runs/42459658935
[sealing-run]: https://github.com/mingley/partitionline/actions/runs/42460603408
[paired-run]: https://github.com/mingley/partitionline/actions/runs/42460753994
[shutdown-run]: https://github.com/mingley/partitionline/actions/runs/42461750015
[timing-run]: https://github.com/mingley/partitionline/actions/runs/42461275246
[timing-artifact]: https://github.com/mingley/partitionline/actions/runs/42461275246/artifacts/13609867189
[sealing-draft]: https://github.com/mingley/partitionline/blob/b7c11784d79b05a36ca624b3de17a952603d4931/docs/evidence/client/producer-completion-draft-20261108/README.md
[assignment]: https://github.com/mingley/partitionline/blob/1036a393eb85f23032b935bf86862b7e98e36e14/src/producer.rs#L10429
[response]: https://github.com/mingley/partitionline/blob/1036a393eb85f23032b935bf86862b7e98e36e14/src/producer.rs#L8670
[close-source]: https://github.com/mingley/partitionline/blob/1036a393eb85f23032b935bf86862b7e98e36e14/src/producer.rs#L4215

[cycle-pending]: https://github.com/mingley/partitionline/blob/262d2544a4352d52538f19ef9ab0c8c5700774d3/src/producer.rs#L858
[cycle-task]: https://github.com/mingley/partitionline/blob/262d2544a4352d52538f19ef9ab0c8c5700774d3/src/producer.rs#L898
[cycle-worker]: https://github.com/mingley/partitionline/blob/262d2544a4352d52538f19ef9ab0c8c5700774d3/src/producer.rs#L7284
[cycle-inner]: https://github.com/mingley/partitionline/blob/262d2544a4352d52538f19ef9ab0c8c5700774d3/src/producer.rs#L1268
