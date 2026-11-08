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

## Controlled cancellation and deadline witness

[Run 42448073643](https://github.com/mingley/partitionline/actions/runs/42448073643)
compared main `7f59ef470340db6027958f439df3716a8dde536c` with candidate
`dca26dfaf47989db15e3d27b0cddafc5483a9d84` on a shared x86_64 GitHub
runner using stable Rust 1.99.1, LLVM 23.1.1 and unoptimized test binaries.
Each arm dropped the same unpolled, one-record cleanup future on a
current-thread Tokio runtime. A timer had already been polled pending; a
separate thread could release the callback latch. The candidate's callback
wait was confirmed before the hold began. The callback wait, release and test
had timeouts.

| Observation | Baseline | Candidate |
| --- | --- | --- |
| Drop duration | less than 1 ms at the logged resolution | 600.342 ms |
| Error callback | not invoked | invoked once and held on the latch |
| Record result | receiver closed | `Timeout` |
| Reserved bytes after Drop | 1 | 0 |

For the candidate, relative to the start of Drop, the callback wait was
observed at 0.134 ms, the timer deadline was at 199.872 ms, and the latch
released at 600.272 ms. Drop returned at 600.342 ms while the timer task was
still incomplete; the task completed after the runtime could run again. The
baseline Drop returned before the same configured deadline. This shows a
specific timer task being delayed by direct cancellation cleanup; it is not a
measurement of `close_timeout` or a universal deadline bound. The baseline
already has other synchronous callback paths.

The baseline and candidate producer SHA-256 values were
`3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f` and
`2055b2882cb9c1e0bcdf242fbee45f1ed2799717c2950ffd9d49adc62f62d476`.
Both received the same test-only fixture, SHA-256
`419150a0803c9fe92ce2b5a810158bc4e21b27a63da4f738a82b6a840ae3ad70`,
in separate build directories. Their recorded binary SHA-256 values were
`872f26149ddae13e90111f61d96aca9d7b8d6cf123a59829835ed81297dfe79c`
and `033e72163374833daf4bb7bf3257a932ca941b8656c8c39b6c74dbeb75234b54`.
The binaries themselves were not retained. Artifact
`failure-cleanup-deadline-42448073643` has ID `13604441645`, SHA-256
`4b436ad492d0ef1d8be422cb9cb9aca1089790429cf8842d4ff4f085d6f90c79`,
and a reported expiry of 2026-12-08.

The candidate's separate [correctness run 42447458557](https://github.com/mingley/partitionline/actions/runs/42447458557)
passed 17 cleanup controls and 13 transaction-state controls in each of the
default and all-feature builds, plus rustfmt and strict all-target/all-feature
Clippy. Three negative controls reproduced on the baseline. Those are local
fixtures, not a complete ownership audit or real-broker qualification.
Earlier attempts are retained: [42446283426](https://github.com/mingley/partitionline/actions/runs/42446283426)
failed test formatting and Clippy; [42446896563](https://github.com/mingley/partitionline/actions/runs/42446896563)
failed test formatting and a baseline checkout omitted examples; and
[42447223619](https://github.com/mingley/partitionline/actions/runs/42447223619)
failed to compile a candidate-only thread control against the older baseline
helper signature. Those failures did not qualify a candidate.

## Completed failure-batch diagnostic

[Run 42447787704](https://github.com/mingley/partitionline/actions/runs/42447787704)
measured the same baseline and combined candidate producer sources in
separate stable-Rust release builds on one pinned x86_64 CPU of a shared
runner. Each sample completed a local batch of 16,385 one-byte records with
the same topic, individual oneshots and `Timeout` outcomes; there were no
interceptors or broker traffic. Each process warmed twice and then took eight
samples. Both arms checked every result, byte accounting, metrics, retained
batch pointer and capacity against the input, and the same callback-yield
sequence.

| Pair | Baseline median | Candidate median |
| --- | ---: | ---: |
| 1 | 564.803 µs | 600.777 µs |
| 2 | 562.879 µs | 600.866 µs |
| 3 | 545.217 µs | 579.574 µs |
| 4 | 566.540 µs | 575.769 µs |
| 5 | 567.827 µs | 595.834 µs |
| 6 | 555.569 µs | 588.162 µs |

Across 48 samples per arm, the baseline median was 562.734 µs
(range 539.524–584.081 µs) and the candidate median was 589.219 µs
(range 569.870–630.130 µs), about 4.7% higher in this fixture. This is a
short local failure-path diagnostic with no competing runtime task; it does
not establish general producer throughput or scheduler latency. The earlier
deadline probe in this benchmark lacked the later timing controls, so the
separate witness above is the evidence for deadline behavior.

The test-only fixture SHA-256 was
`7c8f1ec17d713d60688a5f409be9cff092df71193969b6ca3e0d48c15482c2be`.
Baseline and candidate binary hashes recorded by the job were
`f2bf1bbbb93634928076fdbb989e31e885956b0e6a29cdd1f5beadd1b0ee357e`
and `e96744d528103ba2c604e34fbbf3eb78ed75f85976eb7f6e4e2c213e1b06073c`;
the binaries were not retained. Artifact `failure-cleanup-bench-42447787704`
has ID `13604137763`, SHA-256
`7d1e8b3597a0240f0abefeee20531c0f9cc17383afe62e66731cfe57bbe39b1b`
and a reported expiry of 2026-12-08. The earlier benchmark setup
[42446656150](https://github.com/mingley/partitionline/actions/runs/42446656150)
failed before compilation or timing because a fixture directory was missing.

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

## Receiver lifetime and possible slot ownership

A reserved channel permit is not proof that a cleanup worker is alive.
[Run 42449432639](https://github.com/mingley/partitionline/actions/runs/42449432639)
tested the repository's locked Tokio 1.53.3 on x86_64 with stable Rust 1.99.1.
When the receiver was closed but retained, an already reserved
`OwnedPermit` sent a value and that receiver drained it. When the receiver
was dropped first, `send` still returned a closed sender without a delivery
error; the tracked value was destroyed by the time that final sender was
dropped. The test does not identify which of those two operations destroyed
the value. Both controls passed in default and all-feature builds. These are
channel tests, not producer or broker tests.

The [test source](https://github.com/mingley/partitionline/blob/50eed7b7445c1c1fec4523e3ad480cd253088f7b/tests/owned_permit_closed_witness.rs)
has SHA-256
`50bee13d6104a7e9e4bdac75b46f1ab7824901da8bada156a58dbfd1697cf146`;
`Cargo.lock` has SHA-256
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
[Run 42449260787](https://github.com/mingley/partitionline/actions/runs/42449260787)
failed before tests because the new integration test lacked a crate comment;
the later passing run includes that correction and a bounded receive.

A possible design to investigate is a preallocated registry with one
generation-checked slot per reserved record and a separately rooted
supervisor. A failed batch would transfer its vector into a slot reserved by
one member; the supervisor would settle it independently of the dropping
runtime thread. This is a design sketch, not a proven or implemented fix:

- The root must survive while admission is open or any pre-admission
  reservation, accepted record, ready slot, callback or outcome remains.
  Tokens and producer ingress need weak references back to the registry.
  Otherwise a queued oneshot's waker can retain a task and Producer, forming
  `registry -> job -> sender -> waker -> task -> Producer -> registry`.
  User-provided callbacks can create additional cycles themselves.
- A slot cannot be reused until its batch finishes; its generation must not
  wrap. Every queued batch must own at least one distinct live token.
  Supervisor failure, restart, slot publication, and exhausted capacity need
  explicit fail-closed behavior. A reserved slot or a bounded queue alone
  does not prove nonblocking submission or worker liveness.
- Count pre-admission reservations in the bound and lifetime, or acquire a
  worker permit, *try* a record slot, and release that worker permit before
  any wait if the slot is unavailable. Retries retain their existing token.
  An externally held, unpolled future can otherwise retain capacity past close.
- Failure of a sequenced sticky cohort needs a fence that linearizes with
  retry and fire handoffs. Deferring the current policy update alone permits a
  retry to pass its cancellation check first. Transaction failure publication
  must remain separately generation-scoped.
- A standard cleanup thread changes the callback's Tokio runtime and
  task-local context. Current `Inner::drop` also invokes interceptor
  `close` and notification wakers synchronously. Preserving callback order,
  context and shutdown semantics needs an explicit contract; catching a panic
  from a missing runtime is not equivalent to running the callback. Bound
  process-wide workers and retained records, including timed-out closes.

## Required controls

Test unpolled and partially polled cancellation, every handoff and retry,
retired workers, zero-byte records, queue saturation and closure, shutdown
without a runtime, callback/waker panic, blocked and reentrant callbacks, and
sticky cohorts spanning a failure. Exercise concurrent submitters against
reserved capacity and a cleanup worker that stops unexpectedly. Verify every
record outcome and exactly-once resource release. Model the ownership and
shutdown transitions, then run real-broker tests and matched x86 measurements
for normal throughput, cancellation work, memory, and scheduler latency.
