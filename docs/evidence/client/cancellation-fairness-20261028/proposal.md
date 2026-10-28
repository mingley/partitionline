# Design: bounded cancellation for rebalance commits

Status: design only. No code, new dependencies, or qualification claim. The
relevant current functions are `Consumer::positions_cooperative`,
`ConsumerGroup::rejoin_once`, and `revoked_commits` in src/consumer.rs and
src/group.rs. The later test executable is pinned by `fairness-serial-01.json`,
`fairness-failures-individual/result.json` and `client-share-interval-unit.json`.
It is a different binary from the original full matrix executable; the same
test names were replayed against it, not against the original binary.

## Failure mechanism and measurement

`revoked_commits` takes `Vec<(TopicPartition, i64)>` by value, calls
`into_iter()`, builds a separate output Vec, and yields every 8192 items.
When a `tokio::select!` timer wins, Rust drops the canceled future synchronously.
The drop of the residual `Vec::IntoIter` visits each remaining owned String;
the partial output Vec also has individually owned Strings. This is O(n)
destructor work in the caller before it reaches its elapsed assertion. The
original matrix recorded 131 ms in this test; a later-binary individual replay
took 236 ms and also failed. Yields constrain poll work, not the destructor. This is a source-based
explanation, not yet an instrumented attribution; separately measure select
return time with a borrowed externally-owned input, future drop time, iterator
remaining length, partial output length, and a timer peer before claiming the
relative contribution of drop, HashMap rehash, OS scheduling, or input work.
Retain original and interrupted logs.

## Preferred first step: long-lived group scratch

1. Store bounded `RebalanceScratch` in `ConsumerGroup`, with fields for
   positions, filtered results and explicit phase/generation state. The group,
   not the cancelable `rejoin_once` future, owns all potentially large scratch
   containers from construction through the entire poll and cancellation.
   Change the internal position collector to write into `&mut` owner scratch;
   it must not create another million-element local output while it yields.
2. Generate shared `(Arc<str>, partition, delivered_offset)` values directly
   from assigned state and `delivered_position`. This avoids allocating a new
   String per position. Traverse cooperatively in original order; filter using
   the same revoked membership, nonnegative position, leader epoch and topic-ID
   rules. With no interceptors, `commit_shared_positions` can send this shape.
   If interceptors are installed, construct the same typed values and invoke
   them with the original order, metadata and error behavior. Confirm the v10
   topic-ID path before changing it; do not infer equivalence from type names.
3. On each new attempt, invalidate the prior generation before inspecting any
   output and drain both scratch vectors in small bounded batches, yielding
   between batches. Never call `Vec::clear` or drop a large `IntoIter` on the
   async runtime. If this cleanup is itself canceled, the owning group retains
   the remaining and partially drained state. Do not reuse a partially built
   vector as a successful commit. Mutate scratch only through stable group
   fields, not a `mem::take` that transfers ownership into the cancelable future.
4. Put a separate explicit maximum on scratch bytes/items. Existing consumer
   `buffer_memory` counts record payload and permits zero as unlimited; it does
   not cover positions or this scratch, so it is not a valid substitute. Before
   reserving/growing, use checked arithmetic for `Vec` capacity times element
   size and conservative topic-name/allocator/index allowance. Charge scratch
   and retained canceled work for their full capacities until reclaimed. Reject
   or backpressure *before* allocating above the budget; do not make a transient
   duplicate that exceeds it. Define behavior for a single item larger than the
   cap and configuration zero explicitly. Expose configuration and errors in
   release notes: `ConsumerConfig` currently has public fields, so adding a
   field breaks external struct literals unless introduced by another compatible
   ownership/configuration mechanism. Tests must use a declared cap large enough
   for the existing 2M case, rather than silently shrinking its workload.
5. This approach allocates no cleanup task per call and improves the normal
   path by sharing topic names, but retained scratch occupies memory after a
   canceled operation until the next cleanup or explicit close. Group ownership
   is application-controlled; multiple groups multiply a per-group cap. If a
   process-wide bound is required, reserve a shared byte ledger as well.

## Unpolled cancellation and final owner drop

An `async fn revoked_commits(Vec<...>)` immediately captures its owned input in
the returned future even before first poll. If that future is dropped unpolled,
no async cleanup can run. The helper must instead receive a borrowed
`RebalanceScratch` already owned by the group. Its constructor may not accept
an arbitrary `Vec` and merely await admission later. Regression: construct the
future and drop without polling, measure the caller block, assert all scratch
is still owned and subsequently reclaimed under the advertised budget.

This alone does not bound `ConsumerGroup::drop` or cancellation of `leave(self)`,
which consumes and then drops the owner. Explicit `leave`/`close` must drain
scratch cooperatively before final release, including errors and cancellation;
that cannot make an arbitrary direct Rust `Drop` asynchronous. If nonblocking
final drop is a requirement, add a separate bounded cleanup service:

- A fixed worker count and bounded queue, plus a slot and byte credit reserved
  *before* creating/owning each cancellable work item. Every admitted item is
  guaranteed its own return slot; `try_send` to an unreserved/full queue is not
  a solution, and neither is spawning one detached task/thread per drop.
- A Drop guard moves the work item to its already-reserved slot in O(1), with
  credit held until the worker has actually destroyed it. Fixed workers perform
  the destructor off the async runtime; using chunked items permits fair cleanup
  and bounded per-job latency. Arbitrary user-defined `Drop` must not be sent
  without assessing blocking/panic behavior. Known Kafka data types are suitable.
- Credit covers both active and queued objects, vector capacities, string and
  Arc storage, HashMap bucket/control storage, queue descriptors and scratch;
  allocator overhead must be separately accounted or conservatively bounded.
  Saturation prevents admission without allocating another large input. One huge
  input cannot bypass limits through an unchecked integer conversion or reserve.
- Stop accepting new work on close, cancel/join admitted operations, drain the
  queue and join every worker; retain errors. Worker-start failure occurs before
  accepting ownership. Queue/channel closure or worker panic must not discard
  work or credit. An explicit synchronous fallback on internal failure is safe
  for memory but cannot be advertised as latency-bounded; make it observable.
- A process-wide service needs explicit ownership and close/join semantics.
  Per-group workers are simple but can create an unbounded number of threads as
  applications construct groups; a global singleton without an explicit joined
  shutdown has lifecycle ambiguity. Neither should be introduced without a
  resource contract and measured overhead. Semaphore waits may not own arbitrary
  unbudgeted inputs while pending. Cross-runtime destruction and shutdown from
  a current-thread Tokio runtime require specific tests.

## Test and benchmark plan

- Retain the existing 2M `revoked_commits` shape and 30/80 ms thresholds. Add
  cancellation before first poll, at a yield boundary, repeated cancellation
  during scratch drain, and successful next poll. Verify original ordering,
  epoch, topic IDs, negative-offset exclusion, interceptors, and no duplicate or
  lost commits. Verify that dropping/closing the owner has separately stated
  behavior; never move owner destruction outside the timed region and then claim
  it is bounded.
- Use instrumented Drop counters to check exactly-once destruction after every
  cancellation and joined close. Test empty input, maximum-size names, long
  shared names, all-revoked and no-revoked cases, capacity overflow and failure
  injection. Assert accounted bytes never exceed configured/group/global caps,
  queued credits remain held until cleanup ends, and the next admission sees
  backpressure under saturation. Stress many groups, lost runtime, worker panic,
  and shutdown; assert all work is joined and no task/FD/thread is leaked.
- Instrument time before timer poll, after select winner, before/after future
  drop, and per cleanup chunk with monotonic time; measure multiple isolated
  runs on x86 alongside unmodified matrix. Report wall-clock scheduler delay
  separately. Verify chunk maxima empirically under slow allocators and long
  names; counting 8192 items is not itself a guarantee of an 80ms bound.
- Measure release-mode allocations, CPU and p99 for typical 1/16/1024 partitions
  and wide assignment churn. Compare with baseline, report all results; optional
  cleanup-worker thread and global lock costs must be included. This is an
  implementation plan, not an optimization or production-readiness result.

## What chunking can bound

For an owner-retained Vec with `n` initialized elements, dropping at most `k`
elements per poll yields at least `ceil(n/k)` polls for the cleanup itself. If
each destructor is bounded by `d`, poll-local overhead by `p`, and final buffer
release by `f`, the work bounds are approximately `k*d+p` per non-final poll
and `k*d+p+f` for the final poll. At n=2,000,000 and k=8,192 that means 245
cleanup polls; k=1,024 means 1,954. A count bound is not a wall-time guarantee:
allocator locks, destructors, page faults, scheduling, final buffer deallocation
and user-defined drops may have long or unbounded wall time. Chunking a nested
container is insufficient if dropping its iterator or its owning outer Vec still
destroys every remaining chunk synchronously. Pop/drain the elements while the
owner remains alive, yield explicitly, and keep final-allocation release in the
measurement. Instrument each chunk and include the max, p99 and eventual full
reclamation, not just a low average or the timer winner. No measured chunk time
is asserted here; the host is compiling other targets and cannot currently
provide an isolated timing result.
