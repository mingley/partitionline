# Off-runtime disposal: design sketch

This is a proposal for review, not an implemented or qualified change. The current source candidate stays isolated. The [poll measurements](README.md#combined-observer-diagnostic) show why moving large frees off the async caller matters: a 256 MiB caller spare took a 16.134 ms median to drop in its first public Router poll on the measured host.

## Constraints

- Before its first poll, the caller's future owns the supplied Vecs, has no data permit and has not allocated a replacement. Dropping that future can synchronously free those Vecs; admission controls have not run yet.
- No normalizer or disposal task may release a data permit before every buffer it still owns has either been dropped or transferred to the caller.
- Cancellation and shutdown must not abandon queued cleanup or start an unbounded number of tasks.
- Admission, worker setup and any allocation that can return an error must complete before an actor can make a durable mutation. Completion and reclamation after a mutation must not add a new fallible allocation or change a successful receipt to an error.
- A large allocation and its replacement can coexist during copying. Neither a queue-count bound nor the existing logical request/response budget bounds the capacity of the caller's original Vec or spare, allocator overhead or process RSS.
- Normal in-budget exchanges should keep their existing fast path. A valid request should not gain a new wire error solely because its Vec has surplus capacity.

There is a fundamental overload tradeoff in the current consuming API. If a finite reclaimer is full or unavailable, an arbitrary caller can still pass another large owned Vec. Immediate rejection with only an error must drop that Vec somewhere; refusing to block the caller then requires returning ownership in the error or providing more storage/workers. A finite queue cannot simultaneously promise no caller-thread drop, no retained memory and no new rejection under arbitrary input. The current API cannot return the buffers on error.

## Candidate design

Create a fixed, process-wide worker pool with a fixed number of preallocated data slots and a preallocated bounded queue. Each slot holds its original request, spare, replacement, response as applicable, the Router data permit and a global slot credit. Slot handles contain an index and generation. Reuse must not wrap the generation while an old handle can exist. A per-slot state machine and a queued bit allow at most one queue entry per live slot; stop notifications use separate control state and do not consume slot queue capacity.

For an oversized input or spare, obtain a data permit and a pool slot, install the complete ownership record and enqueue it before the first await. A worker checks stopping/cancellation, drops oversized scratch, reserves and checks the replacement capacity, and copies the input. Original and replacement may overlap, and reserve/free may block a worker. Only after normalization can the actor receive access to the request. If normalization fails, the worker drops all owned Vecs before releasing both permits and reports the existing allocation error; no actor mutation has begun.

When a future is canceled while a work item is queued or copying, it marks the slot canceled. The worker, rather than the caller, owns and drops the buffers. If cancellation occurs after actor admission, the actor remains the owner while it uses the request. Cancellation must not free an active actor's bytes or make a reclaimer wait while holding a worker thread on a storage operation. Instead, the last active owner schedules the slot for reclamation after the actor finishes or unwinds.

A completed but unconsumed reply must not put a large Vec directly inside a caller-dropped oneshot. The receipt should carry a small handle while the slot retains the buffers and permit. Consuming the receipt moves the Vecs to the caller without allocation and then releases the permit. Dropping it marks cancellation and schedules worker-side disposal. Slot-state transitions must make consumption and cancellation mutually exclusive, avoid waking user code under locks, and preserve drop order on panic.

Grouped Produce currently separates request bodies, spares, results and permits into parallel arrays. An asynchronous Vec drop would let the permit be released by the sender array while cleanup is still pending. The grouping path must instead keep each permit attached to its slot until transfer or completed worker disposal. The existing response construction failures remain separate; this proposal does not add output compaction or another post-commit allocation.

The worker queue must not allocate when an already reserved slot is reclaimed. Queue insertion and notification must be infallible under the slot-count invariant, with a safe fallback if an internal invariant or worker fails. A short preallocated ring protected by a mutex is one possible safe-Rust implementation, but lock contention, worker preemption, allocator calls and deallocation can still delay progress. Do not claim a hard poll-time bound from this design.

## Shutdown and overload

Stopping closes admission and marks in-progress normalizations canceled. The actor is drained and joined. Already queued or active worker cleanup remains owned by the pool and is counted until the worker actually drops the buffers and releases permits; shutdown must not discard that queue or detach per-request cleanup. A process-wide pool may remain alive to service receipts that are still held by callers after Router shutdown; that outstanding state must be counted and documented rather than described as fully reclaimed.

A stricter quiescent shutdown can wait for all completed receipts to be consumed or dropped, or report their count while leaving cleanup service alive. Waiting can be indefinite if a caller retains a receipt. Reclaiming an unconsumed successful receipt on shutdown would change its later outcome after a durable mutation, so it cannot be silently substituted. The existing shutdown contract and any new quiescent API need explicit review.

If no pool slot exists, a compatibility fallback can use the existing synchronous normalization/disposal path before actor admission. This preserves the request result and keeps the number of worker jobs bounded, but can stall the caller under overload or worker failure. The fallback runs in the caller's future; it must not spawn another untracked task. Alternatively, an explicitly designed API can return request and spare ownership with an admission error; the caller then decides how to dispose of them. These are different contracts, and neither should be hidden behind an absolute nonblocking claim.

Routing only over-capacity inputs also leaves the baseline behavior for an in-budget but large Vec: its eventual drop may still stall an async caller. Routing based on actual allocation size could cover more cases, at the cost of additional queueing and credit use. Either scope must be stated and measured.

The pool limits the number of accepted cleanup records, not their bytes: an oversized caller Vec can have arbitrary capacity while it waits in the queue. An optional byte-admission policy could account for known Vec capacities, but rejecting otherwise valid requests based on capacity changes behavior unless buffer ownership is returned. The retained logical budget and any such policy must stay distinct from allocator overhead, transient over-reservation and RSS.

## Controls needed before qualification

- Cancel before first poll, after slot admission but before worker claim, during copy, after normalization publication, after actor claim, after durable completion, and after a reply is published but before consumption. Verify exactly one cleanup and that the data permit remains held until the final owned Vec is dropped.
- Pause workers before claim, enqueue cleanup, start shutdown, and verify shutdown accounts for queued and active work. Exercise shutdown cancellation/retry and a held completed receipt without dropping its successful result.
- Saturate every slot with workers paused. Verify bounded queue/thread counts, the selected overload contract, no new post-commit error, and recovery after release. Cover slot generations, duplicate notifications, worker startup failure, worker panic and channel/queue error paths.
- Exercise grouped Produce, acks-zero, Fetch long polling and rejected requests; confirm valid bytes and existing timeout/wire behavior. Force allocator failure and over-reservation before mutation; verify the replacement's actual capacity and every buffer/permit drop order.
- Measure matched in-budget throughput, oversized input/spare first-poll latency, cancellation/reclamation latency and shutdown under a stalled worker, including thread and queue costs. Run full combined-tree broker profiles, strict Clippy/docs and inventory without erasing earlier failures.
