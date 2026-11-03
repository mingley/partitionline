# Removing prefix shifts

The retained `Vec<Pending>` still moves the remaining elements on each
prefix drain. A follow-up representation should remove this cost without
weakening the worker's cancellation ownership.

A `VecDeque<Pending>` can provide amortized constant-time front removal.
Packing and counting must iterate both wrapped slices rather than call
`make_contiguous` on every batch; repeated wraparound at high occupancy can
otherwise reintroduce linear moves. Stable expiry and sticky partitioning
need an adapted, worker-owned split state with original-order restoration on
drop.

Tokio's `recv_many` fills a `Vec`, so a deque needs a worker-owned receive
staging vector. After the receiver succeeds, records in that staging vector
remain the worker's responsibility through every cooperative transfer and
accounting yield. Drop must restore records in admission order, including
staged records and any partially permuted split. Capacity and retained bytes
must stay within the existing resource contract; a second buffer is not free.

Tests should exercise wrapped queues, equality and nonmonotonic deadlines,
older retries arriving after newer records, short and full batches, expiry and
sticky split cancellation at every suspension, callback/byte accounting, and
shutdown with staged records. Count moved records across a drain/refill cycle
as well as deadline comparisons, then benchmark against the same peer settings.
The sketch has not been implemented or measured.
