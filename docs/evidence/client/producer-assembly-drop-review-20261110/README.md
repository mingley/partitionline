# Mixed-batch assembly cancellation: source review

This is a read-only design review and a synthetic witness. No guard or
producer fix is applied. The reviewed main source is
[`ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9`](https://github.com/mingley/partitionline/blob/ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9/src/producer.rs),
producer blob `426dbdb5c5e27fdf655930a76682c5c18fb07e89`.

## Ownership at the yield

In [`assemble_mixed_groups`](https://github.com/mingley/partitionline/blob/ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9/src/producer.rs#L9897),
a suspended mixed-batch assembly can have records in three places: completed
groups in the local output vector, the unconsumed part of the current topic's
`groups.order.drain(..)`, and later topics in `scratch.buckets`.
The [worker Drop](https://github.com/mingley/partitionline/blob/ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9/src/producer.rs#L7564)
handles `pending` and `in_flight` but does not drain those assembly
owners. Restoring records to scratch alone does not make Worker Drop complete
or requeue them.

The isolated [synthetic control](https://github.com/mingley/partitionline/blob/2ed232500456a48d063ca81e09ad638225fa43be/src/producer.rs#L17360)
constructs 8,193 one-record topic buckets, polls assembly once, then drops the
pending future. In [run 42868667760](https://github.com/mingley/partitionline/actions/runs/42868667760),
8,192 receivers observed a closed sender and one bucket remained in scratch.
The assertion that no receiver close failed. The workflow marks this
diagnostic `continue-on-error`, so its overall success does not mean the
diagnostic passed. It is a private, constructed state at the isolated
candidate source; it is not a public producer history or a Kafka result.

A dropped sender already maps to `Error::Closed` in [`send`](https://github.com/mingley/partitionline/blob/ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9/src/producer.rs#L2624),
but that says neither that the entire producer closed nor that Kafka did not
append. `try_send` records have no per-record receiver. Dropping `Pending`
alone does not run the normal error interceptor or release its charged buffer
bytes; its sticky record-slot permit is a separate RAII field.

## Error and cleanup boundary

Assembly happens before the current Produce is encoded or written, but a
`Pending` can be a retry of an earlier ambiguous attempt. A terminal error
at this point must report local completion without asserting that the broker
did not append a previous attempt.

The [public error enum](https://github.com/mingley/partitionline/blob/ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9/src/error.rs#L14)
has no dedicated delivery-outcome-unknown variant. `Timeout` is retryable and
the [error contract](https://github.com/mingley/partitionline/blob/ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9/docs/api-stability.md#L130)
treats produce timeouts as ambiguous, but describes a deadline expiry, which
an early worker abort need not be. `Closed` can describe a closed connection
but is classified fatal, and the guide tells callers to create a new producer.
Choosing either for this new case needs an explicit documented contract; a
broker error code would falsely imply a broker response.

A terminal-cleanup guard is mechanically possible in a limited sense without
adding a Worker field: pass the needed shared context into assembly, restructure
the state so the guard owns the output, the active topic iterator and remaining
topic order across every await, and use the order to find the remaining
relevant buckets. A guard around only the output misses the other owners.
Disarm only after all groups transfer to the successful return. This needs
careful move/drop ordering: an unowned `Vec::Drain` can drop its remaining
records before an outer guard sees them.

For each accepted record, cleanup must retire the sticky ledger, release its
charged bytes exactly once, release its slot permit, account the error, and
settle its receiver and interceptor consistently. The existing
[synchronous failure helper](https://github.com/mingley/partitionline/blob/ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9/src/producer.rs#L11562)
does this work for a complete vector, but invokes user interceptors before
sending each result. Callbacks or wakers may block or panic, and a Drop cannot
await. The separate [cleanup witness](../producer-failure-cleanup-20261108/DEFERRED.md)
observed a callback holding a current-thread runtime past a timer deadline;
it did not measure `close_timeout`. A direct Drop guard cannot promise
bounded completion or all notifications under arbitrary user code. Requeueing
from Drop also needs a full/closed policy and ordering guarantees; it is not
established by merely moving the records back into scratch.

The existing `group_pending` function has earlier await points while it
owns the batch and while records are moved into buckets. A guard scoped only to
`assemble_mixed_groups` would not repair those paths, later delivery, flush,
close or transaction behavior.

## Before applying a local change

Use deterministic controls at each assembly yield with multiple partitions
per active topic and multiple remaining topics; verify explicit outcomes,
callbacks, bytes and sticky permits, including `try_send`, dropped receivers,
retry records and shutdown. Check callback and waker panic/blocking behavior,
reentrancy, queue saturation, and elapsed-time effects separately. Compare the
normal path and cancellation cleanup on the exact candidate. Keep any claim
limited to the tested assembly boundary until broader producer histories and
real-broker behavior are qualified.
