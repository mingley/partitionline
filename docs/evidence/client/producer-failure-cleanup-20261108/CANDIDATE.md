# Failure cleanup ownership candidate

This isolated candidate changes the ownership of one producer failure batch.
It is not a flush-barrier repair or a production-readiness qualification.

The helper now constructs an owner before returning its future, including when
the future is never polled. `Worker::fail_kept_batch` also constructs that
owner before returning its future. Cancellation of either future settles the
batch: sticky retirement and byte release occur once, each record gets a
terminal result when its receiver is present, and sticky record permits are
dropped. Normal execution preserves whole-batch sticky retirement before the
first yield and whole-batch byte release before error callbacks.

A callback panic is caught while settling its record so that the result can
still be sent. The first panic on normal execution is resumed after the rest
of the batch is settled; secondary panics and panics during cancellation
cleanup are contained. A panic in one interceptor still skips the remaining
interceptors in that chain for that record, as before. If copying an
`Error::Io` panics in its underlying formatter, the result falls back to an
I/O error with the original kind and the panic follows the same rule.
Accounting changes are marked complete before notifications invoke task wakers.

Normal callback processing preserves the previous short leading chunk and
yields between chunks of at most 8,192 records. It also preserves the earlier
pre-callback yield count. This is a record-count bound, not a bound on time
spent scanning headers or running callbacks. Whole-batch sticky retirement remains synchronous. Cancellation
Drop runs callbacks synchronously on the thread dropping the future, which may
be outside a Tokio runtime; it must settle the remaining suffix before
returning. An indefinitely blocking callback can therefore block it, and
`panic=abort` cannot be caught. Handling
a panicking waker does not prove that every other waiter was woken.

This owner begins at the helper or batch-wrapper call boundary. It does not
cover records held by callers before that point. In particular,
`fail_kept_groups` and `fail_kept_inflight` still hold sibling group vectors
across awaits; retry, acknowledgement and other detached-batch paths need
separate ownership work. The producer flush completion/error barrier remains
open.

The base witness [run 42444842648](https://github.com/mingley/partitionline/actions/runs/42444842648)
showed a canceled one-record helper closing its receiver and retaining one
reserved byte on source `374dcf45db20ee728b00c0b0188e77ca1f47fa9c`;
the completed control returned Timeout and released that byte. Candidate
qualification and independent review must be recorded separately before
adoption.
