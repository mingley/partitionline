# Metadata socket retirement candidate

This source-only candidate retains ownership of a metadata socket through
replacement, cancellation and close. It adds one producer-scoped deferred retirement
task and controls for partial writes, established requests, deadlines, SASL
and TLS. The [patch](candidate-v3.patch.gz), [preparation](preparation.json)
and [independent review](independent-review.json) bind five files to the v10
candidate inputs. Formatting and exact forward/reverse reconstruction passed.

No Cargo build, Rust test, live peer or benchmark has run. Deferred cleanup
requires the originating runtime to keep progressing and current lock owners
to release their locks. A timed-out or cancelled close does not itself prove
retirement. The TLS mock's close event records handler exit, not a TCP EOF.

The review also found an existing broader close-cancellation gap: a producer
worker can remain parked if close is cancelled before worker shutdown.
This patch does not resolve that gap. It has not been composed with the v12
queue changes, and it is not adopted on `main`.
