# Producer completion model

This is an isolated design model. It is not connected to the producer and does
not fix its flush behavior.

A [public producer test](https://github.com/mingley/partitionline/actions/runs/42458476249)
shows why the work is needed. A mock broker appends a record and drops the
Produce response. The real worker retires; a test-only gate pauses its retry
handoff before the retry counter increments. While the receipt is pending,
`flush_timeout` returns `Ok`. After release, the idempotent retry completes,
one target acknowledgement callback runs, and mock readback contains the
target once. The gate changes scheduling and the mock uses this repository's
codec, so this is not an unmodified-source or real-broker reproduction.
[The public histories](https://github.com/mingley/partitionline/blob/9b6cb3a8a23007c9f1537c94117142b0dbfb0e61/docs/evidence/client/producer-flush-public-20261108/README.md)
record the setup, limits, and source pins.

The [model source](https://github.com/mingley/partitionline/blob/dce2ec64c0366a102568b2b15ca9d23f61a82ba7/docs/evidence/client/producer-completion-model-20261108/model.rs)
is on a separate qualification branch. It assigns admitted records monotonic
sequences, tracks a contiguous completion prefix, and retains bounded
publisher-operation markers through completion and retry. A flush takes its
cutoff on first poll. Modeled recorded errors are claimed when a flush returns
them, not when an intermediate message is sent.

[Run 42458745739](https://github.com/mingley/partitionline/actions/runs/42458745739)
passed all 17 model controls and rustfmt on x86_64 with stable Rust 1.99.1 and
LLVM 23.1.1. Controls cover overlapping and canceled flushes, claim/recovery
and publish/abort races, error and deadline ordering, counter exhaustion, and
50,000 seeded out-of-order operations checked against a reference set. The
model blob is `70af297799d4e0897631154289246a51d8f401d7`; its SHA-256 is
`a9da4159f464f3b51177388a0e7716a3a7ab9ec7dec91fe331828060af69b3b9`.
The hosted job also measures a standalone model loop; it is not a producer or
broker performance comparison. Earlier runs
[42458151225](https://github.com/mingley/partitionline/actions/runs/42458151225),
[42458463274](https://github.com/mingley/partitionline/actions/runs/42458463274),
and [42458620074](https://github.com/mingley/partitionline/actions/runs/42458620074)
passed their then-current controls but failed formatting.

The model does not implement real channels, receiver wakeups, callback or
receipt execution, byte reservations, sticky routing, transaction fencing,
close, or runtime shutdown. Its poll is manual, its first-error slot
coalesces errors without cutoff scoping, and in-flight recovery assumes the
old owner has stopped. Its blocking mutex can delay a deadline. It does not
resolve ambiguous Kafka writes or prove real record ownership. A production
barrier must cover those boundaries together before it can replace the
documented best-effort flush.
