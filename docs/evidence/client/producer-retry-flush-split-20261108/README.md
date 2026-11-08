# Producer retry batches during flush

A controlled public-client history against this repository's mock shows that
`flush` can split a retained idempotent batch at a retry handoff. The mock had
already appended the original batch before dropping its response. Its split
retries failed with error 45, and the two observed delivery futures returned
errors even though their records were present in the mock log. The client
cannot infer from those errors alone that the records were absent.

This is an instrumented, single-partition mock history. It does not establish
that a real Kafka broker gives the same response, or that this schedule occurred
without instrumentation. No producer fix is applied.

## Existing-yield history

The baseline is main commit [`536d5e938aa2ad123471fe6446b0ba406dabe8cd`](https://github.com/mingley/partitionline/commit/536d5e938aa2ad123471fe6446b0ba406dabe8cd).
Its `src/producer.rs` blob is `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`,
SHA-256 `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`.
The test-only [witness commit `23c5c0d`](https://github.com/mingley/partitionline/commit/23c5c0d3440dd8786b2a4bdcae8f365596f45290)
replaces the existing [yield after 8,192 requeue offers](https://github.com/mingley/partitionline/blob/536d5e938aa2ad123471fe6446b0ba406dabe8cd/src/producer.rs#L9244)
with a controllable wait. It also synchronously observes successful
retry-to-worker handoffs. The wait holds that existing suspension point longer
than an ordinary yield; the observer adds work and can change scheduling. It is
not an uninstrumented reproduction.

Each arm used a current-thread runtime, one configured connection slot per
broker, one partition, an idempotent producer and a batch of 8,193 records.
After one acknowledged warmup record, the mock appended the original batch at
base 1 and dropped its response. The gate held the requeuing worker after 8,192
retry offers, while observers checked that those records reached a retry
worker and that the last record had not yet been offered. The test did not
assert distinct worker task identities or count active or overlapping sockets.
Two records had retained `send` futures; the other 8,191 were admitted with
`try_send` and had no individual receipts. The test checked all original mock
log values and their order.

[Run 42482668547](https://github.com/mingley/partitionline/actions/runs/42482668547)
passed the history once with default features and once with all features on
hosted x86-64, stable Rust 1.99.1 and LLVM 23.1.1:

| Arm | Observed mock writes and client results |
| --- | --- |
| Release before flush | A complete retry at base 1, count 8,193. The two retained receipts returned offsets 1 and 8,193, all 8,193 batch acknowledgement callbacks fired, and the mock log contained one copy of each record. |
| Flush while held | The retry at base 1 contained 8,192 records. The mock returned error 45; the first retained receipt and the flush returned error 45, and 8,192 batch error callbacks fired. The original 8,193 records remained in the mock log. |
| Release and flush the last record | A retry at base 8,193 contained one record. The mock returned error 45; the last retained receipt and second flush returned error 45. The aggregate batch error callback count reached 8,193, with the mock log unchanged. |

The fixture's error-45 rule for a repeated base with a different count, and its
sequence checking, are properties of this repository mock. Callback totals do
not provide individual receipts for the `try_send` records.

The qualification commit is [`b71115f3d1c77a0cab3e1a945c9df70844244949`](https://github.com/mingley/partitionline/commit/b71115f3d1c77a0cab3e1a945c9df70844244949).
The instrumented producer blob is `810d388e7fb57fb3959ece4484f1a565b21a915c`
(SHA-256 `a2974f5b855496d2e226ae0bf7eb18b7c40cc88da6043878c80bdc0c3ac75905`);
the test blob is `b1e8f1bce5b4a1da38f2229135153fc54dea036c`
(SHA-256 `66f9cdf32b229401b9167a0261624e15371fcdfc5024574663b2c4cc1e1d7c2b`).
The mock fixture SHA-256 is
`446e536ea6112a9655a89901e240c38eabc78c38087a1afb8df3fee86f63eddd`
and `Cargo.lock` SHA-256 is
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
Formatting was diagnostic only and reported differences in both test-only
files; strict formatting did not pass.

## Other controls and failures

[Run 42482166488](https://github.com/mingley/partitionline/actions/runs/42482166488)
passed both feature profiles for a two-record version. Its gate added an
otherwise absent wait immediately after the first successful handoff; it is a
weaker scheduling control than the existing-yield test. Its released arm
retried base 1/count 2 and got offsets 1 and 2. In the held arm, successive
flushes sent base 1/count 1 and base 2/count 1; the mock returned error 45 for
each, as did the corresponding receipt and flush, while the original two
records remained in the mock log. Its diagnostic formatting check also
reported differences. Its test-only source commit is
[`aa268b72d7033a79fc9e27d12b88e944cba281bf`](https://github.com/mingley/partitionline/commit/aa268b72d7033a79fc9e27d12b88e944cba281bf)
and qualification commit is
[`39a626c4fa28841129c15649ca80fb2de007c00a`](https://github.com/mingley/partitionline/commit/39a626c4fa28841129c15649ca80fb2de007c00a).

A separate private-helper fixture injected records from different partitions
with equal numeric bases. The real leading count returned two even though the
first partition had only one of its declared two batch members; under a
two-record limit, selecting `A1, B1, A2` chose only `A1` from A. This
particular cross-partition retry arrangement is not established by the public
history. In [run 42481770721](https://github.com/mingley/partitionline/actions/runs/42481770721)
both feature profiles observed the helper behavior, but the jobs failed their
strict formatting check on the added test-only function. After formatting that
function, [run 42482309342](https://github.com/mingley/partitionline/actions/runs/42482309342)
passed both workflows and strict formatting. The workflows deliberately
expected and verified that two desired completeness assertions failed on the
existing implementation; a successful workflow does not mean those assertions
passed. The formatted qualification source is
[`f3fc4269d2d778336ecb15975a0f460ca4186302`](https://github.com/mingley/partitionline/commit/f3fc4269d2d778336ecb15975a0f460ca4186302),
with producer blob `0a9de32e778b4b89c6b38e0d995738a2f41a96c7`.

## Repair requirements

A fix must keep the complete accepted batch and its nonreused identity rooted
across retry, worker cancellation, flush, close and failure. Sequence
reservation, producer identity and epoch transitions need a generation-aware
journal that distinguishes never-written work from a possibly written attempt;
an error or retry acknowledgement alone cannot prove every earlier attempt is
settled. Readiness, packing and encoding must share the same complete batch
identity, including when batches interleave or configured limits are reached.

Changing only `can_fire` does not cover the observed
[`flush`/close drain](https://github.com/mingley/partitionline/blob/536d5e938aa2ad123471fe6446b0ba406dabe8cd/src/producer.rs#L9255),
which calls `fire` directly. Simply waiting for missing records in that drain
also needs bounded deadlines, cancellation and close handling, and cannot
recover records whose owner was dropped. The
[ownership design](../producer-ownership-design-20261108/README.md) and
[sequence model](../producer-sequence-ledger-model-20261108/README.md) record
additional constraints. A safe integrated repair and its performance remain
unqualified.
