# Whole-batch retry ownership controls

A retry channel that transfers an entire batch is not sufficient on its own.
Private controls against the current producer helpers show that the packer can
split a complete batch after handoff, and that pending expiration acts on
individual records. These are injected worker states, not public-client or
real-broker histories. No producer change is applied.

## Complete batch and packing

The baseline is [`7dfbe4735d1c4268a1b8a7185bb34f1121f0db13`](https://github.com/mingley/partitionline/commit/7dfbe4735d1c4268a1b8a7185bb34f1121f0db13).
Its `src/producer.rs` blob is `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`
(SHA-256 `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`).

The test injected a worker queue containing one new, unassigned record B
followed by a complete contiguous two-record retry batch A1,A2 for another
partition. With a record limit of two, the real `take_count_in` selected
B,A1, and `group_pending` formed a one-record A group whose declared
original batch length was two. A desired completeness assertion failed.
The fixture does not establish that this exact queue is reachable through the
public API. It does show why transferring A1,A2 together is insufficient if
the worker expands them into the existing record queue before packing.

## Deadlines and sticky cohorts

Another private fixture constructed a worker and two records bearing one
assigned batch's sequence metadata. The first record's deadline was already
expired; the second's was in the future. One call to
[`purge_expired_pending`](https://github.com/mingley/partitionline/blob/7dfbe4735d1c4268a1b8a7185bb34f1121f0db13/src/producer.rs#L7861)
returned `Timeout` for the first record, left the second pending with its
original base and declared length two, and reduced visible buffered bytes
from two to one.

The sticky variant installed a valid two-member cohort. Expiring the first
record canceled that cohort; the first purge still left the second record,
and a second purge returned `Timeout` for it and reduced buffered bytes to
zero. Separately canceling a live two-member cohort caused one purge to fail
both members and release both visible bytes. The tests did not exercise a
broker, a prior write, a public producer schedule, or interceptor callbacks.
They do not prove a dispatch occurred between the two purges, or that sticky
cancellation alone splits a valid cohort.

[Run 42483718716](https://github.com/mingley/partitionline/actions/runs/42483718716)
ran these observations with default and all features. Both observational tests
passed in each profile. The workflow expected and verified the packing
completeness assertion to fail; workflow success does not mean that assertion
passed. It used stable Rust 1.99.1 on hosted x86-64. Formatting was diagnostic
only and reported differences in the test-only source, so strict formatting did
not pass.

The test-only source commit is
[`2fd3c3ec5f4a1ac2cc8d5d088475a7ff675fdfb9`](https://github.com/mingley/partitionline/commit/2fd3c3ec5f4a1ac2cc8d5d088475a7ff675fdfb9)
and the workflow commit is
[`863e63cca0fa65d2cbf2215a7734e6a0645468aa`](https://github.com/mingley/partitionline/commit/863e63cca0fa65d2cbf2215a7734e6a0645468aa).
The instrumented source blob is `636d2b54250e3b7a081753268a47b29c5ea4785c`
(SHA-256 `ba6c38ff96c177e384dfaf4ac14763800cdf871749f912670deb206dd560de1e`);
`Cargo.lock` SHA-256 is
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.

## Implications for a repair

The record and byte limits, normal readiness and the direct flush/close
[`fire` path](https://github.com/mingley/partitionline/blob/7dfbe4735d1c4268a1b8a7185bb34f1121f0db13/src/producer.rs#L9255)
need one selector that treats each assigned batch as an indivisible unit while
preserving per-partition ordering. Expiration or sticky cancellation must
resolve batch disposition and every receipt and reservation without silently
restamping survivors of a possibly written batch.

The batch and record owners must remain rooted across grouping, encoding,
write, response, retry handoff, callback publication, cancellation and timed
close. Sequence reservation, producer identity and outstanding write claims
need one generation-aware journal; an ambiguous earlier attempt needs a
justified settlement or fence. Flush and close need a cutoff covering accepted
work before it enters the retry queue, not only the current `retries_out`
counter. A record or slot credit is needed in addition to byte accounting:
zero-byte records use no byte credit, and `buffer_memory(0)` removes the byte
bound. Cleanup and callbacks need bounded scheduling and explicit behavior
when user code blocks. These are design requirements, not a tested
implementation.
