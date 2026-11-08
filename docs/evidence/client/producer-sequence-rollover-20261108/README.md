# Producer sequence rollover

This note records a producer helper defect and a test plan. The helper
and mock controls inject sequence state near the boundary; they do not
establish a real-broker failure, a completed fix, or producer qualification.
The reviewed producer at main
[`60caa746c3654c89d01557e35e1100cdc2a84b74`][source] has Git blob
`1e7733c136e54b532cdc8bddabd1e0c828e50ed2` and SHA-256
`3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`.

## Arithmetic

Kafka's nonnegative producer sequence space wraps from `i32::MAX` to zero.
The Apache Kafka 3.9.1 client uses [`incrementSequence`][java-increment] for
this; this repository already implements the same operation as
[`RecordBatch::increment_sequence`][rust-increment]. The record-batch helper
has rollover controls, but [producer reservation][reserve] and
[per-record stamping][stamp] instead use saturating addition.

Two counterexamples, assuming one topic, partition, producer ID and epoch,
are:

- Starting with the next sequence at `i32::MAX - 1`, reserving three
  records should stamp `[MAX - 1, MAX, 0]` and leave the next sequence at
  `1`. Current reservation leaves it at `MAX`, and the stamped third
  record is also `MAX`.
- Starting at `MAX`, two consecutive one-record reservations both obtain
  `MAX`; the second should obtain zero. If sent as distinct batches with the
  same identity, the second may be rejected or treated as the earlier batch.
  Kafka 3.9.1's [duplicate lookup][java-duplicate] matches producer epoch and
  exact first/last sequence range. Therefore duplicate treatment could
  acknowledge the prior offset without appending the second payload; this
  outcome has not yet been demonstrated with a real broker.

The [existing mock][mock] also advances its expected sequence using plain
`i32` addition, so it cannot currently validate a boundary crossing in a
debug test without overflow.

## Seeded helper controls

[Run 42466788425][helper-run] executed three controls per profile on hosted
x86_64 using stable Rust 1.99.1 and LLVM 23.1.1. Both default and all-feature
jobs passed all three. One ordinary reservation matched an independent
widened-integer modulo oracle. The two other controls deliberately asserted
that the current helper output differs from that oracle: a three-record batch
starting at `MAX - 1` saturated both its next value and its third record's
stamp, and two one-record reservations starting at `MAX` reused `MAX`.
These passing controls confirm the defect; they are not passing correctness
tests for a fixed implementation.

The [qualification commit][helper-source] appends a `cfg(test)` module to
the producer and changes no production code. Its test-instrumented producer
Git blob is `80c39579b7adff4407ffb4ccd5a3b1439d2eecaa`, SHA-256
`93df1c7e38d9039676b3dc943ab1772c9839ae823a6508ee75f7051841e96e25`.
The controls directly invoke the private helpers with an injected sequence
map; they do not exercise public admission, retries, a mock or a broker.
Artifacts `producer-rollover-helper-default` and
`producer-rollover-helper-all` have IDs `13611159360` and
`13611838243`, respectively, and expire on 2026-12-08.

## Seeded public mock history

[Run 42467249113][mock-run] exercised public `send` twice with distinct
values on one partition. A warmup used a different partition; the fixture
then seeded the unused target partition's next sequence to `MAX` in both
the producer and mock. The mock's expected-sequence arithmetic used
independent `i64` modulo. The test checked the producer identity and epoch,
wire base and count, receipt offsets, acknowledgement callbacks, errors,
buffered bytes and the mock's stored values.

| Profile | Producer path | Target sequence bases | Receipt offsets | Target mock log |
| --- | --- | --- | --- | --- |
| default and all features | baseline | `MAX, MAX` | `0, 0` | `(0, first)` |
| default and all features | arithmetic control | `MAX, 0` | `0, 1` | `(0, first), (1, second)` |

Each of the four histories passed its stated assertions. In the baseline
fixture both sends and callbacks reported success, but the mock treated the
second sequence as a duplicate and did not store its different value. The
control used a throwaway three-line arithmetic mutation: reservation,
stamping and the numeric rollback-end comparison called
`RecordBatch::increment_sequence`. It does not protect rollback against
stale identities, generations, concurrent reservations or ambiguous writes,
and is not a production-ready candidate. The seed bypasses the long natural
history leading to the boundary. The in-repository mock is not a real Kafka
broker; these observations do not establish a real-broker result.

[Qualification commit `e8f2737`][mock-source] adds only a cfg-gated
producer seed, cfg-gated mock seed and modulo oracle, the test and workflow.
The instrumented producer blob is
`ba882563973e1122fcd9bb1044511a2468361663`, SHA-256
`ee56ae1b26b6ecc724551c9792f07d775fa46d6115af9760cedf370ccd431c85`;
mock blob `f6c10ae03d896c2e7474d4543cc3af10416fc038`, SHA-256
`7897fad8934b0d1ac466fecb0047f06e480c587b03d27a2893a662c7ea5e8f65`;
test blob `f00fb2225a0e65bcaf11d0256b40ed8165584ecc`, SHA-256
`d36328902d4ccd3033c994c478baf51976f83c41244527c5ad354ac7d4148422`.
The mutated producer SHA-256 is
`d2b2688aca7708053f8ed325496acdd2438967c125de7bf2f2fe3e4baa180c66`.

Separate Cargo target directories were used. The recorded default baseline
and control binary SHA-256 values were
`4e1e4c03a6b53a9e1d6de8672d8e0ba06a55a29d45320e4118a16afac3f3efde` and
`5f3c571c01eda272c3676fa0819b283ac3a399dea0508bf6c56f44074baf71d8`;
all-feature values were
`fd76a964df48ac90958d6cf1dd6c2f60d6609cbbf3977521bded48406b12f497` and
`65cf31f030189128c5a528e5de614e4014ed0dba245ffc5b5b87f89ef8a1d672`.
The binaries were not retained. Artifacts
`producer-rollover-mock-default` and `producer-rollover-mock-all` contain
the test logs and mutation patch, with IDs `13612695153` and
`13612426531`, and reported expiry on 2026-12-08. The run used hosted x86_64,
stable Rust 1.99.1 and LLVM 23.1.1. No failed setup attempt preceded this run.

## Repair boundaries

A scoped arithmetic patch can reuse `RecordBatch::increment_sequence` for
reservation, stamping, and the expected end in
[`rewind_sequence`][rewind], after checking that base and batch count are
valid nonnegative Kafka values. A count outside the representable record
range must fail before mutating sequence state instead of using a saturated
or zero fallback. Retain the existing cooperative yield in large batches.

Modulo arithmetic alone is not an ownership or concurrency repair:

- Equal next-sequence values can recur after a wrap, or after an identity
  reset. Comparing only the numeric end of a range does not establish that
  it is still the tail. A rollback needs a non-reused reservation identity,
  matching producer identity and generation, and proof no later reservation
  prevents it. Reservation IDs must not silently wrap.
- A retry under the same identity must retain the original batch base and
  count. A batch retained across an identity change must be revalidated and
  assigned for the new identity. A partially stamped batch needs its
  original journaled base and length: calling the current
  [`assign_sequences`][assign] again reserves a second range while leaving
  already stamped records unchanged.
- The current worker separately loads producer ID, epoch and generation;
  identity updates and map clearing are also separate operations. A
  combined owner/sequence design needs a linearizable identity snapshot and
  reservation protocol across those boundaries. These interleavings are
  source analysis, not reproduced public histories.
- Rollback before a write is different from recovery after a potentially
  partial write. The [current error path][write-error] can request rollback
  after a non-retriable write failure; a robust journal must track whether a
  write may have started and preserve ordering or fence when the result is
  ambiguous.

One possible implementation keeps a bounded per-partition reservation journal
with a checked monotonic ticket and identity generation, shared with rooted
batch ownership. The journal permits a rewind only for a provably unwritten
tail, and retains enough history to settle earlier outstanding reservations.
Other designs can meet the same invariants. See the
[producer ownership note][ownership] for cancellation, notification, close,
and transaction requirements that a sequence-only patch does not solve.

## Qualification plan

1. Extend the seeded producer controls to cover a wrapped pre-write rollback,
   an intervening newer reservation, same-identity retry, identity change,
   and checked count and ticket exhaustion. Compare with an independent
   widened-integer modulo oracle. Include partial-stamp recovery and retain
   the existing fairness controls.
2. Extend the seeded mock controls to include a batch crossing the boundary,
   an append-then-lost-response retry, different producer identities and
   epoch changes. Check every available receipt and callback, wire
   identity/base/count, and independent mock-log values and offsets. Retain
   baseline and negative-control results. These are seeded mock histories,
   not real-Kafka qualification.
3. Run real Kafka peer tests on an isolated broker using a valid broker-side
   producer-state fixture and a matching test-only client seed; explicitly
   label this setup. An unseeded rollover history requires the same identity
   to produce at least 2^31 records before validating the boundary. Consume
   and verify distinct records independently and preserve the broker, client,
   configuration and toolchain pins.
4. Qualify exact integrated source under default and all features with
   formatting, strict Clippy, cancellation, retry and transaction controls.
   Compare separate release binaries on x86 for admission, batch throughput,
   latency and allocations, including the size of `Pending`. No speed or
   production-readiness conclusion follows from these proposed tests.

[mock-run]: https://github.com/mingley/partitionline/actions/runs/42467249113
[mock-source]: https://github.com/mingley/partitionline/tree/e8f27370f4e83ae3ce15673955575f6300ad982b
[helper-run]: https://github.com/mingley/partitionline/actions/runs/42466788425
[helper-source]: https://github.com/mingley/partitionline/commit/39d776770b594de9510006d25c1606bebbb37f54
[source]: https://github.com/mingley/partitionline/blob/60caa746c3654c89d01557e35e1100cdc2a84b74/src/producer.rs
[reserve]: https://github.com/mingley/partitionline/blob/60caa746c3654c89d01557e35e1100cdc2a84b74/src/producer.rs#L11217
[stamp]: https://github.com/mingley/partitionline/blob/60caa746c3654c89d01557e35e1100cdc2a84b74/src/producer.rs#L10379
[rewind]: https://github.com/mingley/partitionline/blob/60caa746c3654c89d01557e35e1100cdc2a84b74/src/producer.rs#L10453
[assign]: https://github.com/mingley/partitionline/blob/60caa746c3654c89d01557e35e1100cdc2a84b74/src/producer.rs#L10402
[write-error]: https://github.com/mingley/partitionline/blob/60caa746c3654c89d01557e35e1100cdc2a84b74/src/producer.rs#L8498
[rust-increment]: https://github.com/mingley/partitionline/blob/60caa746c3654c89d01557e35e1100cdc2a84b74/src/protocol/records.rs#L1518
[mock]: https://github.com/mingley/partitionline/blob/60caa746c3654c89d01557e35e1100cdc2a84b74/tests/common/mod.rs#L7535
[ownership]: https://github.com/mingley/partitionline/blob/60caa746c3654c89d01557e35e1100cdc2a84b74/docs/evidence/client/producer-ownership-design-20261108/README.md
[java-increment]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L556
[java-duplicate]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateEntry.java#L134
