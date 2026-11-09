# Producer generation-guard experiment

This experiment is not applied to main. The guard does not establish
producer readiness, sequence rollover correctness, or safe delivery through
identity changes and worker cancellation. The preceding
[seeded stale-generation witness](../producer-stale-rollback-20261109/README.md)
is a synthetic mock state, not a natural Kafka history.

## First qualification

The source-only experiment was based on main `a7b6edc1645e3a4789cf699a3b9e7656c55a3d10`
and recorded in commit `17e2fe10a65f4e28b0b04175e01d8c74000774e7`.
Its unformatted producer blob was `986199bff7db2c94388ba19b01f62f046c4948bd`.
[Run 42673489713](https://github.com/mingley/partitionline/actions/runs/42673489713)
formatted this source before testing; its tested producer blob was
`4ed2aab7759de399441b89d3be3c497ac1f1640b`. It used hosted x86_64 Linux
with stable rustc 1.99.1.

The new unit controls failed to compile in both default and all-feature
profiles: they were placed in a sibling test module without access to the
helpers and imports they use. Each compilation reported 18 unresolved-name
errors, so those controls did not run. Strict all-target Clippy failed on the
same compile errors. The warning-denied all-feature documentation command
completed successfully. In each profile, the seven existing selected public
control commands passed: buffer ownership, producer close errors, produce
cancellation, sticky partitioning, producer initialization, selected
transaction abort controls, and selected fenced-produce controls.

The instrumented seeded stale-rollback public mock control passed once in
each profile, recording `after=1 first_base=1 identity=1000/0`. As in the
baseline witness, the mock expectation and producer generation were seeded;
this is not an uninstrumented identity transition or a Kafka result.

## Corrected test scope and follow-up

The tests were moved into the module containing their helpers. The
[source-only candidate](https://github.com/mingley/partitionline/commit/aec536bc9d85e645f81bab67c4070d6327fa70b4)
contains producer blob `32c19e615709d422cb3d3ce5128adf9f9ddc86e5`.
[Run 42675266282](https://github.com/mingley/partitionline/actions/runs/42675266282)
used workflow commit `37369b8a14e0f69c8d9a447e30e6586488c66d61`
and printed that same producer blob before tests and after restoring the
instrumented witness source. It ran on hosted x86_64 Linux, stable rustc
1.99.1 and LLVM 23.1.1. It concluded failure.

Both default and all-feature profiles passed each selected command:

| Control | Passed per profile |
| --- | ---: |
| `sequence_guard_` unit controls | 4 |
| `buffer_ownership` | 12 |
| `producer_close_errors` | 6 |
| `produce_cancel` | 23 |
| `sticky_partitioner` | 29 |
| `producer_initialization` | 8 |
| `client_api txn_abort_` | 2 |
| `full_surface transactional_fenced_produce_` | 2 |

The unit controls cover a current unwritten tail, stale generation, a retry,
an incomplete batch, a later reservation and generation exhaustion. One
control calls the local EndTxn identity and epoch-change helpers. These are
targeted controls, not an exhaustive transaction or concurrency campaign.

The interrupted-stamping control manually polls an assignment of 8,193
records through the reservation and the first 8,192 stamps. Dropping that
assignment future and manually requesting rollback leaves the sequence counter
at 8,193 because the batch is incomplete. This demonstrates the guard's
retained-reservation limitation in the unit setup; it does not abort a real
producer worker. In a separate branch of the same control, it synthetically
changes the generation and seeds a new-generation counter during partial
stamping; the resumed assignment errors and the stale rollback leaves that
counter unchanged. It does not exercise a real identity-change/write race.

The instrumented public seeded mock witness passed once in each profile,
reporting `after=1 first_base=1 identity=1000/0`. Its producer generation
and mock sequence are seeded; this is not a natural Kafka history.

The strict check step failed. `cargo fmt --all -- --check` reported four
locations in the candidate `src/producer.rs`, all in test layout: extra blank
lines and assertion wrapping. It also reported formatting differences in
the qualification-only mock `tests/common/mod.rs` and witness
`tests/stale_identity_rollback.rs`; these fixture differences are separate
from the source-only candidate. All-target all-feature Clippy failed on two
`manual_noop_waker` errors in the new unit controls.
Warning-denied all-feature rustdoc completed successfully.

## Limits of the experiment

The candidate snapshots and publishes identity fields under the sequence
mutex, checks the generation while assigning and before writing, and refuses
a rollback for a stale generation, a retry, an incomplete batch, or a
noncontiguous or non-tail reservation. Generation exhaustion fails closed for
new assignments and rollback. This does not fix the existing saturating
Kafka record-sequence arithmetic or provide a nonreused logical reservation
identity.

The final generation check occurs before an asynchronous socket write. An
identity can still change after the check, while that write begins or is in
progress. A generation change during stamping and cancellation after partial
stamping can leave reservations and records without a rooted owner; refusing
a rewind alone does not guarantee later sequence progress. Retained batches
can still be reassigned after an identity change, including after an ambiguous
attempt. Retried records can be split across batches. No real partial socket
write, real broker identity race, or real Kafka history is demonstrated here.

The candidate conservatively declines to rewind retries even if an attempt was
actually unwritten, so it may leave a sequence gap. It scans the batch while
holding the shared sequence mutex; its scheduling and performance have not
been measured. The public seeded mock control does not prove safety under
arbitrary concurrent schedules. Do not apply this candidate until batch
ownership and identity/write fencing are repaired and tested together.


## Work needed before reconsidering the guard

1. Root each admitted partition batch and its immutable retry boundary in an
   owner that survives worker cancellation. Register a unique reservation
   before any yielding stamp, retain its records until their terminal
   callbacks and memory accounting are completed, and prevent stale or
   repeated cleanup from acting on a later numerically equal reservation.
   A checked logical reservation counter must fail closed on exhaustion.
2. Track definitely-unwritten, write-started/ambiguous and acknowledged
   states for each owned batch. Transfer or finish ownership at every
   cancellation point. Rewind only the matching current tail when a request
   is known not to have written any bytes; ambiguous attempts must retain
   their exact batch and identity while their outcome is resolved or
   explicitly failed.
3. Serialize identity publication with actual write admission and in-flight
   resolution. The serialization must cover the interval after the current
   final check and through write start, and must prevent stale responses or
   retries from mutating a newer identity. A request already in progress when
   identity changes needs an explicit fencing and ambiguous-outcome policy,
   not silent reassignment to a new identity.
4. Implement and verify Kafka wire-sequence rollover and batch boundaries
   independently of logical reservation identity. Test current and stale
   rollback, numeric reuse, split prevention and checked exhaustion.
   Add deterministic partial-write and cancellation controls at each await,
   concurrent identity changes and delayed replies, then a real-Kafka
   compatibility control for the relevant retry, fencing and rollover
   behaviors. Measure lock contention and scheduler impact after correctness.

The guard remains unapplied. Its selected mock and unit results do not qualify
production readiness, delivery semantics, Kafka interoperability at the
sequence boundary, or comparative performance.
