# Producer stale-generation rollback witness

This is a seeded mock control, not a real Kafka result or a completed
producer repair. It exercises the production rollback helper and then
observes a public producer send from an instrumented build.

## Controlled history

The fixture warms a producer on partition 1. A test-only hook then stages a
previous-generation pending record at sequence 0 and sets a current-generation
counter for unused partition 0 to 1. It calls the existing rollback helper on
the old record. The repository mock is independently seeded to expect sequence
1 on the target partition for the producer's current ID and epoch.

In the all-feature job of
[run 42672055044](https://github.com/mingley/partitionline/actions/runs/42672055044),
the helper changed the counter from 1 to 0 and the next public send's first
observed Produce used base sequence 0. The test expected both values to be 1
and failed. The job passed only because it required that exact negative
result. This shows a public consequence of the seeded state; it does not show
a naturally reached identity transition, a real broker outcome, or the final
outcome of every retry.

The producer generation and mock expected sequence are seeded directly. The
mock and the producer do not represent a complete prior Kafka history, and the
test does not prove that an identity change and rollback can reach this schedule
in an uninstrumented runtime.

## Scope and provenance

- Baseline source: main
  [`a7b6edc`](https://github.com/mingley/partitionline/tree/a7b6edc1645e3a4789cf699a3b9e7656c55a3d10),
  producer source blob `61b084c065291f077aa6112fdba6dc15b33848bd`.
- [Witness source](https://github.com/mingley/partitionline/tree/9a2fee0f6a4d382a2301e903c0e3dfa3f2197ae6):
  producer blob `b484da7c49bcf3e74cb46fe11df261cdec33798d`,
  mock blob `0820931a0b264b46a0a1517c01d1195ed1696a69` and
  public control blob `e6de26fcbc70a59c9f39576a77b2e57a352a5f2c`.
- The single recorded all-feature job used hosted x86_64 Linux and stable
  rustc 1.99.1. The overall workflow concluded failure and its default-profile
  job is absent. The cause is unknown; default is unrun.
- [Run 42671527122](https://github.com/mingley/partitionline/actions/runs/42671527122)
  concluded failure with no jobs; it produced no test result and its cause is
  unknown.

A generation guard must not mistake numeric equality for ownership of a
sequence reservation. It also has to distinguish a definitely unwritten
attempt from one that may have been partially written. A guard alone cannot
supply rooted batch ownership, prevent worker-cancellation loss, implement
sequence rollover, or fence a write that races an identity change.
