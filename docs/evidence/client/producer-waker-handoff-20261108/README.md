# Producer handoff and a panicking receiver waker

Two isolated unit-test runs show an accounting failure when a deliberately
injected receiver waker panics during a producer worker handoff. This is a
private test fixture; neither run establishes a public API trigger or a failure
of an ordinary Tokio-spawned worker.

The test registers a custom waker on a Tokio `mpsc::Receiver`, then uses the
producer's `WorkerPermit::send` with a live `BufferReservation`. The waker
checks that the worker admission mutex is held and panics. The test catches that
panic and observes:

1. The pending record was already in the receiver queue.
2. Unwinding dropped the still-armed buffer reservation and released its byte.
3. Passing the queued record to the producer's `fail_pendings` released that
   byte again, wrapping the counter from zero to `u64::MAX`.

The fixture resets the test counter after the assertion. It has no installed
interceptor, does not call the public producer API, and uses a local TCP
fixture rather than a Kafka broker. It reconstructs the handoff around the
actual private producer types; it does not run `enqueue_one` end to end.

## Runs

| Run | Control | Result |
| --- | --- | --- |
| [42457823195][first-run] | Ordinary handoff with no registered receiver waker | Default and all features: 2 passed in each run. |
| [42458458236][second-run] | Ordinary handoff with a registered, nonpanicking custom waker; the test asserts it ran. | Default and all features: 2 passed in each run. |

In both runs the ordinary handoff retained the byte until the queued record was
failed, then released it once. The injected-panic test observed the wrapped
counter described above. Each test command filtered out 2,244 other library
tests; these were focused controls, not complete test suites.

Both jobs used x86_64 Linux, Rust 1.99.1 (LLVM 23.1.1), Tokio 1.53.3 from
`Cargo.lock`, and `cargo test --locked --lib`, once with default features
and once with `--all-features`. They checked the producer blob from main
commit [6298f6b][baseline] before appending a test-module declaration:

- Producer Git blob: `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`; SHA-256:
  `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`.
- Test-instrumented producer file, both runs, SHA-256:
  `4c49ba496c5c828e924ee401241a56588a1646edc63a6db893b316ad6bc39c57`.
- `Cargo.lock`, both runs, SHA-256:
  `bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
- [First test source][first-test], commit `db8666df4d7b7c3eda3b3974f182931a18508e75`,
  SHA-256: `43521744cfca1a0eab28c822fbbe184208b43973d9fe1bb8b418137738a27238`.
- [Strengthened test source][second-test], commit
  `1b6aabae8b21c080e87fad75a5a6d2c71039d843`,
  SHA-256: `3515fa841f0c3ba7db1fd840376d6592ceda72b5f77f95906c0063c21c310b6a`.

The first workflow setup attempt, [42457759932][setup-run], failed YAML
validation before any job or test ran. The workflow was corrected before the
two successful runs.

## Limits and follow-up

The tests do not show that a custom receiver waker can be installed on a
normally spawned producer worker. They also do not establish whether a Tokio
worker waker can or cannot unwind in practice, including with tracing or
subscriber instrumentation. The runs make no claim about an observed
production incident.

A completion-owner design should treat a handoff as potentially published
before wake returns or unwinds. It must preserve the accepted record and its
single byte reservation across that boundary, avoid running user-provided
wake or cleanup code under admission locks, and arbitrate a late queued
descriptor against recovery. That design remains unimplemented and requires
public API, cancellation, worker-retirement, shutdown, and callback tests.

[first-run]: https://github.com/mingley/partitionline/actions/runs/42457823195
[second-run]: https://github.com/mingley/partitionline/actions/runs/42458458236
[setup-run]: https://github.com/mingley/partitionline/actions/runs/42457759932
[baseline]: https://github.com/mingley/partitionline/blob/6298f6bfbed2ab98189d291ebebd2190a2ee75b6/src/producer.rs
[first-test]: https://github.com/mingley/partitionline/blob/db8666df4d7b7c3eda3b3974f182931a18508e75/src/producer/flush_waker_witness.rs
[second-test]: https://github.com/mingley/partitionline/blob/1b6aabae8b21c080e87fad75a5a6d2c71039d843/src/producer/flush_waker_witness.rs
