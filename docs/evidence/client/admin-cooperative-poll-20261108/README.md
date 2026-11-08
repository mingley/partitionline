# Admin cooperative poll controls

The [test-only change][candidate] replaces two Admin tests that required
a large operation to finish or yield within 80 ms of wall time. The changed
tests check the futures' cooperative polling behavior directly. The only
change outside the test module is a `cfg(test)` progress observer in
`index_described_producers`; it is absent from non-test builds.

## Controls

The description-extension test manually polls a 20,000-topic future outside
a Tokio runtime. It drops separate copies after one and two polls and
checks that each returned `Pending`, woke the supplied waker and advanced
the output; the second partial output must contain more entries. A separate
future is driven to completion and its full output and index are checked.
A one-topic input must finish on its first poll.

The producer-indexing test uses a thread-local `cfg(test)` counter after
each insertion. A scoped guard restores the previous counter even when the
test unwinds. The test observes positive progress and a wake after each of
two `Pending` polls, then completes the 20,000-partition input and checks
its index. A small input checks first-poll completion and preserves the
duplicate-partition behavior. The observer adds a thread-local check to
each indexing iteration in unit-test builds and can affect their timing.

These controls show cooperative polling, wakeups and output for these
fixtures. They do not establish an 80 ms limit, Tokio's scheduling order,
a bound for every larger input, or a bound on cancellation and destructor
time. The former tests used million-element fixtures; the new controls
check full results at 20,000 elements.

## Earlier wall-time results

The failures that motivated the work remain unexplained. In
[42468906570][history-broad], the description-extension assertion measured
85.792914 ms in default and 84.884409 ms with all features; the
producer-indexing assertion measured 83.441912 ms and 81.221322 ms.
All four exceeded the original 80 ms threshold. That default run also
failed a separate coordinator-index timing test; it was not changed here.

In [42469879254][constant-broad], the later default Admin suite passed,
while the all-feature suite measured 86.008040 ms and 85.124694 ms for the
same two assertions and failed them. A separate
[same-host baseline/history comparison][fairness] retained the original
thresholds: extension failed 4/4 baseline and 4/4 candidate samples in
each profile; producer indexing failed 2/4 baseline versus 0/4 candidate
in default and 2/4 versus 1/4 with all features. Those samples establish
that the unchanged baseline can fail on that host, not why the earlier
failures occurred. The [pagination evidence][pagination] retains all
32 raw observations.

The first [draft qualification][first] passed 604 existing Admin tests with
all features but default had 603 passes and a separate coordinator-index
failure at 82.471785 ms. That draft predates the progress observer and is
not the final source. Its optional `rg` reference check did not execute
because `rg` was unavailable; a later [tracked-file search][refs] used
`git grep` and found no old test-name references outside retained evidence.

## Qualification

The [exact-source run][exact] passed both new controls and all 604 selected
Admin library tests in each feature profile. Formatting and strict
all-target/all-feature Clippy passed. Each suite recorded
`src/admin.rs` SHA-256
`929aaafb35e3ef1f376de7debd090367960ec22428cccf79a4bd806c50992d79`,
Git blob `540592a38a92ebeaada716b51d3b0f735b72b8be`.
The earlier wall-time failures remain unexplained.

The [restored mutation run][mutants] applied each mutation independently
to the same formatted candidate and required the corresponding test to fail
with the expected assertion. All eight mutations were detected in each
feature profile, for 16 expected failures:

| Mutation for each helper | Detected by |
| --- | --- |
| Remove all loop yields | First poll completes instead of returning `Pending`. |
| Yield once, then finish without later yields | Second poll completes instead of returning `Pending`. |
| Return `Pending` without waking | The wake assertion fails. |
| Yield twice before any work, then stop yielding | The progress assertion fails. |

The mutation fixtures exercise the controls, not production behavior.
The checked lockfile SHA-256 was
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
Removing the two `cfg(test)` observer blocks from the source prefix leaves
the non-test source unchanged from main `0708730`.

[candidate]: https://github.com/mingley/partitionline/commit/6c6055b78b544bd3749a1e45ba80d794bc6e9478
[history-broad]: https://github.com/mingley/partitionline/actions/runs/42468906570
[constant-broad]: https://github.com/mingley/partitionline/actions/runs/42469879254
[fairness]: https://github.com/mingley/partitionline/actions/runs/42469318221
[pagination]: ../admin-pagination-20261108/README.md
[first]: https://github.com/mingley/partitionline/actions/runs/42471605593
[refs]: https://github.com/mingley/partitionline/actions/runs/42472019560

[exact]: https://github.com/mingley/partitionline/actions/runs/42472371988
[mutants]: https://github.com/mingley/partitionline/actions/runs/42471911008
