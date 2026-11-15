# Broker disconnect test synchronization

This is a test-only change. It waits for explicit handler-start and
handler-drop notifications instead of treating 32 executor yields as evidence
that the server observed a socket close. The production transport is
unchanged.

The source-only candidate is commit
`619c38da19c05dff8aa7e4f3b300ca1ab32a8f76`, parent main
`c4819e83e6f0f5a8a4e1f9dd0f0bbdcad81b862c`, transport blob
`913ebb4caef03382bf4dfd0a02f249c2b6c5c1ef`. Its changes are confined to
`#[cfg(test)] mod disconnect_tests`; bytes before and after that module match
the prior transport blob `b84ca76cab8d8340147795e9665584326657cc9c`.
Broker Cargo.toml is `4e23c42f8d9ac722005098a4b58e1046b711e1a3` and
Cargo.lock is `19652acd96b929402b1e4a9394a33d12a7e69449`.

The handler guard increments the existing stopped counter and then calls
`Notify::notify_one`, which retains a permit if the test has not yet begun
waiting. All three tests use a start notification with a bounded setup wait.
For the two disconnect tests, the drop notification and exact count are
captured **before** releasing the handler or requesting shutdown. Cleanup is
then attempted before asserting that captured result. A setup failure also
cleans up before failing. Start, stop and shutdown waits use two seconds as a
test harness bound, not a transport latency guarantee. A timed-out shutdown
aborts and awaits the runner; it is not counted as a successful joined
shutdown.

## Evidence

- Main [6902 run 42476127572](../../releases/main-ci-6902-20261108/README.md)
  failed the original `client_disconnect_stops_the_handler` assertion with
  stopped count 0 after 32 yields; its default broker library result was 490
  passed, 1 failed and 4 ignored. The original failure's cause remains
  unknown. [Controlled EOF evidence](../disconnect-eof-20261108/README.md)
  showed a possible delayed observation with real TCP EOF deliberately gated,
  then a guard drop after the gate opened; it does not prove the CI schedule.
- The first event-based precursor
  [42477285861](https://github.com/mingley/partitionline/actions/runs/42477285861)
  passed its focused tests and mutants, but still had unbounded handler-start
  loops. A revised staged run
  [42477515811](https://github.com/mingley/partitionline/actions/runs/42477515811)
  passed its focused default/all positives, strict Clippy, no-EOF and early
  notice mutants. Its missing-start mutant **did not reach the test**: an outer
  15-second limit included recompilation and expired during compilation. That
  run failed as a harness timeout.
- Corrected exact-source controls
  [42477877165](https://github.com/mingley/partitionline/actions/runs/42477877165),
  workflow-only child `7020c160e938ad9b24a98283fbea0d47039fe56b`, verified
  the committed and working-tree source blob. All three disconnect/flood
  tests passed in default and all-feature profiles; format and strict
  all-target/all-feature Clippy passed. In the default profile, independently
  restored mutants suppressing EOF cancellation failed the normal and
  pipelined tests with `observed=false`, pre-cleanup count 0 and
  `joined=true`. A combined premature-notice and suppressed-EOF mutant
  produced `observed=true`, pre-cleanup count 0 and `joined=true`, and
  still failed at the intended assertion. Suppressing the start notice made
  the client and flood tests fail at the bounded setup assertion after joined
  cleanup. The corrected workflow compiled the mutant before applying the
  outer test execution limit.
- In the separate exact-source
  [42478027083](https://github.com/mingley/partitionline/actions/runs/42478027083),
  forcing the handler operation to remain pending before permit acquisition
  made the client and flood tests fail at the bounded setup assertion after
  joined cleanup. This tests stalled admission in addition to a missing test
  notification.
- Ordinary locked broker behavioral profiles on the exact source ran once
  each in [42478231282](https://github.com/mingley/partitionline/actions/runs/42478231282),
  workflow-only child `b96b3a7fa1d2de54a90ae5657921c28e3a214534`.
  Default features passed **781 tests, 4 ignored**; all features passed
  **929 tests, 4 ignored**. Each total covers 36 test executables and includes
  all three disconnect tests. These results are not a production-readiness
  or comparative-performance qualification.

Separately, [main 7fb run 42476521782](../../releases/main-ci-7fb-partial-20261108/README.md)
failed a five-node Raft test when `fault-delayed-old-reply-proposal` to node2
returned `Node(NotLeader)` (490 passed, 1 failed, 4 ignored in the default
library suite). That earlier failure remains unexplained. The successful
isolated profiles neither establish a cause for it nor change the 6902
disconnect observation.
