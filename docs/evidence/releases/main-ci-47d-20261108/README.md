# Main CI at 47df27f

This records CI for main commit
[`47df27f9f3e04fc524849f3a9753c0b0469f8947`](https://github.com/mingley/partitionline/commit/47df27f9f3e04fc524849f3a9753c0b0469f8947),
which includes the broker frame-read fairness change. Client CI failed; broker
and selected capability checks passed.

## Client

[Client run 42470021233](https://github.com/mingley/partitionline/actions/runs/42470021233)
finished with 18 successful jobs, nine failures and one skipped job. Library
failures stopped the all-target commands before later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,229 passed, 15 failed |
| Linux all-feature library | 2,222 passed, 22 failed |
| Windows default library | 2,226 passed, 18 failed |
| macOS arm64 default library | 2,229 passed, 15 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and timeouts after retries |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict Clippy, audit, dependency policy, docs, package, release
rehearsal, authentication and integrity smoke, and the other completed client
checks passed. The resource-soak pass does not explain the OS permission error
that left the workload incomplete in the [earlier run](../main-ci-262-20261108/README.md).

## Broker

[Broker run 42470021226](https://github.com/mingley/partitionline/actions/runs/42470021226)
passed 779 default and 927 all-feature tests, with four ignored in each
profile. The ignored tests include three descriptor diagnostics and one manual
microbenchmark. Formatting, strict all-target/all-feature Clippy, Rustdoc,
both doctest profiles, pinned API inventory and fuzz supervision passed.

The frame-read change has focused correctness and performance evidence in its
[report](../../broker/frame-read-fairness-20261108/README.md). This pass does
not explain the earlier three-node Raft `NotLeader` failure or establish
whether the mailbox change contributed; the separately collected node samples
did not identify its cause. Other earlier failures also remain open.

## Capabilities

[Capability run 42470021224](https://github.com/mingley/partitionline/actions/runs/42470021224)
passed both selected profiles. Its archives were not independently inspected.
These checks do not qualify production readiness or a performance ranking.
