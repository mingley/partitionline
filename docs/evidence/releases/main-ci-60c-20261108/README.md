# Main CI at 60caa746

This records CI for main commit
[`60caa746c3654c89d01557e35e1100cdc2a84b74`](https://github.com/mingley/partitionline/commit/60caa746c3654c89d01557e35e1100cdc2a84b74).
The changes from the preceding recorded commit `262d254` were documentation
only. Client CI failed; broker and selected capability checks passed.

## Client

[Client run 42466488523](https://github.com/mingley/partitionline/actions/runs/42466488523)
finished with 18 successful jobs, nine failures and one skipped job. Library
failures stopped the all-target commands before later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,224 passed, 20 failed |
| Linux all-feature library | 2,225 passed, 19 failed |
| Windows default library | 2,231 passed, 13 failed |
| macOS arm64 default library | 2,233 passed, 11 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and timeouts after retries |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict Clippy, audit, dependency policy, docs, package, release
rehearsal, authentication and integrity smoke, and the other completed client
checks passed. The resource-soak pass does not explain the OS permission error
that left the workload incomplete in the [preceding run](../main-ci-262-20261108/README.md).

## Broker

[Broker run 42466488499](https://github.com/mingley/partitionline/actions/runs/42466488499)
passed 773 default and 921 all-feature tests, with four ignored in each
profile. The ignored tests include three descriptor diagnostics and one manual
microbenchmark. Formatting, strict all-target/all-feature Clippy, Rustdoc,
both doctest profiles, pinned API inventory and fuzz supervision passed.

This pass does not explain the preceding three-node Raft `NotLeader` failure
or establish whether the mailbox change contributed; the separately collected
node samples did not identify its cause. Other earlier failures also remain
open.

## Capabilities

[Capability run 42466488708](https://github.com/mingley/partitionline/actions/runs/42466488708)
passed both selected profiles. Its archives were not independently inspected.
These checks do not qualify production readiness or a performance ranking.
