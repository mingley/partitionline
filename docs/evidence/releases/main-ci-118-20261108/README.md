# Main CI at 1188156

This records CI for main commit
[`1188156ec955a6141a1f06d2c25cddfa2856fa8b`](https://github.com/mingley/partitionline/commit/1188156ec955a6141a1f06d2c25cddfa2856fa8b).
Changes from the [preceding recorded commit](../main-ci-c7-20261108/README.md)
were documentation only. Client CI failed; broker and selected capability
checks passed.

## Client

[Client run 42468785595](https://github.com/mingley/partitionline/actions/runs/42468785595)
finished with 18 successful jobs, nine failures and one skipped job. Library
failures stopped the all-target commands before later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,225 passed, 19 failed |
| Linux all-feature library | 2,221 passed, 23 failed |
| Windows default library | 2,225 passed, 19 failed |
| macOS arm64 default library | 2,232 passed, 12 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and timeouts after retries |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict Clippy, audit, dependency policy, docs, package, release
rehearsal, authentication and integrity smoke, and the other completed client
checks passed. The resource-soak pass does not explain the OS permission error
that left the workload incomplete in the [earlier run](../main-ci-262-20261108/README.md).

## Broker

[Broker run 42468785547](https://github.com/mingley/partitionline/actions/runs/42468785547)
passed 773 default and 921 all-feature tests, with four ignored in each
profile. The ignored tests include three descriptor diagnostics and one manual
microbenchmark. Formatting, strict all-target/all-feature Clippy, Rustdoc,
both doctest profiles, pinned API inventory and fuzz supervision passed.

This pass does not explain the earlier three-node Raft `NotLeader` failure
or establish whether the mailbox change contributed; the separately collected
node samples did not identify its cause. Other earlier failures also remain
open.

## Capabilities

[Capability run 42468785480](https://github.com/mingley/partitionline/actions/runs/42468785480)
passed both selected profiles. Its archives were not independently inspected.
These checks do not qualify production readiness or a performance ranking.
