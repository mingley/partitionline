# Main CI at c7a91ce

This records CI for main commit
[`c7a91ce50cd231bb7d85e90297d98344e5a98dec`](https://github.com/mingley/partitionline/commit/c7a91ce50cd231bb7d85e90297d98344e5a98dec).
Changes from the [preceding recorded commit](../main-ci-60c-20261108/README.md)
were documentation only. Client CI failed; broker and selected capability
checks passed.

## Client

[Client run 42467695055](https://github.com/mingley/partitionline/actions/runs/42467695055)
finished with 18 successful jobs, nine failures and one skipped job. Library
failures stopped the all-target commands before later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,225 passed, 19 failed |
| Linux all-feature library | 2,225 passed, 19 failed |
| Windows default library | 2,214 passed, 30 failed |
| macOS arm64 default library | 2,233 passed, 11 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and timeouts after retries |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict Clippy, audit, dependency policy, docs, package, release
rehearsal, authentication and integrity smoke, and the other completed client
checks passed. The resource-soak pass does not explain the OS permission error
that left the workload incomplete in the [earlier run](../main-ci-262-20261108/README.md).

## Broker

[Broker run 42467695062](https://github.com/mingley/partitionline/actions/runs/42467695062)
passed 773 default and 921 all-feature tests, with four ignored in each
profile. The ignored tests include three descriptor diagnostics and one manual
microbenchmark. Formatting, strict all-target/all-feature Clippy, Rustdoc,
both doctest profiles, pinned API inventory and fuzz supervision passed.

This pass does not explain the earlier three-node Raft `NotLeader` failure
or establish whether the mailbox change contributed; the separately collected
node samples did not identify its cause. Other earlier failures also remain
open.

## Capabilities

[Capability run 42467695022](https://github.com/mingley/partitionline/actions/runs/42467695022)
passed both selected profiles. Its archives were not independently inspected.
These checks do not qualify production readiness or a performance ranking.
