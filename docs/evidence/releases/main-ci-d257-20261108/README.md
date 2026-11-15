# Main CI at d2575d2

This records CI for main commit
[`d2575d27ca53af47a627ca48f27a12bba34945bd`](https://github.com/mingley/partitionline/commit/d2575d27ca53af47a627ca48f27a12bba34945bd).
Client CI failed; broker and selected capability checks passed.

## Client

[Client run 42474542094](https://github.com/mingley/partitionline/actions/runs/42474542094)
finished with 18 successful jobs, nine failures and one skipped job. Library
failures stopped all-target commands before later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,237 passed, 17 failed |
| Linux all-feature library | 2,235 passed, 19 failed |
| Windows default library | 2,237 passed, 17 failed |
| macOS arm64 default library | 2,243 passed, 11 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and timeouts after retries |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict all-target/all-feature Clippy, audit, dependency policy,
docs, package, release rehearsal, authentication and integrity smoke, and the
other completed client checks passed. The resource-soak pass does not explain
the OS permission error that left the workload incomplete in the
[earlier run](../main-ci-262-20261108/README.md).

## Broker

[Broker run 42474542071](https://github.com/mingley/partitionline/actions/runs/42474542071)
passed **779 default and 927 all-feature tests**, with four ignored in each
profile. Strict formatting, Clippy, Rustdoc and both doctest commands passed;
the two doctest commands found zero tests. Pinned API inventory and fuzz
supervision passed in separate jobs. This pass does not explain the earlier
[e487 proposal failure](../../broker/raft-delayed-proposal-20261108/README.md)
or establish its append or commit outcome.

## Capabilities

[Capability run 42474542088](https://github.com/mingley/partitionline/actions/runs/42474542088)
passed both selected profiles. Its archives were not independently inspected.
These checks do not qualify production readiness or a performance ranking.

## Preceding partial run

At main `b51f645d9e90a3158906c52bdeaafbf86dffcac0`,
[broker run 42474244284](https://github.com/mingley/partitionline/actions/runs/42474244284)
passed 779 default and 927 all-feature tests with four ignored per profile,
plus strict checks and fuzz supervision; its inventory job was canceled when
main advanced. [Client run 42474244373](https://github.com/mingley/partitionline/actions/runs/42474244373)
finished with seven successful jobs, 20 canceled and one skipped. Its
[capability run](https://github.com/mingley/partitionline/actions/runs/42474244325)
passed both selected profiles; the archives were not independently inspected.
Canceled jobs have no qualification result. The intervening commits to
`d2575d2` changed documentation only.
