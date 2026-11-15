# Main CI at e487f72

This records CI for main commit
[`e487f7294773f75112f15437992ed780b53c7307`](https://github.com/mingley/partitionline/commit/e487f7294773f75112f15437992ed780b53c7307).
Client and broker CI failed; selected capability checks passed.

## Client

[Client run 42473006953](https://github.com/mingley/partitionline/actions/runs/42473006953)
finished with 18 successful jobs, nine failures and one skipped job. Library
failures stopped all-target commands before later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,232 passed, 22 failed |
| Linux all-feature library | 2,239 passed, 15 failed |
| Windows default library | 2,226 passed, 28 failed |
| macOS arm64 default library | 2,243 passed, 11 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and timeouts after retries |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict all-target/all-feature Clippy, audit, dependency policy,
docs, package, release rehearsal, authentication and integrity smoke, and the
other completed client checks passed. The resource-soak pass does not explain
the OS permission error that left the workload incomplete in the
[earlier run](../main-ci-262-20261108/README.md). This snapshot precedes later
producer changes.

## Broker

[Broker run 42473006883](https://github.com/mingley/partitionline/actions/runs/42473006883)
stopped in the default all-target command at **488 passed, one failed and four
ignored**. A five-node Raft test submitted
`fault-delayed-old-reply-proposal` to node 2 and received
`Node(NotLeader)`. Later non-atomic samples showed node 3 as a term-6 leader
and node 2 as a term-6 follower. Those samples do not establish the cause,
when leadership changed, or the proposal's append or commit outcome. The
proposal was not retried. The [failure note](../../broker/raft-delayed-proposal-20261108/README.md)
reviews the selection and proposal boundaries.

Later default targets, the all-feature suite, broker Clippy, Rustdoc and
doctests did not run. Pinned API inventory and fuzz supervision passed
separately. A [prior broker run](https://github.com/mingley/partitionline/actions/runs/42472398405)
passed 779 default and 927 all-feature tests with four ignored per profile,
plus strict checks, inventory and fuzz supervision. The intervening main
changes modified client Admin tests and documentation, not broker source or
manifests; the prior pass does not explain this failure.

## Capabilities

[Capability run 42473006893](https://github.com/mingley/partitionline/actions/runs/42473006893)
passed both selected profiles. Its archives were not independently inspected.
These checks do not qualify production readiness or a performance ranking.
