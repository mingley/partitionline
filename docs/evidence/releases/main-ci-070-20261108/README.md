# Main CI at 0708730

This records CI for main commit
[`0708730155d869ab4fa52654a2e95d4013c4e594`](https://github.com/mingley/partitionline/commit/0708730155d869ab4fa52654a2e95d4013c4e594),
which includes the Admin pagination change. Client and broker CI failed;
selected capability checks passed.

## Client

[Client run 42471273752](https://github.com/mingley/partitionline/actions/runs/42471273752)
finished with 18 successful jobs, nine failures and one skipped job. Library
failures stopped the all-target commands before later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,234 passed, 20 failed |
| Linux all-feature library | 2,232 passed, 22 failed |
| Windows default library | 2,237 passed, 17 failed |
| macOS arm64 default library | 2,237 passed, 17 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and timeouts after retries |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict Clippy, audit, dependency policy, docs, package, release
rehearsal, authentication and integrity smoke, and the other completed client
checks passed. The resource-soak pass does not explain the OS permission error
that left the workload incomplete in the [earlier run](../main-ci-262-20261108/README.md).

## Broker

[Broker run 42471273652](https://github.com/mingley/partitionline/actions/runs/42471273652)
stopped in the default all-target command at **702 passed, one failed and four
ignored**. A five-node Raft test received `Node(NotLeader)` when submitting
`observer-addition` to node 1. Later separately sampled states showed node 0
as term-5 leader and node 1 as term-5 follower; they do not establish when
leadership changed or the cause. The operation was not retried. The
[broker failure note](../../broker/main-ci-070-partial-20261108/README.md)
records the exact test and diagnostics.

The broker all-feature suite, Clippy, Rustdoc and doctests in that job did not
run. Pinned API inventory and instrumented fuzz supervision passed separately.
The preceding [broker run](https://github.com/mingley/partitionline/actions/runs/42470021226)
passed both profiles and strict checks. The intervening commits changed client
Admin source and documentation, not broker source or manifests; the prior pass
does not explain this failure.

## Capabilities

[Capability run 42471273683](https://github.com/mingley/partitionline/actions/runs/42471273683)
passed both selected profiles. Its archives were not independently inspected.
These checks do not qualify production readiness or a performance ranking.
