# Main CI at 02c443c

This records CI for main commit
[`02c443cacd39a59d3991faeaf577032020ecb071`](https://github.com/mingley/partitionline/commit/02c443cacd39a59d3991faeaf577032020ecb071).
Both the client and broker workflows failed. The selected capability jobs
passed. These results do not qualify the project for production or establish
a performance ranking.

## Client

[Client run 42462115925](https://github.com/mingley/partitionline/actions/runs/42462115925)
finished with 18 successful jobs, nine failures and one skipped job. The
library failures stopped the all-target commands before later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,231 passed, 13 failed |
| Linux all-feature library | 2,231 passed, 13 failed |
| Windows default library | 2,221 passed, 23 failed |
| macOS arm64 default library | 2,228 passed, 16 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and EOS timeouts after retries |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict Clippy, audit, dependency policy, docs, package, release
rehearsal, authentication and integrity smoke, and the other completed client
checks passed. The earlier
[resource-soak permission failure](../main-ci-440-20261108/README.md)
remains incomplete for that run; a later pass does not establish its cause.

## Broker

[Broker run 42462115954](https://github.com/mingley/partitionline/actions/runs/42462115954)
passed formatting and the focused stale-leader proposal control. Its default
all-target command stopped with 688 passed, two failed and three ignored.
The five-peer observer test received `Node(NotLeader)` at the
`observer-addition` phase. The five-node minority-partition test received
`Node(NotLeader)` at `fault-delayed-old-reply-proposal`. The reported node
snapshots were gathered after the failures and are not an atomic cluster
observation. The causes remain unknown.

The all-feature behavioral tests, broker Clippy, Rustdoc and doctests did not
run in that job. Inventory with its negative controls and fuzz supervision
passed in separate jobs. The preceding
[main snapshot](../main-ci-1f37-20261108/README.md) passed both broker
profiles; that pass does not explain these failures.

## Capabilities

[Capability run 42462115921](https://github.com/mingley/partitionline/actions/runs/42462115921)
passed both selected profiles. Its artifacts were not independently inspected.
