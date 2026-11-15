# Main CI at 262d254

This records CI for main commit
[`262d2544a4352d52538f19ef9ab0c8c5700774d3`](https://github.com/mingley/partitionline/commit/262d2544a4352d52538f19ef9ab0c8c5700774d3),
which includes the Raft vote mailbox change. Both the client and broker
workflows failed. The selected capability jobs passed.

## Client

[Client run 42465424086](https://github.com/mingley/partitionline/actions/runs/42465424086)
finished with 17 successful jobs, ten failures and one skipped job. Library
failures stopped the all-target commands before later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,233 passed, 11 failed |
| Linux all-feature library | 2,233 passed, 11 failed |
| Windows default library | 2,227 passed, 17 failed |
| macOS arm64 default library | 2,230 passed, 14 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and EOS timeouts after retries |
| Resource soak | Failed with `Permission denied: '/proc/3132/fd'`; the workload result is incomplete |
| Live conformance | Skipped |

Formatting, strict Clippy, audit, dependency policy, docs, package, release
rehearsal, authentication and integrity smoke, and the other completed client
checks passed. The resource-soak log does not establish why the directory was
inaccessible or how the workload would have completed.

## Broker

[Broker run 42465423955](https://github.com/mingley/partitionline/actions/runs/42465423955)
passed formatting and its focused stale-leader proposal control. Its default
all-target command stopped with 696 passed, one failed and four ignored.
The three-node minority-partition test received `Node(NotLeader)` at
`fault-partition-majority-proposal`. Subsequent separately collected
samples showed node 0 as a term-7 follower, node 1 as a term-7 leader and
node 2 as a term-5 leader. These samples were not atomic and do not establish
the cause or whether the mailbox change contributed.

The all-feature behavioral tests, broker Clippy, Rustdoc and doctests did not
run in that job. Inventory with its negative controls and fuzz supervision
passed separately. The mailbox's
[pre-integration combined checks](../../broker/raft-vote-mailbox-20261108/README.md)
passed both broker profiles; they do not explain this failure.

## Capabilities

[Capability run 42465423957](https://github.com/mingley/partitionline/actions/runs/42465423957)
passed both selected profiles. Its artifacts were not independently inspected.
These results do not qualify production readiness or a performance ranking.
