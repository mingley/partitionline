# Main CI at 426041c

This records CI for main commit
[`426041c9f5526dfb6a788344a0e03b3edfefffb0`](https://github.com/mingley/partitionline/commit/426041c9f5526dfb6a788344a0e03b3edfefffb0),
which includes the Admin pagination identity fix. Client CI failed; broker
and selected capability checks passed.

## Client

[Client run 42479881790](https://github.com/mingley/partitionline/actions/runs/42479881790)
finished with 18 successful jobs, nine failures and one skipped job. Library
failures stopped all-target commands before later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,259 passed, 13 failed |
| Linux all-feature library | 2,261 passed, 11 failed |
| Windows default library | 2,257 passed, 15 failed |
| macOS arm64 default library | 2,264 passed, 8 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and timeouts after retries |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict all-target/all-feature Clippy, audit, dependency policy,
docs, package, release rehearsal, authentication and integrity smoke, and the
other completed client checks passed. The resource-soak pass does not explain
the OS permission error that left the workload incomplete in an
[earlier run](../main-ci-262-20261108/README.md).

## Broker

[Broker run 42479881545](https://github.com/mingley/partitionline/actions/runs/42479881545)
passed **781 default and 929 all-feature tests**, with four ignored in each
profile. Strict formatting, Clippy, Rustdoc and both doctest commands passed;
the two doctest commands found zero tests. Pinned API inventory and fuzz
supervision passed in separate jobs. This pass does not explain the prior
[Raft runtime failures](../main-ci-912-partial-20261108/README.md) or establish
the failed proposal's outcome.

## Capabilities

[Capability run 42479881830](https://github.com/mingley/partitionline/actions/runs/42479881830)
passed both selected profiles. Its archives were not independently inspected.
These results do not qualify production readiness or a performance ranking.
