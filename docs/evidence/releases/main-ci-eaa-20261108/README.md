# Main CI at eaa8ac6

This snapshot records GitHub Actions results for main commit
[`eaa8ac6`](https://github.com/mingley/partitionline/commit/eaa8ac6ddfe767d6ae7d45b19eb945a6a1578c9b)
on 8 November 2026. It includes the coordinator, journal ownership, and
actor-owned admission repairs. It does not qualify later commits.

## Client

The [client run](https://github.com/mingley/partitionline/actions/runs/42452432100)
completed with 18 successful jobs, nine failed jobs and one skipped job.

| Scope | Result |
| --- | --- |
| Linux default-feature library | 2,225 passed, 19 failed; the all-target command stopped at the library. |
| Linux all-feature library | 2,219 passed, 25 failed; later targets did not run. |
| Windows default-feature library | 2,229 passed, 15 failed. |
| macOS arm64 default-feature library | 2,235 passed, nine failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow. |
| Kafka 3.9.1 and 4.1.0 smoke | Both reported JoinGroup error 16 and exhausted the EOS example retries with timeouts. |
| Format, strict all-target/all-feature Clippy, docs, audit, dependency policy, package and release rehearsal | Passed. |
| Live conformance | Skipped. |

The three known-coordinator retirement controls and the write-timeout fixture
passed in all four client library jobs. Other library tests failed; the raw
job logs list each case. Both [capability profiles](https://github.com/mingley/partitionline/actions/runs/42452432080)
passed selected checks; their archives were not independently inspected.
These runs do not establish live-broker failover or the complete release matrix.

## Broker

The [broker run](https://github.com/mingley/partitionline/actions/runs/42452432054)
passed 754 default and 902 all-feature tests, with three descriptor diagnostics
ignored in each suite. Those diagnostics passed separately in both profiles in
the [combined-source controls](https://github.com/mingley/partitionline/actions/runs/42450883159).
Strict all-target/all-feature Clippy, Rustdoc, inventory and fuzz supervision
passed. Both doctest commands found zero tests.

This pass does not resolve the prior [Raft test failures](../../broker/main-ci-3fc-20261108/README.md)
or the [ambiguous quorum delete error](../../broker/main-ci-c471-20261108/README.md).
It does not demonstrate durability under a long fault campaign.

The release and benchmark gates remain open. These results do not establish
production readiness or a ranking against other Kafka clients or servers.
