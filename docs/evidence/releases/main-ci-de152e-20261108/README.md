# Main CI at de152e7

This snapshot records GitHub Actions results for main commit
[`de152e7`](https://github.com/mingley/partitionline/commit/de152e78ca6d85e6cd4ea5915058b2bb6141cf42)
on 8 November 2026. It includes the coordinator, journal ownership and
actor-owned admission repairs and adds a focused Raft proposal-boundary test.
It does not qualify later commits.

## Client

The [client run](https://github.com/mingley/partitionline/actions/runs/42453796321)
completed with 18 successful jobs, nine failed jobs and one skipped job.

| Scope | Result |
| --- | --- |
| Linux default-feature library | 2,226 passed, 18 failed; the all-target command stopped at the library. |
| Linux all-feature library | 2,221 passed, 23 failed; later targets did not run. |
| Windows default-feature library | 2,235 passed, nine failed. |
| macOS arm64 default-feature library | 2,230 passed, 14 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow. |
| Kafka 3.9.1 and 4.1.0 smoke | Both exhausted the EOS example retries with timeouts. The 4.1.0 job also reported JoinGroup error 16. |
| Format, strict all-target/all-feature Clippy, docs, audit, dependency policy, package and release rehearsal | Passed. |
| Live conformance | Skipped. |

Raw job logs list the individual library failures. Both
[capability profiles](https://github.com/mingley/partitionline/actions/runs/42453796467)
passed selected checks; their archives were not independently inspected.
These runs do not establish live-broker failover or the complete release matrix.
Failure counts differ from earlier runs and do not by themselves establish a fix.

## Broker

The [broker run](https://github.com/mingley/partitionline/actions/runs/42453796336)
passed 755 default and 903 all-feature tests, with three descriptor diagnostics
ignored in each suite. Those diagnostics passed separately in both profiles on
the same broker production source in the
[combined-source controls](https://github.com/mingley/partitionline/actions/runs/42450883159).
Strict all-target/all-feature Clippy, Rustdoc, inventory and fuzz supervision
passed. Both doctest commands found zero tests.

This pass does not resolve the prior [Raft test failures](../../broker/main-ci-3fc-20261108/README.md)
or the [ambiguous quorum delete error](../../broker/main-ci-c471-20261108/README.md).
A [separate Delete diagnostic](../../broker/quorum-delete-diagnostic-20261108/README.md)
did not reproduce that error. An unmerged
[Raft test candidate](https://github.com/mingley/partitionline/actions/runs/42454616584)
later observed prolonged election churn under short election timeouts; its
cause remains unknown. The main CI pass does not demonstrate durability under
a long fault campaign.

The release and benchmark gates remain open. These results do not establish
production readiness or a ranking against other Kafka clients or servers.
