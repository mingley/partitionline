# Main CI at ba8d72f

This snapshot records GitHub Actions results for main commit [`ba8d72f`](https://github.com/mingley/partitionline/commit/ba8d72f51c23b4bfbd1057e61bbd483da831aa91) on 8 November 2026. It includes the known-coordinator socket retirement repair. It does not qualify later commits.

## Client

The [client run](https://github.com/mingley/partitionline/actions/runs/42448888605) completed with 18 successful jobs, nine failed jobs and one skipped job.

| Scope | Result |
| --- | --- |
| Linux default-feature library | 2,227 passed, 17 failed; the all-target command stopped at the library. |
| Linux all-feature library | 2,237 passed, seven failed; later targets did not run. |
| Windows default-feature library | 2,234 passed, 10 failed. |
| macOS arm64 default-feature library | 2,229 passed, 15 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow. |
| Kafka 3.9.1 and 4.1.0 smoke | Both reported JoinGroup error 16 and exhausted the EOS example retries with timeouts. |
| Format, strict all-target/all-feature Clippy, docs, audit, dependency policy, package and release rehearsal | Passed. |
| Live conformance | Skipped. |

The three known-coordinator retirement controls and the write-timeout fixture passed in all four client library jobs. Other library tests failed; the raw job logs list each case. These runs do not establish live-broker failover or a complete release matrix.

## Broker

The [broker run](https://github.com/mingley/partitionline/actions/runs/42448888661) passed all 739 default and 887 all-feature tests across its targets. Strict all-target/all-feature Clippy, Rustdoc, inventory and fuzz supervision passed. Both doctest commands found zero tests. These results do not resolve earlier quorum or Raft failures or demonstrate durability under a long fault campaign.

The release and benchmark gates remain open. These results do not establish production readiness or a ranking against other Kafka clients or servers.
