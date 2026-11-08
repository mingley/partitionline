# Main CI at 7f59ef4

This snapshot records GitHub Actions results for main commit [`7f59ef4`](https://github.com/mingley/partitionline/commit/7f59ef470340db6027958f439df3716a8dde536c) on 8 November 2026. It predates the coordinator retirement change in [`eeb339d`](https://github.com/mingley/partitionline/commit/eeb339dcf677f5089f88e4c71d1a665e9e988a29). It is not a qualification of later commits.

## Client

The [client run](https://github.com/mingley/partitionline/actions/runs/42447310249) completed with 18 successful jobs, nine failed jobs and one skipped job.

| Scope | Result |
| --- | --- |
| Linux default-feature library | 2,195 passed, 25 failed; the all-target command stopped at the library. |
| Linux all-feature library | 2,194 passed, 26 failed; later targets did not run. |
| Windows default-feature library | 2,197 passed, 23 failed. |
| macOS arm64 default-feature library | 2,195 passed, 25 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow. |
| Kafka 3.9.1 and 4.1.0 smoke | Both reported JoinGroup error 16 and exhausted the EOS example retries with timeouts. |
| Format, strict all-target/all-feature Clippy, docs, audit, dependency policy, package and release rehearsal | Passed. |
| Live conformance | Skipped. |

The client library failures include socket-retirement, scheduler responsiveness and completed-dial controls, among others. The raw job logs list each case. Later focused work addressed known-coordinator socket retirement, but the [comparison run](../../client/group-coordinator-retirement-20261108/README.md) still failed overall because the unchanged baseline failed its original write-timeout fixture. That focused work does not make this client run green.

## Broker

The [broker run](https://github.com/mingley/partitionline/actions/runs/42447310173) passed all 739 default and 887 all-feature tests across its targets. Strict all-target/all-feature Clippy, Rustdoc, inventory and fuzz supervision passed. Both doctest commands found zero tests. These results do not resolve earlier quorum or Raft failures or demonstrate durability under a long fault campaign.

The release and benchmark gates remain open. These results do not establish production readiness or a ranking against other Kafka clients or servers.
