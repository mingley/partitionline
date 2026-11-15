# Main CI at 1f37163

This records runs for main commit
[`1f371635261b8c222f597100c06f83d06c3dab18`](https://github.com/mingley/partitionline/commit/1f371635261b8c222f597100c06f83d06c3dab18).
The broker and selected capability jobs passed. Client CI failed.

## Client

[Client run 42460903651](https://github.com/mingley/partitionline/actions/runs/42460903651)
finished with 18 successful jobs, nine failures and one skipped job. Library
failures stopped the all-target commands before later targets.

| Job | Observed result |
| --- | --- |
| Linux default library | 2,229 passed, 15 failed |
| Linux all-feature library | 2,223 passed, 21 failed |
| macOS arm64 default library | 2,235 passed, 9 failed |
| Windows default library | 2,224 passed, 20 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed; logs include JoinGroup error 16 and EOS timeouts after retries |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict Clippy, audit, dependency policy, docs, package, release
rehearsal, authentication and integrity smoke, and the other completed client
checks passed. The earlier
[resource-soak permission failure](../main-ci-440-20261108/README.md)
remains an incomplete result for that run; this pass does not establish its
cause. Passing checks do not qualify the failed or skipped jobs.

## Broker and capabilities

[Broker run 42460903678](https://github.com/mingley/partitionline/actions/runs/42460903678)
passed 766 default and 914 all-feature all-target tests, with three ignored
descriptor diagnostics per profile. Both doctest commands found zero tests.
Strict Clippy and Rustdoc, inventory and fuzz supervision passed. The
descriptor diagnostics had passed separately on the same broker production
source in an earlier run; they were not run by this broker job.

[Capability run 42460903643](https://github.com/mingley/partitionline/actions/runs/42460903643)
passed both selected profiles. Its artifacts were not independently inspected.

These results do not explain earlier Raft, quorum or frame-deadline failures
and do not qualify production readiness or performance rankings.
