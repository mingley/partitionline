# Main CI at 4409633

This records runs for main commit
[`4409633fa989effe1083e623f6e59fcc0d179eee`](https://github.com/mingley/partitionline/commit/4409633fa989effe1083e623f6e59fcc0d179eee).
The broker and selected capability jobs passed. Client CI failed; one smoke
job was canceled when main advanced, so it has no final result.

## Client

[Client run 42459181316](https://github.com/mingley/partitionline/actions/runs/42459181316)
finished with 17 successful jobs, nine failures, one cancellation and one
skipped job. The library failure stopped each all-target test command before
its later targets.

| Job | Result |
| --- | --- |
| Linux default library | 2,233 passed, 11 failed |
| Linux all-feature library | 2,232 passed, 12 failed |
| Windows default library | 2,225 passed, 19 failed |
| macOS arm64 default library | 2,230 passed, 14 failed |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Kafka 3.9.1 smoke | Failed; logs include JoinGroup error 16 and EOS timeouts after retries |
| Kafka 4.1.0 smoke | Canceled when main advanced; no final result |
| Resource soak | Failed with `Permission denied: '/proc/3151/fd'` while running the slow-application test; the workload result is incomplete |
| Live conformance | Skipped |

The resource-soak error is an operating-system permission failure. The log does
not establish why that directory was inaccessible or how the workload would
have completed. Formatting, strict Clippy, audit, dependency policy, docs,
package, release rehearsal, authentication and integrity smoke, and the
other completed client checks passed. Passing checks do not qualify the failed
or canceled jobs.

## Broker and capabilities

[Broker run 42459181380](https://github.com/mingley/partitionline/actions/runs/42459181380)
passed 764 default and 912 all-feature all-target tests, with three ignored
descriptor diagnostics per profile. Both doctest commands found zero tests.
Strict Clippy and Rustdoc, inventory and fuzz supervision passed. The
descriptor diagnostics had passed separately on the same broker production
source in an earlier run; they were not run by this broker job.

[Capability run 42459181276](https://github.com/mingley/partitionline/actions/runs/42459181276)
passed both selected profiles. Its artifacts were not independently inspected.

These results do not explain earlier Raft, quorum or frame-deadline failures
and do not qualify production readiness or performance rankings. The
[previous complete snapshot](../main-ci-1036-20261108/README.md) retains its
own results.
