# Main CI at f092d69

This records CI for main commit
[`f092d69d43ebb300c756cca006f4964c9820e491`](https://github.com/mingley/partitionline/commit/f092d69d43ebb300c756cca006f4964c9820e491).
The broker and selected capability jobs passed. Client CI failed and four
jobs were canceled when main advanced.

## Client

[Client run 42460194939](https://github.com/mingley/partitionline/actions/runs/42460194939)
finished with 18 successful jobs, five failures, four cancellations and one
skipped job.

| Job | Result |
| --- | --- |
| Linux default library | 2,229 passed, 15 failed; later all-target tests did not run |
| Windows default library | 2,223 passed, 21 failed; later default targets did not run |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow |
| Linux all features and macOS arm64 | Canceled when main advanced; no final result |
| Kafka 3.9.1 and 4.1.0 smoke | Canceled when main advanced; no final result |
| Resource soak | Passed |
| Live conformance | Skipped |

Formatting, strict Clippy, audit, dependency policy, docs, package, release
rehearsal, authentication and integrity smoke, and the other completed client
checks passed. The earlier
[resource-soak permission failure](../main-ci-440-20261108/README.md)
remains an incomplete result for that run; this pass does not establish its
cause. Passing checks do not qualify the failed or canceled jobs.

## Broker and capabilities

[Broker run 42460194861](https://github.com/mingley/partitionline/actions/runs/42460194861)
passed 766 default and 914 all-feature all-target tests, with three ignored
descriptor diagnostics per profile. Both doctest commands found zero tests.
Strict Clippy and Rustdoc, inventory and fuzz supervision passed. The
descriptor diagnostics had passed separately on the same broker production
source in an earlier run; they were not run by this broker job.

[Capability run 42460194888](https://github.com/mingley/partitionline/actions/runs/42460194888)
passed both selected profiles. Its artifacts were not independently inspected.

These results do not explain earlier Raft, quorum or frame-deadline failures
and do not qualify production readiness or performance rankings.
