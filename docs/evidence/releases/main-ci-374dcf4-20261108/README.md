# Main CI at 374dcf4

The [client CI run](https://github.com/mingley/partitionline/actions/runs/42444029075),
[broker CI run](https://github.com/mingley/partitionline/actions/runs/42444029278),
and [capability run](https://github.com/mingley/partitionline/actions/runs/42444029191)
tested [commit 374dcf4](https://github.com/mingley/partitionline/commit/374dcf45db20ee728b00c0b0188e77ca1f47fa9c)
on 8 November 2026. Client CI failed; broker CI and both capability profiles
passed. These results do not cover later changes.

| Check | Result |
| --- | --- |
| [Linux client default library](https://github.com/mingley/partitionline/actions/runs/42444029075/job/128619077229) | 2,190 passed and 17 failed; all-target testing stopped at the library. |
| [Linux client all-feature library](https://github.com/mingley/partitionline/actions/runs/42444029075/job/128619077470) | 2,180 passed and 27 failed; later targets did not run. |
| [Windows default library](https://github.com/mingley/partitionline/actions/runs/42444029075/job/128619077490) | 2,184 passed and 23 failed. |
| [macOS arm64 default library](https://github.com/mingley/partitionline/actions/runs/42444029075/job/128619077273) | 2,189 passed and 18 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed after repeated EOS timeouts. |
| [Broker default and all-feature targets](https://github.com/mingley/partitionline/actions/runs/42444029278/job/128619078065) | 738 and 886 passed, respectively. Strict all-target Clippy and Rustdoc passed; both doctest commands found zero tests. |
| Broker inventory and fuzz supervision | Both passed. |
| Client capability profiles | Both passed their selected checks; their archives were not independently inspected. |
| Client format, all-target Clippy, audit, docs, package, auth and integrity smoke | Passed. Live conformance was skipped. |

The client failures include fairness and socket-lifecycle checks. The logs do
not establish one shared cause. The broker pass does not explain the earlier
[Raft failures](../main-ci-b631-20261108/README.md) or the
[quorum failure](../main-ci-264322f-20261108/README.md), and does not establish
reliability under a sustained fault campaign.

A separate [producer cancellation investigation](../../client/addpart-retryfix-264-20261108/REPORT.md)
reproduced lost record completions and retained byte accounting in synthetic
worker and cleanup controls. The fixes under investigation are not applied.
No new release is ready.
