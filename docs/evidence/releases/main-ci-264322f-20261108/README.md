# Main CI at 264322f

The [client CI run](https://github.com/mingley/partitionline/actions/runs/42442368705)
and [broker CI run](https://github.com/mingley/partitionline/actions/runs/42442368708)
tested [commit 264322f](https://github.com/mingley/partitionline/commit/264322ff430b0594f4c3864e53dcd832de2a930b)
on 8 November 2026. Both failed. The results do not cover later changes.

| Check | Result |
| --- | --- |
| [Linux client default library](https://github.com/mingley/partitionline/actions/runs/42442368705/job/128614107119) | 2,185 passed and 22 failed; the all-target run stopped at the library. |
| [Linux client all-feature library](https://github.com/mingley/partitionline/actions/runs/42442368705/job/128614107192) | 2,182 passed and 25 failed; later targets did not run. |
| [Windows default library](https://github.com/mingley/partitionline/actions/runs/42442368705/job/128614107309) | 2,193 passed and 14 failed. |
| [macOS arm64 default library](https://github.com/mingley/partitionline/actions/runs/42442368705/job/128614107242) | 2,193 passed and 14 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All overflowed during the required compatibility test after the classic-join marker. |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed after repeated EOS timeouts. |
| [Broker default targets](https://github.com/mingley/partitionline/actions/runs/42442368708/job/128614106720) | 737 passed. |
| Broker all-feature targets | 622 passed and the three-process metadata quorum test failed with an ambiguous or unexpected delete error 56. Remaining targets did not run. |
| Broker inventory and fuzz supervision | Passed. |
| Client format, all-target Clippy, audit, docs, package, auth and integrity smoke | Passed. Live conformance was skipped. |

The client failures include fairness and socket-lifecycle checks; the logs do
not establish that they share a cause. The broker failure follows earlier Raft
failures at [b631809](../main-ci-b631-20261108/README.md); neither run establishes
the cause of the other.

A separate [nontransactional cancellation control](https://github.com/mingley/partitionline/actions/runs/42443409232)
on this source fetched a second record while an interval commit was pending.
After canceling the poll, the consumer position was 2 and a retry returned no
record at offset 1. The workflow recognized the expected failing assertion;
it is not a passing fix. It did not exercise ReadCommitted.

Focused qualification of later patches does not replace a clean full matrix.
No new release is ready.
