# Main client CI at e02c1b8

The [client run](https://github.com/mingley/partitionline/actions/runs/42455943466)
for main commit [`e02c1b8`](https://github.com/mingley/partitionline/commit/e02c1b84a6e50e01eb12e530ed53d0f1cb7b4c93)
completed with 17 successful jobs, six failed jobs, four canceled jobs and one
skipped job. Main advanced while the run was in progress, and the workflow's
per-ref concurrency canceled the unfinished jobs. It is not a complete client
qualification.

| Scope | Result |
| --- | --- |
| Linux default-feature library | 2,225 passed and 19 failed; the all-target command stopped at the library. |
| Windows default-feature library | 2,232 passed and 12 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow. |
| Kafka 3.9.1 smoke | The EOS example exhausted its retries after timeouts. |
| Linux all-feature, macOS arm64, Kafka 4.1.0 smoke, package | Canceled; no result. |
| Live conformance | Skipped. |

Both [capability profiles](https://github.com/mingley/partitionline/actions/runs/42455943304)
passed selected checks; their archives were not independently inspected. The
[broker run](../../broker/main-ci-e02-20261108/README.md) failed separately
on a CreateTopics error 56. These results do not identify the cause of that
error or establish production readiness. The later commit changes are not
qualified by this snapshot.
