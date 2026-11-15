# Main CI at 1036a39

This snapshot records GitHub Actions results for main commit
[`1036a39`](https://github.com/mingley/partitionline/commit/1036a393eb85f23032b935bf86862b7e98e36e14)
on 8 November 2026. It includes the record validation, grouped journal,
Admin reservation and CreateTopics diagnostic changes. It does not qualify
later commits.

## Client

The [client run](https://github.com/mingley/partitionline/actions/runs/42457917903)
completed with 18 successful jobs, nine failed jobs and one skipped job.

| Scope | Result |
| --- | --- |
| Linux default-feature library | 2,223 passed, 21 failed; the all-target command stopped at the library. |
| Linux all-feature library | 2,225 passed, 19 failed; later targets did not run. |
| Windows default-feature library | 2,226 passed, 18 failed. |
| macOS arm64 default-feature library | 2,233 passed, 11 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow. |
| Kafka 3.9.1 and 4.1.0 smoke | Both reported JoinGroup error 16 and exhausted EOS example retries with timeouts. |
| Format, strict all-target/all-feature Clippy, docs, audit, dependency policy, package and release rehearsal | Passed. |
| Live conformance | Skipped. |

Raw job logs list individual library failures. Both
[capability profiles](https://github.com/mingley/partitionline/actions/runs/42457917944)
passed selected checks; their archives were not independently inspected.
Failure counts vary across runs and do not by themselves establish a fix or
the complete release matrix.

## Broker

The [broker run](https://github.com/mingley/partitionline/actions/runs/42457917899)
passed 762 default and 910 all-feature all-target tests, with three ignored in
each suite. Those descriptor diagnostics passed separately in both profiles
on the same broker production source in
[run 42456150975](https://github.com/mingley/partitionline/actions/runs/42456150975).
Strict all-target/all-feature Clippy, Rustdoc, inventory and fuzz supervision
passed. Both doctest commands found zero tests.

This pass does not explain the earlier [frame-read test failure](../../broker/frame-read-deadline-test-20261108/README.md),
[CreateTopics error 56](../../broker/main-ci-e02-20261108/README.md),
[Delete error 56](../../broker/main-ci-c471-20261108/README.md) or
[Raft liveness failures](../../broker/raft-liveness-20261108/README.md).
The release and benchmark gates remain open; the project is not yet qualified
for production use or a ranking against other Kafka clients or servers.
