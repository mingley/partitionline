# Main CI at 6298f6b

This snapshot records GitHub Actions results for main commit
[`6298f6b`](https://github.com/mingley/partitionline/commit/6298f6bfbed2ab98189d291ebebd2190a2ee75b6)
on 8 November 2026. It includes the header-aware record validator, grouped
journal reservation repair and Admin response reservation change. It does not
qualify later commits.

## Client

The [client run](https://github.com/mingley/partitionline/actions/runs/42456887123)
completed with 18 successful jobs, nine failed jobs and one skipped job.

| Scope | Result |
| --- | --- |
| Linux default-feature library | 2,230 passed, 14 failed; the all-target command stopped at the library. |
| Linux all-feature library | 2,232 passed, 12 failed; later targets did not run. |
| Windows default-feature library | 2,233 passed, 11 failed. |
| macOS arm64 default-feature library | 2,234 passed, 10 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each job aborted with a stack overflow. |
| Kafka 3.9.1 and 4.1.0 smoke | Both reported JoinGroup error 16 and exhausted EOS example retries with timeouts. |
| Format, strict all-target/all-feature Clippy, docs, audit, dependency policy, package and release rehearsal | Passed. |
| Live conformance | Skipped. |

Raw job logs list individual library failures. Both
[capability profiles](https://github.com/mingley/partitionline/actions/runs/42456887240)
passed selected checks; their archives were not independently inspected.
Failure counts differ from earlier runs and do not by themselves establish a
fix or the complete release matrix.

## Broker

The [broker run](https://github.com/mingley/partitionline/actions/runs/42456887194)
passed 761 default-feature tests with three ignored. Its all-feature command
stopped in the library at 516 passed, one failed and three ignored. The
`read_deadline_stops_a_ready_frame_body` test used a 30 ms deadline and an
80 ms wall-time assertion; it reported the expected deadline error after
82.681715 ms. The log does not record scheduler or individual poll timing, so
it cannot establish the cause.

Later all-feature test targets and the broker job's strict Clippy, Rustdoc and
doctest steps did not run. Broker formatting, inventory and fuzz supervision
passed separately. The three ignored descriptor diagnostics passed separately
in both profiles on the combined parser and journal source in
[run 42456150975](https://github.com/mingley/partitionline/actions/runs/42456150975).

This failure is distinct from the unresolved
[CreateTopics error 56](../../broker/main-ci-e02-20261108/README.md),
[earlier Delete error 56](../../broker/main-ci-c471-20261108/README.md) and
[Raft liveness failures](../../broker/raft-liveness-20261108/README.md).
The release and benchmark gates remain open; the project is not yet qualified
for production use or a ranking against other Kafka clients or servers.
