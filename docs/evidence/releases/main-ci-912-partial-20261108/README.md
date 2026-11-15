# Partial broker CI at 912254c

[Broker run 42478671158](https://github.com/mingley/partitionline/actions/runs/42478671158)
tested main commit [`912254c8cf38c8272606c10b16ef29109b73999d`](https://github.com/mingley/partitionline/commit/912254c8cf38c8272606c10b16ef29109b73999d).
The default profile passed 781 tests with four ignored. In the all-feature
profile, 789 tests passed, three failed and four were ignored before the
command stopped; later all-feature targets did not run.

The failing `raft_runtime` target reported failures in the five-peer and
three-peer observer image tests and in the five-node minority-partition
test. In the latter, `fault-delayed-old-reply-proposal` to node 2 returned
`Node(NotLeader)`. Later separate samples showed node 0 as term-13 leader
and node 2 as term-13 follower. They do not establish the cause, the
leadership transition time, or the proposal's append or commit outcome. The
proposal was not retried. The relationship to prior failures is unknown.

Broker Clippy, Rustdoc and both doctest steps in that job were skipped.
Pinned inventory and fuzz supervision passed separately. The
[preceding isolated profiles](../../broker/disconnect-event-tests-20261108/README.md)
passed on the same test source; that pass does not explain this failure.
Both selected [capability profiles](https://github.com/mingley/partitionline/actions/runs/42478671165)
passed; their archives were not independently inspected.

The [client run](https://github.com/mingley/partitionline/actions/runs/42478671140)
finished with 18 successful jobs, eight failures, one canceled and one skipped.
Linux default had 2,237 passed and 17 failed; Linux all features had 2,243
passed and 11 failed. Windows had 2,245 passed and nine failed; macOS arm64
had 2,241 passed and 13 failed. Later targets did not run after the library
failures. Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility jobs aborted with stack
overflows. Kafka 4.1.0 smoke reported JoinGroup error 16 and timeouts. The
Kafka 3.9.1 smoke was canceled when main advanced and has no result.
