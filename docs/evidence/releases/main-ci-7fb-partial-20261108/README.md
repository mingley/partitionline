# Partial broker CI at 7fb973f

[Broker run 42476521782](https://github.com/mingley/partitionline/actions/runs/42476521782)
tested main commit [`7fb973f531f63aca32da18cccf7f1a852f49ee68`](https://github.com/mingley/partitionline/commit/7fb973f531f63aca32da18cccf7f1a852f49ee68).
Its default library reported **490 passed, one failed and four ignored**.

The five-node Raft test sent `fault-delayed-old-reply-proposal` to node 2
and received `Node(NotLeader)`. Later, separately sampled diagnostics showed
node 0 as a term-4 leader and node 2 as a term-4 follower. These samples do
not establish when leadership changed, the reason for the response, or the
proposal's append or commit outcome. The failed proposal was not retried.
An [earlier run](../../broker/raft-delayed-proposal-20261108/README.md)
failed on the same labeled operation; the relationship between these failures
is unknown.

Later default targets, all-feature behavioral tests, broker Clippy, Rustdoc
and doctests did not run. Pinned inventory and fuzz supervision passed in
separate jobs.

The [client run](https://github.com/mingley/partitionline/actions/runs/42476521821)
finished with 18 successful jobs, eight failures, one canceled and one skipped.
Linux default had 2,239 passed and 15 failed; Linux all features had 2,240
passed and 14 failed. Windows had 2,246 passed and eight failed, and macOS
arm64 had 2,242 passed and 12 failed. Later targets did not run after the
library failures. Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility jobs aborted
with stack overflows; the Kafka 3.9.1 smoke reported JoinGroup error 16 and
timeouts. The Kafka 4.1.0 smoke was canceled when main advanced and has no
qualification result.

Both selected [capability profiles](https://github.com/mingley/partitionline/actions/runs/42476521802)
passed; their archives were not independently inspected.
