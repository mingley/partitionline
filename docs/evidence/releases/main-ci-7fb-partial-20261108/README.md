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
separate jobs. At the time this note was prepared, the client and capability
runs for this commit were still in progress; they are not counted as results
here.
