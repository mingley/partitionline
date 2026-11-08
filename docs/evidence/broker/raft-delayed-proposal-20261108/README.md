# Raft proposal after a delayed-reply gate

This note preserves [main broker run 42473006883][run] and reviews its
leader-selection and proposal boundary. It does not establish why the
proposal was refused. The failed proposal was not retried.

## CI observation

The run tested main commit [`e487f7294773f75112f15437992ed780b53c7307`][source].
The default broker library stopped at **488 passed, one failed and four
ignored**. Later default targets, all-feature behavioral tests, broker Clippy,
Rustdoc and doctests did not run. The separate API inventory and fuzz
supervision jobs passed.

The five-node fault test sent `fault-delayed-old-reply-proposal` to node 2.
The returned response was `error private peer runtime: Node(NotLeader)`.
Diagnostics collected afterward showed node 3 as term-6 leader with last
index 5 and committed index 4, and node 2 as term-6 follower with last and
committed index 4. Node 0, node 1 and node 4 were separately sampled as
term-6 followers with last and committed index 4.

These are later, non-atomic samples from different processes. They do not
establish the role, term, tail or commit outcome at the proposal's execution
point, when any leader transition occurred, or its cause. They also do not
establish whether a code change, scheduling or transport behavior contributed.

## Selection and proposal boundary

The [test][fault] chooses a node using [`Cluster::leader`][select] at the
beginning and retains that node for the later proposal. The selector reads
published state and a separate readiness observation; it is not a leadership
lease. A previously selected leader can lose authority before its later
command is processed. That is a possible explanation for the response, not a
reconstruction of this run. A later sample or a fresh pre-command state check
cannot make the selection and the operation atomic.

Immediately before submitting the labeled command, the fixture arms an
[800 ms one-shot reply delay][phase] for one route. The failed response
occurs before the subsequent commit check. The retained log does not establish
whether a particular reply consumed that delay, crossed its RPC deadline, or
influenced leadership in this run.

The public [`Handle::propose`][handle] dispatches an owner command that calls
[`Node::propose`][propose]. That method checks leadership before appending,
then synchronizes the summary and calls `advance_commit`. The latter also
has a [`NotLeader` guard][commit] after the append in source order. This
review does not show whether that latter guard is reachable in this schedule
or was reached in the failed call. The response alone is insufficient to
establish a general pre-append or commit outcome, so it does not justify
reissuing this proposal.

The same CI job's separate
[`stale_genesis_leader_observation_is_fenced_before_proposal_append`][control]
control passed. It forces an expired-leadership schedule and verifies that
one refused proposal leaves the log unchanged. A distinct
[leader-removal control][removal] also covers a specific pre-append refusal.
Neither control establishes the result or cause of this failed proposal.

## Source pins

At `e487f7294773f75112f15437992ed780b53c7307`, the Git blobs were
`5a9610523130bf3d381b00cf04ebd36bd539fb05` for the shared process-test
fixture, `d8fec08072b9128bbcb9623dfdf267212689d63d` for the runtime,
`458cafb8478f79d941d75bfec8490ec731029d7c` for replication, and
`01ab3587147d45688611a22f289ed7a824d6f021` for the membership controls.
This note makes no production or test-code change.

[run]: https://github.com/mingley/partitionline/actions/runs/42473006883
[source]: https://github.com/mingley/partitionline/commit/e487f7294773f75112f15437992ed780b53c7307
[fault]: https://github.com/mingley/partitionline/blob/e487f7294773f75112f15437992ed780b53c7307/partitionline-broker/tests/common/raft_runtime.rs#L1416
[select]: https://github.com/mingley/partitionline/blob/e487f7294773f75112f15437992ed780b53c7307/partitionline-broker/tests/common/raft_runtime.rs#L961
[phase]: https://github.com/mingley/partitionline/blob/e487f7294773f75112f15437992ed780b53c7307/partitionline-broker/tests/common/raft_runtime.rs#L1456
[handle]: https://github.com/mingley/partitionline/blob/e487f7294773f75112f15437992ed780b53c7307/partitionline-broker/src/raft/runtime.rs#L1109
[propose]: https://github.com/mingley/partitionline/blob/e487f7294773f75112f15437992ed780b53c7307/partitionline-broker/src/raft/replication.rs#L2756
[commit]: https://github.com/mingley/partitionline/blob/e487f7294773f75112f15437992ed780b53c7307/partitionline-broker/src/raft/replication.rs#L3174
[control]: https://github.com/mingley/partitionline/blob/e487f7294773f75112f15437992ed780b53c7307/partitionline-broker/tests/raft_membership.rs#L480
[removal]: https://github.com/mingley/partitionline/blob/e487f7294773f75112f15437992ed780b53c7307/docs/evidence/broker/quorum-proposal-boundary-20261108/README.md
