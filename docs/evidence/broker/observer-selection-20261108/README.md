# Observer-addition leader selection

This note records a source review and an independent control for the
[failed main broker run 42471273652](https://github.com/mingley/partitionline/actions/runs/42471273652).
It does not explain that failure or establish a production or test root cause.
The failed addition was not retried.

## Main CI observation

The run tested main commit
[`0708730155d869ab4fa52654a2e95d4013c4e594`](https://github.com/mingley/partitionline/commit/0708730155d869ab4fa52654a2e95d4013c4e594).
Its default broker run stopped with 702 passed, one failed and four ignored;
the all-feature suite, broker Clippy, Rustdoc and doctests did not run.
Inventory and fuzz supervision passed in separate jobs.

The five-peer observer-image test sent `observer-addition` to node 1 and
received `error private peer runtime: Node(NotLeader)`. Later, separately
sampled node states showed node 0 as term-5 leader and node 1 as term-5
follower. These samples were not atomic with the failed command or with one
another. The log does not show when leadership changed, why node 1 refused
the addition, or whether a production defect contributed.

## Selection and submission

At that commit, [`observer_image_history`][history] calls
[`Cluster::leader`][select] once, excluding the observer, and retains the
returned node for two prefix proposals, both commit waits, checkpoint creation
and the observer-addition command. It does not refresh that selection before
the addition. A selected leader may cease being leader during any of these
steps; even refreshing a state snapshot just before submission cannot make
the later operation atomic with that snapshot.

The child obtains `handle.state()` and `handle.fetch_committed()` in
separate calls, then publishes the combined [state file][child]. Selection
reads the role, term and committed record from one file read and reads the
readiness field from [another][ready]. The file publication and samples do
not constitute a lease or one atomic cluster observation. The field named
`ready_to_write` by the test is actually [`State::ready`][state]: it says
storage and election summaries agree and storage is usable. It can be true
for a follower and does not independently promise leadership.

The [public addition][handle] first submits `Command::Add`, then polls
`Command::Addition`. The owner [handles Add][owner-add] by calling
[`probe_addition`][probe], whose first precondition calls
[`leader(now)`][leader] before creating the feature probe or appending a
voter record. The later Addition polling branch returns a receipt, `Peer`,
or `Deadline`; the outer call can also fail for other transport, admission
or deadline reasons. This source ordering associates the exact
`Node(NotLeader)` error with refusal at the initial leadership check. It
does not establish the state of other concurrent operations or make
`Deadline` and other outcomes safe to retry.

A leader change after selection is therefore a plausible explanation for a
`NotLeader` response in this test. The retained CI evidence does not prove
that schedule occurred, that any leader change was expected under its
workload, or that election and transport behavior are correct. Blindly
retrying the failed proposal would discard that distinction.

## Independent forced control

[Run 42471844337][control] tested an isolated, test-only
[commit `e7465a4`][test-commit], based on the CI source above. The run's
workflow commit is [`7270208`][workflow-commit]. The only source change adds
an owner-level control; it has not been applied to main. Both x86_64
default and all-feature jobs passed that control once with stable Rust 1.99.1
and LLVM 23.1.1. Strict all-target/all-feature Clippy passed. The diagnostic
formatting command suppressed its exit status and is **not** a strict format
pass. No full broker suite ran in this qualification.

The control uses real in-process owners and directly delivers their generated
vote messages; it does not use the failed process cluster or a network proxy.

- In the positive arm, node 0 was observed as term-2 leader with a committed
  current-term barrier. Its initial `Command::Add` admitted a probe, and
  `Command::Addition` reported that it was pending. The control did not
  complete the voter addition.
- In the other arm, a real peer generated a higher-term campaign after the
  leader observation. Node 0 granted it and became a term-3 follower. Its
  `ready` field remained true. A distinct public `add_voter` call returned
  `Node(NotLeader)`. Relative to the state immediately after stepdown, the
  metadata operation count, tail, committed end, election durable-state
  count and voter set did not change, and no owner addition state appeared.

This is one forced schedule; it supports the source-level check boundary but
does not recreate the original CI outcome or prove all membership-change and
cancellation cases. The positive control proves only admission, not catch-up
or commit.

## Source pins

At `0708730`, the Git blobs were `d8fec08072b9128bbcb9623dfdf267212689d63d`
for the runtime, `458cafb8478f79d941d75bfec8490ec731029d7c` for
replication, and `5a9610523130bf3d381b00cf04ebd36bd539fb05` for the
shared process-test fixture. The test-only runtime blob in `e7465a4` was
`94a86af5bb79bb7b395aab4e9113e2da48c523aa`, SHA-256
`8508dde73608dd53a7d796cce5c6fb08d0490e6218a3021715ba6a3bcd5758e3`.

[history]: https://github.com/mingley/partitionline/blob/0708730155d869ab4fa52654a2e95d4013c4e594/partitionline-broker/tests/common/raft_runtime.rs#L1302
[select]: https://github.com/mingley/partitionline/blob/0708730155d869ab4fa52654a2e95d4013c4e594/partitionline-broker/tests/common/raft_runtime.rs#L961
[ready]: https://github.com/mingley/partitionline/blob/0708730155d869ab4fa52654a2e95d4013c4e594/partitionline-broker/tests/common/raft_runtime.rs#L911
[child]: https://github.com/mingley/partitionline/blob/0708730155d869ab4fa52654a2e95d4013c4e594/partitionline-broker/tests/common/raft_runtime.rs#L377
[state]: https://github.com/mingley/partitionline/blob/0708730155d869ab4fa52654a2e95d4013c4e594/partitionline-broker/src/raft/replication.rs#L1603
[handle]: https://github.com/mingley/partitionline/blob/0708730155d869ab4fa52654a2e95d4013c4e594/partitionline-broker/src/raft/runtime.rs#L1170
[owner-add]: https://github.com/mingley/partitionline/blob/0708730155d869ab4fa52654a2e95d4013c4e594/partitionline-broker/src/raft/runtime.rs#L1463
[probe]: https://github.com/mingley/partitionline/blob/0708730155d869ab4fa52654a2e95d4013c4e594/partitionline-broker/src/raft/replication.rs#L4511
[leader]: https://github.com/mingley/partitionline/blob/0708730155d869ab4fa52654a2e95d4013c4e594/partitionline-broker/src/raft/replication.rs#L2747
[control]: https://github.com/mingley/partitionline/actions/runs/42471844337
[test-commit]: https://github.com/mingley/partitionline/commit/e7465a4ee25eb2c4aa7c6e9ef8b76b220cffa470
[workflow-commit]: https://github.com/mingley/partitionline/commit/7270208acd0bc8243a7555dcb7e54eec5c06ff5a
