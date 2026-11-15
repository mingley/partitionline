# Stale higher-term Raft vote replies, 2026-11-08

A vote reply can match an in-flight private TCP RPC after its campaign has
already been replaced. In the controlled history below, the genuine peer's
higher term reached the owner but was rejected with the expired correlation.
The same higher term on a current, valid correlation made the source persist
the term and become a follower.

This records the behavior and the boundaries around it. It does not decide
whether an expired but wire-matched reply should advance authority, identify
the cause of a production or CI election failure, or propose a production
behavior change.

## Source and trust boundary

The control used main
[`262d2544a4352d52538f19ef9ab0c8c5700774d3`](https://github.com/mingley/partitionline/commit/262d2544a4352d52538f19ef9ab0c8c5700774d3).
The relevant source blobs at that base were:

| File | Git blob |
| --- | --- |
| `partitionline-broker/src/raft/runtime.rs` | `d8fec08072b9128bbcb9623dfdf267212689d63d` |
| `partitionline-broker/src/raft/replication.rs` | `458cafb8478f79d941d75bfec8490ec731029d7c` |
| `partitionline-broker/src/raft/election.rs` | `874f901e513efb7a31b023d790ecf57bafb96acf` |
| `partitionline-broker/src/raft/peer_codec.rs` | `c32c2b794368c2f757cf428023d8680f8efde76b` |

The private Raft transport is explicitly for a trusted network. Its `Hello`
and `HelloAck` compare the group and claimed configured directory keys; they
do **not** cryptographically authenticate the peer. The controls below use
separate genuine local owners and a controlled loopback endpoint. They prove
handling of configured-route and wire identities under that fixture, not
authenticated peer identity.

The outbound worker verifies the connection-local RPC ID and exact echoed vote
request before passing a vote response to the owner. The owner then requires
the exact request to remain in the current campaign's correlation vector
before forwarding the response to the election controller. For a current
correlation, the controller checks the expected voter and candidate identity
before handling a higher term.

## Genuine peer and replay controls

[Test-only commit `4354316b4df3182564b5c600929dc5ab3bce0454`](https://github.com/mingley/partitionline/commit/4354316b4df3182564b5c600929dc5ab3bce0454)
adds bounded owner-observation telemetry and three focused tests in runtime.rs.
Its test-source blob is `2b4723c2f371c8b175582e1175d1280f959bd4d7`;
the production election and replication source blobs are unchanged in the
positive controls. The test-only branch was not merged.

[Run 42465905209](https://github.com/mingley/partitionline/actions/runs/42465905209)
passed each focused test once under both the default and all-feature broker
profiles on a GitHub-hosted x86_64 runner, using stable Rust. The endpoint read
real frames from the outbound worker, asked a separate durable local peer owner
to process each vote, and wrote that genuine response back over the worker's
TCP connection. It did not exercise the peer's full `serve` implementation.

In the first fixture:

1. The source sent a term-2 request on a real outbound RPC. The peer advanced
   through real candidate campaigns to term 7, and the source began a term-3 campaign
   while the term-2 RPC was still in flight.
2. The peer's genuine, denied response to the term-2 request contained its
   actual term 7. The transport matched the RPC ID and echoed request. Bounded
   test-only owner telemetry recorded the exact response and
   `InvalidPeer`: term 3 remained the source's candidate term and its
   durable election-state count did not change.
3. The worker then sent the term-3 request on a new connection. The same peer's
   genuine term-7 response matched the still-current correlation. The owner
   reported `SteppedDown`, synchronized one new election state, and became
   a follower at term 7 without voting for a leader.
4. A direct replay of that now-consumed current response was rejected with
   `InvalidPeer` and did not change the sampled election authority or durable
   counters.

The log line was identical in both profiles:

```text
higher-term matched_old=2 current=3 peer=7 old_owner=InvalidPeer current_owner=SteppedDown replay=InvalidPeer
```

The synchronization count is the owner's reported journal count; this fixture
did not reopen the source to verify recovery. The assertions cover election
authority and durable counters, not every volatile field: for example,
`Node::clock` is called before the correlation check.

A separate fixture generated a real high-term reply for an earlier request,
closed the first connection without delivering that reply, and replayed the
same old response on a new connection carrying the current request. Both
connection-local RPC IDs were 1, but the echoed request differed. The worker
rejected the replay before it reached the owner; the source remained a
candidate at its current term. The repeated ID shows why the request echo,
rather than the connection-local number alone, matters in this schedule.

## Malformed-response controls

Each case used a fresh source and a genuine peer at term 7. The test changed
one field of the peer's genuine higher-term response before writing it:

| Alteration | Observed boundary | Election authority |
| --- | --- | --- |
| Connection-local RPC ID | Rejected before owner | Unchanged |
| Echoed request sequence | Rejected before owner | Unchanged |
| Echoed peer directory | Rejected before owner | Unchanged |
| Response voter ID | Reached owner and rejected | Unchanged |
| Response candidate ID | Reached owner and rejected | Unchanged |

These are deliberately malformed responses, not authenticated traffic. Both
profiles passed all five cases. The authority assertions again cover persistent
election state, role and synchronized election-state count, not all volatile
state.

The workflow also required four deliberate mutants to fail at the intended
assertion:

- Bypassing the owner's exact-correlation lookup allowed the old matched
  response to reach the controller, producing `SteppedDown` where the
  baseline expected `InvalidPeer`.
- Removing the TCP request-echo check allowed the genuine old reply replayed
  on the new connection to reach the owner.
- Removing the response-voter check allowed the wrong-voter high-term reply
  to produce `SteppedDown`.
- Removing the response-candidate check allowed the wrong-candidate high-term
  reply to produce `SteppedDown`.

Each mutation was applied only to the isolated test checkout and restored
before the next case. The workflow required its specific assertion text;
a compile or setup failure did not count as a successful negative control.

## Protocol comparison and open decision

The usual Raft term rule treats a validated higher term as a reason to become
a follower, independently of counting a vote. What constitutes a response
still eligible for that rule depends on the implementation's request,
membership and trust boundary.

At Apache Kafka commit
[`ef6a6c1edb449b41e7804ca67793f638674c87ea`](https://github.com/apache/kafka/commit/ef6a6c1edb449b41e7804ca67793f638674c87ea),
`KafkaRaftClient`
[resets its request manager on candidate transition](https://github.com/apache/kafka/blob/ef6a6c1edb449b41e7804ca67793f638674c87ea/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L723),
[ignores responses no longer expected by source and correlation ID](https://github.com/apache/kafka/blob/ef6a6c1edb449b41e7804ca67793f638674c87ea/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L2858),
and for an expected response
[handles a higher epoch before vote tallying](https://github.com/apache/kafka/blob/ef6a6c1edb449b41e7804ca67793f638674c87ea/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L2657).
Its request manager's
[reset](https://github.com/apache/kafka/blob/ef6a6c1edb449b41e7804ca67793f638674c87ea/raft/src/main/java/org/apache/kafka/raft/RequestManager.java#L275)
clears tracked connections. These are source comparisons, not a claim that
Kafka and this private protocol have identical wire, membership or security
semantics.

At etcd/raft commit
[`c51f010c851edd0dada5af41bafbf68e54617767`](https://github.com/etcd-io/raft/commit/c51f010c851edd0dada5af41bafbf68e54617767),
[`Step` handles higher message terms before role-specific processing](https://github.com/etcd-io/raft/blob/c51f010c851edd0dada5af41bafbf68e54617767/raft.go#L1096),
with explicit pre-vote exceptions. Its message and transport assumptions
also differ. Neither reference alone decides this repository's policy.

Accepting higher terms from an expired response would need a precise proof of
where an original in-flight request and the actual route identity are bound,
which current voter/directory and configuration epoch are eligible, how
removed or replaced peers and replays are fenced, and what is trusted when the
network does not authenticate the peer. Treating arbitrary unmatched response
bytes as authority could cause an unjustified durable term change or loss of
leadership. The bounded fixture shows the current delay, but not that a
broader acceptance rule improves availability or preserves all required
invariants. No production behavior change is proposed here.

The main [run 42465423955](https://github.com/mingley/partitionline/actions/runs/42465423955)
on the same base failed a three-node partition test when a majority proposal
on node 0 returned `Node(NotLeader)`. Its later samples showed node 0 as a
term-7 follower, node 1 as a term-7 leader and node 2 as a term-5 leader.
Those samples were not atomic and do not identify a cause. The controls above
do not attribute that failure to either stale higher-term replies or the
pending-vote mailbox.
