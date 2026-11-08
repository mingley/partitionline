# Raft runtime liveness investigation, 2026-11-08

The five-node short-election history remains unresolved. In the all-feature
broker suite of [run 42454616584](https://github.com/mingley/partitionline/actions/runs/42454616584),
the integration test
`actual_five_node_tcp_minority_partition_late_ack_image_restart_and_removal`
waited 15 seconds without finding a stable committed leader. Its final sampled
state showed all five nodes at term 163, committed index 5, last indexes
31, 30, 16, 31 and 11, and only followers or a candidate. A matched AppendReply for a request from node 3 to node 4, RPC 14 on
connection 4, had previously been held for 800 ms: the proxy selected it at
1861 ms, observed source EOF at 2362 ms and began forwarding at 2662 ms.
These are proxy observations; they do not show that an owner consumed the
delayed reply.

This run used the unmerged test candidate
`1c762b58d87d7c19c86a4aced888b32c3722fb70`, based on main
`de152e78ca6d85e6cd4ea5915058b2bb6141cf42`. The short-election
history used 80–160 ms elections and a 500 ms peer RPC budget. The run log
retains the sampled state and proxy timing, but no full owner or packet timeline
for the failure. Main's later record-header parser was not part of this source;
there is no basis to attribute the failure to it.

A separate default-suite failure in
[run 42455234485](https://github.com/mingley/partitionline/actions/runs/42455234485),
on grouped-journal candidate `772c2ef916518b698156d7b4bec471c31d56b707`,
returned `Deadline` from the five-peer observer-addition operation. Sampled
nodes were in terms 3–5, including three candidates. The operation's outcome
is ambiguous; it was not retried. This is not proof that the two failures share
a cause.

## Earlier controls and test candidates

The main-source failures in
[run 42451588960](https://github.com/mingley/partitionline/actions/runs/42451588960)
were a checkpoint base of 4 versus last data index 3, and a proposal sent to
a node that reported `NotLeader`. In
[run 42452307732](https://github.com/mingley/partitionline/actions/runs/42452307732),
forced elections reproduced a committed election barrier after data index 3,
giving checkpoint base 4, in three repetitions per profile. A separate forced
demotion returned `NotLeader` with unchanged observed WAL state in three
repetitions per profile. These controls show possible mechanisms, not the
original failures' causes.

| Run | Result |
| --- | --- |
| [42452918584](https://github.com/mingley/partitionline/actions/runs/42452918584) | Four focused/full profile jobs failed the new reconnect observation; static checks and both negative-control jobs passed. |
| [42453497337](https://github.com/mingley/partitionline/actions/runs/42453497337) | Six of seven jobs failed. Follow-up proposals saw `NotLeader` after leadership changed; strict Clippy also found an unused assignment. The all-feature negative job failed before its intended reconnect assertion. |
| [42453919358](https://github.com/mingley/partitionline/actions/runs/42453919358) | Five jobs passed. Default focused testing failed a post-partition proposal with `NotLeader`; the all-feature suite failed a forced checkpoint control with `NotLeader` at an unidentified stage. |
| [42454616584](https://github.com/mingley/partitionline/actions/runs/42454616584) | Six jobs passed, including default all-target, static checks, both 24-test focused sequences and both five-mode negative-control jobs. The all-feature all-target suite failed the five-node integration history described above. |

The last candidate checked every entry through a checkpoint base against the
expected data or an empty barrier. It also checked a matched request/reply,
source EOF while the reply was held, and, in a separate longer-election
control, a same-pair connection observed after EOF. These observations do not
qualify the full broker suite. No ambiguous proposal was retried.

## Fresh controls and queue observations

All attempts used new temporary clusters on x86_64 Linux with stable Rust
1.99.1. [Run 42455113769](https://github.com/mingley/partitionline/actions/runs/42455113769)
passed 6/6 default-scheduling integration-target invocations, 8/8 uninstrumented
one-core five-node unit invocations and 3/3 one-core captured invocations.
The term-163, 15-second stall did not recur.

The separate test-only instrumentation in candidate
`cf93521a6ebee95c6ec056684716403ce60a21bb` was used only for
[run 42455613866](https://github.com/mingley/partitionline/actions/runs/42455613866).
A real bounded-channel control passed and distinguished accepted, full, closed
and missing routes. Five of five uninstrumented and five of five captured
one-core histories passed. Across the five successful captured histories the
hook recorded 87 queue-full outcomes, distributed 14, 0, 0, 2 and 71. The
fifth history reached term 82 and completed in 24.19 seconds. The observations
include the tests' deliberate partitions; they establish that queues filled
in those captured histories, not that queue-full caused the term-163 failure.

The diagnostic hook records queue results, worker dequeue, request writes,
matched reply reads and owner-call outcomes separately. Existing proxy receipts
record reads and forwarding; the owner's trace records owner commands. The
capture performs synchronous I/O and changes scheduling. Owner clocks are
per process and must not be directly compared with the parent proxy clock.
No hook limit or write warning was reported in these runs.

Raw controls and captures are available for 30 days in the run artifacts:
[42455113769](https://github.com/mingley/partitionline/actions/runs/42455113769)
(captured artifact `13607237231`) and
[42455613866](https://github.com/mingley/partitionline/actions/runs/42455613866)
(captured artifact `13608020525`), expiring 2026-12-08 UTC. Existing per-owner traces and the per-cluster proxy
capture each have a 16 MiB limit; the extra queue trace has a 4 MiB
per-process limit. Later successful controls do not replace the missing
failure trace.

No production fix or causal explanation has been established. Keep the
short-election failure open and preserve the distinct observer timeout until a
failure is reproduced with correlated queue, transport and owner evidence.
