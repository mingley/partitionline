# Observer leader selection and proposal fencing

The c034 all-features broker job 125729388284 reported a distinct failure in
`actual_five_peer_tcp_observer_multichunk_image_install_reopen_and_join`:
`observer-prefix-proposal on node3` received
`error private peer runtime: Node(NotLeader)`. Its later diagnostic reads showed
node3's published file as term 2 Leader, while nodes1 and2 had term 4 and 7
Candidate snapshots. The files are written independently and the log has no
owner-event or network ordering trace. These observations do not establish a
single causal order or a protocol defect. The same job's two fault-history
`InvalidDigit` failures are separate and have no captured response or phase.

A later job at commit 33744862cd (run 41580681915, job 125734535528)
failed a different operation: the five-node fault history received an actual
`Node(NotLeader)` reply for `fault-current-leader-removal`. Subsequent state
snapshots reported node0 as Follower and node2 as Leader in term 8. This shows
a failed removal reply, not its acceptance/side-effect status; it neither
proves the earlier observer failure's cause nor explains the bare parse errors.
No removal retry is proposed.

In the checked source, the child publishes a state file, fsyncs it and its
parent directory, then reads the test control inbox and eventually calls
`Handle::propose`. The test parent selects one leader from these files and
fsyncs a command file before the owner executes it. Leadership may change at
any of those boundaries. The child writes a proposal reply before its next
state-file publication. Therefore, the observed NotLeader reply and a still
older file saying Leader are consistent with a stale observation. The test uses
80–160 ms election timers, and process scheduling and synchronous fsync may
contribute to election churn, but the available log does not establish that.
`State.ready` is an integrity/reconciliation condition, not a leadership lease.

A NotLeader response at the first `Node::leader(now)` call occurs before the
proposal WAL append, and the included deterministic virtual-time control
exercises that path for a committed genesis configuration and checks the WAL
bytes, last position and committed records. It is a single negative case, not a
proof for every state. `Node::propose` also calls `advance_commit` after the
WAL append, and `advance_commit` can itself return `Error::NotLeader` if its
role or active-term check fails. The actor is serialized, and the
pre-membership observer prefix appears to preserve membership across a data
append, but a general guarantee that this later branch is unreachable is not
stated or established. The same wire string therefore must not be assumed to
prove that all prior mutations are absent. Deadline, cancellation, transport,
closed, I/O and storage errors are potentially ambiguous and must remain
terminal without a separate idempotency contract.

A safe retry would need an explicit, tested typed result that means the owner
rejected the proposal before any data WAL append. A bare formatted error is not
that result. If such a contract is added, a test driver can capture one
absolute deadline before the first selection, use the same deadline for all
leader refreshes, sleeps and submitted commands, bound attempts and owned
in-flight work, and retry only a confirmed pre-append rejection. It must stop
on cancellation or any ambiguous result, never resend while a prior command is
in flight, and preserve the original per-history payload, committed-prefix,
snapshot, catch-up and membership assertions. It should report all retries and
terms, and preserve the original unchanged observer test as a required control
so elections or availability regressions are not hidden by the new behavior.

The proposed patch adds a deterministic control only; it does not retry the
tested proposal, change production code, alter existing test deadlines or claim
the CI failure fixed. Its setup uses the existing bounded catch_up helper,
which can perform up to four in-process replication exchanges per follower. It tests that one stale observed genesis leader can be fenced after
quorum expiry without a data WAL change and that an earlier live proposal was
accepted. New test source is uncompiled and unrun. Subsequent peer review,
Rust typechecking, fault/race tests and native execution of the exact source
remain necessary before adoption or a claim about runtime behavior.
