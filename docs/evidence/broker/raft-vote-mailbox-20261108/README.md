# Raft vote mailbox qualification, 2026-11-08

A full per-peer outbound route previously kept its older queued vote and
canceled the newer campaign's vote. A controlled two-voter schedule showed one
otherwise eligible campaign lost this way. The mailbox keeps the
newest pending vote, returning the displaced exact job for cancellation. The
worker's already in-flight RPC and its deadline are unchanged.

This establishes a bounded behavior and a targeted correction. It does not
establish the cause of the previously observed election stalls or observer
failures, prove eventual election under every schedule, or qualify the broker
for production.

## Source and scope

The source-only candidate commit is
[`2774e2dc77ef9fdaeb3820ef05e211a0c3b0b935`](https://github.com/mingley/partitionline/commit/2774e2dc77ef9fdaeb3820ef05e211a0c3b0b935),
based on main
[`36424a580913e3d77571beed65b91d6751d7b108`](https://github.com/mingley/partitionline/commit/36424a580913e3d77571beed65b91d6751d7b108).
It changes only `partitionline-broker/src/raft/runtime.rs`, from Git blob
`5564ae9829ee60433ae81b8db43ea9618330a161` to
`d8fec08072b9128bbcb9623dfdf267212689d63d`. The commit includes
the private mailbox and tests; it adds no dependency or public API.

A later comparison with main
`059e1d2fcb60510cfdec2f93a0786bc69378c351` found only documentation
and root client-test changes since the tested base. The broker source, broker
manifest and lockfile, toolchain file, inventory verifier and registry remained
unchanged. The qualification below still refers to the 36424a5 combined tree,
not a run of the later tree.

The production supervisor is the only route producer. It serially dispatches
each owner-generated campaign before requesting the next tick; a later
campaign has a higher term and replaces the owner's vote-correlation vector.
The mailbox holds at most one pending job per peer in addition to the existing
in-flight RPC. Under normal dispatch, a replaced or unadmitted exact job is
passed to the owner for cancellation. The slot uses a short synchronous
mutex section, with a field-scoped Clippy exception: it moves bounded state,
does no I/O or user callbacks, and never holds a guard across an await. The
separate await-holding-lock lint remains enabled. The receive path consumes a
Tokio cooperative budget so continuously ready work yields to other tasks.
A mutex holder can still be delayed by scheduling; these controls do not
establish a wall-time bound or total process RSS.

## Controlled campaign and lifecycle checks

The earlier baseline and its setup failures are recorded in
[Raft vote route control](../raft-vote-route-20261108/README.md).
There, an actual outbound worker sent term 2 while its response was held,
term 3 occupied the route, and term 4 was canceled on a full route. Genuine
term-2 and term-3 grants were sent on the wire, a genuine term-4 grant was
rejected by the source, and a later term-5 wire grant elected it. A separate
fresh counterfactual delivered the genuine term-4 grant directly and elected
that term.

On the candidate, the matching fresh fixture held the term-2 RPC, queued term 3,
and then replaced it with term 4. After the term-2 response was released, the
real worker sent term 4 and its genuine peer grant elected the candidate. The
test also submitted the genuine term-3 grant directly and confirmed its stale
correlation was fenced. This is a forced two-voter schedule using durable local
owners and a controlled TCP peer; it is not an autonomous-cluster benchmark.

[Exact-source run 42464545456](https://github.com/mingley/partitionline/actions/runs/42464545456)
used commit
`1914d7ca20a5eb481bf5d01bb716614ec09c2836`, the same candidate
runtime blob and a test workflow on the earlier 02c443c main base. Eight
focused cases passed once in each broker profile. They checked replacement and
genuine grants, closed and missing routes, notification and canceled waits,
10,000 concurrent offers, 1,000 close/offer races, a current-thread
sustained-ready progress witness, forced dispatch abort, owner step-down while
dispatch waited, and a stopped worker's pending cancellation. The Clippy job
checked all targets and all features against the committed blob.

The forced-abort case is a limitation, not immediate cleanup: while the owner
command queue was full, independently aborting dispatch after its first
replacement left an unoffered current correlation registered and a genuine
grant for it was accepted. The displaced older correlation was already fenced.
A later campaign fenced both queued and unoffered old correlations, and a
real higher-term vote stepped the owner down and fenced a still-current
correlation. In the production supervisor, the selected tick branch awaits
dispatch, while ordinary shutdown signals stop and retains the supervisor for
joining; the artificial abort control does not prove arbitrary task abortion
performs immediate cancellation.

In a separate controlled schedule, a genuine higher-term vote changed the
owner from candidate term 3 to follower term 4 while cancellation waited. The
next tick was issued only after dispatch completed, as in the supervisor; its
term-5 jobs replaced all four older pending routes. This supports the observed
term ordering and fencing in that schedule, not a proof for every execution.

[Lifecycle run 42464685981](https://github.com/mingley/partitionline/actions/runs/42464685981)
used test-only hook commit
`7706663ed69e15f2f2f6843180a5b1209babbd88`, based on the source-only
candidate. With a term-2 RPC in flight and the supervisor held immediately
after replacement, normal shutdown canceled the exact term-4 pending vote,
closed the peer socket, joined the worker and owner, and released the listener.
The same run injected a worker panic with a vote still pending; shutdown
reported a panic JoinError after the owner had been awaited, and the measured
task and socket counts were zero. That error path stops the runtime; the test
does not show that the pending correlation was individually canceled before
the owner was destroyed. Both controls passed once per profile. The hooks are
not present in the landed runtime source.

The exact-source workflow used five separately compiled mutants. Retaining the
old pending vote failed at the expected newest-vote assertion; removing
`notify_one` failed the waiting-receiver assertion; removing cooperative
budgeting failed the competing-task progress assertion; and removing either
closed-route or missing-route cancellation made the genuine-grant rejection
test fail. The lifecycle workflow also rejected a mutant that dropped the
pending vote instead of canceling it and a control with the panic fault
disabled. These negative controls required the expected failure messages, so a
compile or setup failure could not satisfy them.

## Combined broker checks

The final checks ran on GitHub-hosted x86_64 Linux runners with stable Rust
1.99.1. The combined test workflow commit was
`2fdc1c1e42b7fde339970025eae52e6a67b2ff52` and pinned the candidate
runtime blob explicitly.

[Run 42464888238](https://github.com/mingley/partitionline/actions/runs/42464888238)
completed:

- Default `cargo test --locked --all-targets`: 773 passed, 0 failed,
  4 ignored across 36 test artifacts.
- All-features `cargo test --locked --all-targets --all-features`:
  921 passed, 0 failed, 4 ignored across 36 test artifacts.
- Strict all-target/all-feature Clippy and strict all-feature Rustdoc passed.
  Both strict doctest commands passed; each reported zero doctests.

One ignored test in each profile is the candidate's manual microbenchmark.
[Run 42464890929](https://github.com/mingley/partitionline/actions/runs/42464890929)
checked the same runtime blob, ran the existing broker inventory unchanged,
and passed all 55 existing consistency and corruption tests. The inventory
does not itself hash runtime.rs; the workflow separately checked its Git blob.

An earlier version of the candidate, runtime blob
`ac3287a639926e7c92507c5d83c8879d9e456c75`, passed its two full test
profiles and Rustdoc but failed strict Clippy in
[run 42462842649](https://github.com/mingley/partitionline/actions/runs/42462842649):
the synchronous mutex violated the configured disallowed-type rule and the
`Result` error variant was large. The final source uses the scoped,
explained mutex exception and returns `Option<Job>` for the exact job needing
cancellation. It does not allocate a box or add a dependency. The earlier
failure is not a result for the final source.

## Performance and unresolved observations

The exact-source release microbenchmark in run 42464545456 alternated 200,000
uncontended local offer/receive operations per round. Across eight rounds the
slot took 11.427–11.612 ms and Tokio mpsc took 11.870–12.068 ms. A previous
shared runner for the same revised design instead favored mpsc: 12.893–13.066
ms for the slot and 12.626–12.885 ms for mpsc. These measurements exclude
networking, storage, contention and real election throughput. They do not
establish a repeatable speedup or any ranking against other Kafka software.

The candidate cannot interrupt an older in-flight RPC. If that RPC lasts longer
than repeated election windows, even the newest queued request may be stale
before it reaches the peer. A stale vote response is correlation-fenced before
its response term is processed; whether higher-term information from such a
response should be retained is a separate unqualified protocol question.

The following failures remain unexplained and are not attributed to this
mailbox behavior:

- [Main run 42462115954](https://github.com/mingley/partitionline/actions/runs/42462115954)
  on 02c443c failed two default `raft_runtime` integration cases: a
  five-peer observer addition and a five-node delayed-old-reply proposal both
  returned `Node(NotLeader)`. The printed role/term samples were taken after
  the failures and are not a complete causal timeline.
- [Isolated run 42457826681](https://github.com/mingley/partitionline/actions/runs/42457826681)
  failed four default Raft cases: three- and five-node delayed-reply proposals
  returned `NotLeader`; a five-peer observer image failed the greater-than-64
  KiB assertion; and a three-peer observer image base comparison was 4 versus
  3. The small-image log did not retain its exact size or prefix identities.
- [Isolated run 42454616584](https://github.com/mingley/partitionline/actions/runs/42454616584)
  timed out after 15 seconds waiting for a stable leader in a five-node
  short-election history. The retained observations do not identify a cause.
- [Main run 42456887194](https://github.com/mingley/partitionline/actions/runs/42456887194)
  reported the frame-body deadline test at 82.681715 ms against its 80 ms
  wall threshold, with the 30 ms deadline flag true. The log does not
  distinguish scheduler delay from a long poll. Later deterministic
  test-only controls did not establish that original cause.

Passing the current gates does not resolve those earlier observations, establish
production readiness, or establish that this broker or client is the fastest.
