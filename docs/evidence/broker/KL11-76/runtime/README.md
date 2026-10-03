# KL11-76 source preparation

The implementation sources are prepared under the published claim
`b7440d52eb1e4eb55cab90b777671c660d19cb89`. Stable and Rust 1.85 Rustfmt
checks pass for all five owned Rust paths. Compilation, tests, actual TCP fault
histories and independent safety verification have not run. This evidence does
not qualify the runtime; a pushed implementation checkpoint and compiler lease
are still required. The prior qualified membership core and immutable403 proofs
remain intact.

One supervisor retains every network worker and the exclusive blocking Node
owner. Numeric, immutable, directory-qualified routes require a trusted isolated
network. The private CRC framing preserves group/genesis, exact directory keys,
Node correlations and per-session RPC IDs. It supplies no native Kafka wire
advertisement, authentication, autonomous discovery or performance result.
Node still rejects a leader whose authoritative directory/configuration history
the receiver has not learned, even when that leader has a configured route.

Frames, chunks, queues, records, tasks, sockets, timer/deadline values and a checked
512 MiB Rust-buffer envelope are explicit configuration. Numeric dial addresses
produce zero DNS admissions. The resource formula covers retained queued/started
operations and completed unconsumed results. OS socket buffers, allocator
overhead and process RSS are outside this configured envelope. Capture headers
record actual configuration and the owner's actual channel capacity, including
the intentional one-/sixteen-slot unit-control queues. Owner trace messages are
typed bodies; they do not constitute captured TCP packets.

Stop and exact-correlation cleanup are reliably admitted without an artificial
command expiry. Successful shutdown joins network workers before the storage
owner; cancellation retains the supervisor so a later shutdown can join it.
An already running OS storage operation can delay joining. Public commit/add
waits share one absolute deadline and cannot renew it on each internal command.

The written tests use three/five real helper processes and TCP proxies, minority
process exits, partitions, delayed old replies, image/suffix catch-up, durable
reopen, committed leader removal and prefix comparisons. Owner tests drop real
candidate vote-reply vectors, saturate route/command queues, require exact cleanup,
reject expired leader writes and exercise canceled joined shutdown. These are
written test sources awaiting execution. The helper test is inactive outside its
explicit child environment.

The only membership-core extension removes one complete matching current-campaign
vote request. It changes no clock, term, vote or durable bytes and preserves other
real grants. Its written regression checks forged/duplicate cancellation, a real
late grant, genuine remaining-majority grants and byte-exact content/election
journals. No election, snapshot, membership or controller behavior was modified.

`draft-source-handoff.json` binds current source hashes and the exact staging
paths. `development/source-preparation-01/validation.json` retains both executed
format commands and source/mode identities. That directory also retains the
narrow patches, earlier WORK handoff/format receipt, additive capture-settings
receipt and independent decoder source review. Earlier uncompiled drafts and
syntax stages remain under `/workspace/work/raft-runtime-76/`; their receipt
labels are preserved. Coordinator-owned exports and the independent raw TCP,
WAL/election/image causal checker are outside this worker's write set.
