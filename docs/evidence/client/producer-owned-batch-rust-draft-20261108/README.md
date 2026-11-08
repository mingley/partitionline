# Producer ownership Rust draft

The public producer still has the retry-split and worker-cancellation
failures described in [the public regression evidence](../producer-ownership-regression-controls-20261108/README.md).
No end-to-end repair is proposed. The Rust code below is an isolated,
unwired ownership draft; it is not used by `Producer`, and its tests do not
exercise Kafka or the public producer. It must not be interpreted as
production qualification.

The unchanged production `src/producer.rs` is Git blob
`dcd012b3f86a8b9d4254f88d7f48946ce8f9e922`. It owns actual `Pending`
records in worker-local vectors across yielding drain, grouping, sequence
stamping, retry and completion paths. Its requests can contain multiple
partitions. A journal added only at retry or next to those values would not
preserve their ownership through cancellation.

## Draft and review

The draft roots synthetic byte payloads and records whole per-partition
batch membership, an identity, a sequence reservation and a claim generation.
The draft treats zero record, logical-byte and partition limits as unlimited;
its record and partition limits are experimental and are not existing public
configuration fields. In the public client, `buffer_memory(0)` disables
the client-side byte cap, while per-connection channel bounds still apply.
The draft accounts for logical payload bytes in its root map, not total
memory. It has no actual encoder, socket, retry actor, callbacks,
receipts, transaction fencing or public flush implementation.

An independent review of
[the Arc revision](https://github.com/mingley/partitionline/commit/cd24e2d8e13d366982a6723612df8eecfdd132eb)
found that claims and attempt tokens retain the root, and that
`Arc::ptr_eq` distinguishes attempts from separate roots. In the isolated
controls, a token from a different producer identity was rejected and a claim
and then an attempt retained the root. The successful-retirement destructor
probe also reentered the root after its mutex was unlocked. These controls do
not prove prompt cleanup, callback ordering, request quiescence or broker
settlement.

The review and subsequent controls found remaining blockers:

- The last claim or token can synchronously destroy the entire root on the
  dropping thread. Strong tokens stored inside that same root can create a
  cycle. A separately retained supervisor and a cleanup protocol are needed.
- The draft clears its batch fence and releases record and byte accounting
  before removed records finish destruction. A flush-style barrier can report
  drained during that destruction. Callback/receipt completion and payload
  or encoding leases are not represented.
- Record keys, batch keys and barriers are unscoped local counters. The
  attempt-token pointer check does not protect those other operations.
- A per-batch offer is not an atomic offer of a multi-partition socket
  request. A successful offer can also be paused before returning until after
  a timeout has fenced it. Neither case establishes that no I/O starts
  after close.
- A definitive response after the earliest record deadline is treated as
  unknown, and a response after timeout has changed the state to unknown is
  rejected. User deadline publication and protocol settlement need separate
  state; a real solution must still account for earlier ambiguous writes.
- An expired queued member can block selection and has no terminal
  publication or reclamation path. In particular, an expired member outside
  the byte limit blocks an otherwise valid fitting prefix. Topic incarnation,
  producer identity replacement, sequence authority and transaction fencing
  are also unresolved.

A viable integration must root accepted actual records, receipts, callbacks and
credits before worker handoff; use owner-scoped, generation-checked batch and
encoding leases throughout every yielding path; journal the entire
multi-partition request before I/O; and separate broker settlement, individual
timeouts, completion publication and reclamation. Flush and close then need
cutoffs, admission revocation and progress over that shared state. The current
draft does not implement this sequence and provides no basis for changing
public close/error behavior.

## Qualification and retained failures

All runs below used hosted x86-64 Linux, stable Rust 1.99.1 and LLVM 23.1.1.
They are prototype tests, not benchmarks.

- [Run 42498084876](https://github.com/mingley/partitionline/actions/runs/42498084876),
  commit `0af1646a5f0d58a53ee2587c4812bf3aee0fc3a0`, used source blob
  `dd67ff0e7de91854f1104bff1aeceaf317c4d4eb` and test blob
  `dc26e8ce9354ed6a0e12d8b339de0334bca63e68`. Both feature profiles
  failed before tests with 11 denied unused-result errors. Targeted strict
  Clippy reported 18 errors including those compilation errors, disallowed
  `std::sync::Mutex`, identical branches, ignored must-use results and an
  unfulfilled lint expectation. The selected-file formatting check failed.
- [Run 42498462381](https://github.com/mingley/partitionline/actions/runs/42498462381),
  commit `4f1f8f260eea7db8644e0f27addc7e03d7a99a5b`, compiled the corrected
  fixture. Each profile had 12 normal controls pass and four intentionally
  ignored controls. Running those four separately reproduced the unscoped
  attempt, lost root, late acknowledgement and expired-suffix
  counterexamples. Targeted all-feature Clippy passed; selected-file
  formatting failed.
- [Run 42498968440](https://github.com/mingley/partitionline/actions/runs/42498968440),
  commit `cd24e2d8e13d366982a6723612df8eecfdd132eb`, tested the Arc
  revision. Each profile had 15 normal controls pass and two ignored.
  Running the ignored controls separately reproduced the late
  acknowledgement and expired-suffix failures. Targeted all-feature Clippy
  passed; selected-file formatting failed.
- [Run 42499295083](https://github.com/mingley/partitionline/actions/runs/42499295083),
  commit `0046a996363bc7cc07ab8229f700a4717161ed0c`, added four more
  isolated counterexamples. Each profile had 15 normal controls pass and six
  ignored. Targeted all-feature Clippy passed; selected-file formatting
  failed. The normal controls include observations of current draft behavior;
  their success is not a safety claim.

The last run pinned `src/producer/ownership_draft.rs` to Git blob
`66d5fb713252fa852c2f663ec04d6465a9fbeefc`, SHA-256
`a72819be724931ab32873fb022d5613c290787a49cc21cd371be659146a00509`,
and `tests/producer_owned_batch_draft.rs` to blob
`8defcb37a54900ec066ff265370bd4f4681f4079`, SHA-256
`e6d5ac0957f0aac42b1fb032007666d9b6555bfb4c585b367766ba055531c55f`.
These files exist only on the isolated qualification branch. The workflow
blob is `bc7d32e77630fc38e412fdac5b9b3b8c060dbffa`. The default and
all-feature normal test jobs are `128788328350` and `128788328566`;
the corresponding expected-failure jobs are `128788328537` and
`128788328506`. The targeted Clippy job is `128788328479` and the
formatting job is `128788328487`. The workflow runs the six ignored tests
separately and requires them to fail with the specified markers; success of
those jobs means the failures occurred.

Both profiles produced the same six isolated outcomes:

| Control | Observed draft result |
| --- | --- |
| Definitive acknowledgement at the member deadline | `Err(Expired)` |
| Expired record beyond the byte-limited prefix | `Err(Expired)` for the fitting prefix |
| Barrier from an empty different owner | `Ok(true)` despite an outstanding record |
| Valid response after timeout marked the attempt unknown | `Err(Stale)` |
| Final claim Arc drop with an instrumented blocking destructor | Drop remained blocked during the 100 ms observation |
| Reclaim with two instrumented destructors, one held | A full-limit new payload was admitted and the old barrier reported drained before release |

The destructor holds are test instrumentation. The 100 ms observation does
not quantify production latency, and the simulated acknowledgement does not
establish a real Kafka response. The test fixture explicitly expects
`clippy::expect_used` and the otherwise-disallowed standard mutex; the
Clippy job covers only this test target, not all targets. No full client
suite, live broker qualification, throughput measurement or public-regression
success was run against a repair. The public red histories remain open.
