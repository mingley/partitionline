# Metadata quorum Delete failure

In [broker CI 42452041729](https://github.com/mingley/partitionline/actions/runs/42452041729),
the default-feature process test passed and the all-feature execution returned
error 56 for one DeleteTopics v1 request. The log did not preserve its token,
underlying error, term/index, or resulting catalog state. Its outcome and cause
remain unknown; the original Delete was not retried. Error 56 can represent
several failures before or after proposal. The different test outcomes do not
identify a feature as the cause.

Three isolated diagnostic attempts followed:

- [Run 42453487104](https://github.com/mingley/partitionline/actions/runs/42453487104)
  failed before compilation because sparse checkout omitted root Cargo
  targets. No fixture or mutation ran. Its
  [setup artifact](https://github.com/mingley/partitionline/actions/runs/42453487104/artifacts/13606028668)
  is retained.
- [Run 42453736306](https://github.com/mingley/partitionline/actions/runs/42453736306)
  stopped before any minority Create or Delete. Its one post-failover
  synthetic Create returned 41; the request was not resubmitted. In the
  [read-only trace inspection](https://github.com/mingley/partitionline/actions/runs/42454044712),
  that exact command returned `Runtime(Node(NotLeader))` while the node was
  Leader in term 3 with `active_term=None`. The observed last position
  (term 2/index 2) and durable-operation count (3) were unchanged. This
  observation applies only to that synthetic request.
- [Run 42454099113](https://github.com/mingley/partitionline/actions/runs/42454099113)
  used a new fixture and topic names. Its single minority Create returned 7.
  The matching command was admitted locally at term 3/index 5 while the
  observed committed end was 4. Its one subsequent Delete returned 0, with
  token `0c16c7806ff4418398e668cb1053f765` and target
  `985f5ea5d1064394b5349731929256a0` traced through the owner append,
  commit wait and Applied receipt at term 8/index 7. Neither request was
  replayed. This run did not reproduce the original 56.

A [read-only correlation](https://github.com/mingley/partitionline/actions/runs/42454504311)
rechecked all 12 copied file hashes and used the retained structural
extraction. In all three post-shutdown copies, the minority Create token
`222e36b4acac4f4dcfe4ddf92e407a1b` was present at term 3/index 5 at or
below the locally recorded committed end, with a matching application receipt.
That extraction does not establish the application's semantic outcome. All
three copies retained the Delete at term 8/index 7, but only node 0 recorded a
committed end of 7 and an application receipt; nodes 1 and 2 recorded a
committed end of 6. These observations describe the later copies, not the
state at either response.

The last run used opt-in diagnostic instrumentation in commit
`d0e6bef611c5a97a7d50a5f3f3d7f04cb9aaafca`, built on the failed CI source,
on hosted x86_64 with stable Rust 1.99.1. Its operator binary SHA-256 was
`e803041c64301e27460f1c6fc4076484b790aedd9dde17ccbc208ccf45fcc496`.
The sampled leader state was a hint, not a lease or a guarantee that leadership
could not change before a request.

Both executed fixtures stopped and joined their processes and log readers
before copying the synthetic identity and journal files. The bounded copies
had no missing files, dropped diagnostic bytes, CRC parse errors or changed
copy hashes. Their post-shutdown snapshots are later than the responses; the
parser extracts journal records but does not validate every Raft authority or
voter invariant. Kafka Metadata observations can refresh and sync a local
application projection and do not establish a linearizable result. The
[second fixture's logs and snapshots](https://github.com/mingley/partitionline/actions/runs/42453736306/artifacts/13607275348)
and [final fixture's logs and snapshots](https://github.com/mingley/partitionline/actions/runs/42454099113/artifacts/13606798205)
are retained as Actions artifacts. Their retention expires on December 8,
2026.
