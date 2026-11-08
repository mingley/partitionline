# Status

Updated 8 November 2026.

The project is **not yet qualified for production use**. It has not
established that it is the fastest Kafka client or server.

The client provides producer, consumer, group, transaction and Admin APIs.
The broker is experimental: it has an RF1 data path and a separate metadata
quorum, but does not yet replicate partition data or implement native Kafka
KRaft. The [support matrix](support.md) and [protocol gaps](gaps.md) describe
the available features and limits.

Client version 0.1.0 is published on crates.io. This repository contains later
changes and uses the latest stable Rust. The next client release must be at
least 0.2.0 because the public API and compiler requirements have changed.
The broker and schema companion are not published. No new release is ready.
The TLS dependency graph includes Ring C and assembly, so the complete
dependency graph is not Rust-only.

## Validation

The latest complete recorded [CI snapshot][ci] is main commit
`a3748caecae60a893de96c0700e1e5ab04854443`. The current branch includes
later changes; see the linked runs for their exact source and scope.

| Check | Result at `a3748ca` |
| --- | --- |
| Client library, Linux default | 2,299 passed and 18 failed; later targets did not run. |
| Client library, Linux all features | 2,295 passed and 22 failed; later targets did not run. |
| Client library, Windows default | 2,302 passed and 15 failed. |
| Client library, macOS arm64 default | 2,307 passed and 10 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 3.9.1 and 4.1.0 smoke | Both jobs failed. |
| Resource soak | Passed. |
| Broker default targets | 806 passed and four ignored. |
| Broker all-feature targets | 954 passed and four ignored. |
| Broker strict checks, inventory and fuzz supervision | Passed. |
| Client capability profiles | Both passed selected checks; their archives were not independently inspected. |
| Client format, all-target Clippy, audit, docs, package, auth and integrity smoke | Passed. Other client jobs failed or were skipped. |
| Instruction-count gate | Ran without a baseline for this Rust version; no regression comparison. |

An [earlier complete snapshot][ci-342] also failed client checks and passed
both broker profiles.

An earlier [partial main run][ci-a0] failed the Linux default library and
three Kafka compatibility jobs; five jobs were canceled. Its instruction and
allocation gates stopped before measurement because the codec benchmark
lockfile was stale. The [lockfile follow-up][admin-locks] updates six
standalone consumers; allocation tests passed, while the instruction counter
had no baseline for this Rust version. The separate [broker interoperability
peer fix][interop-peer] resolves two type errors and passes focused checks;
its gate did not test a live broker. The broker default suite passed,
but its all-feature
library stopped at 534 passed, one failed and four ignored. Both selected
capability profiles passed; later broker checks did not run.

A [following client run](https://github.com/mingley/partitionline/actions/runs/42492934484)
failed the package adopter check on unpinned roadmap links. The links are now
pinned and a subsequent packed-crate adopter check passed. Its remaining jobs
were still in progress when recorded.

An earlier [partial main run][ci-f5] passed both broker and capability
profiles. Linux and Windows client libraries and three Kafka compatibility
jobs failed; the macOS and both Kafka smoke jobs were canceled without
results. An [earlier complete snapshot][ci-97] also failed client checks and
passed both broker profiles.

The [previous complete snapshot][ci-74] also failed client checks and passed
both broker profiles. An [earlier snapshot][ci-464] stopped its broker
default library after two Raft observer failures. Later passes do not explain
those failures.

An earlier [partial main CI snapshot][ci-536] passed 781 broker default tests,
with four ignored, but its all-feature library stopped after one Raft failure
(534 passed, four ignored). Client libraries and four completed Kafka jobs
failed; the Kafka 3.9.1 smoke was later canceled without a result. Both
selected capability profiles passed. The earlier broker pass does not explain the later
failure.

An [earlier partial broker run][ci-912] passed its default profile but stopped
the all-feature profile after three Raft runtime failures. Its later strict
checks did not run. Inventory, fuzz supervision and selected capability
profiles passed; the failures remain unexplained.

An [earlier partial CI run][ci-c481] passed both broker profiles, inventory,
fuzz supervision and selected capability profiles. The client failed eight
jobs and canceled one Kafka smoke. That broker pass does not explain the
earlier disconnect or Raft failures.

Another [earlier partial broker run][ci-7fb] stopped at 490 default library tests
passed, one failed and four ignored after a five-node Raft proposal returned
`NotLeader`. The cause and the proposal's append and commit outcome are
unknown; it was not retried. Inventory and fuzz supervision passed separately.
The same commit's client run had eight failed jobs and one canceled job; the
canceled Kafka smoke has no result.

Another [earlier partial broker run][ci-6902] stopped at 490 default library tests
passed, one failed and four ignored. A disconnect test observed that its
handler guard had not dropped after 32 yields; the cause and later behavior
are unknown. A [gated EOF control][disconnect-eof] showed one possible delayed
observation and a separate loopback control stopped its handler; neither
establishes the CI cause. A [test revision][disconnect-events] waits for
explicit start and drop events and passed both full broker profiles; it does
not explain the earlier failure. Later behavioral and strict checks in that
failed job did not run; inventory and fuzz supervision passed separately.

The [earlier broker proposal failure][ci-e487-raft] returned `NotLeader` after a
previously selected leader was used. Later node samples do not establish the
cause or whether the proposal was appended or committed. It was not retried. The later broker pass does not explain that failure.

The [previous broker failure][ci-070] was a `NotLeader` response to a five-node
observer-addition request. Later non-atomic samples do not establish the
cause. An [independent control][observer] shows that a node can remain ready
after losing leadership, but it did not recreate the CI failure. The preceding
broker run passed both profiles; that pass does not explain this failure.

An [earlier snapshot][ci-262] had an OS permission error accessing a
`/proc` file-descriptor directory, so that resource-soak workload was
incomplete. Its broker default suite also stopped on a three-node Raft
`NotLeader` response. The node samples do not establish the cause or whether
the mailbox change contributed; later passes do not explain the failure.
An [earlier snapshot][ci-02c] retains two more Raft failures. The
[work plan][next] links other earlier failures and candidate runs.

The producer [redial test][redial-gate] now checks concurrent discovery
handshakes using an event gate. The former 450 ms assertion failed on both
baseline and candidate sources; the replacement does not establish a total
latency bound.

## Open work

- **Producer delivery and ordering.** A [seeded mock rollover history][sequence-rollover]
  returned success for two distinct records at the same offset while the mock
  stored only the first. Producer sequence allocation saturates at the
  boundary instead of wrapping. A small arithmetic control corrected that
  history but does not solve identity, rollback or ownership races. An
  [isolated ledger model][sequence-model] keeps an uncertainty fence after
  ambiguous writes but can block progress indefinitely; it is not applied.
  A gated [public cancellation history][sequence-public] also left accepted records
  without callbacks and 65,544 bytes in buffer accounting after a worker
  stopped; a later worker sent a sequence gap that the mock rejected. These
  are controlled mock histories, not real-Kafka results. Another instrumented
  [retry/flush history][retry-flush-split] sent an incomplete retry after the
  mock stored the original batch; both observed receipts and flush returned
  errors. It held an existing yield and does not establish a Kafka outcome.
  [Private batch controls][batch-controls] also show that packing can split an
  injected complete retry and expiration acts on individual records. An
  [owned-batch model][owned-model] checks proposed admission, batch, close and
  uncertainty rules, but does not run the Rust producer. Its unknown-write
  fence can retain capacity indefinitely, and callback and close semantics
  need decisions before implementation. A
  [timestamp history][timestamp-history] observed mismatched times in a mock.
  The applied [timestamp fix][timestamp-fix] retains implicit times across
  retry and acknowledgement; a delayed-ack Kafka 4.1.0 smoke passed. Paired
  x86 encoder tests showed a 1.8–4.7% cost for single-record implicit batches.
  Broad producer suites failed for both candidate and matched baseline; batch
  ownership and cancellation remain open. An [in-flight
  close control][callback] also showed a blocking interceptor delaying a
  150 ms close to 751 ms on a current-thread runtime, with the target appended
  by the mock and its response withheld. [Flush controls][flush-public]
  show that a retiring worker can hand off retries after flush returns, and a
  [separate history][flush-sent] shows a sent error can be discarded when an
  unpolled waiter is dropped. Repair batch and error ownership, retry order,
  shutdown, transaction fencing and sequence recovery together. The
  [ownership design][owner-design] and [cleanup review][cleanup-service] are
  not implementations.
- **Consumer and group correctness.** Resolve the classic-join stack overflow,
  group retry and topic-recreation failures, and cancellation across buffered
  records, offsets and transaction state. A [coordinator socket repair][socket]
  has focused controls; live failover and other authentication paths need
  testing. The [transaction error-state repair][txn-state] addresses separate
  fatal and abortable state races, while transaction generations and
  already-admitted operations remain open.
- **Broker durability and limits.** Replicated partition data, authenticated
  network leadership, committed-watermark protection, transaction marker
  propagation, controller queue accounting and a process-wide memory bound
  remain open. The in-memory [quota clock fix][quota-clock] clamps stale
  timestamps to the latest observed time; caller-supplied future times remain
  trusted. A [quota error mapping fix][quota-map] stops labeling every local
  refusal as Kafka error 89. This changes behavior for Git consumers. Focused
  controls passed, but both full broker candidate suites stopped on Raft or
  quorum failures; their causes remain unknown. Kafka wire throttling and
  server-wide fairness are still open.
  A [Produce cancellation fix][cancel] removes unsynced stages
  at a scoped boundary; rolling storage can retain a durable prefix. The
  [data admission repair][admission] keeps permits with actor-owned work and
  unconsumed replies, but its logical payload budget does not bound process
  RSS or every allocation. [Journal ownership][journal-repair] and
  [grouped reservation][journal-reserve] fixes have passing controls; a
  sync descriptor clone can still fail after a write. A
  [process-exit control][segment-crash] found that a freshly created
  segmented log can fail to reopen before its initial manifest is published.
  The [Raft vote mailbox][vote-repair] retains the newest pending vote but
  cannot interrupt an older in-flight RPC. The handling of higher terms in
  [expired vote replies][stale-vote] remains a design question; private Raft
  transport does not cryptographically authenticate peers. An unmerged
  [dispatch candidate][dispatch] offered votes before waiting on cancellations,
  but its final all-feature broker run failed a separate `NotLeader` test.
  The cause and any connection to the candidate remain unknown. The applied
  [frame-read change][frame-read] limits ready frame reads before requesting a
  yield. It does not bound one read poll or synchronous complete-frame copying.
- **Security and protocol coverage.** [Admin pagination][admin-pages] uses
  one deadline across DescribeTopicPartitions pages and rejects an immediate
  cursor repeat. The [identity fix][admin-null] retains topic errors, rejects
  conflicting known IDs, and returns `Unsupported` for null wire names in
  the aggregate; raw pages remain available. Focused controls and a Kafka
  4.1.2 smoke passed in both profiles. Both full Admin suites failed timing
  assertions; a matched run with the original fixture passed the candidate
  and failed the baseline. The cause remains unknown. An earlier
  [test revision][admin-poll] checks cooperative progress without establishing
  a wall-time bound. The applied [name validation][admin-name] rejects
  unrequested topic rows and cursors in the aggregate while preserving the
  raw API. In a paired mock comparison, 256 reordered error rows were about
  19% slower and reordered successful rows about 12% slower. Both full Admin
  suites failed timing assertions; matched baseline suites also failed.
  The applied [slot index][admin-slot] avoids a second owned name per distinct
  topic. On one x86 mock, 500,000 distinct rows fell from a 579 ms median to
  381 ms across three pairs; a separate 256-row lexical mock improved 12–15%
  in paired medians. Focused controls, a full Admin run and a Kafka smoke
  passed. A later combined gate failed timing assertions in both profiles;
  its all-feature baseline also failed one. The cause remains unknown.
  These measurements do not establish Kafka throughput or a ranking.
  Longer cursor cycles can duplicate results, and cleanup and memory are not
  globally bounded. The applied
  [row checkpoint][admin-row-checkpoint] yields after every 8,192 completed
  conversions, including across page boundaries. A paired mock showed shorter
  8,192-row polls, while 500,000-row maxima remained about 62–66 ms in both
  arms. A separate [poll-phase diagnostic][admin-phases] found long Pending
  and Ready polls. An instrumented [index-growth diagnostic][admin-growth]
  associated a resize with long Pending polls, but its reserved control changes
  allocation and table layout. Polls include other work; neither this nor the
  earlier [baseline][admin-row] establishes a Kafka
  throughput or wall-time bound. [Admin response reservations][admin-reserve] cap
  initial capacity requests in seven response families, but results can grow.
  A separate [DescribeTopicPartitions reservation candidate][dtp-reserve]
  remains unapplied after valid-decode regressions. GSSAPI and
  connection-driven SASL reauthentication remain incomplete; see the
  [protocol gaps](gaps.md).
- **Reliability and performance.** No 24-hour fault campaign or seven-day
  continuity run has passed. Matched x86 comparisons against other Kafka
  implementations with independent record verification remain incomplete;
  [two attempts][ack-rejected] failed their comparison gates. An applied
  [record-header validation change][records-perf] improved a short
  header-heavy codec workload on a shared x86 runner. An unapplied
  [dense Produce candidate][dense-produce] improved in-process tests for large
  unordered requests to a missing topic. Append samples with two valid
  partitions were too noisy to establish a gain; a successful multi-partition
  workload is being measured separately. A separate
  [one-byte varint experiment][onebyte] and three
  [batch-header candidates][batch-header] had mixed results and remain
  unapplied. A [send_all candidate][sendall] greatly shortened a large mock
  poll but changed batching and introduced a blocking-cancellation boundary;
  it remains unapplied. A [no-added-yield candidate][sendall-no-yield] also
  shortened scoped mock polls, but its [combined qualification][sendall-combined]
  failed 17 of 45 mock-producer tests in each profile; their cause is unknown.
  It remains unapplied. A separate [buffered-frame candidate][frame-classify]
  regressed a scoped TCP/TLS loopback workload and remains unapplied. New
  [test-only controls][frame-regression] check buffered cancellation and
  invalid lengths on the existing path. The frame-read change reduced work
  per poll but slowed a scoped
  256 KiB loopback test by about 2% over TCP and 1.6% over TLS/TCP. In a
  [follow-up][frame-followup] the TCP result changed direction on a different
  runner. These measurements do not rank Kafka implementations.

An automatic security review stopped specific segmented-log recovery and
consumer/group investigations, stopped an independent v18 audit, and rejected
an earlier ReadCommitted rollback validation. Those specific tasks remain
stopped. The [work plan][next] records the broader outstanding work and
retained evidence. The [benchmark contract](benchmark-contract.md) and
[release guide](RELEASE.md) describe the remaining qualification gates.

[ci]: https://github.com/mingley/partitionline/blob/7f15017fd312945612886c3b6a8c26ffc8098458/docs/evidence/releases/main-ci-a374-20261108/README.md
[ci-342]: https://github.com/mingley/partitionline/blob/7f15017fd312945612886c3b6a8c26ffc8098458/docs/evidence/releases/main-ci-342e-20261108/README.md
[dense-produce]: https://github.com/mingley/partitionline/blob/7f15017fd312945612886c3b6a8c26ffc8098458/docs/evidence/broker/produce-dense-partition-20261108/README.md
[ci-a0]: https://github.com/mingley/partitionline/blob/61af02b51269edb0ec12edf3c49118285f767c3b/docs/evidence/releases/main-ci-a0ad-partial-20261108/README.md
[ci-97]: https://github.com/mingley/partitionline/blob/6667a8df9205f5ae9efe5d648ec21c0e997afb32/docs/evidence/releases/main-ci-97a-20261108/README.md
[admin-slot]: https://github.com/mingley/partitionline/blob/89a8e2e5ae2ab28942be66c8bd7e0390032b4450/docs/evidence/client/admin-dtp-slot-index-20261108/README.md
[admin-locks]: https://github.com/mingley/partitionline/blob/89a8e2e5ae2ab28942be66c8bd7e0390032b4450/docs/evidence/client/admin-slot-locks-20261108/README.md
[interop-peer]: https://github.com/mingley/partitionline/blob/89a8e2e5ae2ab28942be66c8bd7e0390032b4450/docs/evidence/client/broker-interop-peer-types-20261108/README.md
[ci-262]: https://github.com/mingley/partitionline/blob/cb55f7c603e2344775de8a395c6a584737f5bb95/docs/evidence/releases/main-ci-262-20261108/README.md
[ci-02c]: https://github.com/mingley/partitionline/blob/0ce81683fe7107386e30f388cd2a4af31735d522/docs/evidence/releases/main-ci-02c-20261108/README.md
[next]: https://github.com/mingley/partitionline/blob/7f15017fd312945612886c3b6a8c26ffc8098458/docs/plan/next-work.md
[flush-public]: https://github.com/mingley/partitionline/blob/f93e91499e2ba6bdc88c0a25a0d83b8737d00d3c/docs/evidence/client/producer-flush-public-20261108/README.md
[flush-sent]: https://github.com/mingley/partitionline/blob/efa7f23314e98f3aaa4559c3cf38302bdcdb9fb4/docs/evidence/client/producer-flush-sent-20261108/README.md
[owner-design]: https://github.com/mingley/partitionline/blob/cb55f7c603e2344775de8a395c6a584737f5bb95/docs/evidence/client/producer-ownership-design-20261108/README.md
[cleanup-service]: https://github.com/mingley/partitionline/blob/efa7f23314e98f3aaa4559c3cf38302bdcdb9fb4/docs/evidence/client/producer-cleanup-service-20261108/README.md
[sequence-public]: https://github.com/mingley/partitionline/blob/efa7f23314e98f3aaa4559c3cf38302bdcdb9fb4/docs/evidence/client/producer-sequence-public-20261108/README.md
[sequence-rollover]: https://github.com/mingley/partitionline/blob/671bbc4e6f54393480468594700184fc53861e87/docs/evidence/client/producer-sequence-rollover-20261108/README.md
[txn-state]: https://github.com/mingley/partitionline/blob/cf02f56c4e4c49b11032f7abc1eb46b14ed9ded2/docs/evidence/client/txn-error-state-374-20261108/REPORT.md
[socket]: https://github.com/mingley/partitionline/blob/c86a2eee826459672e3a25b8ea6ea744dccb0119/docs/evidence/client/group-coordinator-retirement-20261108/README.md
[admin-reserve]: https://github.com/mingley/partitionline/blob/cd20f43f018f36b3aaa8311839fe11fc74dac3af/docs/evidence/client/admin-response-reservation-20261108/README.md
[admission]: https://github.com/mingley/partitionline/blob/b2d9bcaec1e12c2d79535382ec2c402038c0c6b9/docs/evidence/broker/fetch-owned-admission-20261108/README.md
[cancel]: https://github.com/mingley/partitionline/blob/277746439899c250871aebb7fe5e1ed959826592/docs/evidence/broker/produce-cancel-20261108/README.md
[journal-repair]: https://github.com/mingley/partitionline/blob/b2d9bcaec1e12c2d79535382ec2c402038c0c6b9/docs/evidence/broker/journal-ownership-repair-20261108/README.md
[journal-reserve]: https://github.com/mingley/partitionline/blob/732b833aaab797527f2662d2f410c072c7a5af36/docs/evidence/broker/grouped-journal-reservations-20261108/README.md
[segment-crash]: https://github.com/mingley/partitionline/blob/752b1bb0cfd97448b41244533a9fea36a2e3c843/docs/evidence/broker/segment-initial-crash-20261108/README.md
[vote-repair]: https://github.com/mingley/partitionline/blob/873b5d01ce0edef5ee3fd49f112c346aa4cc8fef/docs/evidence/broker/raft-vote-mailbox-20261108/README.md
[stale-vote]: https://github.com/mingley/partitionline/blob/cb55f7c603e2344775de8a395c6a584737f5bb95/docs/evidence/broker/raft-stale-vote-term-20261108/README.md
[onebyte]: https://github.com/mingley/partitionline/blob/efa7f23314e98f3aaa4559c3cf38302bdcdb9fb4/docs/evidence/client/one-byte-varint-20261108/README.md
[records-perf]: https://github.com/mingley/partitionline/blob/732b833aaab797527f2662d2f410c072c7a5af36/docs/evidence/broker/records-header-validation-20261108/README.md
[ack-rejected]: https://github.com/mingley/partitionline/blob/932a7835939034c730b92a12f867cf7c945320ba/docs/evidence/client/ack-matched-rejected-20261108/README.md
[callback]: https://github.com/mingley/partitionline/blob/671bbc4e6f54393480468594700184fc53861e87/docs/evidence/client/producer-inflight-close-callback-20261108/README.md
[frame-read]: https://github.com/mingley/partitionline/blob/a9b011e488b38091fec7575c88fd6b5a32718a0c/docs/evidence/broker/frame-read-fairness-20261108/README.md
[dispatch]: https://github.com/mingley/partitionline/blob/57fc47669e2fe8b6db6d69d9d3e3898c10f6672a/docs/evidence/broker/raft-vote-dispatch-20261108/README.md
[batch-header]: https://github.com/mingley/partitionline/blob/a9b011e488b38091fec7575c88fd6b5a32718a0c/docs/evidence/client/batch-header-write-20261108/README.md
[dtp-reserve]: https://github.com/mingley/partitionline/blob/a9b011e488b38091fec7575c88fd6b5a32718a0c/docs/evidence/client/admin-dtp-reservation-20261108/README.md
[frame-followup]: https://github.com/mingley/partitionline/blob/a9b011e488b38091fec7575c88fd6b5a32718a0c/docs/evidence/broker/frame-read-fairness-20261108/network-rtt-followup.md
[admin-pages]: https://github.com/mingley/partitionline/blob/db0fac19e50efef965bb3e0af454cc4b3d41f9d5/docs/evidence/client/admin-pagination-20261108/README.md
[ci-070]: https://github.com/mingley/partitionline/blob/ca14593d5611937a11379fea1fd1345db6cca49a/docs/evidence/broker/main-ci-070-partial-20261108/README.md
[observer]: https://github.com/mingley/partitionline/blob/ca14593d5611937a11379fea1fd1345db6cca49a/docs/evidence/broker/observer-selection-20261108/README.md
[sendall]: https://github.com/mingley/partitionline/blob/ca14593d5611937a11379fea1fd1345db6cca49a/docs/evidence/perf/sendall-yield-perf-20261108/README.md
[admin-poll]: https://github.com/mingley/partitionline/blob/4a9dd9599d1dd1aacb76cf75baa49ba8b4e1f3a7/docs/evidence/client/admin-cooperative-poll-20261108/README.md

[ci-e487-raft]: https://github.com/mingley/partitionline/blob/c27d634f6fd781bbf50afc7e012808dbb88452a2/docs/evidence/broker/raft-delayed-proposal-20261108/README.md
[redial-gate]: https://github.com/mingley/partitionline/blob/c27d634f6fd781bbf50afc7e012808dbb88452a2/docs/evidence/client/sendall-redial-gate-20261108/README.md

[sendall-no-yield]: https://github.com/mingley/partitionline/blob/ec1bd5191b0410df771838574f8bbff752fb31c0/docs/evidence/perf/sendall-no-yield-20261108/README.md
[sendall-combined]: https://github.com/mingley/partitionline/blob/ec1bd5191b0410df771838574f8bbff752fb31c0/docs/evidence/client/send-all-combined-qualification-20261108/README.md

[sequence-model]: https://github.com/mingley/partitionline/blob/ec1bd5191b0410df771838574f8bbff752fb31c0/docs/evidence/client/producer-sequence-ledger-model-20261108/README.md
[frame-classify]: https://github.com/mingley/partitionline/blob/41ba724701109a935856e9a9343edd6a474360d8/docs/evidence/broker/frame-classification-20261108/README.md

[frame-regression]: https://github.com/mingley/partitionline/blob/41ba724701109a935856e9a9343edd6a474360d8/docs/evidence/broker/frame-read-regression-tests-20261108/README.md

[ci-6902]: https://github.com/mingley/partitionline/blob/ed1f82f06d30d92a77a996223234ca229db6ffe0/docs/evidence/releases/main-ci-6902-20261108/README.md

[ci-7fb]: https://github.com/mingley/partitionline/blob/c3531f20b76ac7a9578b02a548ad37c0aba1bd99/docs/evidence/releases/main-ci-7fb-partial-20261108/README.md
[disconnect-eof]: https://github.com/mingley/partitionline/blob/45430d95ca633701db2e449604e4a7ea4e360e5c/docs/evidence/broker/disconnect-eof-20261108/README.md
[admin-identity]: https://github.com/mingley/partitionline/blob/ad452fea03dcdde7e946322b3c55ca35bbccc224/docs/evidence/client/admin-pagination-identity-20261108/README.md

[disconnect-events]: https://github.com/mingley/partitionline/blob/9b9bdf7d1c8ab9ef4ca79d7dd090941c16d7b85c/docs/evidence/broker/disconnect-event-tests-20261108/README.md

[admin-null]: https://github.com/mingley/partitionline/blob/ad452fea03dcdde7e946322b3c55ca35bbccc224/docs/evidence/client/admin-topic-identity-null-20261108/README.md
[ci-c481]: https://github.com/mingley/partitionline/blob/ad452fea03dcdde7e946322b3c55ca35bbccc224/docs/evidence/releases/main-ci-c481-partial-20261108/README.md
[ci-912]: https://github.com/mingley/partitionline/blob/ed10995700da3252dbefb4696e1afd7dfe5a008d/docs/evidence/releases/main-ci-912-partial-20261108/README.md

[ci-536]: https://github.com/mingley/partitionline/blob/8d1461a039b40c9bc79b6cf501aebf2a79054a85/docs/evidence/releases/main-ci-536-partial-20261108/README.md
[admin-row]: https://github.com/mingley/partitionline/blob/a0deb28cfdb8eb85fd16a1b3264b35fceb2c74e1/docs/evidence/client/admin-dtp-row-poll-20261108/README.md

[retry-flush-split]: https://github.com/mingley/partitionline/blob/8d1461a039b40c9bc79b6cf501aebf2a79054a85/docs/evidence/client/producer-retry-flush-split-20261108/README.md
[admin-name]: https://github.com/mingley/partitionline/blob/b7ab4f229385d239a60f2a44b5ebafdaa5656631/docs/evidence/client/admin-requested-topic-hint-20261108/README.md

[admin-row-checkpoint]: https://github.com/mingley/partitionline/blob/a3abed795162f380336e87b49a5260cf58325c3d/docs/evidence/client/admin-dtp-row-checkpoint-20261108/README.md

[ci-426]: https://github.com/mingley/partitionline/blob/ed10995700da3252dbefb4696e1afd7dfe5a008d/docs/evidence/releases/main-ci-426-20261108/README.md
[batch-controls]: https://github.com/mingley/partitionline/blob/a8403decd42d794b1da89ac985e2f390f32e01a7/docs/evidence/client/producer-batch-ownership-controls-20261108/README.md
[owned-model]: https://github.com/mingley/partitionline/blob/7f15017fd312945612886c3b6a8c26ffc8098458/docs/evidence/client/producer-owned-batch-model-20261108/README.md
[timestamp-history]: https://github.com/mingley/partitionline/blob/a8403decd42d794b1da89ac985e2f390f32e01a7/docs/evidence/client/producer-retry-timestamp-20261108/README.md
[admin-phases]: https://github.com/mingley/partitionline/blob/a8403decd42d794b1da89ac985e2f390f32e01a7/docs/evidence/client/admin-dtp-poll-phases-20261108/README.md

[ci-464]: https://github.com/mingley/partitionline/blob/a8403decd42d794b1da89ac985e2f390f32e01a7/docs/evidence/releases/main-ci-464-20261108/README.md

[ci-74]: https://github.com/mingley/partitionline/blob/a3abed795162f380336e87b49a5260cf58325c3d/docs/evidence/releases/main-ci-74dd-20261108/README.md
[quota-clock]: https://github.com/mingley/partitionline/blob/bd37eb53e247b73160da8bbbb2cf53f9d77eb6c3/docs/evidence/broker/quota-logical-clock-20261108/README.md
[quota-map]: https://github.com/mingley/partitionline/blob/e69b81e83db26264d2197a907b5b4dcca558fe0a/docs/evidence/broker/quota-wire-boundary-20261108/README.md
[admin-growth]: https://github.com/mingley/partitionline/blob/bd37eb53e247b73160da8bbbb2cf53f9d77eb6c3/docs/evidence/client/admin-dtp-index-growth-20261108/README.md

[ci-f5]: https://github.com/mingley/partitionline/blob/6667a8df9205f5ae9efe5d648ec21c0e997afb32/docs/evidence/releases/main-ci-f5ed-partial-20261108/README.md
[timestamp-fix]: https://github.com/mingley/partitionline/blob/b7ab4f229385d239a60f2a44b5ebafdaa5656631/docs/evidence/client/producer-timestamp-candidate-20261108/README.md
