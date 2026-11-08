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
`0708730155d869ab4fa52654a2e95d4013c4e594`. The current branch includes
later changes; see the linked runs for their exact source and scope.

| Check | Result at `0708730` |
| --- | --- |
| Client library, Linux default | 2,234 passed and 20 failed; later targets did not run. |
| Client library, Linux all features | 2,232 passed and 22 failed; later targets did not run. |
| Client library, Windows default | 2,237 passed and 17 failed. |
| Client library, macOS arm64 default | 2,237 passed and 17 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed with stack overflows. |
| Kafka 3.9.1 and 4.1.0 smoke | Both reported JoinGroup error 16 and timeouts after retries. |
| Resource soak | Passed. |
| Broker default targets | 702 passed, one failed and four ignored; later targets did not run. |
| Broker all-feature targets, Clippy, Rustdoc and doctests | Not run after the default failure. |
| Broker inventory and fuzz supervision | Passed in separate jobs. |
| Client capability profiles | Both passed selected checks; their archives were not independently inspected. |
| Client format, all-target Clippy, audit, docs, package, auth and integrity smoke | Passed. Other client jobs failed or were skipped. |

The [broker failure][ci-070] was a `NotLeader` response to a five-node
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

## Open work

- **Producer delivery and ordering.** A [seeded mock rollover history][sequence-rollover]
  returned success for two distinct records at the same offset while the mock
  stored only the first. Producer sequence allocation saturates at the
  boundary instead of wrapping. A small arithmetic control corrected that
  history but does not solve identity, rollback or ownership races. A gated
  [public cancellation history][sequence-public] also left accepted records
  without callbacks and 65,544 bytes in buffer accounting after a worker
  stopped; a later worker sent a sequence gap that the mock rejected. These
  are controlled mock histories, not real-Kafka results. An [in-flight
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
  remain open. A [Produce cancellation fix][cancel] removes unsynced stages
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
- **Security and protocol coverage.** [Admin pagination][admin-pages] now uses
  one deadline across DescribeTopicPartitions pages and rejects an immediate
  cursor repeat. Focused controls and a Kafka 4.1.2 smoke passed in both
  profiles; the earlier broad all-feature Admin suite failed two timing checks,
  while the default suite passed. A [test revision][admin-poll] checks
  cooperative yields and progress; both selected Admin suites passed, but the
  earlier timings remain unexplained. Longer cycles can continue until timeout,
  and accumulated results are not bounded by this change. [Admin response
  reservations][admin-reserve] cap initial vector capacity requests in seven
  response families, but results
  can grow and process memory is not bounded by that change. A separate
  [DescribeTopicPartitions reservation candidate][dtp-reserve] remains
  unapplied after large valid-decode regressions. GSSAPI and connection-driven
  SASL reauthentication remain incomplete. See the
  [protocol gaps](gaps.md) for other missing functionality.
- **Reliability and performance.** No 24-hour fault campaign or seven-day
  continuity run has passed. Matched x86 comparisons against other Kafka
  implementations with independent record verification remain incomplete;
  [two attempts][ack-rejected] failed their comparison gates. An applied
  [record-header validation change][records-perf] improved a short
  header-heavy codec workload on a shared x86 runner. A separate
  [one-byte varint experiment][onebyte] and three
  [batch-header candidates][batch-header] had mixed results and remain
  unapplied. A [send_all candidate][sendall] greatly shortened a large mock
  poll but changed batching and introduced a blocking-cancellation boundary;
  it also remains unapplied. The frame-read change reduced work per poll but slowed a scoped
  256 KiB loopback test by about 2% over TCP and 1.6% over TLS/TCP. In a
  [follow-up][frame-followup] the TCP result changed direction on a different
  runner. These measurements do not rank Kafka implementations.

An automatic security review stopped specific segmented-log recovery and
consumer/group investigations, stopped an independent v18 audit, and rejected
an earlier ReadCommitted rollback validation. Those specific tasks remain
stopped. The [work plan][next] records the broader outstanding work and
retained evidence. The [benchmark contract](benchmark-contract.md) and
[release guide](RELEASE.md) describe the remaining qualification gates.

[ci]: https://github.com/mingley/partitionline/blob/ca14593d5611937a11379fea1fd1345db6cca49a/docs/evidence/releases/main-ci-070-20261108/README.md
[ci-262]: https://github.com/mingley/partitionline/blob/cb55f7c603e2344775de8a395c6a584737f5bb95/docs/evidence/releases/main-ci-262-20261108/README.md
[ci-02c]: https://github.com/mingley/partitionline/blob/0ce81683fe7107386e30f388cd2a4af31735d522/docs/evidence/releases/main-ci-02c-20261108/README.md
[next]: https://github.com/mingley/partitionline/blob/4a9dd9599d1dd1aacb76cf75baa49ba8b4e1f3a7/docs/plan/next-work.md
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
