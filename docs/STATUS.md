# Status

Updated 8 November 2026.

`partitionline` 0.1.0 is published on crates.io. This repository contains later
changes and uses the latest stable Rust. The next client release must be at
least 0.2.0 because the public API and compiler requirements have changed. The
broker and schema companion are not published.

The project is **not yet qualified for production use**, and it has not
established that it is the fastest Kafka client or server. The client provides
producer, consumer, group, transaction and Admin APIs. The broker is
experimental: it has an RF1 data path and a separate metadata quorum, but no
replicated partition data or native Kafka KRaft.

## Validation

The latest complete snapshot recorded here is commit `de152e78ca6d85e6cd4ea5915058b2bb6141cf42`.
It includes the coordinator, journal and admission repairs, plus a focused
Raft proposal-boundary test. The [CI evidence][ci] links the runs and their
limits.

| Check | Result at `de152e7` |
| --- | --- |
| Client library, Linux default | 2,226 passed and 18 failed; the all-target run stopped at the library. |
| Client library, Linux all features | 2,221 passed and 23 failed; later targets did not run. |
| Windows default library | 2,235 passed and nine failed. |
| macOS arm64 default library | 2,230 passed and 14 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed with stack overflows. |
| Kafka 3.9.1 and 4.1.0 smoke | Both exhausted EOS retries after timeouts; 4.1.0 also reported JoinGroup error 16. |
| Broker default and all-feature targets | 755 and 903 passed, respectively; three descriptor diagnostics were ignored in each suite and passed separately on the same production source. Both doctest commands found zero tests. |
| Broker Clippy, Rustdoc, inventory and fuzz supervision | Passed. |
| Client capability profiles | Both passed selected checks; their archives were not independently inspected. |
| Client format, all-target Clippy, audit, docs, package, auth and integrity smoke | Passed. Other client jobs failed or were skipped. |

A [broker CI run][previous-broker] at `3fc8059` failed two all-feature
Raft tests. A [subsequent run][latest-broker] at `c471f5e` passed the
default suite, then failed an all-feature quorum test when a delete returned
error 56. The log does not establish whether it committed. Later targets and
the broker job's Clippy and Rustdoc steps did not run; inventory and fuzz
supervision passed separately. The later `eaa8ac6` and `de152e7` broker passes do not
explain either failure; both remain open. A [separate diagnostic][quorum-diag]
did not reproduce error 56, and a [proposal-boundary test][quorum-boundary]
covers only one leader-removal schedule. An unmerged [Raft test run][raft-churn]
also observed prolonged election churn in a five-node short-timeout fixture;
its cause remains unknown.

A [debugger trace][gdb] on the earlier `8b5de2e` source caught a stack fault
while opening a consumer bootstrap connection. It identifies a failure point,
not a complete fix or compatibility pass.

Earlier results and candidate runs are retained in the [work plan][next],
[previous CI evidence][previous-ci] and [previous status][previous]. A focused
pass does not replace the full release matrix.

## Open work

- **Producer delivery and ordering.** `flush` can miss delivery timeouts or
  return before a retiring worker hands off retries. Cancellation can also
  consume an error without returning it. [Focused x86 tests][flush] reproduced
  these failures in both feature profiles. Repair the barrier and error
  ownership together. Partition issue order, sequence wrap, transaction
  fencing and unresolved-batch replay need further work and independent
  histories. The [transaction lock][lock] and [replacement][replacement]
  fixes bound several coordinator waits and preserve the slot during
  cancellation. The [error-state repair][txn-state] makes fatal and abortable
  claims visible before their causes are installed and prevents stale claims
  from overwriting newer ones. Complete deadlines, concurrent discovery,
  transaction generations and already-admitted operations remain open.
  [Cancellation controls][cleanup] reproduced lost record
  completions and retained buffer bytes during retry and failure cleanup.
  A [cleanup candidate][cleanup-deferred] remains unapplied because a user
  callback can block the dropping runtime thread past a timer deadline.
  An unapplied [transaction-response candidate][txn]
  passed focused mocks and one [single-node Kafka 3.9.1 test][txn-live];
  failure and concurrency coverage remain open. Use `send` for an individual
  delivery result; with `acks=0`, success only confirms the local write.
- **Consumer and group correctness.** A [socket repair][socket] retires known
  coordinators before reconnecting; live failover and other authentication paths
  need testing. Resolve the classic-join stack overflow,
  group retry and topic-recreation failures, and cancellation across buffered
  records, offsets and aborted-transaction state. A [nontransactional
  control][poll-loss] reproduced a lost record after canceling a poll while an
  interval commit was pending. An automated security review stopped a specific
  ReadCommitted repair validation; that repair remains open. A later
  consumer/group investigation was stopped by an automatic security review
  that did not identify the specific action; that work remains stopped.
- **Security and protocol coverage.** GSSAPI and connection-driven SASL
  reauthentication are incomplete. The [protocol support](gaps.md) page lists
  other feature gaps. An independent v18 candidate audit was also stopped by an
  automated security review; the draft remains unapplied.
- **Broker durability and limits.** A [scoped Produce fix][cancel] removes
  unsynced journal stages when cancellation is observed between partitions.
  Rolling storage can retain a durable prefix; transport cancellation,
  truncation failures and power loss need more coverage. Replicated partition
  data, authenticated network leadership, committed-watermark protection,
  transaction participant and marker propagation, controller queue accounting
  and a process-wide memory bound remain open. The [Fetch admission
  fixture][fetch-admission] checks completed, unconsumed replies. A [queue
  witness][fetch-queue] demonstrates a possible cause of a separate
  transport-shutdown `QueueFull` failure; its exact cause remains unproven.
  The updated fixture passed focused controls and 887 all-feature tests, while
  its full default run stopped on a quorum `Journal(AlreadyOpen)` failure.
  A [modeled cancellation overlap][fetch-overlap] retained two 4,096-byte
  inputs under a 4,160-byte configured admission ceiling; it did not measure
  process RSS. The [admission repair][admission] keeps data permits with
  actor-owned requests and unconsumed replies. Vector capacity, supplied spares,
  caches and process RSS remain outside the logical payload budget. An earlier
  default transport test failure remains unexplained. Earlier Raft failures
  remain open. The [journal repair][journal-repair]
  pins inode identities through ownership and staged syncs; it passed both full
  broker profiles and descriptor controls. It adds file descriptors on Unix.
  [Earlier journal controls][journal] did not establish the cause of the quorum
  failure.
- **Reliability and performance.** No 24-hour fault campaign or seven-day
  continuity run has passed. Matched x86 comparisons against other Kafka
  implementations, with independent record verification, have not been
  completed. A [closed-loop diagnostic][ack] verified individual deliveries,
  but its short runs have mismatched settings and driver costs. A [decode
  experiment][perf] improved large synthetic loopback responses but remains
  unapplied. A [journal comparison][journal-perf] measured added latency on
  one shared host with a writeback filesystem. [Record-header experiments][records-perf] improved some cases, while
  several variants regressed others. A later candidate is still under test
  and remains unapplied. See the
  [benchmark contract](benchmark-contract.md) for performance claim requirements.

The [work plan][next] tracks the current candidates and outstanding tests.
The [support matrix](support.md) describes configured coverage, and the
[release guide](RELEASE.md) lists publication gates. No new release is ready.

[ci]: https://github.com/mingley/partitionline/blob/a4d381f21701656389b95324fd3231c2c22a03a3/docs/evidence/releases/main-ci-de152e-20261108/README.md
[previous-ci]: https://github.com/mingley/partitionline/blob/7ebb3cc81adc695cc4c74409a87a1a1dd4be9730/docs/evidence/releases/main-ci-ba8-20261108/README.md
[next]: https://github.com/mingley/partitionline/blob/a4d381f21701656389b95324fd3231c2c22a03a3/docs/plan/next-work.md
[previous]: https://github.com/mingley/partitionline/blob/5c2a000639d9a8a7dccd56e8c1d6f3a58dca1601/docs/STATUS.md
[flush]: https://github.com/mingley/partitionline/blob/e980829c439aa43edc01312c91838e58a412221a/docs/evidence/client/producer-flush-native-86f2-20261103/README.md
[txn]: https://github.com/mingley/partitionline/blob/49ad56c6bce1366e2357c272f11c5c34160d60da/docs/evidence/client/add-partitions-native-4f0c-20261103/README.md
[cancel]: https://github.com/mingley/partitionline/blob/277746439899c250871aebb7fe5e1ed959826592/docs/evidence/broker/produce-cancel-20261108/README.md
[lock]: https://github.com/mingley/partitionline/blob/447493b1fbee45c04fd339d199fd661746adbf25/docs/evidence/client/txn-lock-deadline-20261108/README.md
[txn-live]: https://github.com/mingley/partitionline/actions/runs/42438994537
[replacement]: https://github.com/mingley/partitionline/blob/892f04504d0ce02dad5e823795624920fd2413f9/docs/evidence/client/txn-replacement-20261108/README.md
[perf]: https://github.com/mingley/partitionline/blob/447493b1fbee45c04fd339d199fd661746adbf25/docs/evidence/client/txn-decode-20261108/README.md
[gdb]: https://github.com/mingley/partitionline/actions/runs/42440732574
[clippy]: https://github.com/mingley/partitionline/actions/runs/42441786927
[fetch-admission]: https://github.com/mingley/partitionline/blob/0a81a55234b70c60d335ee15d29784f9ae265e8d/docs/evidence/broker/fetch-admission-20261108/README.md
[poll-loss]: https://github.com/mingley/partitionline/actions/runs/42443409232
[cleanup]: https://github.com/mingley/partitionline/blob/2b5e830fd514a9ad3dea27d937d85977a5bea041/docs/evidence/client/addpart-retryfix-264-20261108/REPORT.md
[ack]: https://github.com/mingley/partitionline/blob/2b5e830fd514a9ad3dea27d937d85977a5bea041/docs/evidence/client/ack-closed-loop-20261108/README.md
[fetch-queue]: https://github.com/mingley/partitionline/blob/4d830d8990f7d50bd853ad92ad0e85a9850d8f94/docs/evidence/broker/fetch-canceled-queue-20261108/README.md
[fetch-overlap]: https://github.com/mingley/partitionline/blob/d358d94ff0773e23a9429e85f79c9aa0e48d6a8c/docs/evidence/broker/fetch-actor-overlap-20261108/README.md
[txn-state]: https://github.com/mingley/partitionline/blob/cf02f56c4e4c49b11032f7abc1eb46b14ed9ded2/docs/evidence/client/txn-error-state-374-20261108/REPORT.md
[socket]: https://github.com/mingley/partitionline/blob/c86a2eee826459672e3a25b8ea6ea744dccb0119/docs/evidence/client/group-coordinator-retirement-20261108/README.md
[cleanup-deferred]: https://github.com/mingley/partitionline/blob/7ebb3cc81adc695cc4c74409a87a1a1dd4be9730/docs/evidence/client/producer-failure-cleanup-20261108/DEFERRED.md
[journal]: https://github.com/mingley/partitionline/blob/cf02f56c4e4c49b11032f7abc1eb46b14ed9ded2/docs/qualification/journal-ownership-2026-11-08.md
[journal-repair]: https://github.com/mingley/partitionline/blob/b2d9bcaec1e12c2d79535382ec2c402038c0c6b9/docs/evidence/broker/journal-ownership-repair-20261108/README.md
[admission]: https://github.com/mingley/partitionline/blob/b2d9bcaec1e12c2d79535382ec2c402038c0c6b9/docs/evidence/broker/fetch-owned-admission-20261108/README.md
[journal-perf]: https://github.com/mingley/partitionline/blob/6f50202cd72c789b4396ee10c69a1089fc10bc41/docs/evidence/broker/journal-release-cost-20261108/README.md
[records-perf]: https://github.com/mingley/partitionline/blob/a4d381f21701656389b95324fd3231c2c22a03a3/docs/evidence/broker/records-header-validation-20261108/README.md
[latest-broker]: https://github.com/mingley/partitionline/blob/ae2c6f6098ba41844fb7f74d3b02fb48655d3cb1/docs/evidence/broker/main-ci-c471-20261108/README.md
[previous-broker]: https://github.com/mingley/partitionline/blob/3b29e8122e834ffbfacbd8c779b7c47dab264ebe/docs/evidence/broker/main-ci-3fc-20261108/README.md
[quorum-diag]: https://github.com/mingley/partitionline/blob/a4d381f21701656389b95324fd3231c2c22a03a3/docs/evidence/broker/quorum-delete-diagnostic-20261108/README.md
[quorum-boundary]: https://github.com/mingley/partitionline/blob/de152e78ca6d85e6cd4ea5915058b2bb6141cf42/docs/evidence/broker/quorum-proposal-boundary-20261108/README.md
[raft-churn]: https://github.com/mingley/partitionline/actions/runs/42454616584
