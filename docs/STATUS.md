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

The latest complete snapshot recorded here is commit `1036a393eb85f23032b935bf86862b7e98e36e14`.
It includes the record validation, grouped journal reservation and Admin
response allocation changes. The [CI evidence][ci] links the runs and their
limits.

| Check | Result at `1036a39` |
| --- | --- |
| Client library, Linux default | 2,223 passed and 21 failed; the all-target run stopped at the library. |
| Client library, Linux all features | 2,225 passed and 19 failed; later targets did not run. |
| Windows default library | 2,226 passed and 18 failed. |
| macOS arm64 default library | 2,233 passed and 11 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed with stack overflows. |
| Kafka 3.9.1 and 4.1.0 smoke | Both reported JoinGroup error 16 and exhausted EOS retries after timeouts. |
| Broker default and all-feature targets | 762 and 910 passed, respectively; three descriptor diagnostics were ignored in each suite and passed separately on the same production source. Both doctest commands found zero tests. |
| Broker Clippy, Rustdoc, inventory and fuzz supervision | Passed. |
| Client capability profiles | Both passed selected checks; their archives were not independently inspected. |
| Client format, all-target Clippy, audit, docs, package, auth and integrity smoke | Passed. Other client jobs failed or were skipped. |

A newer [client CI run][ci-440-client] at `4409633` was still running
when this record was written. Linux default and all-feature libraries reported
2,233/11 and 2,232/12 passed/failed; Windows reported 2,225/19 and macOS
arm64 2,230/14. The three current Kafka compatibility jobs failed with stack
overflows; the Kafka 3.9.1 smoke reported JoinGroup error 16. The
resource-soak job failed because the runner denied access to a process's
`/proc` file-descriptor directory; the workload result is incomplete. The
Kafka 4.1.0 smoke was still running. See the run for its final state. The
[broker CI run][ci-440-broker] passed 764 default and 912 all-feature tests,
with three ignored per profile; its strict checks, inventory and fuzz
supervision passed. Both selected [capability profiles][ci-440-capabilities]
passed; their archives were not independently inspected.

An earlier [frame-read test failure][deadline-test] used a 30 ms deadline
and required total elapsed time under 80 ms; it observed the deadline error
after 82.681715 ms. The cause is unknown. The revised test retains deadline
and yield controls without imposing that wall-time threshold; production
transport logic is unchanged. An [observer diagnostic][observer-diag] now
captures bounded checkpoint failure details; its sampled prefix is non-atomic,
and the original small-image failure remains unexplained.

An [earlier partial client run][e02-client] at `e02c1b8` recorded 17 successful jobs,
six failures, four cancellations and one skip. Linux default and Windows
library tests failed, along with three Kafka compatibility jobs and the
Kafka 3.9.1 smoke job. The all-feature Linux, macOS, Kafka 4.1.0 smoke and
package jobs were canceled when main advanced; they have no result for that
commit.

A [more recent broker run][e02-broker] at `e02c1b8` passed 757 default
tests with three ignored. Its all-feature suite stopped at 641 passed, one
failed and three ignored when a before-failover CreateTopics request returned
error 56. The log does not identify the cause or whether the operation
committed. Later broker targets, Clippy and Rustdoc did not run; inventory
and fuzz supervision passed separately. A [test diagnostic][create-diag]
now records the error message and contacted node on future unexpected Create
failures; it does not establish their commit outcome.

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

- **Producer delivery and ordering.** `flush` can return before a retiring
  worker hands off retries, and cancellation can consume a recorded worker
  error without returning it. [Focused x86 tests][flush] cover both. A
  [public mock history][flush-public] used a timing gate to observe flush
  returning while an accepted record receipt was pending. An isolated
  [completion model][flush-model] is not connected to the producer. A private
  [sequence control][sequence-cancel] closed receipts after an artificial
  owner was dropped while the reserved counter stayed advanced; it did not
  execute the public producer or Kafka I/O.
  Individual delivery timeouts remain reported through the record's own
  result or callback under the documented contract. Repair the barrier and
  error ownership together. Partition issue order, sequence wrap, transaction
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
- **Security and protocol coverage.** [Admin response reservations][admin-reserve]
  now limit initial `Vec` capacity requests in seven response families.
  Results can grow beyond that request, and other decoders and process memory
  remain outside its scope. Two short diagnostics measured roughly 6% higher
  median decode time for 20,000 valid CreateTopics results. GSSAPI and
  connection-driven SASL reauthentication are incomplete. The [protocol support](gaps.md) page lists
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
  pins inode identities through ownership and staged syncs; it adds file
  descriptors on Unix. The [grouped reservation repair][journal-reserve]
  reserves index and staging capacity before each write. Combined broker
  suites and separate descriptor controls passed; cloning the sync descriptor
  can still fail after a write. [Earlier journal controls][journal] did not
  establish the cause of the quorum failure.
- **Reliability and performance.** No 24-hour fault campaign or seven-day
  continuity run has passed. Matched x86 comparisons against other Kafka
  implementations, with independent record verification, have not been
  completed. A [closed-loop diagnostic][ack] verified individual deliveries,
  but its short runs have mismatched settings and driver costs. Two newer
  [comparison attempts][ack-rejected] also failed their socket-equality gate;
  one arm was not independently verified. They do not establish a ranking.
  A [decode
  experiment][perf] improved large synthetic loopback responses but remains
  unapplied. A [journal comparison][journal-perf] measured added latency on
  one shared host with a writeback filesystem. [Record-header validation][records-perf] now uses a resumable header-aware
  scan. A short x86 benchmark improved header-heavy cases, while its
  1,024-record header-free median was 0.3% higher. An earlier candidate suite
  and a baseline comparison failed separate Raft tests. See the
  [benchmark contract](benchmark-contract.md) for performance claim requirements.

The [work plan][next] tracks the current candidates and outstanding tests.
The [support matrix](support.md) describes configured coverage, and the
[release guide](RELEASE.md) lists publication gates. No new release is ready.

[ci]: https://github.com/mingley/partitionline/blob/f93e91499e2ba6bdc88c0a25a0d83b8737d00d3c/docs/evidence/releases/main-ci-1036-20261108/README.md
[previous-ci]: https://github.com/mingley/partitionline/blob/7ebb3cc81adc695cc4c74409a87a1a1dd4be9730/docs/evidence/releases/main-ci-ba8-20261108/README.md
[next]: https://github.com/mingley/partitionline/blob/932a7835939034c730b92a12f867cf7c945320ba/docs/plan/next-work.md
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
[records-perf]: https://github.com/mingley/partitionline/blob/732b833aaab797527f2662d2f410c072c7a5af36/docs/evidence/broker/records-header-validation-20261108/README.md
[latest-broker]: https://github.com/mingley/partitionline/blob/ae2c6f6098ba41844fb7f74d3b02fb48655d3cb1/docs/evidence/broker/main-ci-c471-20261108/README.md
[previous-broker]: https://github.com/mingley/partitionline/blob/3b29e8122e834ffbfacbd8c779b7c47dab264ebe/docs/evidence/broker/main-ci-3fc-20261108/README.md
[quorum-diag]: https://github.com/mingley/partitionline/blob/a4d381f21701656389b95324fd3231c2c22a03a3/docs/evidence/broker/quorum-delete-diagnostic-20261108/README.md
[quorum-boundary]: https://github.com/mingley/partitionline/blob/de152e78ca6d85e6cd4ea5915058b2bb6141cf42/docs/evidence/broker/quorum-proposal-boundary-20261108/README.md
[raft-churn]: https://github.com/mingley/partitionline/blob/732b833aaab797527f2662d2f410c072c7a5af36/docs/evidence/broker/raft-liveness-20261108/README.md
[e02-broker]: https://github.com/mingley/partitionline/blob/732b833aaab797527f2662d2f410c072c7a5af36/docs/evidence/broker/main-ci-e02-20261108/README.md
[journal-reserve]: https://github.com/mingley/partitionline/blob/732b833aaab797527f2662d2f410c072c7a5af36/docs/evidence/broker/grouped-journal-reservations-20261108/README.md
[e02-client]: https://github.com/mingley/partitionline/blob/cd20f43f018f36b3aaa8311839fe11fc74dac3af/docs/evidence/releases/main-ci-e02-partial-20261108/README.md
[admin-reserve]: https://github.com/mingley/partitionline/blob/cd20f43f018f36b3aaa8311839fe11fc74dac3af/docs/evidence/client/admin-response-reservation-20261108/README.md
[create-diag]: https://github.com/mingley/partitionline/blob/be20c7f063bfd721e9a0821ad7711cb1c7847d8f/docs/evidence/broker/create-response-diagnostic-20261108/README.md
[deadline-test]: https://github.com/mingley/partitionline/blob/f93e91499e2ba6bdc88c0a25a0d83b8737d00d3c/docs/evidence/broker/frame-read-deadline-test-20261108/README.md
[flush-public]: https://github.com/mingley/partitionline/blob/f93e91499e2ba6bdc88c0a25a0d83b8737d00d3c/docs/evidence/client/producer-flush-public-20261108/README.md
[flush-model]: https://github.com/mingley/partitionline/blob/f93e91499e2ba6bdc88c0a25a0d83b8737d00d3c/docs/evidence/client/producer-completion-horizon-20261108/README.md

[ack-rejected]: https://github.com/mingley/partitionline/blob/932a7835939034c730b92a12f867cf7c945320ba/docs/evidence/client/ack-matched-rejected-20261108/README.md
[observer-diag]: https://github.com/mingley/partitionline/blob/932a7835939034c730b92a12f867cf7c945320ba/docs/evidence/broker/observer-image-diagnostic-20261108/README.md
[sequence-cancel]: https://github.com/mingley/partitionline/blob/932a7835939034c730b92a12f867cf7c945320ba/docs/evidence/client/producer-sequence-cancel-20261108/README.md
[ci-440-client]: https://github.com/mingley/partitionline/actions/runs/42459181316
[ci-440-broker]: https://github.com/mingley/partitionline/actions/runs/42459181380
[ci-440-capabilities]: https://github.com/mingley/partitionline/actions/runs/42459181276
