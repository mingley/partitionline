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

The latest complete snapshot recorded here is commit `b631809f0e03ba6f65f808e51d8e47e3fd9380d1`.
It does not cover later changes. The [CI evidence][ci] links the runs and
their limits.

| Check | Result at `b631809` |
| --- | --- |
| Client library, Linux default | 2,190 passed and 17 failed; the all-target run stopped at the library. |
| Client all features | Stopped at an all-target Clippy lint before tests. A later [test-only correction][clippy] passed strict Clippy and 12 selected tests. |
| Windows default library | 2,193 passed and 14 failed. |
| macOS arm64 default library | 2,183 passed and 24 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each overflowed after the classic-join marker. |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed after repeated EOS timeouts. |
| Broker default library | 446 passed and four Raft runtime tests failed; later targets and all features did not run. |
| Broker inventory and fuzz supervision | Both passed. |
| Client capability profiles | Both jobs passed; their archives were not independently inspected for this status. |
| Client format, audit, docs, package, auth and integrity smoke | Passed. Other client and live-broker jobs failed or were skipped. |

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
  cancellation. Complete deadlines, concurrent discovery and error-state
  races remain open. An unapplied [transaction-response candidate][txn]
  passed focused mocks and one [single-node Kafka 3.9.1 test][txn-live];
  failure and concurrency coverage remain open. Use `send` for an individual
  delivery result; with `acks=0`, success only confirms the local write.
- **Consumer and group correctness.** Resolve the classic-join stack overflow,
  group retry and topic-recreation failures, and cancellation across buffered
  records, offsets and aborted-transaction state. An automated security review
  stopped a specific ReadCommitted repair validation; that repair remains open.
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
  and a process-wide memory bound remain open. Earlier intermittent Raft
  failures remain unexplained.
- **Reliability and performance.** No 24-hour fault campaign or seven-day
  continuity run has passed. Matched x86 comparisons against other Kafka
  implementations, with independent record verification, have not been
  completed. A [decode experiment][perf] improved large synthetic loopback
  responses but remains unapplied. See the [benchmark contract](benchmark-contract.md)
  for the settings and evidence required for a performance claim.

The [work plan][next] tracks the current candidates and outstanding tests.
The [support matrix](support.md) describes configured coverage, and the
[release guide](RELEASE.md) lists publication gates. No new release is ready.

[ci]: https://github.com/mingley/partitionline/blob/892f04504d0ce02dad5e823795624920fd2413f9/docs/evidence/releases/main-ci-b631-20261108/README.md
[previous-ci]: https://github.com/mingley/partitionline/blob/447493b1fbee45c04fd339d199fd661746adbf25/docs/evidence/releases/main-ci-8b5-20261108/README.md
[next]: https://github.com/mingley/partitionline/blob/892f04504d0ce02dad5e823795624920fd2413f9/docs/plan/next-work.md
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
