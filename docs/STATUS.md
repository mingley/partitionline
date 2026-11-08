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

The latest complete snapshot recorded here is commit `3f45fe6255e72abc6b0c4488bc13117779c50787`.
It does not cover later changes. The [CI evidence][ci] links the runs and
their limits.

| Check | Result at `3f45fe6` |
| --- | --- |
| Client library, Linux default | 2,180 passed and 16 failed; the all-target run stopped at the library. |
| Client library, Linux all features | 2,170 passed and 26 failed; the all-target run stopped at the library. |
| Windows default library | 2,170 passed and 26 failed. |
| macOS arm64 default library | 2,181 passed and 15 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each overflowed after the classic-join marker. The failing frame is unknown. |
| Kafka 3.9.1 and 4.1.0 smoke | Both failed after repeated EOS timeouts. |
| Broker default and all features | 734 and 882 tests passed, respectively. Clippy, docs, inventory and short fuzz supervision passed; doctests found no tests. |
| Client capability profiles | Both jobs passed; their archives were not independently inspected for this status. |
| Resource-soak controls | All 11 passed; no sustained campaign is credited. |
| Client format, Clippy, audit, documentation and package jobs | Passed. Other client and live-broker jobs failed or were skipped. |

At commit `f5ba3420e`, the [broker workflow][broker-main] passed 737 default
and 885 all-feature tests, Clippy, docs, inventory and short fuzz checks. In
the [main CI][main-partial], the Linux default client library reported 2,165
passed and 31 failed, and Kafka 4.3.1 again overflowed after the classic-join
marker. Other jobs had not all finished when these results were recorded.

A closer [classic-join witness][recent-ci] overflowed in both the baseline and
boxed-library arms; it does not qualify the complete compatibility test.

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
  histories. Two [transaction lock waits][lock] now use the request budget,
  but replacement, recovery and error-state races remain open. An unapplied
  [transaction-response candidate][txn] passed focused mocks and one
  [single-node Kafka 3.9.1 test][txn-live]; failure and concurrency coverage
  remain open. Use `send` for an individual delivery result; with `acks=0`,
  a successful result only confirms the local write.
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
  completed. See the [benchmark contract](benchmark-contract.md) for the
  settings and evidence required for a performance claim.

The [work plan][next] tracks the current candidates and outstanding tests.
The [support matrix](support.md) describes configured coverage, and the
[release guide](RELEASE.md) lists publication gates. No new release is ready.

[ci]: https://github.com/mingley/partitionline/blob/ffa7b9bba3733aa554c75f853dac46a66500a4dd/docs/evidence/releases/main-ci-3f45-20261106/README.md
[previous-ci]: https://github.com/mingley/partitionline/blob/f6eecf19896650a6b18789df7cc8848d3a670cb4/docs/evidence/releases/main-ci-74923475-20261105/README.md
[next]: https://github.com/mingley/partitionline/blob/d32e10aaaa66a90d470ceb825b35226244f2e823/docs/plan/next-work.md
[previous]: https://github.com/mingley/partitionline/blob/5c2a000639d9a8a7dccd56e8c1d6f3a58dca1601/docs/STATUS.md
[flush]: https://github.com/mingley/partitionline/blob/e980829c439aa43edc01312c91838e58a412221a/docs/evidence/client/producer-flush-native-86f2-20261103/README.md
[txn]: https://github.com/mingley/partitionline/blob/49ad56c6bce1366e2357c272f11c5c34160d60da/docs/evidence/client/add-partitions-native-4f0c-20261103/README.md
[recent-ci]: https://github.com/mingley/partitionline/blob/767cc33acd4487141ba40ac80bb1b963bd72efad/docs/evidence/releases/main-ci-19fef2c-20261103/README.md
[cancel]: https://github.com/mingley/partitionline/blob/277746439899c250871aebb7fe5e1ed959826592/docs/evidence/broker/produce-cancel-20261108/README.md
[lock]: https://github.com/mingley/partitionline/blob/d32e10aaaa66a90d470ceb825b35226244f2e823/docs/evidence/client/txn-lock-deadline-20261108/README.md
[txn-live]: https://github.com/mingley/partitionline/actions/runs/42438994537
[broker-main]: https://github.com/mingley/partitionline/actions/runs/42439153552
[main-partial]: https://github.com/mingley/partitionline/actions/runs/42439153574
