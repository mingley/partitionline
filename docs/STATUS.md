# Status

Updated 5 November 2026.

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

The latest complete snapshot recorded here is commit `74923475a`. It does not
cover later changes. The [CI evidence][ci] gives the runs and result limits.

| Check | Result at `74923475a` |
| --- | --- |
| Client library, default | 2,179 passed and 17 failed; the all-target run stopped at the library. |
| Client library, all features | 2,173 passed and 23 failed; the all-target run stopped at the library. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | Each overflowed after the classic-join marker. The failing frame is unknown. |
| Windows default library | 2,177 passed and 19 failed. |
| macOS arm64 default library | 2,175 passed and 21 failed. |
| Broker default and all features | 734 and 882 tests passed, respectively. Clippy, docs, inventory and short fuzz supervision passed; doctests found no tests. |
| Client capability profiles | Both workflows passed; their archives were not inspected for this status. |
| Client format, Clippy, audit, documentation and package jobs | Passed. Other client and live-broker jobs failed or were skipped. |

A closer [classic-join witness][recent-ci] overflowed in both the baseline and
boxed-library arms; it does not qualify the complete compatibility test.

Earlier results and candidate runs are retained in the [work plan][next] and
[previous status][previous]. A focused pass does not replace the full release
matrix.

## Open work

- **Producer delivery and ordering.** `flush` can miss delivery timeouts or
  return before a retiring worker hands off retries. Cancellation can also
  consume an error without returning it. [Focused x86 tests][flush] reproduced
  these failures in both feature profiles. Repair the barrier and error
  ownership together. Partition issue order, sequence wrap, transaction
  fencing and unresolved-batch replay need further work and independent
  histories. Source review also found transaction begin and recovery races. A [transaction-response candidate][txn] passed focused mock
  tests; real-broker and concurrency coverage remain open. Use `send` for an
  individual delivery result; with `acks=0`, a
  successful result only confirms the local write.
- **Consumer and group correctness.** Resolve the classic-join stack overflow,
  group retry and topic-recreation failures, and cancellation across buffered
  records, offsets and aborted-transaction state. An automated security review
  stopped a specific ReadCommitted repair validation; that repair remains open.
- **Security and protocol coverage.** GSSAPI and connection-driven SASL
  reauthentication are incomplete. The [protocol support](gaps.md) page lists
  other feature gaps. An independent v18 candidate audit was also stopped by an
  automated security review; the draft remains unapplied.
- **Broker durability and limits.** Add replicated partition data, authenticated
  network leadership, committed-watermark protection, and transaction
  participant and marker propagation. Controller queue memory accounting,
  real failure and transport coverage, and a process-wide memory bound remain
  open. Earlier intermittent Raft failures remain unexplained.
- **Reliability and performance.** No 24-hour fault campaign or seven-day
  continuity run has passed. Matched x86 comparisons against other Kafka
  implementations, with independent record verification, have not been
  completed. See the [benchmark contract](benchmark-contract.md) for the
  settings and evidence required for a performance claim.

The [work plan][next] tracks the current candidates and outstanding tests.
The [support matrix](support.md) describes configured coverage, and the
[release guide](RELEASE.md) lists publication gates. No new release is ready.

[ci]: https://github.com/mingley/partitionline/blob/f6eecf19896650a6b18789df7cc8848d3a670cb4/docs/evidence/releases/main-ci-74923475-20261105/README.md
[next]: https://github.com/mingley/partitionline/blob/5ef23ef6395ae79ea9b93b39057a3ac26989fbe8/docs/plan/next-work.md
[previous]: https://github.com/mingley/partitionline/blob/5c2a000639d9a8a7dccd56e8c1d6f3a58dca1601/docs/STATUS.md
[flush]: https://github.com/mingley/partitionline/blob/e980829c439aa43edc01312c91838e58a412221a/docs/evidence/client/producer-flush-native-86f2-20261103/README.md
[txn]: https://github.com/mingley/partitionline/blob/49ad56c6bce1366e2357c272f11c5c34160d60da/docs/evidence/client/add-partitions-native-4f0c-20261103/README.md
[recent-ci]: https://github.com/mingley/partitionline/blob/767cc33acd4487141ba40ac80bb1b963bd72efad/docs/evidence/releases/main-ci-19fef2c-20261103/README.md
