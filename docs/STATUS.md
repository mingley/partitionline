# Status

Updated 9 November 2026.

Partitionline is **not yet qualified for production use**. It has not
established that it is the fastest Kafka client or server.

The client provides producer, consumer, group, transaction and Admin APIs.
Version 0.1.0 is published; the repository contains later changes. The next
client release must be at least 0.2.0 because the public API and compiler
requirements have changed. The broker and Schema Registry companion are
unpublished. No new release is ready.

The broker is experimental. It has an RF1 data path and a separate metadata
quorum, but does not replicate partition data or implement native Kafka KRaft.
See the [support matrix](support.md) and [protocol gaps](gaps.md) for feature
coverage.

Build the current source with the latest stable Rust. The TLS dependency
graph includes Ring C and assembly, and broker OIDC also uses Ring directly.
The [provider assessment][tls] did not qualify a Rust-only alternative for
the current TLS and OIDC behavior.

## Validation

The latest complete recorded snapshot is main commit
`133d2413217cee4130bca2267e4e36b7225ff86f`; the [CI record][ci] links
the individual runs and results. The current branch contains later changes.

| Check at `133d241` | Result |
| --- | --- |
| Client library, Linux default | 2,299 passed, 18 failed; later targets did not run. |
| Client library, Linux all features | 2,302 passed, 15 failed; later targets did not run. |
| Client library, Windows and macOS | 2,296 passed and 21 failed on each platform. |
| Kafka compatibility | 4.1.2, 4.2.1 and 4.3.1 profiles failed; 3.9.1 and 4.1.0 smoke jobs failed. |
| Resource-soak job | A child exited with code 1; the cause is unknown. |
| Broker targets | 806 default and 954 all-feature tests passed, with four ignored in each profile. |
| Broker strict checks, inventory and fuzz supervision | Passed. |
| Client capability profiles | Both selected profiles passed; their archives were not independently inspected. |
| Client format, Clippy, audit, docs, package, auth and integrity smoke | Passed. Other jobs failed or were skipped. |
| Instruction-count gate | No baseline for this Rust version; no regression comparison. |

An [additional client run][partial] on `dfe179d` passed 18 jobs, failed
three Kafka compatibility jobs, canceled six and skipped one. Its
resource-soak driver built and 11 short tests passed; that does not explain
the earlier failure or qualify a sustained campaign. Its instruction gate
again had no baseline. The [broker run][broker-run] passed 806 default and
954 all-feature tests, with four ignored per profile, plus strict checks,
inventory and fuzz supervision. Both selected [capability profiles][caps-run]
passed; their archives were not independently inspected.

A focused [Schema Registry retry fix][schema-retry] now honors HTTP 429
`Retry-After` delay-seconds across the supported response framings. Its mock
controls and schema feature profiles passed; no live registry was tested.

Earlier broker suites sometimes failed in integration or Raft tests. Later
passes do not establish their causes. The [previous detailed status][history]
and [work plan][next] retain those results and earlier evidence.

## Open work

- **Producer delivery.** Controlled mock histories expose sequence rollover,
  retry ordering and ownership problems. Accepted records can lose callbacks
  and leave buffer accounting nonzero after a worker stops. The
  [public controls][owner-controls] and [unwired batch draft][owner-draft]
  do not establish a fix or a real-Kafka outcome. Repair ownership, sequence
  allocation, flush and shutdown together, then qualify delivery and
  transaction behavior against Kafka.
- **Client compatibility.** Current client library and Kafka jobs fail.
  Consumer and group behavior, cancellation, authentication and protocol
  coverage remain incomplete. The [protocol gaps](gaps.md) describe the
  individual APIs.
- **Broker durability and limits.** Partition replication, native KRaft,
  authenticated network leadership, transaction marker propagation and a
  process-wide memory bound remain open. Kafka wire throttling is not
  implemented; the [quota design][quota] describes the remaining accounting
  and transport work.
- **Reliability.** No 24-hour fault campaign or seven-day continuity run has
  passed. Exact-commit CI, package checks and release rehearsal are still
  required by the [release guide](RELEASE.md).
- **Performance.** Matched x86 comparisons against other Kafka
  implementations with independent record verification remain incomplete.
  Scoped results, including the [Admin index comparison][admin-index], are
  not a client or server ranking. A [vectored Fetch experiment][fetch-exp]
  showed lower median time in several in-process workloads. Its Windows
  file-backed test failed before Fetch and strict checks remain incomplete;
  it is not applied. A
  [producer comparison harness repair][sampler-exp] also remains isolated
  pending integrated testing. The [benchmark contract](benchmark-contract.md)
  sets out the measurement requirements.

An automatic security review stopped specific segmented-log recovery and
consumer/group investigations, stopped an independent v18 audit, and rejected
an earlier ReadCommitted rollback validation. Those specific tasks remain
stopped.

The [work plan][next] and its linked evidence track the outstanding work.
The [previous detailed status][history] preserves earlier qualification
results and experiments.

[ci]: https://github.com/mingley/partitionline/blob/d0c001b65353f865604e6cf16c94bd0d4482aa08/docs/evidence/releases/main-ci-133d-20261108/README.md
[partial]: https://github.com/mingley/partitionline/actions/runs/42662585679
[broker-run]: https://github.com/mingley/partitionline/actions/runs/42662585577
[caps-run]: https://github.com/mingley/partitionline/actions/runs/42662585652
[history]: https://github.com/mingley/partitionline/blob/dfe179d7066e2c1afecb88df5ba76d2920308c46/docs/STATUS.md
[next]: https://github.com/mingley/partitionline/blob/b0ae69f703bc1cec5ae3d8ce860035f08a37cb93/docs/plan/next-work.md
[tls]: https://github.com/mingley/partitionline/blob/e18186f0cb9c25393117fe41148ae5860f52a8a0/docs/evidence/security/tls-provider-assessment-20261108/README.md
[owner-controls]: https://github.com/mingley/partitionline/blob/e18186f0cb9c25393117fe41148ae5860f52a8a0/docs/evidence/client/producer-ownership-regression-controls-20261108/README.md
[owner-draft]: https://github.com/mingley/partitionline/blob/ed86c9c937dd3f5a7edb73858a6474b04073a827/docs/evidence/client/producer-owned-batch-rust-draft-20261108/README.md
[quota]: https://github.com/mingley/partitionline/blob/ed86c9c937dd3f5a7edb73858a6474b04073a827/docs/evidence/broker/quota-produce-design-20261108/README.md
[admin-index]: https://github.com/mingley/partitionline/blob/89a8e2e5ae2ab28942be66c8bd7e0390032b4450/docs/evidence/client/admin-dtp-slot-index-20261108/README.md
[schema-retry]: https://github.com/mingley/partitionline/blob/ef20cb151dc7d310ab6f16d72b82ebc6c1071574/docs/evidence/schema/retry-after-20261109/README.md
[fetch-exp]: https://github.com/mingley/partitionline/blob/b0ae69f703bc1cec5ae3d8ce860035f08a37cb93/docs/evidence/broker/fetch-indexed-vectored-20261109/README.md
[sampler-exp]: https://github.com/mingley/partitionline/blob/b0ae69f703bc1cec5ae3d8ce860035f08a37cb93/docs/evidence/perf/kafka-ack-sampling-20261109/README.md
