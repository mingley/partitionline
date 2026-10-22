# Status

Partitionline 0.1.0 is the crates.io release. This repository is the working
source after that release. Build it with the latest stable Rust.

The client is a usable asynchronous Kafka client: produce, fetch, groups,
transactions, and Admin. The broker is an experimental crate in this
repository. It is not a production cluster, and it is not published.

No fastest-client or fastest-server result is established. Suite HOLD stays
in effect until the [benchmark contract](benchmark-contract.md) comparison
requirements are met.

## Where to look

| Question | Read |
| --- | --- |
| What the client can call | [Protocol support](gaps.md) and rustdoc |
| Which brokers and platforms are tested | [Support matrix](support.md) |
| What the broker implements | [Broker README](../partitionline-broker/README.md) |
| Which work items are open | [Task registry](plan/tasks.json) |
| How a benchmark number should be read | [Benchmark guide](benchmark.md) |

The task registry has 343 cards: 258 done, 9 in progress, and 76 pending.
Those counts were checked against `tasks.json` on 22 October 2026. The
file's own `updated` field still says 2026-10-08. The file is the queue.
This page does not restate each card.

## Client

The client speaks the versions listed in the [design notes](design.md#wire-format-notes).
Selected tests cover Apache Kafka 3.9.1, 4.1.0, 4.1.2, 4.2.1, and 4.3.1.
Which combination was run is in the [support matrix](support.md).

Ordinary produce, fetch, groups, and transactions are in the tree.
InitProducerId negotiates v0–v6 and leaves both two-phase flags false
(KL05-35). These items are still open:

| Gap | Standing |
|---|---|
| Opt-in two-phase initialization, and resume when a prepared transaction is unavailable | KL05-36, pending |
| SASL reauthentication driven by producer connections | KL06-11, in progress. Shared round-trip callers are KL06-12, pending. Client loops do not reauthenticate on a timer. |
| Broker SASL connection reauthentication | KL11-71, in progress |
| Recorded producer, consumer, and transaction fault histories | KL03-19 and KL03-20, pending. These are evidence cards. |
| GSSAPI / Kerberos | Not implemented. KL06-08, the provider proposal, is in progress. |

## Broker

KL11-01 through KL11-09 are done. That range covers framed transport, the
checksummed journal, topic metadata, validated Produce, Fetch, ListOffsets,
and independent clients against one persisted node. KL11-46, rolling
mixed-version cluster behavior, is done. Unit and integration tests do not
by themselves make the broker production-ready. The
[broker plan](plan/broker-implementation.md) records the remaining checks,
including sustained fuzz, resource soaks, and peer authentication.

## Snapshot from 8 October 2026

The notes below describe that date's source. They are not a count of today's
test suite.

Earlier full-suite checks with Rust 1.99.0 passed 2,090 default-feature tests
and 2,102 all-feature tests. Formatting, strict Clippy, and rustdoc checks
passed. The packaged default, tracing, zstd, and combined-feature builds each
compiled 24 documentation examples.

Whole-broker checks passed 344 default and 484 all-feature tests. A fixed-peer
metadata runtime passed finite three-node and five-node private TCP fault,
snapshot, and restart histories with independent journal replay. Native Kafka
replication and production qualification were still open.

UpdateFeatures v0 rejected validation-only requests before encoding or
dispatch. The qualification record is
[q-conf-updatefeatures](evidence/conformance/q-conf-updatefeatures/qualification-20261008/README.md).

A pinned local baseline recorded codec, null-broker, native broker, and
open-loop latency measurements. All five 80% latency profiles contained
bounded-capacity rejections. Those failures and the missing-cell list are in
the [baseline evidence](evidence/perf/baseline/README.md). A
[build-profile comparison](evidence/perf/build-profiles/README.md) recorded
LTO, codegen-unit, CPU-target, and PGO variants. Those local results did not
change library defaults.

The benchmark report enforced the complete schema. Corrected null-broker
produce, fetch, and connect outputs passed, with explicit fields for absent
durability and warmup. See
[format evidence](evidence/perf/null-result-schema/README.md).

Pinned Java, librdkafka, and franz-go clients passed bounded Produce and Fetch
checks against the null broker. Fetch used the fixture's seeded stream.
[Peer evidence](evidence/perf/nullbroker-peers/README.md) records the limit,
source hashes, retained failures, and process cleanup. That is fixture
compatibility, not a storage or speed claim.

The orchestrator rejected client-ceiling cells inside Kafka-throughput
manifests. Its null-broker tier still needed qualified adapters. The franz-go
driver was missing ten settings required by the comparison preflight, and
those omissions were reported. [Orchestrator evidence](evidence/perf/matrix-tiers/README.md)
keeps the earlier tier-mixing failure. KL09-68 was open in that snapshot.

The Go peer applied its reported `none` compression setting explicitly.
[Compression evidence](evidence/perf/franz-compression-20261008/README.md)
records the tested source. The shared configuration was incomplete, so the
correction did not qualify a comparable campaign.

The [workspace archive](evidence/workspace-publication-20261008/README.md)
preserves the original evidence files from that publication.
