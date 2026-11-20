# Status

Updated 20 November 2026. The client is available as
[partitionline 0.2.0](https://crates.io/crates/partitionline/0.2.0). The
broker and Schema Registry companion are unpublished. The broker is experimental.

## Client

- Kafka 4.3.1 verified 3,072 producer receipts under each of default and all
  features. After flush and close, Apache's Java tools checked all 3,840
  stored offsets, keys and values in each lane.
  [Live delivery](https://github.com/mingley/partitionline/blob/189a35e2da9ac4aac0a2acb97f651e6434bd2213/docs/evidence/client/producer-live-receipts-20261119/README.md)
- Five pinned Apache SDKs checked 106 Produce request/response pairs in each
  direction across v3–v13 and 280 selected ListOffsets pairs across v1–v11.
  Each peer covered its shared versions. The offline corpora passed in the
  checkout and extracted crate.
  [Produce](https://github.com/mingley/partitionline/blob/774df94f41906d8b95137e050dbbe17fbf3fa3db/docs/evidence/conformance/q-conf-producewire/all-supported-versions-20261119.md) ·
  [ListOffsets](https://github.com/mingley/partitionline/blob/cb5bbece36d085de82d1ea856889a662c4aafe63/docs/evidence/conformance/q-conf-listoffsetswire/README.md)
- Producer grouping keeps accepted records across cancellation. Mixed-topic
  reservation yields after at most 8,192 partition visits. Its paired wide
  grouping median improved 3.51% and its one-record sample median rose 0.94 ns.
  [Reservation checks](https://github.com/mingley/partitionline/blob/72ffaeb7b484abe272ba512ef4b396c9c315a986/docs/evidence/client/producer-group-reservation-20261119/slot-replacement.md)

- The producer sizes each batch and computes its maximum timestamp in
  one pass. In 14 paired encoding workloads, request bytes matched.
  Throughput rose 3.33–5.10% for one uncompressed record and 4.50% for
  the 128-partition LZ4 workload. One-record gzip fell 0.09%.
  [Timing](https://github.com/mingley/partitionline/actions/runs/44639969911) · [Wire and allocation checks](https://github.com/mingley/partitionline/actions/runs/44642069000)

## Broker

- The RF1 broker can pace Produce on an opt-in plaintext listener. Client
  labels share bounded buckets across connections. Stable Rust passed 826
  default and 974 all-feature tests, with four ignored per profile. The
  Apache Kafka 4.3.1 comparison checked v3/v6 wire ordering and pacing.
  [Quota checks](https://github.com/mingley/partitionline/blob/007ed3e50161a4c790205be5297941f473436e7a/docs/evidence/broker/plaintext-produce-pacing-20261119/README.md)
- Reusable journal sync workers raised median throughput 25.79–31.49% in
  three local multi-partition workloads. The single-partition result was
  inconclusive. The later pacing-disabled probe had median changes from
  −2.72% to +1.10%, and each observed paired range included zero.
  [Sync measurements](https://github.com/mingley/partitionline/blob/8977c6b986d5077c5886be75c6cc13fe99cdec5b/docs/evidence/broker/reusable-sync-20261119/README.md) ·
  [Pacing controls](https://github.com/mingley/partitionline/blob/007ed3e50161a4c790205be5297941f473436e7a/docs/evidence/broker/plaintext-produce-pacing-20261119/README.md)

- The broker crate archive passed locked release installation in both
  feature profiles. The installed operator verified byte-identical admin
  journal backup and restore and preserved the journal on a downgrade.
  [Archive checks](https://github.com/mingley/partitionline/actions/runs/44647817744)

## Schema Registry

Both Redpanda 26.2.2 lanes passed HTTP cache and verified HTTPS lookup
checks. The all-feature lane resolved an Avro schema and named reference
over HTTPS, produced with `acks=all`, and fetched and decoded the exact
frame through a plaintext Kafka listener.
[Live Registry and Kafka](https://github.com/mingley/partitionline/blob/d5d9ff7065560cb8cab2f14d1b9b18190b3d6209/docs/evidence/schema/registry-avro-roundtrip-20261119/README.md)

Both profiles also passed authenticated lookup using HTTP Basic over
HTTPS. The companion archive passed 112 default, 20 no-default and 153
all-feature checks, including doctests. The two live tests in that
archive run were ignored and ran in the separate live workflow.
[Basic auth](https://github.com/mingley/partitionline/actions/runs/44640038645) · [Companion package](https://github.com/mingley/partitionline/actions/runs/44640185362)

The first JSON parser trial cut the 64 MiB parse median from 111 to 71 ms.
Its Avro 8,192-field median rose 27.14%, and peak RSS rose 28.96% at 8 MiB.
The trial stayed isolated. [Paired JSON results](https://github.com/mingley/partitionline/actions/runs/44647583240)

## Validation

Linux client checks, native macOS and Windows checks, docs, packaging,
the release rehearsal, and the finite resource soak passed on `e7c6e3c`.
The broker suite failed a Raft integration test.
[Client CI](https://github.com/mingley/partitionline/actions/runs/44642981045) · [Broker CI](https://github.com/mingley/partitionline/actions/runs/44642981053)

LZ4 decode measured 920,136 instructions against 903,610 on x86_64,
a 1.83% increase against a 1% limit. Seven trials missed the instruction
or paired timing checks. The two latest covered header-free record parsing
and the frame input reader. Both passed record and allocation checks.
[Instruction gate](https://github.com/mingley/partitionline/actions/runs/44642981045/job/135978609101) · [Record parser](https://github.com/mingley/partitionline/actions/runs/44646514535) ·
[Frame reader](https://github.com/mingley/partitionline/actions/runs/44647341685)

## Release gates

Open work includes broker replication, authenticated quotas and quota
administration, live bearer tokens and credential rotation, 24-hour fault qualification
and seven-day continuity. All 12 required client peer cases are still
`not_run`. Performance leadership requires matched x86_64 comparisons
and independent record checks in a separate reproduction.
[Work plan](https://github.com/mingley/partitionline/blob/774df94f41906d8b95137e050dbbe17fbf3fa3db/docs/plan/next-work.md) · [Benchmark contract](benchmark-contract.md)

Earlier desktop CI failures and resource evidence are in the
[previous status](https://github.com/mingley/partitionline/blob/b6d6d44ead84a6aacc183b570a26100f751ef77a/docs/STATUS.md).
