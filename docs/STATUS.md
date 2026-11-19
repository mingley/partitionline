# Status

Updated 19 November 2026. The client is available as
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

## Schema Registry

Both Redpanda 26.2.2 lanes passed HTTP cache and verified HTTPS lookup
checks. The all-feature lane resolved an Avro schema and named reference
over HTTPS, produced with `acks=all`, and fetched and decoded the exact
frame through a plaintext Kafka listener.
[Live Registry and Kafka](https://github.com/mingley/partitionline/blob/d5d9ff7065560cb8cab2f14d1b9b18190b3d6209/docs/evidence/schema/registry-avro-roundtrip-20261119/README.md)

## Validation

Docs, packaging and the finite resource soak passed at `b6d6d44`.
Its x86_64 instruction gate measured 920,136 LZ4 decode instructions
against 903,610 recorded, a 1.83% increase against a 1% limit. Four new
LZ4 trials missed either the instruction or paired timing checks. The
trial records retain the raw results.
[CI](https://github.com/mingley/partitionline/actions/runs/44635563626) ·
[Decoder trials](https://github.com/mingley/partitionline/blob/f91f62ed412ec94c102f5654b5b741e1987ee057/docs/evidence/client/lz4-read-dispatch-20261119/README.md)

## Release gates

Open work includes broker replication, authenticated quotas and quota
administration, live Registry credentials, 24-hour fault qualification
and seven-day continuity. All 12 required client peer cases are still
`not_run`. Performance leadership requires matched x86_64 comparisons
and independent record checks in a separate reproduction.
[Work plan](https://github.com/mingley/partitionline/blob/774df94f41906d8b95137e050dbbe17fbf3fa3db/docs/plan/next-work.md) · [Benchmark contract](benchmark-contract.md)

Earlier desktop CI failures and resource evidence are in the
[previous status](https://github.com/mingley/partitionline/blob/b6d6d44ead84a6aacc183b570a26100f751ef77a/docs/STATUS.md).
