# Status

Updated 19 November 2026. The [partitionline client](https://crates.io/crates/partitionline/0.2.0)
is at version 0.2.0. The broker is experimental. The broker and Schema
Registry companion are unpublished.

## Validation

At [`713c8a3`](https://github.com/mingley/partitionline/commit/713c8a3d861a843bc1e6e229eecf53ce5632b3bf),
all 27 configured client jobs passed and the opt-in live lane was skipped.
The run covered current Apache Kafka 4.1.2, 4.2.1 and 4.3.1, historical 3.9.1
and 4.1.0 smoke, packaged consumers and the finite resource soak.
[Client CI](https://github.com/mingley/partitionline/actions/runs/44414668464)

At `51a4d86`, broker CI, both live Registry lanes, docs and packaging passed.
The docs job compiled examples from the packaged crate. The macOS default
suite passed 2,334 tests and failed the producer reservation timing assertion
at 80.56225 ms against its 80 ms limit. The Windows tracing lane also
failed a runtime contract test.
[Broker CI](https://github.com/mingley/partitionline/actions/runs/44618884602) ·
[Registry CI](https://github.com/mingley/partitionline/actions/runs/44618884606) ·
[Client CI](https://github.com/mingley/partitionline/actions/runs/44618884541)

The earlier broker suite at `ce1de24` failed a Raft image-installation test.
The soak at `753a35b` stopped when the OS denied descriptor sampling.
The 0.2.0 release came from its own tag.
[Earlier broker run](https://github.com/mingley/partitionline/actions/runs/44615559884) ·
[Soak failure](https://github.com/mingley/partitionline/actions/runs/43692389498/job/132745594665) ·
[Release record](RELEASE.md)

## Changes since that client run

- Flat-journal Produce reuses workers within its sync limit. Three paired
  x86_64 multi-partition workloads improved 25.79–31.49% in median throughput.
  The one-partition result was inconclusive. Both builds verified all 122,496
  records across graceful restarts.
  [Broker measurements](https://github.com/mingley/partitionline/blob/8977c6b986d5077c5886be75c6cc13fe99cdec5b/docs/evidence/broker/reusable-sync-20261119/README.md)
- The producer keeps accepted records with its worker during grouping,
  including across cooperative yields. Five cancellation and retry-order
  controls passed in each feature configuration. All seven paired shapes
  had lower median grouping time, with a 49.44% reduction for a 64-record
  single-partition batch and one slower individual pair.
  [Producer checks](https://github.com/mingley/partitionline/blob/4ba4341da14d173382d81a71775927ce94966c4e/docs/evidence/client/producer-small-group-20261119/README.md)
- Mixed-topic reservations now yield after at most 8,192 partition visits.
  The deterministic checks confirmed the wide-topic yield. The selected
  change improved median wide grouping by 3.51%. The one-record sample
  median increased by 0.94 ns. The paired results retain every shape.
  [Reservation results](https://github.com/mingley/partitionline/blob/72ffaeb7b484abe272ba512ef4b396c9c315a986/docs/evidence/client/producer-group-reservation-20261119/slot-replacement.md)
- Kafka 4.3.1 verified 3,072 producer receipts per feature profile. Apache's
  Java tools checked every offset, key and value after flush and close,
  including records accepted by `try_send`. Both profiles verified all
  3,840 records.
  [Live producer evidence](https://github.com/mingley/partitionline/blob/189a35e2da9ac4aac0a2acb97f651e6434bd2213/docs/evidence/client/producer-live-receipts-20261119/README.md)
- Five Apache Kafka SDKs checked 280 selected ListOffsets request/response
  pairs in each direction, including the v10 timeout field. The retained
  corpus passed in the checkout and the packaged crate.
  [Wire checks](https://github.com/mingley/partitionline/blob/cb5bbece36d085de82d1ea856889a662c4aafe63/docs/evidence/conformance/q-conf-listoffsetswire/README.md)
- The live Redpanda Registry check passed with default and all features.
  It covered Avro lookup, exact and latest versions, named references,
  cache refresh and a missing-subject error over plain HTTP.
  [Registry check](https://github.com/mingley/partitionline/blob/5697254c9ca8f2dec88f7758e8b50868753295cc/docs/evidence/schema/registry-live-20261119/README.md)
- The x86_64 codec gate measures ten instruction counts on stable Rust.
  Its self-test caught a 6.03% increase against the 1% limit. CI at
  `51a4d86` measured 920,136 LZ4 decode instructions against 903,610
  recorded, a 1.83% increase. The linked frame-reader code differs between
  builds with the same codec source and fixture. The writer and direct-copy
  decoder trials remain experimental.
  [Codec gate](https://github.com/mingley/partitionline/blob/4e1e280b794eef89cc1707d715c265f98cb039f4/docs/evidence/perf/codec-instructions-20261119/README.md) ·
  [LZ4 trace](https://github.com/mingley/partitionline/blob/9b11c6080ca964574b0ad8b5944bdaa03e583842/docs/evidence/client/lz4-main-decode-20261119/linked-code.md) ·
  [Decoder trial](https://github.com/mingley/partitionline/blob/3bb3caf0f839b92ef201154f04677ca361e4e900/docs/evidence/client/lz4-direct-decode-20261119/README.md) ·
  [Writer trials](https://github.com/mingley/partitionline/blob/a47467878eee0264fa5db0ae4a5443ef53432919/docs/evidence/client/record-varint-20261119/single-byte-inline/README.md) ·
  [Cursor trial](https://github.com/mingley/partitionline/blob/c37d5719e6f8dd431249245714a941b29ae84248/docs/evidence/client/lz4-slice-cursor-20261119/README.md)
- The report checker can verify local bundle files by length and SHA-256.
  [Benchmark contract](benchmark-contract.md)

## Release gates

The client needs more live compatibility and delivery histories. The broker
still has an RF1 data path. Partition replication, native KRaft, transaction
markers, Kafka wire quotas and process-wide bounds are open. The Schema
Registry companion still needs live TLS and a Kafka round trip. The 24-hour
fault and seven-day continuity campaigns are open.
[Support matrix](support.md) · [Work plan](https://github.com/mingley/partitionline/blob/da2f6217de83824fad70bb3cb23bd32f807c4dc5/docs/plan/next-work.md)

All 12 required client comparison cases remain `not_run` in the scenario
registry. Controlled x86_64 client and broker comparisons and a separate
reproduction with independent record checks are required for profile claims.
Suite HOLD remains active.
[Benchmark contract](benchmark-contract.md)

The [previous status](https://github.com/mingley/partitionline/blob/ce1de24a9f81db66d35a76194e8c8ad1dbfa1982/docs/STATUS.md)
retains prior evidence and stopped work.
