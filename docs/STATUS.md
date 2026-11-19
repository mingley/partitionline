# Status

Updated 19 November 2026.

The Kafka client is available as [partitionline 0.2.0](https://crates.io/crates/partitionline/0.2.0).
Production qualification is ongoing. The broker is experimental and the
broker and Schema Registry companion are both unpublished. Comparative
performance results are incomplete, so Suite HOLD remains in effect.

## Validation

These runs tested [`713c8a3`](https://github.com/mingley/partitionline/commit/713c8a3d861a843bc1e6e229eecf53ce5632b3bf) on
`main`. All 27 required client jobs passed. The optional live conformance
job was skipped. [Client CI](https://github.com/mingley/partitionline/actions/runs/44414668464)

| Check | Result |
| --- | --- |
| Client | Linux default and all features each passed 2,330 library tests, plus integration targets. Windows x86_64 and macOS arm64 passed. |
| Kafka | Current Apache 4.1.2, 4.2.1 and 4.3.1 profiles passed. Historical 3.9.1 and 4.1.0 smoke passed. |
| Packaging | Packaged consumer, docs and release rehearsal passed. |
| Resources | Finite resource soak passed. The 24-hour fault and seven-day continuity campaigns are open. |
| Codec gate | The prior client run recorded counts without a matching baseline. The new x86_64 baseline covers ten cases and the gate caught an injected regression. [Gate evidence](https://github.com/mingley/partitionline/blob/4e1e280b794eef89cc1707d715c265f98cb039f4/docs/evidence/perf/codec-instructions-20261119/README.md) |
| Broker | Default and all-feature tests, strict checks, inventory and fuzz supervision passed. [Broker CI](https://github.com/mingley/partitionline/actions/runs/44414668424) |
| Produce wire | Apache v4, v6, v7 and v12 body fixtures passed, including extracted-crate checks. [Wire CI](https://github.com/mingley/partitionline/actions/runs/44414668497) |
| Capability reports | Default and all-feature jobs passed. [Capabilities](https://github.com/mingley/partitionline/actions/runs/44414668456) |

The earlier resource-soak run at `753a35b` stopped when the OS denied
descriptor sampling. The failure remains in the
[run record](https://github.com/mingley/partitionline/actions/runs/43692389498/job/132745594665).
The 0.2.0 release was built separately from tag `v0.2.0`. See the
[release record](RELEASE.md).

## Open work

- **Producer.** Canceling the queue scan now leaves accepted records and
  bookkeeping with the worker. Both feature profiles passed the
  [drain controls](https://github.com/mingley/partitionline/actions/runs/44411433690).
  The [paired x86_64 measurements](https://github.com/mingley/partitionline/blob/a0fd891dc584404045ece3f05e22e3722733acb2/docs/evidence/client/producer-drain-cancellation-20261118/README.md)
  show a lower median for two partial drains and mixed full-drain timings.
  Grouping, sticky selection, shutdown and ambiguous retries still need
  separate ownership and Kafka delivery checks.
- **Client compatibility.** The current Kafka profiles cover a finite
  single-broker scenario. Broader protocol support, older peer behavior,
  group churn, authentication renewal and transaction recovery need further
  evidence. The [Produce fixtures](https://github.com/mingley/partitionline/blob/a0fd891dc584404045ece3f05e22e3722733acb2/docs/evidence/conformance/q-conf-producewire/README.md)
  cover selected v4, v6, v7 and v12 wire bodies. The new [ListOffsets fixtures](https://github.com/mingley/partitionline/blob/cb5bbece36d085de82d1ea856889a662c4aafe63/docs/evidence/conformance/q-conf-listoffsetswire/README.md)
  cover selected v1–v11 bodies in both directions with five Apache SDKs.
  See the [support matrix](support.md) and [protocol gaps](gaps.md).
- **Broker.** Flat-journal Produce now reuses sync workers. Three paired x86_64
  workloads with multiple partitions improved 25.79–31.49% in median
  throughput. The one-partition result was inconclusive. Both builds verified
  122,496 records across graceful restarts. RF1 remains the data path.
  Partition replication, native KRaft, transaction marker propagation,
  Kafka wire quotas and a process-wide memory bound remain open.
  [Sync worker evidence](https://github.com/mingley/partitionline/blob/8977c6b986d5077c5886be75c6cc13fe99cdec5b/docs/evidence/broker/reusable-sync-20261119/README.md)
- **Schema Registry.** The companion has passed mock and codec checks.
  Live Registry interoperability and resource limits for synchronous JSON
  parsing still need qualification. See the [companion guide](schema-companion.md).
- **Reliability and performance.** The 24-hour fault and seven-day continuity
  campaigns are open. Matched x86_64 comparisons with independent record
  verification are still needed. See the [benchmark contract](benchmark-contract.md).

The [work plan](https://github.com/mingley/partitionline/blob/3a2e48fc6a5a727b5b8a56deea4d32c11ef23d60/docs/plan/next-work.md)
tracks the remaining work. The [previous status](https://github.com/mingley/partitionline/blob/753a35b19cebad4155d37938a14eee219193d581/docs/STATUS.md)
preserves detailed experiment records, earlier CI failures and stopped investigations.
