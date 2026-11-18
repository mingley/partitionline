# Status

Updated 18 November 2026.

The Kafka client is available as [partitionline 0.2.0](https://crates.io/crates/partitionline/0.2.0).
Production qualification is ongoing. The broker is experimental and the
broker and Schema Registry companion are both unpublished. Comparative
performance results are incomplete, so Suite HOLD remains in effect.

## Validation

The following runs tested main commit
[`753a35b`](https://github.com/mingley/partitionline/commit/753a35b19cebad4155d37938a14eee219193d581).

| Check | Result |
| --- | --- |
| [Client CI](https://github.com/mingley/partitionline/actions/runs/43692389498) | 26 jobs passed and one failed. Live conformance was skipped. |
| Client platforms | Linux default and all features, Windows x86_64 and macOS arm64 passed. |
| Kafka interoperability | 4.1.2, 4.2.1 and 4.3.1 current profiles passed. 3.9.1 and 4.1.0 smoke passed. |
| Package and docs | Packaged consumer, documentation, and partial release rehearsal passed. |
| [Resource soak](https://github.com/mingley/partitionline/actions/runs/43692389498/job/132745594665) | The OS denied process descriptor sampling. The run failed. |
| [Broker CI](https://github.com/mingley/partitionline/actions/runs/43692389931) | Both feature profiles, strict checks, inventory, and fuzz supervision passed. |
| [Metadata wire](https://github.com/mingley/partitionline/actions/runs/43692389526) and [v13](https://github.com/mingley/partitionline/actions/runs/43692389531) | Both fixture workflows passed. |
| [Capabilities](https://github.com/mingley/partitionline/actions/runs/43692389581) | Default and all-feature jobs passed. |

The preceding [client run](https://github.com/mingley/partitionline/actions/runs/43687797505)
passed its finite resource soak at `b41727f`. The release corresponds to
`87eb1b8`, which passed its own checks. See the [release record](RELEASE.md)
for the tag and registry result.

The later [combined client run](https://github.com/mingley/partitionline/actions/runs/44412321206)
at `b248dd2` passed the current Kafka profiles, docs, package and finite
resource soak. The release rehearsal unit test step failed due to stale
test expectations, which passed after correction in the
[release test run](https://github.com/mingley/partitionline/actions/runs/44414059916).
The [combined broker run](https://github.com/mingley/partitionline/actions/runs/44412320790)
passed.

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
  cover selected v4, v6, v7 and v12 wire bodies. See the
  [support matrix](support.md) and [protocol gaps](gaps.md).
- **Broker.** The server has an RF1 data path and a separate metadata quorum.
  Partition replication, native KRaft, transaction marker propagation,
  Kafka wire quotas and a process-wide memory bound remain open.
- **Schema Registry.** The companion has passed mock and codec checks.
  Live Registry interoperability and resource limits for synchronous JSON
  parsing still need qualification. See the [companion guide](schema-companion.md).
- **Reliability and performance.** The 24-hour fault and seven-day continuity
  campaigns are open. Matched x86_64 comparisons with independent record
  verification are still needed. See the [benchmark contract](benchmark-contract.md).

The [work plan](https://github.com/mingley/partitionline/blob/cba307157896f91076a58200ac53164a632a8e3f/docs/plan/next-work.md)
tracks the remaining work. The [previous status](https://github.com/mingley/partitionline/blob/753a35b19cebad4155d37938a14eee219193d581/docs/STATUS.md)
preserves detailed experiment records, earlier CI failures and stopped investigations.
