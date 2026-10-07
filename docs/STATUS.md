# Status

This describes the working source as of October 7, 2026. Version0.1.0 is
published on crates.io; the repository contains changes made after that release.
Current development uses the latest stable Rust. The client and experimental
broker still have open implementation and qualification work.

## Client

The client has producer, manual-consumer, group, transaction and Admin APIs.
Selected tests cover Apache Kafka3.9.1,4.1.0,4.1.2,4.2.1 and4.3.1. Coverage
varies by API and version; see the [support matrix](support.md).

Recent completed work includes producer startup retries, retained-buffer
accounting, zstd encoding/decoding, cross-SDK codec checks, InitProducerId v6,
reassignment options, WriteTxnMarkers v2, share-offset lag support, Streams v0
codecs, caller-driven Streams heartbeats, typed group descriptions, UUID offset routing and all-broker
transaction listings with transaction-ID pattern filters. Legacy Metadata0 and
GROUP coordinator discovery0 now negotiate with bounded decoding and retries. Admin
capability checks use actual Apache SDK frames and public Java calls. Their
scripted peers do not implement broker transaction or share state.

Current gaps include prepared transaction initialization. Fault recovery, session reauthentication and
mixed-version behavior need further implementation or independent checks.

## Broker

The broker is experimental. It includes protocol handling, storage, coordinator
and replication code, but distributed recovery, security, resource limits and
current-version compatibility remain open. Selected unit and integration tests
do not establish production readiness. The [broker plan](ROADMAP.md)
records the remaining work.

## Development checks

Latest-stable Rust1.99.0 passed2,079 default-feature tests and2,091 all-feature
tests. Formatting, strict Clippy and rustdoc checks passed. The packaged default,
tracing, zstd and combined-feature builds each compiled24 documentation examples.
These are retained local results for the current uncommitted source, not hosted
CI results or a replacement for the remaining fault and soak tests.

The task registry has203 completed cards and137 open cards. The client
conformance registry has51 independently qualified cases out of182 required
cases. Core and full conformance gates remain incomplete. The [development
roadmap](ROADMAP.md) links the plans and registries.

## Performance

The repository has pinned benchmark peers, result checks, codec benchmarks and
a resource-soak runner. Java peer checks passed against an isolated Kafka4.3.1
broker. Those short runs qualify the driver and its delivery accounting.

Controlled x86_64/arm64 comparisons, paired repetitions, confidence intervals
and independent reproduction remain open. Paired-run orchestration now has
source-bound process tests and an isolated Kafka rehearsal. The miniz fallback gzip allocation
baseline also needs reconciliation. No fastest-client or fastest-server result
is established. Suite HOLD remains in effect until the comparison requirements
in the [benchmark contract](benchmark-contract.md) are met.

[Earlier status notes](https://github.com/mingley/partitionline/blob/8a50e8d18df40787d86eb714ff363b9d1e41ce43/docs/STATUS.md)
retain the release and CI history.
