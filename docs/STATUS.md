# Status

This describes the working source as of October 8, 2026. Version 0.1.0 is
published on crates.io; the repository contains changes made after that release.
Current development uses the latest stable Rust. The client and experimental
broker still have open implementation and qualification work.

## Client

The client has producer, manual-consumer, group, transaction and Admin APIs.
Selected tests cover Apache Kafka 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1. Coverage
varies by API and version; see the [support matrix](support.md).

Recent completed work includes producer startup retries, retained-buffer
accounting, zstd encoding/decoding, cross-SDK codec checks, InitProducerId v6,
reassignment options, WriteTxnMarkers v2, share-offset lag support, Streams v0
codecs, caller-driven Streams heartbeats, typed group descriptions, UUID offset routing and all-broker
transaction listings with transaction-ID pattern filters. Legacy Metadata v0 and
GROUP coordinator discovery v0 now negotiate with bounded decoding and retries.
DescribeQuorum codecs and public Admin inspection now have bounded messages
and retries, with checks against all three current SDKs and native brokers. Admin
capability checks use actual Apache SDK frames and public Java calls. Their
scripted peers do not implement broker transaction or share state.

UpdateFeatures v0 rejects validation-only requests before encoding or dispatch.
Default and all-feature checks cover the three current SDKs, native public
callers, controller migration, operation deadlines and recovery, argument
validation, malformed fields and per-feature errors. The source mapping records
48 parameter invocations. Java public v0 is unsupported; Rust v0 wire support
uses an independent SDK parser. See the [qualification record](evidence/conformance/q-conf-updatefeatures/qualification-20261008/README.md).

Current gaps include prepared transaction initialization. Fault recovery, session reauthentication and
mixed-version behavior need further implementation or independent checks.

## Broker

The broker is experimental. It includes protocol handling, storage, coordinator
and replication code, but distributed recovery, security, resource limits and
current-version compatibility remain open. Selected unit and integration tests
do not establish production readiness. The [broker plan](ROADMAP.md)
records the remaining work.

The fixed-peer metadata runtime passed finite three/five-node private TCP fault,
snapshot and restart histories with independent journal replay. Whole broker
checks passed 344 default and 484 all-feature tests on latest stable Rust. Native
Kafka replication, peer authentication and production qualification remain open.

## Development checks

Earlier full-suite checks with Rust 1.99.0 passed 2,090 default-feature tests and
2,102 all-feature tests. Formatting, strict Clippy and rustdoc checks passed. The packaged default,
tracing, zstd and combined-feature builds each compiled 24 documentation examples.
These retained results refer to their cited source snapshots. Newer bounded
UpdateFeatures checks pass on default and all-feature builds. They are source-bound SDK and native-broker
checks; fault and soak qualification remains separate.

The task registry has 214 completed cards and 128 open cards. The client
conformance registry has 75 independently qualified cases out of 182 required
cases. Core and full conformance gates remain incomplete. The [development
roadmap](ROADMAP.md) links the plans and registries.

## Performance

The repository has pinned benchmark peers, result checks, codec benchmarks and
a resource-soak runner. Java peer checks passed against an isolated Kafka 4.3.1
broker. Those short runs qualify the driver and its delivery accounting.

A pinned local baseline now records repeated codec, null-broker, native broker
and open-loop latency measurements, with confidence intervals and throughput
reruns. All five 80% latency profiles contain bounded-capacity rejections. The
[baseline evidence](evidence/perf/baseline/README.md) retains those failures and
the missing-cell list. The [publication inventory](evidence/perf/baseline-publication.json)
identifies the raw files retained in the original workspace.

A [build-profile comparison](evidence/perf/build-profiles/README.md) now records
LTO, codegen-unit, CPU-target and PGO variants with matched repetitions and a
fresh baseline cohort. Native results pass the complete schema. The original
null-broker format has recorded schema failures tracked by KL09-73. These local
results did not change library defaults.

Controlled x86_64/arm64 comparisons and independent peer reproduction remain open. Paired-run orchestration now has
source-bound process tests and an isolated Kafka rehearsal. The miniz fallback gzip allocation
baseline also needs reconciliation. No fastest-client or fastest-server result
is established. Suite HOLD remains in effect until the comparison requirements
in the [benchmark contract](benchmark-contract.md) are met.

[Earlier status notes](https://github.com/mingley/partitionline/blob/8a50e8d18df40787d86eb714ff363b9d1e41ce43/docs/STATUS.md)
retain the release and CI history.

The benchmark report now enforces the complete schema. Corrected null-broker
produce, fetch and connect outputs pass, and retain explicit fields for absent
durability and warmup. [Format evidence](evidence/perf/null-result-schema/README.md)
includes native-result checks and twenty rejected changed-result controls.


Pinned Java, librdkafka and franz-go clients now pass bounded Produce and Fetch
checks against the null broker, with observed protocol versions and zero
validation failures. Fetch uses the fixture's independent seeded stream.
The [peer evidence](evidence/perf/nullbroker-peers/README.md) records that limit,
source hashes, retained failures and process cleanup. These checks establish
functional fixture compatibility, with no storage or speed claim.


The orchestrator rejects client-ceiling cells and results in Kafka-throughput
manifests. Its null-broker tier still needs qualified adapters and fixture
lifecycle handling. The current franz-go driver also lacks ten settings required
by the comparison preflight; those omissions are reported explicitly.
[Orchestrator evidence](evidence/perf/matrix-tiers/README.md) retains the prior
tier-mixing failure and the current checks. KL09-68 remains open.
