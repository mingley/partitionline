# API stability

While the crate is **0.x**, nothing is permanently frozen. This document
tells callers what is safe to build on versus what may churn.

## Stable (prefer these)

Breaking these is a `0.MINOR` bump and a CHANGELOG entry:

| Surface | Notes |
|---|---|
| `Producer`, `ProducerConfig`, `ProduceRecord`, `RecordMetadata` | Produce path |
| `Consumer`, `ConsumerConfig`, `ConsumerRecords`, `FetchedRecord` | Manual fetch |
| `ConsumerGroup`, `ConsumerGroupMetadata`, group join helpers | Classic / KIP-848 |
| `ShareGroup`, `ShareRecords`, acknowledge helpers | KIP-932 |
| `Admin`, `AdminConfig`, `NewTopic`, common admin options | Java-shaped admin |
| `Sasl`, `TlsConfig`, `OidcConfig`, `Acks`, `IsolationLevel`, `Compression` | Config enums |
| `Error`, `Result`, `ApiError` | Error surface |
| `TopicPartition`, `OffsetAndMetadata`, `OffsetAndTimestamp` | Shared types |
| `Partitioner` / `DefaultPartitioner` | Custom partitioning |
| `ProducerInterceptor` / `ConsumerInterceptor` | Interceptors |
| `ProducerMetrics` / `ConsumerMetrics` / `ShareMetrics` / `AdminMetrics` | Snapshots |
| `CLIENT_NAME`, `CLIENT_VERSION` | ApiVersions identity |

Method-level rustdoc that names a Java `Admin` / consumer / producer call is
part of the contract for those methods.

## Evolving

Expect additive churn; renaming or removing requires a CHANGELOG note:

| Surface | Notes |
|---|---|
| `partitionline::protocol` | Wire codecs; public for tests and advanced tools |
| Large `admin` type soup (every `*Request` / `*Response` re-export) | Prefer high-level `Admin` methods |
| New Kafka API versions and KIP helpers | Added as brokers ship them |
| Metrics field sets | Counters may grow; existing fields stay |

## Out of scope (this crate)

- Schema Registry (companion crate only; see `gaps.md`)
- Drop-in `rd_kafka_*` / rust-rdkafka types
- Default features that link C (zstd, Kerberos / GSSAPI)

Supported broker/MSRV/OS combinations are listed in [`support.md`](support.md).
That matrix is operational honesty for 0.1.x, not a permanent 1.0 promise.

## Experimental

Nothing is marked `#[doc(hidden)]` experimental today. If an API is added for
a single deployment need before it hardens, mark it in rustdoc with
`Experimental:` and list it here in the same PR.
