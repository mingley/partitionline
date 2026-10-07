# Client capabilities

The client provides asynchronous producer, consumer, group, transaction and Admin
APIs. This page summarizes the callable surface. See the
[support matrix](support.md) for tested broker versions and platforms, and the
[feature registry](../tests/conformance/features.json) for implementation and
qualification details.

| Capability | Current implementation |
|---|---|
| Producer | Batching, routing, acknowledgments, retries, idempotence, delivery deadlines, interceptors and custom partitioners. |
| Manual consumer | Assignment, seek, pause/resume, wakeup, bounded polls, leader recovery, incremental Fetch sessions and committed isolation. |
| Classic groups | Range, sticky and cooperative-sticky assignment; subscriptions, commits, rebalance callbacks and static membership. |
| Consumer protocol | KIP-848 group join, heartbeat and polling APIs. |
| Committed offsets | Name-based methods and typed UUID requests; v10 group/Admin routing, member epochs and bounded batched fetches. |
| Share groups | Join, polling, accept/release/reject, version negotiation, acquisition modes and lock renewal. |
| Transactions | Initialize, begin, commit/abort and send group offsets with output records. |
| Admin | Topics, configs, ACLs, offsets, groups, transactions, quotas, log directories, leader elections and typed quorum operations. |
| Compression | None, gzip, Snappy and LZ4. Enable `zstd` for bounded zstd encoding and decoding. |
| TLS | `rustls`, custom CA certificates or Mozilla roots, and optional mutual TLS. |
| SASL | PLAIN, SCRAM-SHA-256/512 and OAUTHBEARER; OIDC token acquisition and application-owned refresh. |
| Kerberos/GSSAPI | Not implemented. Provider selection and platform credential lifecycle remain open. |
| Diagnostics | Producer, consumer, share and Admin metric snapshots; optional `tracing`. |
| Schema Registry | Separate unpublished companion with lookup, bounded caches and caller-selected format adapters. |

Method-level rustdoc describes Kafka API versions, options, fallbacks and error
results. Feature availability depends on the connected broker's negotiated APIs
and enabled features.

## Remaining work

Production qualification is ongoing. The [task registry](https://github.com/mingley/partitionline/blob/8a50e8d18df40787d86eb714ff363b9d1e41ce43/docs/plan/tasks.json)
tracks protocol option gaps, fault histories, security lifecycle tests, resource
limits, diagnostics and platform checks. Common deployment support and optional
enterprise features have separate test profiles.

Producer byte counters charge the visible key, value and header bytes. Admission
compacts backing allocations; allocator and record-object overhead, encoding
scratch, TLS and socket buffers add memory. See
[buffer ownership](guide.md#buffer-ownership-and-overload-mock).

Codec interoperability checks are described in [the codec guide](zstd-spike.md).
Schema adapter scope is described in [the companion guide](schema-companion.md).
Streams protocol codecs and caller-driven heartbeats are included. Streams and
Connect execution engines and a drop-in librdkafka C ABI are outside this client's
scope.

[Benchmark results](benchmark.md) describe their tested workloads and hardware.
Current comparative performance and broader client/server production
qualification remain open.
