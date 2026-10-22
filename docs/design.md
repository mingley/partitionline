# How it works

The client sends Kafka requests over TCP or TLS. This page describes how
records move, how groups and admin calls are shaped, and the wire versions
this crate speaks.

Rust behavior is authoritative. A Java name in this page is a porting
cross-link, not a second specification.

## Producer

1. You call `try_send` (throughput), `send_all` (many offsets), or `send` (one offset future).
2. The record is assigned a partition before it is queued. The default partitioner is murmur2 when there is a key, and round-robin when there is not. You can replace it with `ProducerConfig::partitioner`. Until metadata for that topic is cached, `try_send` returns `QueueFull`. `send` and `send_all` wait.
3. The record is queued on one connection: `partition % connections`. Idempotent sequences for a partition never share a socket with another worker.
4. That connection's worker waits for `linger` or until the batch is large enough, then writes a Produce request.
5. Several Produce requests can be in flight on the same socket.
6. `flush` waits for those responses and returns the first broker error. `try_send` returning `Ok` only means the record was queued.

The hot path copies each payload once into the record batch and checksums it with CRC32-C.

`ProducerConfig`, `ConsumerConfig`, and `AdminConfig` are chainable builders. The raw fields stay writable. `ConsumerConfig.isolation_level` is `IsolationLevel`, not a raw `i8`.

An owned, nonblocking delivery API has been proposed and is not implemented. It is described in [API stability](api-stability.md#proposed-owned-delivery-api-kl10-07-awaiting-maintainer-review).

## Consumer

`Consumer` is manual assignment. You name a topic, partition, and offset, then call `fetch`. `fetch` and group `poll` return `ConsumerRecords`. Share `poll` returns `ShareRecords`.

`fetch` sends one request per partition leader. When several leaders are involved, it returns after the first response and about 10 ms for the others. A slower leader stays in flight. Seek, assignment changes, and close drop that round.

`seek_to_beginning` and `seek_to_end` call ListOffsets. `seek` and `seek_to` clear the last fetched epoch. `seek_with_metadata` keeps it (KIP-320). `pause` and `resume` skip assigned partitions without dropping them. Pause survives a rebalance. `position` is the next fetch offset.

`max.poll.records` caps how many records one `fetch` returns. The rest stay buffered. `fetch.max.bytes` and `max.partition.fetch.bytes` are independent. `max_bytes()` sets both. The defaults are 16 MiB each. Java uses 50 MiB and 1 MiB.

`assign_many` and `assign_partitions` replace a manual assignment. `assign_partitions` follows `auto.offset.reset`. `unassign` drops it.

`Consumer::wakeup`, and a cloneable `WakeupHandle`, interrupt a fetch. Interceptors can observe or rewrite records. `close` and `close_timeout` drop fetch connections.

## Groups

`ConsumerGroup` joins, heartbeats, fetches, and commits.

| Method | Assignment |
|---|---|
| `join_topics` | Range. Each topic is split across members subscribed to it. |
| `join_sticky_topics` | Sticky. A joining member takes load from the others. |
| `join_cooperative_sticky_topics` | Cooperative-sticky (KIP-429). Owners keep partitions until they revoke them, then rejoin. |
| `join_consumer_topics` | KIP-848 server-side assignment. |

`subscribe_matching` and the `join_*_matching` methods follow Java `subscribe(Pattern)`. Topic names that start with `__` are skipped. The pattern is re-evaluated when `metadata.max.age.ms` elapses.

`group_instance_id` is static membership (`group.instance.id`) on JoinGroup, Heartbeat, and KIP-848. `rack` is also sent on KIP-848 and share-group heartbeats.

`auto_offset_reset` runs when OffsetFetch has no commit. The default is earliest. `allow_auto_create_topics` defaults to false.

`commit` and `commit_timeout` are synchronous commits. `commit_async` queues an OffsetCommit and sends it on the next poll, leave, close, or unsubscribe. It does not spawn a task. Pass `next_offsets` to commit what the last poll returned. `committed` reads the current assignment back.

Auto-commit is off by default. A zero interval commits after every `poll`. `session_timeout` and `heartbeat_interval` control the classic group. If `max.poll.interval.ms` is exceeded, the next `poll` returns `Error::MaxPollInterval` and the heartbeat task leaves the group.

`on_rebalance` receives `(revoked, assigned)`. `enforce_rebalance` rejoins on the next poll. `close_timeout` caps `leave`.

`Producer::send_offsets_to_transaction` and `send_offsets_with_metadata` commit offsets inside a transaction.

## Share groups

`ShareGroup` is KIP-932 queue sharing. `join_topics` subscribes to several topics. The same pattern subscribe used by consumer groups is available. `close_timeout` caps leave.

## Admin

Admin methods follow the Java Admin names. A `*_timeout` method is that operation's options timeout: it is the RPC deadline. When the request has no TimeoutMs field, the deadline still bounds coordinator and controller retries. When the broker does not advertise the API, the method returns `Error::Unsupported`.

`Admin::new` does not require the optional admin APIs. Calling one that the broker omitted returns `Unsupported`. `Admin::close_timeout` does not send LeaveGroup.

`Admin::metrics` is a client-side snapshot of I/O errors and RPC latency. It is not a broker error code.

The method list, including quotas, ACLs, delegation tokens, log directories, features, and share-group offsets, is in the rustdoc for `Admin`. This page does not repeat each Java overload.

## Metrics and client id

`Producer::metrics`, `Consumer::metrics`, `ShareGroup::metrics`, and `Admin::metrics` are counter snapshots. Latency is min, mean, max, p50, and p99 over the last 1,024 samples. Produce and fetch metrics also have one row per topic.

`client_instance_id` is the KIP-714 client instance id. The timeout variant's deadline applies to GetTelemetrySubscriptions. The id is cached after the first success.

## Wire format notes

These notes are for people changing encoders and decoders. The table is the set of versions this crate speaks. Field order, where throttle time sits, and which KIP added a field are written out in [Wire versions](wire-versions.md).

A few rules apply across APIs:

- Request `ClientId` is always a classic nullable string, even on flexible headers.
- The ApiVersions **response** header is classic. A flexible parse treats the error code as a compact-string length.
- Produce throttle time comes **after** the topic array. Metadata throttle time comes first.
- Record batch magic 2 CRC is CRC32-C over bytes from attributes to the end.
- Record lengths are zigzag varints. Compact protocol lengths are unsigned varint of `n+1` (`0` means null).
- Without `InitProducerId`, producer id / epoch / sequence must be `-1`. Zero is a real id.
- `acks=0` means the broker sends no Produce response. Do not read one.

| API | Spoken versions |
| --- | --- |
| InitProducerId | 0–6 |
| Produce | 3–13 |
| Fetch | 4–18 |
| ListOffsets | 1–11 |
| OffsetForLeaderEpoch | 0–4 |
| AddPartitionsToTxn | 0–3 |
| AddOffsetsToTxn | 0–4 |
| EndTxn | 0–5 |
| TxnOffsetCommit | 0–5 |
| WriteTxnMarkers | 0–2 |
| ElectLeaders | 0–2 |
| DescribeQuorum | 0–2 |
| AddRaftVoter | 0–1 |
| RemoveRaftVoter | 0 |
| FindCoordinator | 0–6 |
| OffsetCommit | 2–10 |
| OffsetFetch | 1–10 |
| OffsetDelete | 0 |
| Heartbeat | 0–4 |
| SyncGroup | 0–5 |
| JoinGroup | 2–9 |
| LeaveGroup | 0–5 |
| ConsumerGroupHeartbeat | 0–1 |
| ConsumerGroupDescribe | 0–1 |
| ShareGroupHeartbeat | 0–1 |
| ShareGroupDescribe | 0–1 |
| StreamsGroupHeartbeat | 0 |
| StreamsGroupDescribe | 0 |
| ShareFetch | 0–2 |
| ShareAcknowledge | 0–2 |
| DescribeShareGroupOffsets | 0 |
| AlterShareGroupOffsets | 0 |
| DeleteShareGroupOffsets | 0 |
| DescribeTopicPartitions | 0 |
| ListConfigResources | 0–1 |
| GetTelemetrySubscriptions | 0 |
| PushTelemetry | 0 |
| AssignReplicasToDirs | 0 |
| SaslHandshake | 0–1 |
| SaslAuthenticate | 0–2 |
| ListTransactions | 0–2 |
| DescribeTransactions | 0 |
| CreateTopics | 0–7 |
| DeleteTopics | 0–6 |
| DescribeConfigs | 0–4 |
| CreatePartitions | 0–3 |
| AlterPartitionReassignments | 0 |
| ListPartitionReassignments | 0 |
| UpdateFeatures | 0–2 |
| AlterUserScramCredentials | 0 |
| DescribeUserScramCredentials | 0 |
| AlterClientQuotas | 0–1 |
| DescribeClientQuotas | 0–1 |
| IncrementalAlterConfigs | 0–1 |
| AlterReplicaLogDirs | 1–2 |
| DescribeLogDirs | 1–5 |
| CreateDelegationToken | 1–3 |
| RenewDelegationToken | 1–2 |
| ExpireDelegationToken | 1–2 |
| DescribeDelegationToken | 1–3 |
| DescribeGroups | 0–6 |
| ListGroups | 0–5 |
| DeleteGroups | 0–2 |
| AlterConfigs | 0–2 |
| DeleteRecords | 0–2 |
| DescribeProducers | 0 |
| AllocateProducerIds | 0 |
| UnregisterBroker | 0 |
| DescribeCluster | 0–2 |
| CreateAcls, DescribeAcls, DeleteAcls | 0–3 |
| Metadata | 0–13 |
| ApiVersions | 0–4 and sends v4 on connect |

Kafka 4.0 removed some older versions. When a spoken range still includes them, this crate accepts them. The detail page names the versions this crate does not send.

## Compression

gzip uses `flate2`: zlib-rs by default (`unsafe` and SIMD inside the
dependency), or miniz_oxide with `default-features = false`. Both decode each
other's output and the Java and librdkafka fixtures; compressed bytes differ.
See [gzip-backend.md](gzip-backend.md). snappy uses the `snap` crate
(snappy-java framing on produce, raw snappy accepted on fetch). lz4 uses
`lz4_flex` LZ4 frames (independent 64KiB blocks, proper header checksum for
magic ≥ 1). The optional zstd feature uses `zstd-rs 0.1.0`, with bounded
decoding and explicit encoder levels. See [zstd-spike.md](zstd-spike.md).

## TLS

Set `ProducerConfig.tls` / `ConsumerConfig.tls` to a `TlsConfig`. Handshake is
`rustls` with the `ring` backend (not OpenSSL). Custom CA PEM, or Mozilla
roots if `ca_pem` is omitted. Optional client cert/key for mTLS. SNI defaults
to the bootstrap host. Plain TCP stays a `TcpStream`; TLS is a separate
connection type so the uncompressed hot path does not pay for rustls.

Writes pump reads into the connection buffer. TLS (and a full TCP window)
can otherwise stall `poll_write` until `poll_read` runs, which deadlocks a
pipelined producer that only reads after `max_in_flight` writes.
