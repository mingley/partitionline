# How it works

The library talks Kafka's network protocol itself. There is no C Kafka library in the process.

## Producer

1. You call `try_send` (throughput), `send_all` (many offsets), or `send`
   (one offset future per record).
2. The record is given a partition **before** it is queued (the
   [`Partitioner`](../src/partitioner.rs): murmur2 if there is a key,
   round-robin if not, or `ProducerConfig::partitioner`). Until metadata for
   that topic is cached, `try_send` returns `QueueFull` and `send` /
   `send_all` wait.
3. The record goes onto the queue for **one** TCP connection: `partition % connections`. Idempotent sequences for a partition never share a socket with another worker.
4. That connection's worker waits a few milliseconds (`linger`) or until the batch is big enough, then writes a Produce request.
5. Several Produce requests can be in flight on the same socket.
6. `flush` waits for those responses and returns the first broker error. `try_send` Ok only means queued.

`ProducerConfig`, `ConsumerConfig`, and `AdminConfig` accept chainable
builders (`acks`, `sasl`, `tls`, `isolation`, `delivery_timeout`, `max_block`,
`buffer_memory`, `max_request_size`, `retry_backoff`, `reconnect_backoff`, `connections_max_idle`, `transaction_timeout`, `metadata_max_age`, …). The raw fields remain writable.

`ConsumerConfig.isolation_level` is [`IsolationLevel`](../src/config.rs)
(not a raw `i8`). `ConfigResourceType` and `ScramMechanism` type admin
config-resource and user-SCRAM calls.

The hot path copies each payload once into the Kafka record batch and checksums it with CRC32-C.

## Consumer

`Consumer` is manual: you say topic, partition, offset, then `fetch`.
`fetch` / group `poll` return [`ConsumerRecords`](../src/consumer.rs)
(Java `count` / `partitions` / `records` / `nextOffsets`). Share `poll` returns
[`ShareRecords`](../src/share.rs).
`fetch` sends one request per partition leader and waits for all of them
when there is more than one. `seek_to_beginning` / `seek_to_end` call
ListOffsets for every assigned partition; `seek_to_beginning_of` /
`seek_to_end_of` take a partition list (Java `seekToBeginning` /
`seekToEnd`). `pause` / `resume` skip
assigned partitions without dropping them; pause survives group rebalance.
`position` is the next fetch offset (`position_of` takes `TopicPartition`).
`partitions_for` / `beginning_offsets` / `end_offsets` wrap Metadata and
ListOffsets and take `TopicPartition`. Each has a `_timeout` variant
(Java `partitionsFor(String, Duration)` / `beginningOffsets` /
`endOffsets` / `listTopics(Duration)` / `offsetsForTimes(Map, Duration)`).
`partitions_for` includes leader epoch
and offline replicas (Java `offlineReplicas`). `list_offset` is ListOffsets for one
partition. `Admin::list_offsets` is Java `Admin.listOffsets` (earliest / latest /
timestamp; one ListOffsets RPC per partition leader; returns
`OffsetAndTimestamp`). `Admin::list_offsets_with_isolation` is Java
`listOffsets` plus `ListOffsetsOptions.isolationLevel`. `Admin::fence_producers` is Java
`Admin.fenceProducers` (InitProducerId on the transaction coordinator).
`Admin::force_terminate_transaction` is Java `forceTerminateTransaction`
(same InitProducerId fence for one `transactional.id`).
`Admin::abort_transaction` is Java `abortTransaction` (WriteTxnMarkers
ABORT on the partition leader).
`Admin::remove_members_from_consumer_group` is Java
`removeMembersFromConsumerGroup` (LeaveGroup v3–v5 by `group.instance.id`;
v5 sends `DEFAULT_LEAVE_GROUP_REASON`).
`Admin::remove_all_members_from_consumer_group` is Java
`RemoveMembersFromConsumerGroupOptions.removeAll` (DescribeGroups then LeaveGroup).
`Admin::describe_features` is Java `describeFeatures` (ApiVersions v3
tagged fields; [`FeatureMetadata`](../src/admin.rs)).
`Admin::list_topics` / `Admin::describe_topics` are Java `listTopics` /
`describeTopics`. `Admin::describe_classic_groups` is Java
`describeClassicGroups` (DescribeGroups). `Admin::describe_consumer_groups` is Java
`describeConsumerGroups` (DescribeGroups). `Admin::list_consumer_groups` is Java
`listConsumerGroups` (ListGroups). `Admin::delete_consumer_groups` is Java
`deleteConsumerGroups` (DeleteGroups). `Admin::describe_share_groups` is Java
`describeShareGroups` (ShareGroupDescribe). `Admin::list_client_metrics_resources` is Java
`listClientMetricsResources` (ListConfigResources CLIENT_METRICS). `Admin::list_share_group_offsets` is Java
`listShareGroupOffsets` (DescribeShareGroupOffsets). `Admin::delete_consumer_group_offsets` is Java
`deleteConsumerGroupOffsets` (OffsetDelete). `Admin::delete_share_groups` is Java
`deleteShareGroups` (DeleteGroups). `Admin::describe_replica_log_dirs` is Java
`describeReplicaLogDirs`. `Admin::describe_broker_log_dirs` is Java
`describeLogDirs(Collection<Integer>)` (null-topics DescribeLogDirs on
each broker). `Admin::metrics` is Java `Admin.metrics()` (`AdminMetrics`
snapshot; I/O errors, not broker `error_code`). `assignment` is Java `assignment`
(`assigned_partitions` is the same list; `positions` is next fetch offset).
`max.poll.records` caps
how many records one `fetch` returns; the rest stay buffered.
`fetch.max.bytes` (`ConsumerConfig::max_bytes` / `fetch_max_bytes`) and
`max.partition.fetch.bytes` (`max_partition_fetch_bytes`) are independent;
`max_bytes()` sets both (default 16 MiB each; Java is 50 MiB / 1 MiB).

`ConsumerGroup` joins a group, heartbeats, fetches, and can commit offsets.
`join_topics` (range), `join_sticky_topics`, `join_cooperative_sticky_topics`,
and `join_consumer_topics`
subscribe to several topics. Range and sticky assign each topic independently
among members who subscribed to it. Sticky rebalances load when a member joins.
Cooperative-sticky (KIP-429) keeps owned partitions until the owner revokes
them, then rejoins so the new owner can take them. `ConsumerConfig::group_instance_id` is
Kafka `group.instance.id` (static membership) on JoinGroup, Heartbeat, and
KIP-848 heartbeats. `ConsumerConfig::rack` is also sent on KIP-848.
`ConsumerConfig::auto_offset_reset` runs when OffsetFetch has no committed
offset (`Earliest` by default). `ConsumerConfig::allow_auto_create_topics` is
Kafka `allow.auto.create.topics` on Metadata (default `false`). `committed` / `committed_timeout` are OffsetFetch
for the current assignment (Java `committed` / `committed(Duration)`). `commit` / `commit_timeout` are Java `commitSync` / `commitSync(Duration)`. `commit_offsets` commits caller-chosen offsets
([`TopicPartition`](../src/consumer.rs) plus the next fetch offset).
`commit_with_metadata` sends [`OffsetAndMetadata`](../src/consumer.rs)
(leader epoch and a metadata string). `commit_with_metadata_timeout` is Java
`commitSync(Map, Duration)`. Pass
[`ConsumerRecords::next_offsets`](../src/consumer.rs) to match Java
`commitSync(records.nextOffsets())`. `committed` returns the same type.
`current_lag` is high watermark minus position. `subscription` is the
topic list. `enforce_rebalance` rejoins on the next poll.
`subscribe` / `unsubscribe` change the topic list without dropping the
handle. `subscribe_matching` / `join_matching` / `join_sticky_matching` /
`join_cooperative_sticky_matching` / `join_consumer_matching` are Java
`subscribe(Pattern)` (re-list cluster topics on poll when `metadata.max.age.ms`
elapses; names starting with `__` are skipped). Share groups have the same
`subscribe_matching` / `join_matching`. `group_metadata` is Java `ConsumerGroupMetadata`. `list_topics`
is cluster Metadata. `fetch_timeout` / `poll_timeout` are Java
`poll(Duration)`. [`Producer::send_offsets_to_transaction`](../src/producer.rs)
takes [`TopicPartition`](../src/consumer.rs).
[`Producer::send_offsets_with_metadata`](../src/producer.rs)
commits transactional offsets with epoch and a metadata string.
`enable.auto.commit` is off by default; a zero interval commits after every
`poll`. `ConsumerConfig::session_timeout` / `heartbeat_interval` control classic
JoinGroup and the heartbeat loop. `on_rebalance` is `(revoked, assigned)`.
`max.poll.interval.ms` errors on the next `poll` if exceeded (`Error::MaxPollInterval`)
and the heartbeat thread leaves the group.
`Producer::metrics` / `Consumer::metrics` / `ShareGroup::metrics` / `Admin::metrics` are counter snapshots
plus latency min/mean/max and p50/p99 over the last 1024 samples (produce-ack / fetch round / Admin RPC),
and per-topic rows on `ProducerMetrics::topics` / `ConsumerMetrics::topics` /
`ShareMetrics::topics`. `AdminMetrics` is Java `Admin.metrics()`.
`client_instance_id` is Java `clientInstanceId` (KIP-714).
`Consumer::wakeup` (and a cloneable [`WakeupHandle`](../src/consumer.rs)) interrupts
fetch. `ProducerConfig::interceptor` / `ConsumerConfig::interceptor` observe or rewrite
records. [`TopicPartition`](../src/consumer.rs) and `offsets_for_times` are Java
`offsetsForTimes` (`OffsetAndTimestamp.leader_epoch` is Java `getLeaderEpoch`).
leader epoch. `FetchedRecord.serialized_key_size` / `serialized_value_size`
match Java. `assign_many` / `assign_partitions` / `unassign` replace or drop a
manual assignment (`assign_partitions` is Java `assign(Collection)` and uses
`auto.offset.reset`).
`Consumer::close` / `Consumer::close_timeout` drop fetch connections.
`ConsumerGroup::close_timeout` / `ShareGroup::close_timeout` cap `leave`
(Java `close(Duration)`).
`Admin::close_timeout` is Java `close(Duration)` (unused; no LeaveGroup).

`ShareGroup` is KIP-932 queue sharing. `join_topics` subscribes to several
topics.

## Wire format notes (for people changing encode/decode)

- Request `ClientId` is always a classic nullable string, even on flexible headers.
- ApiVersions **response** header is never flexible. If you parse it as flexible you eat the error code.
- Produce throttle time comes **after** the topic array. Metadata throttle time comes first.
- Record batch magic 2 CRC is CRC32-C over bytes from attributes to the end.
- Record lengths are zigzag varints. Compact protocol lengths are unsigned varint of `n+1` (`0` means null).
- Without `InitProducerId`, producer id / epoch / sequence must be `-1`. Zero is a real id.
- InitProducerId v0–v1 are classic; v2–v5 are flexible (compact transactional id plus tagged fields; request header 2, response header 1). v3+ adds ProducerId / ProducerEpoch on the request (KIP-360; first init sends `-1` / `-1`). v4 is PRODUCER_FENCED; v5 is TRANSACTION_ABORTABLE (KIP-890). Kafka 4.0 `validVersions` is `0-5`. v6+ (KIP-939 2PC) is not spoken.
- `acks=0` means the broker sends no Produce response. Do not read one.
- This client uses Produce versions 3–9 (v3–v8 classic record bytes; v9 is compact arrays/strings/bytes plus tagged fields; request header 2, response header 1). Kafka 4.0 removed v0–v2. v10+ (KIP-951 CurrentLeader tagged fields) is not spoken.
- Fetch v11 is classic (RackId is a non-nullable STRING). v12 is flexible (compact arrays/strings/bytes plus tagged fields; LastFetchedEpoch after FetchOffset; request header 2, response header 1). Kafka 4.0 removed v0–v3. v13+ (topic IDs, KIP-516) is not spoken. This crate sends LastFetchedEpoch `-1`.
- ListOffsets v4+ has `current_leader_epoch` before timestamp. The v4+ response has `leader_epoch` after offset. v1–v5 are classic; v6 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). Kafka 4.0 removed v0. v7+ (max timestamp, KIP-734) is not spoken.
- AddPartitionsToTxn v0–v2 are classic; v3 is flexible (compact strings/arrays plus tagged fields on topics / top-level; request header 2, response header 1). This crate speaks 0–3. v4+ (batched transactions, KIP-890 broker layout) is not spoken.
- FindCoordinator v1–v2 are classic (Key + KeyType). v3 is flexible (compact key plus tagged fields; request header 2, response header 1). This crate speaks 1–3. v0 (no KeyType) and v4+ (KIP-699 CoordinatorKeys batch) are not spoken.
- ApiVersions v3+ response tagged fields (KIP-482): 0 `supportedFeatures` (name, min, max), 1 `finalizedFeaturesEpoch` INT64 (`-1` omitted), 2 `finalizedFeatures` (name, **max** then min), 3 `zkMigrationReady`. Empty/default tags are omitted.

## Compression

gzip uses `flate2` with its Rust backend. snappy uses the `snap` crate
(snappy-java framing on produce, raw snappy accepted on fetch). lz4 uses
`lz4_flex` LZ4 frames (independent 64KiB blocks, proper header checksum for
magic ≥ 1). zstd is not implemented; the Kafka ecosystem codec is typically C
(`zstd-sys`).

## TLS

Set `ProducerConfig.tls` / `ConsumerConfig.tls` to a `TlsConfig`. Handshake is
`rustls` with the `ring` backend (not OpenSSL). Custom CA PEM, or Mozilla
roots if `ca_pem` is omitted. Optional client cert/key for mTLS. SNI defaults
to the bootstrap host. Plain TCP stays a `TcpStream`; TLS is a separate
connection type so the uncompressed hot path does not pay for rustls.

Writes pump reads into the connection buffer. TLS (and a full TCP window)
can otherwise stall `poll_write` until `poll_read` runs, which deadlocks a
pipelined producer that only reads after `max_in_flight` writes.
