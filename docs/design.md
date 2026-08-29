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
`seek_with_metadata` is Java `seek(TopicPartition, OffsetAndMetadata)`:
the offset is the next fetch position and the leader epoch is Fetch
`LastFetchedEpoch` (KIP-320). `seek` / `seek_to` still clear the epoch.
`partitions_for` / `beginning_offsets` / `end_offsets` wrap Metadata and
ListOffsets and take `TopicPartition`. Each has a `_timeout` variant
(Java `partitionsFor(String, Duration)` / `beginningOffsets` /
`endOffsets` / `listTopics(Duration)` / `offsetsForTimes(Map, Duration)`).
`partitions_for` includes leader epoch
and offline replicas (Java `offlineReplicas`). `list_offset` is ListOffsets for one
partition. `Admin::list_offsets` is Java `Admin.listOffsets` (earliest / latest /
timestamp; one ListOffsets RPC per partition leader; returns
`OffsetAndTimestamp`). `Admin::list_offsets_with_isolation` is Java
`listOffsets` plus `ListOffsetsOptions.isolationLevel`.
`Admin::list_offsets_timeout` / `list_offsets_with_isolation_timeout` are Java
`ListOffsetsOptions.timeoutMs` (RPC deadline and ListOffsets v10 TimeoutMs).
`Admin::fence_producers` is Java
`Admin.fenceProducers` (InitProducerId on the transaction coordinator).
`Admin::fence_producers_timeout` is Java `FenceProducersOptions.timeoutMs`
(RPC deadline and `transaction.timeout.ms`).
`Admin::force_terminate_transaction` is Java `forceTerminateTransaction`
(same InitProducerId fence for one `transactional.id`).
`Admin::force_terminate_transaction_timeout` is the same plus timeout.
`Admin::abort_transaction` is Java `abortTransaction` (WriteTxnMarkers
ABORT on the partition leader).
`Admin::remove_members_from_consumer_group` is Java
`removeMembersFromConsumerGroup` (LeaveGroup v3–v5 by `group.instance.id`;
v5 sends `DEFAULT_LEAVE_GROUP_REASON`).
`Admin::remove_all_members_from_consumer_group` is Java
`RemoveMembersFromConsumerGroupOptions.removeAll` (DescribeGroups then LeaveGroup).
`Admin::describe_features` is Java `describeFeatures` (ApiVersions v3–v4
tagged fields; v4 SupportedFeatures.MinVersion 0, KAFKA-17011;
[`FeatureMetadata`](../src/admin.rs)).
`Admin::list_topics` / `Admin::describe_topics` are Java `listTopics` /
`describeTopics`. `Admin::describe_classic_groups` is Java
`describeClassicGroups` (DescribeGroups v0–v6). `Admin::describe_consumer_groups` is Java
`describeConsumerGroups` (DescribeGroups v0–v6). `Admin::consumer_group_describe` is
ConsumerGroupDescribe v0–v1 (flexible from v0; v1 MemberType). `Admin::list_consumer_groups` is Java
`listConsumerGroups` (ListGroups v0–v5). `Admin::delete_consumer_groups` is Java
`deleteConsumerGroups` (DeleteGroups v0–v2; classic through v1, flexible v2,
throttle v0+). `Admin::describe_share_groups` is Java
`describeShareGroups` (ShareGroupDescribe v0–v1). `Admin::list_client_metrics_resources` is Java
`listClientMetricsResources` (ListConfigResources v0–v1 CLIENT_METRICS). `Admin::list_share_group_offsets` is Java
`listShareGroupOffsets` (DescribeShareGroupOffsets). `Admin::delete_consumer_group_offsets` is Java
`deleteConsumerGroupOffsets` (OffsetDelete). `Admin::delete_share_groups` is Java
`deleteShareGroups` (DeleteGroups v0–v2). `Admin::describe_client_quotas` /
`Admin::alter_client_quotas` are Java `describeClientQuotas` /
`alterClientQuotas` (v0–v1; classic v0, flexible v1). `Admin::alter_replica_log_dirs` is Java
`alterReplicaLogDirs` (v1–v2; classic v1, flexible v2). `Admin::create_delegation_token` is Java
`createDelegationToken` (v1–v3; classic v1, flexible v2, owner/requester v3). `Admin::renew_delegation_token` is Java
`renewDelegationToken` (v1–v2; classic v1, flexible v2). `Admin::expire_delegation_token` is Java
`expireDelegationToken` (v1–v2; classic v1, flexible v2). `Admin::describe_delegation_token` is Java
`describeDelegationToken` (v1–v3; classic v1, flexible v2, TokenRequester v3). `Admin::describe_replica_log_dirs` is Java
`describeReplicaLogDirs`. `Admin::describe_broker_log_dirs` is Java
`describeLogDirs(Collection<Integer>)` (null-topics DescribeLogDirs v1–v4 on
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
KIP-848 heartbeats (ConsumerGroupHeartbeat v0–v1; v1 client-generated member id). `ConsumerConfig::rack` is also sent on KIP-848.
`ConsumerConfig::auto_offset_reset` runs when OffsetFetch has no committed
offset (`Earliest` by default). `ConsumerConfig::allow_auto_create_topics` is
Kafka `allow.auto.create.topics` on Metadata (default `false`). `committed` / `committed_timeout` are OffsetFetch
for the current assignment (Java `committed` / `committed(Duration)`). `commit` / `commit_timeout` are Java `commitSync` / `commitSync(Duration)`. `commit_offsets` commits caller-chosen offsets
([`TopicPartition`](../src/consumer.rs) plus the next fetch offset).
`seek_with_metadata` takes [`OffsetAndMetadata`](../src/consumer.rs)
(Java `seek(TopicPartition, OffsetAndMetadata)`; Fetch `LastFetchedEpoch`
from the leader epoch; metadata string ignored).
`commit_with_metadata` sends [`OffsetAndMetadata`](../src/consumer.rs)
(leader epoch and a metadata string). `commit_with_metadata_timeout` is Java
`commitSync(Map, Duration)`. Pass
[`ConsumerRecords::next_offsets`](../src/consumer.rs) to match Java
`commitSync(records.nextOffsets())`. `committed` returns the same type.
`commit_async` / `commit_async_with` are Java `commitAsync` / `commitAsync(OffsetCommitCallback)`:
the OffsetCommit is queued and sent on the next poll, leave, close, or unsubscribe (no spawned task).
`commit_with_metadata_async` is Java `commitAsync(Map, …)`.
`current_lag` is high watermark minus position. `subscription` is the
topic list. `enforce_rebalance` / `enforce_rebalance_with` rejoin on the
next poll (Java `enforceRebalance` / `enforceRebalance(String)`; JoinGroup
v8+ Reason, default `"rebalance enforced by user"`).
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
`client_instance_id_timeout` is Java `clientInstanceId(Duration)` (GetTelemetrySubscriptions RPC deadline; cached after the first successful call).
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
- InitProducerId v0–v1 are classic; v2–v5 are flexible (compact transactional id plus tagged fields; request header 2, response header 1). v3+ adds ProducerId / ProducerEpoch on the request (KIP-360; first init sends `-1` / `-1`). After UNKNOWN_PRODUCER_ID the idempotent producer bumps the epoch locally and retries. After UNKNOWN_PRODUCER_ID / INVALID_PRODUCER_EPOCH / INVALID_PRODUCER_ID_MAPPING, a transactional abort still EndTxn-aborts (a Produce that already completed `send` with that error does not fail abort) and re-inits with the last producer id and epoch when EndTxn is below v5 (EndTxn v5 already returns the bumped identity). `commit_transaction` still fails `flush` on the Produce error. v4 is PRODUCER_FENCED; v5 is TRANSACTION_ABORTABLE (KIP-890). Kafka 4.0 `validVersions` is `0-5`. v6+ (KIP-939 2PC) is not spoken.
- `acks=0` means the broker sends no Produce response. Do not read one.
- This client uses Produce versions 3–12 (v3–v8 classic record bytes; v9–v12 are compact arrays/strings/bytes plus tagged fields; request header 2, response header 1). v10+ adds partition CurrentLeader tagged field 0 (KIP-951; `LeaderId` INT32 + `LeaderEpoch` INT32 + nested tagged fields) and top-level NodeEndpoints tagged field 0 (compact array of `NodeId` INT32 + `Host` compact STRING + `Port` INT32 + `Rack` compact nullable STRING + nested tagged fields). When Produce fails with a retriable error and CurrentLeader names a known broker, the producer patches that partition’s leader cache and skips a Metadata refresh. Unknown CurrentLeader brokers are inserted from NodeEndpoints first, then applied the same way. v11 is TRANSACTION_ABORTABLE (same layout as v10). v12 is the same layout (KIP-890 Part 2 transaction V2). When the broker advertises v12, transactional produce skips AddPartitionsToTxn (the partition leader performs that work). Kafka 4.0 removed v0–v2. Kafka 4.0 `validVersions` is `3-12`. v13+ (topic IDs) is not spoken.
- Fetch v11 is classic (RackId is a non-nullable STRING). v12–v17 are flexible (compact arrays/strings/bytes plus tagged fields; LastFetchedEpoch after FetchOffset; request header 2, response header 1). v13 replaces topic names with topic ids on request Topics, ForgottenTopics, and response Responses (KIP-516). v14 is the same layout as v13 (`OffsetMovedToTieredStorageException`, KIP-405). v15 drops untagged ReplicaId; ReplicaState is tagged field 1 (KIP-903; this crate omits it because consumer defaults are `-1` / `-1`). v16 is the same request as v15 (KIP-951). Partition CurrentLeader tagged field 1 (`LeaderId` INT32 + `LeaderEpoch` INT32 + nested tagged fields) is decoded from v12+; when Fetch fails with a retriable error and CurrentLeader names a known broker, the consumer patches that partition’s leader cache and retries without a Metadata refresh. Unknown CurrentLeader brokers are inserted from top-level NodeEndpoints tagged field 0 (v16+; same inner layout as Produce) first, then applied the same way. Preferred-replica redirects and DivergingEpoch seeks also retry without Metadata. v17 is the same consumer request as v16 (ReplicaDirectoryId tagged field 0 is follower-only and omitted). Kafka 4.0 removed v0–v3. Kafka 4.0 `validVersions` is `4-17`. This crate speaks 4–17. v18+ (KIP-1166 HighWatermark) is not spoken. This crate sends LastFetchedEpoch from the last consumed record-batch leader epoch (`-1` after assign/seek until a batch is consumed, and from OffsetFetch `leader_epoch` on group assign). Fetch v12+ DivergingEpoch tagged field 0 (`Epoch` INT32 + `EndOffset` INT64 + nested tagged fields) is decoded; when present the consumer seeks to that end offset and retries without waiting `retry.backoff.ms`.
- ListOffsets v4+ has `current_leader_epoch` before timestamp. The v4+ response has `leader_epoch` after offset. v1–v5 are classic; v6–v10 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). v7 is MAX_TIMESTAMP `-3` (KIP-734). v8 is EARLIEST_LOCAL `-4` (KIP-405). v9 is LATEST_TIERED `-5` (KIP-1005). v10 adds TimeoutMs after Topics (KIP-1075; this crate sends `request_timeout`, or the one-shot timeout from `list_offsets_timeout` / `list_offsets_with_isolation_timeout`). Kafka 4.0 `validVersions` is `1-10`. This crate speaks 1–10. v11+ is not spoken.
- OffsetForLeaderEpoch v0–v3 are classic; v4 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1). v1 response adds LeaderEpoch. v2 adds CurrentLeaderEpoch on the request and ThrottleTimeMs on the response. v3 adds ReplicaId (`-1` for a consumer). Kafka 4.0 `validVersions` is `2-4` (v0–v1 removed). This crate speaks 0–4. v5+ is not spoken.
- AddPartitionsToTxn v0–v2 are classic; v3 is flexible (compact strings/arrays plus tagged fields on topics / top-level; request header 2, response header 1). This crate speaks 0–3. v4+ (batched transactions, KIP-890 broker layout) is not spoken.
- AddOffsetsToTxn v0–v2 are classic; v3–v4 are flexible (compact strings plus tagged fields; request header 2, response header 1). v4 is TRANSACTION_ABORTABLE (KIP-890; same layout as v3). Kafka 4.0 `validVersions` is `0-4`. This crate speaks 0–4. v5+ is not spoken.
- EndTxn v0–v2 are classic; v3–v5 are flexible (compact strings plus tagged fields; request header 2, response header 1). v4 is TRANSACTION_ABORTABLE (KIP-890; same request layout as v3). v5 adds ProducerId / ProducerEpoch on the response (KIP-890 Part 2). After a successful EndTxn v5 the producer stores those fields when `producer_id >= 0` and clears per-partition sequences if the identity changed. Kafka 4.0 `validVersions` is `0-5`. This crate speaks 0–5. v6+ is not spoken.
- TxnOffsetCommit v0–v2 are classic (v2 adds committed leader epoch). v3–v5 are flexible (compact strings/arrays plus tagged fields on partitions / topics / top-level; request header 2, response header 1) and add GenerationId / MemberId / GroupInstanceId. `send_offsets_for_group` sends those fields from `ConsumerGroupMetadata`; `send_offsets_to_transaction` sends `-1` / empty / null. v4 is TRANSACTION_ABORTABLE (KIP-890; same layout as v3). v5 is the same layout (KIP-890 Part 2 transaction V2). When the broker advertises v5, `send_offsets_*` skips AddOffsetsToTxn (the group coordinator performs that work). Kafka 4.0 `validVersions` is `0-5`. This crate speaks 0–5. v6+ is not spoken.
- FindCoordinator v1–v2 are classic (Key + KeyType). v3 is flexible (compact key plus tagged fields; request header 2, response header 1). v4–v6 replace Key with CoordinatorKeys and the top-level coordinator fields with Coordinators (KIP-699; v5 is TRANSACTION_ABORTABLE; v6 is share groups, KIP-932). This crate speaks 1–6. v0 (no KeyType) and v7+ are not spoken.
- OffsetCommit v2–v7 are classic; v8–v9 are flexible (compact strings/arrays plus tagged fields on partitions / topics / top-level; request header 2, response header 1). Official JSON: v3 and v4 match v2 (RetentionTimeMs; this crate sends `-1`). v5 drops retention. v6 CommittedLeaderEpoch. v7 GroupInstanceId. v3+ ThrottleTimeMs. v9 is KIP-848 error codes (same layout as v8). Kafka 4.0 `validVersions` is `2-9` (v0–v1 removed). This crate speaks 2–9. v0–v1 and v10+ are not spoken.
- OffsetFetch v1–v5 are classic; v6–v9 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1). Official JSON: v3, v4, and v5 match v2 on the request (GroupId, Topics). v2 nullable Topics and top-level ErrorCode. v3 ThrottleTimeMs. v5 CommittedLeaderEpoch (decode fills `-1` below v5). v7 RequireStable (`true` when `isolation.level` is read-committed). v8 replaces GroupId / Topics with Groups (KIP-709; this crate sends one group). v9 adds MemberId / MemberEpoch on each group (KIP-848; classic groups send null / `-1`). Kafka 4.0 `validVersions` is `1-9` (v0 removed). This crate speaks 1–9. v0 and v10+ (topic IDs) are not spoken.
- Heartbeat v0–v3 are classic; v4 is flexible (compact strings plus tagged fields; request header 2, response header 1). Official JSON: v1 and v2 match v0. v1+ ThrottleTimeMs. v3 GroupInstanceId. Kafka 4.0 `validVersions` is `0-4`. This crate speaks 0–4. v5+ is not spoken.
- SyncGroup v0–v3 are classic; v4–v5 are flexible (compact strings/bytes/arrays plus tagged fields; request header 2, response header 1). Official JSON: v1 and v2 match v0. v1+ ThrottleTimeMs. v3 GroupInstanceId. v5 ProtocolType / ProtocolName (KIP-559; this crate sends `"consumer"` and the selected assignor). Kafka 4.0 `validVersions` is `0-5`. This crate speaks 0–5. v6+ is not spoken.
- JoinGroup v2–v5 are classic; v6–v9 are flexible (compact strings/bytes/arrays plus tagged fields; request header 2, response header 1). Official JSON: v2 and v3 match v1 (RebalanceTimeoutMs). v4 second join with assigned id. v5 GroupInstanceId. v7 response adds ProtocolType (KIP-559) and nullable ProtocolName. v8 adds Reason (KIP-800; first join is null; `enforce_rebalance` / `enforce_rebalance_with` send the reason). v9 adds SkipAssignment; when true the leader does not run the assignor. Kafka 4.0 `validVersions` is `2-9` (v0–v1 removed). This crate speaks 2–9. v0–v1 and v10+ are not spoken.
- LeaveGroup v0–v3 are classic; v4–v5 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1). v0–v2 are GroupId + MemberId (v1 and v2 match v0). v1+ ThrottleTimeMs. v3 Members + GroupInstanceId. v5 Reason (KIP-800). Kafka 4.0 `validVersions` is `0-5`. This crate speaks 0–5. Classic `ConsumerGroup::leave` / `close` send `"the consumer is being closed"`; `unsubscribe` sends `"the consumer unsubscribed from all topics"`; `max.poll.interval.ms` expiry sends `"consumer poll timeout has expired."`. Admin `removeMembersFromConsumerGroup` stays v3–v5 with `"member was removed by an admin"`. v6+ is not spoken.
- ConsumerGroupHeartbeat v0–v1 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1). v1 adds SubscribedTopicRegex after SubscribedTopicNames (KIP-848) and requires the consumer to generate its own MemberId (KIP-1082; Kafka `Uuid` URL-safe Base64). `join_consumer_matching` still expands topics locally (`Fn(&str) -> bool`) and sends SubscribedTopicNames; the regex field is null. Kafka 4.0 `validVersions` is `0-1`. This crate speaks 0–1. v2+ is not spoken. v1 response matches v0.
- ShareGroupHeartbeat v0–v1 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1). Same fields. Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`). Kafka 4.1 `validVersions` is `"1"` (v0 removed). This crate speaks 0–1. v2+ is not spoken. v1 response matches v0.
- ShareGroupDescribe v0–v1 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1). Same fields. Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`). Kafka 4.1 `validVersions` is `"1"` (v0 removed). This crate speaks 0–1. v2+ is not spoken. v1 response matches v0. ErrorCode is per-group. Official Java `DescribeShareGroupsHandler` uses `CoordinatorType.GROUP`.
- ShareFetch v0–v1 are flexible (compact strings/arrays/bytes plus tagged fields; request header 2, response header 1). Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`). Kafka 4.1 `validVersions` is `"1"` (v0 removed). v0 PartitionMaxBytes on each partition. v1 MaxRecords / BatchSize after MaxBytes (no PartitionMaxBytes) and AcquisitionLockTimeoutMs after ErrorMessage. This crate speaks 0–1. v2+ is not spoken.
- ShareAcknowledge v0–v1 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1). Same fields. Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`). Kafka 4.1 `validVersions` is `"1"` (v0 removed). This crate speaks 0–1. v2+ is not spoken. v1 response matches v0.
- SaslHandshake v0–v1 are classic (Apache JSON `flexibleVersions: "none"`). Same fields. v1 enables SaslAuthenticate. Kafka 4.0 `validVersions` is `0-1`. This crate speaks 0–1. v2+ is not spoken (KAFKA-9577).
- SaslAuthenticate v0–v1 are classic. v2 is flexible (compact bytes/strings plus tagged fields; request header 2, response header 1). v0 and v1 request match (AuthBytes). v1+ SessionLifetimeMs. Kafka 4.0 `validVersions` is `0-2`. This crate speaks 0–2. v3+ is not spoken.
- ListTransactions v0–v1 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). v1 adds DurationFilter INT64 after ProducerIdFilters (KIP-994; `< 0` means no filter). Kafka 4.0 `validVersions` is `0-1`. This crate speaks 0–1. v2+ (TransactionalIdPattern) is not spoken. v1 response matches v0.
- CreateTopics v0–v4 are classic. v5–v7 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). v5 returns NumPartitions / ReplicationFactor / Configs (KIP-525). v6 is the same layout (KIP-599 THROTTLING_QUOTA_EXCEEDED). v7 adds TopicId UUID after Name (KIP-516). Kafka 4.0 `validVersions` is `2-7` (v0–v1 removed). This crate speaks 0–7. v8+ is not spoken.
- DeleteTopics v0–v3 are classic (TopicNames + TimeoutMs). v4–v6 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). v5 adds ErrorMessage (KIP-599). v6 replaces TopicNames with Topics of Name + TopicId (KIP-516; this crate deletes by name and sends a zero UUID). Kafka 4.0 `validVersions` is `1-6` (v0 removed). This crate speaks 0–6. v7+ is not spoken.
- DescribeConfigs v0–v3 are classic. v1 adds IncludeSynonyms / ConfigSource / Synonyms. v2 is the same layout as v1. v3 adds IncludeDocumentation, ConfigType, and Documentation (KIP-226). v4 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). Kafka 4.0 `validVersions` is `1-4` (v0 removed). This crate speaks 0–4. v5+ is not spoken. `describe_configs_with_documentation` is Java `DescribeConfigsOptions.includeDocumentation`; v0–v2 omit the field.
- CreatePartitions v0–v1 are classic. v2–v3 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). v3 is the same layout (KIP-599 THROTTLING_QUOTA_EXCEEDED). Kafka 4.0 `validVersions` is `0-3`. This crate speaks 0–3. v4+ is not spoken.
- IncrementalAlterConfigs v0 is classic. v1 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). Kafka 4.0 `validVersions` is `0-1`. This crate speaks 0–1. v2+ is not spoken.
- AlterReplicaLogDirs v1 is classic. v2 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). Same fields. Kafka 4.0 `validVersions` is `1-2` (v0 removed). This crate speaks 1–2. v0 and v3+ are not spoken.
- DescribeLogDirs v1 is classic. v2–v4 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). v3 top-level ErrorCode (KIP-784). v4 TotalBytes / UsableBytes (KIP-827; decode fills `-1` on v1–v3). Kafka 4.0 `validVersions` is `1-4` (v0 removed). This crate speaks 1–4. v0 and v5+ are not spoken. v5 is a named STATUS hole.
- CreateDelegationToken v1 is classic. v2–v3 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). v3 OwnerPrincipalType / OwnerPrincipalName and TokenRequesterPrincipalType / TokenRequesterPrincipalName (decode fills `None` / empty on v1–v2). ErrorCode is the first field (bytes 0–1); ThrottleTimeMs is last. Kafka 4.0 `validVersions` is `1-3` (v0 removed). This crate speaks 1–3. v0 and v4+ are not spoken. Broker-only (`LeastLoadedNodeProvider`); broker-side `forwardToController` is not a client 41 hop.
- RenewDelegationToken v1 is classic. v2 is flexible (compact bytes plus tagged fields; request header 2, response header 1). Same fields. ErrorCode is the first field (bytes 0–1); ThrottleTimeMs is last. Kafka 4.0 `validVersions` is `1-2` (v0 removed). This crate speaks 1–2. v0 and v3+ are not spoken. Broker-only (`LeastLoadedNodeProvider`); broker-side `forwardToController` is not a client 41 hop.
- ExpireDelegationToken v1 is classic. v2 is flexible (compact bytes plus tagged fields; request header 2, response header 1). Same fields. ErrorCode is the first field (bytes 0–1); ThrottleTimeMs is last. Kafka 4.0 `validVersions` is `1-2` (v0 removed). This crate speaks 1–2. v0 and v3+ are not spoken. Broker-only (`LeastLoadedNodeProvider`); broker-side `forwardToController` is not a client 41 hop.
- DescribeDelegationToken v1 is classic. v2–v3 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). Request Owners is the same on v1–v3. v3 TokenRequesterPrincipalType / TokenRequesterPrincipalName on each token (decode fills empty on v1–v2). ErrorCode is the first field (bytes 0–1); ThrottleTimeMs is last. Kafka 4.0 `validVersions` is `1-3` (v0 removed). This crate speaks 1–3. v0 and v4+ are not spoken. Broker-only (`LeastLoadedNodeProvider`); the handler does not `forwardToController`.
- DescribeGroups v0–v4 are classic. v5–v6 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1). v1 ThrottleTimeMs. v3 IncludeAuthorizedOperations / AuthorizedOperations. v4 GroupInstanceId. v6 ErrorMessage and GROUP_ID_NOT_FOUND (KIP-1043). Kafka 4.0 `validVersions` is `0-6`. This crate speaks 0–6. v7+ is not spoken.
- ListGroups v0–v2 are classic. v3–v5 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1). v1 ThrottleTimeMs. v4 StatesFilter / GroupState (KIP-518). v5 TypesFilter / GroupType (KIP-848). Kafka 4.0 `validVersions` is `0-5`. This crate speaks 0–5. v6+ is not spoken.
- AlterConfigs (legacy api 33) v0–v1 are classic. v1 response adds ThrottleTimeMs (KIP-219). v2 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). Kafka 4.0 `validVersions` is `0-2`. This crate speaks 0–2. v3+ is not spoken.
- DeleteRecords v0–v1 are classic. v1 response adds ThrottleTimeMs (KIP-219). v2 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). Kafka 4.0 `validVersions` is `0-2`. This crate speaks 0–2. v3+ is not spoken.
- CreateAcls / DescribeAcls / DeleteAcls v0–v1 are classic. v1 adds ResourcePatternType / PatternTypeFilter (LITERAL on create; ANY on describe/delete filters). v2–v3 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). v3 is the same layout (user resource type). Kafka 4.0 `validVersions` is `1-3` (v0 removed). This crate speaks 0–3. v4+ is not spoken.
- Metadata v1–v8 are classic; v9–v13 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1). v12 supports topicId. v13 adds top-level ErrorCode INT16 after Topics (and after ClusterAuthorizedOperations on v8–v10) and before tagged fields. Kafka 4.0 `validVersions` is `0-13`. This crate speaks 1–13. v0 (empty array means all topics) and v14+ are not spoken.
- ApiVersions v0–v2 are classic (empty request; v1+ ThrottleTimeMs). v3–v4 are flexible in the body only (compact strings plus tagged fields; request header 2; response header stays 0, KIP-482). v3 adds ClientSoftwareName / ClientSoftwareVersion. v4 is the same request as v3 and allows SupportedFeatures.MinVersion 0 (KAFKA-17011 / KAFKA-17492; v0–v3 omit those features). Tagged fields: 0 `supportedFeatures` (name, min, max), 1 `finalizedFeaturesEpoch` INT64 (`-1` omitted), 2 `finalizedFeatures` (name, **max** then min), 3 `zkMigrationReady`. Empty/default tags are omitted. Kafka 4.0 `validVersions` is `0-4`. This crate speaks 0–4 and sends v4 on connect. When the broker returns `UNSUPPORTED_VERSION` (KIP-511, brokers 2.4+), the error body is v0 and lists the supported ApiVersions range; the client retries at `pick_version(broker_min, broker_max, 0, 4)`. v5+ is not spoken.

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
