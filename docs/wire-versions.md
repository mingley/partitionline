# Wire versions

This page is the field-level companion to the [design notes](design.md#wire-format-notes). Each section is one API. The first line is the versions this crate speaks. The paragraphs after it are the layout: classic or flexible encoding, where throttle time sits, and which KIP added a field.

Rust behavior is authoritative. A Java name is a porting cross-link, not a second specification.

## Layout rules
- Request `ClientId` is always a classic nullable string, even on flexible headers.
- ApiVersions **response** header is never flexible. If you parse it as flexible you eat the error code.
- Produce throttle time comes **after** the topic array. Metadata throttle time comes first.
- Record batch magic 2 CRC is CRC32-C over bytes from attributes to the end.
- Record lengths are zigzag varints. Compact protocol lengths are unsigned varint of `n+1` (`0` means null).
- Without `InitProducerId`, producer id / epoch / sequence must be `-1`. Zero is a real id.
- `acks=0` means the broker sends no Produce response. Do not read one.

## APIs

### InitProducerId

Spoken versions: 0–6.

InitProducerId v0–v1 are classic; v2–v6 are flexible (compact transactional id plus tagged fields; request header 2, response header 1).

v3+ adds ProducerId / ProducerEpoch on the request (KIP-360; first init sends `-1` / `-1`).

After UNKNOWN_PRODUCER_ID the idempotent producer bumps the epoch locally and retries.

After UNKNOWN_PRODUCER_ID / INVALID_PRODUCER_EPOCH / INVALID_PRODUCER_ID_MAPPING, a transactional abort still EndTxn-aborts (a Produce that already completed `send` with that error does not fail abort) and re-inits with the last producer id and epoch when EndTxn is below v5 (EndTxn v5 already returns the bumped identity).

`commit_transaction` still fails `flush` on the Produce error.

v4 is PRODUCER_FENCED; v5 is TRANSACTION_ABORTABLE (KIP-890).

ThrottleTimeMs is JSON `0+` (`encode_init_producer_id_response_with_throttle`; encode previously always wrote `0` on v1+ and omitted the field on v0; decode discarded it; convenience encode still writes `0`; official Java `InitProducerIdResponse.throttleTimeMs` / `InitProducerIdResponseData.throttleTimeMs`; Java `getErrorResponse` sets `throttleTimeMs` to `0` even when the argument is non-zero).

`InitProducerIdResponse.error_counts` is Java `InitProducerIdResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`).

Empty-error v0 and v1 bodies match; v2, v3, v4, and v5 bodies match.

Top-level ErrorCode is at bytes 4–5.

Kafka 4.0 `validVersions` is `0-5`.

v6 adds `enable2Pc` and `keepPreparedTxn` (KIP-939). Idempotent and ordinary transactional producers negotiate 0–6 and send both flags false. A true flag is rejected by an older peer before the request is sent. Prepared-transaction lifecycle operations are separate from this initialization. v2 through v6 share the flexible header.

v7+ is not spoken.

### Produce

Spoken versions: 3–13.

This client uses Produce versions 3–12 (v3–v8 classic record bytes; v9–v12 are compact arrays/strings/bytes plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `1+` after Responses (`encode_produce_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-Responses v3, v4, v5, v6, v7, and v8 bodies match; v9, v10, v11, and v12 bodies match.

There is no top-level ErrorCode.

Official Java `getErrorResponse` sets `throttleTimeMs` from the argument.

v10+ adds partition CurrentLeader tagged field 0 (KIP-951; `LeaderId` INT32 + `LeaderEpoch` INT32 + nested tagged fields) and top-level NodeEndpoints tagged field 0 (compact array of `NodeId` INT32 + `Host` compact STRING + `Port` INT32 + `Rack` compact nullable STRING + nested tagged fields).

When Produce fails with a retriable error and CurrentLeader names a known broker, the producer patches that partition’s leader cache and skips a Metadata refresh.

Unknown CurrentLeader brokers are inserted from NodeEndpoints first, then applied the same way.

v11 is TRANSACTION_ABORTABLE (same layout as v10).

v12 is the same layout (KIP-890 Part 2 transaction V2).

When the broker advertises v12, transactional produce skips AddPartitionsToTxn (the partition leader performs that work).

Kafka 4.0 removed v0–v2.

Kafka 4.0 `validVersions` is `3-12`.

v13 uses topic IDs. The producer negotiates 3–13. It stays at 12 when Metadata is below v10. A transactional producer stays at v11 unless the cluster has finalized transaction version 2 and EndTxn v5. v14+ is not spoken.

### Fetch

Spoken versions: 4–18.

Fetch v11 is classic (RackId is a non-nullable STRING).

Request SessionId / SessionEpoch are v7+ (`FetchMetadata`; `encode_fetch_request` writes LEGACY; a non-LEGACY session is omitted below v7).

ForgottenTopicsData is v7+ (`encode_fetch_request_with_forgotten`; `encode_fetch_request` writes empty; a non-empty list is omitted below v7; v13+ uses TopicId).

Request MaxWaitMs is JSON `0+` (decode returns it; encode already takes `max_wait_ms`; ReplicaId is untagged through v14 then MaxWaitMs; v15+ MaxWaitMs is the first untagged field).

Request MinBytes is JSON `0+` (decode returns it; encode already takes `min_bytes`).

Request ReplicaId is JSON `0-14` (untagged INT32; `encode_fetch_request_with_replica_id`; encode previously always wrote `CONSUMER_REPLICA_ID` and decode discarded; convenience encode still writes `CONSUMER_REPLICA_ID`; v15+ omit even when non-default and decode fills `CONSUMER_REPLICA_ID`; not ReplicaState tagged field 1).

Request partition LogStartOffset is JSON `5+` (INT64 after LastFetchedEpoch; encode writes `FetchPartition.log_start_offset`; below v5 omit even when non-default and decode fills `INVALID_LOG_START_OFFSET`; official Java `FetchRequest.PartitionData.logStartOffset`).

v12–v18 are flexible (compact arrays/strings/bytes plus tagged fields; LastFetchedEpoch after FetchOffset; request header 2, response header 1).

ThrottleTimeMs is JSON `1+` first field (`encode_fetch_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-Responses v4, v5, and v6 bodies match; v7, v8, v9, v10, and v11 bodies match; v12, v13, v14, v15, v16, and v17 bodies match.

Top-level ErrorCode is at bytes 4–5 on v7+.

Official Java `getErrorResponse` sets `throttleTimeMs` from the argument.

v13 replaces topic names with topic ids on request Topics, ForgottenTopics, and response Responses (KIP-516).

v14 is the same layout as v13 (`OffsetMovedToTieredStorageException`, KIP-405).

v15 drops untagged ReplicaId; ReplicaState is tagged field 1 (KIP-903; this crate omits it because consumer defaults are `-1` / `-1`).

v16 is the same request as v15 (KIP-951).

Partition CurrentLeader tagged field 1 (`LeaderId` INT32 + `LeaderEpoch` INT32 + nested tagged fields) is decoded from v12+; when Fetch fails with a retriable error and CurrentLeader names a known broker, the consumer patches that partition’s leader cache and retries without a Metadata refresh.

Unknown CurrentLeader brokers are inserted from top-level NodeEndpoints tagged field 0 (v16+; same inner layout as Produce) first, then applied the same way.

Preferred-replica redirects and DivergingEpoch seeks also retry without Metadata.

v17 is the same consumer request as v16 (ReplicaDirectoryId tagged field 0 is follower-only and omitted).

Kafka 4.0 removed v0–v3.

Kafka 4.0 `validVersions` is `4-17`.

v18 adds request partition HighWatermark tag 1 (KIP-1166). Ordinary consumers omit that tag. The response layout matches v17. v19+ is not spoken.

This crate sends LastFetchedEpoch from the last consumed record-batch leader epoch (`-1` after assign/seek until a batch is consumed, and from OffsetFetch `leader_epoch` on group assign).

Fetch v12+ DivergingEpoch tagged field 0 (`Epoch` INT32 + `EndOffset` INT64 + nested tagged fields) is decoded; when present the consumer seeks to that end offset and retries without waiting `retry.backoff.ms`.

Partition SnapshotId tagged field 2 (`EndOffset` INT64 then `Epoch` INT32 + nested tagged fields; JSON field order is the reverse of DivergingEpoch) is decoded from v12+; omitted fills `-1` / `-1`.

Below v12 encode omits SnapshotId even when the body is non-default.

This is not the FetchSnapshot API and does not start those RPCs.

### ListOffsets

Spoken versions: 1–11.

ListOffsets v4+ has `current_leader_epoch` before timestamp.

The v4+ response has `leader_epoch` after offset.

v1–v5 are classic; v6–v11 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

Request ReplicaId is JSON `0+` (INT32 first field; `encode_list_offsets_topics_request_with_replica_id`; encode previously always wrote `CONSUMER_REPLICA_ID` and decode discarded; convenience encode still writes `CONSUMER_REPLICA_ID`; official Java `ListOffsetsRequest.replicaId()`).

v7 is MAX_TIMESTAMP `-3` (KIP-734).

v8 is EARLIEST_LOCAL `-4` (KIP-405).

v9 is LATEST_TIERED `-5` (KIP-1005).

v10 adds TimeoutMs after Topics (KIP-1075; this crate sends `request_timeout`, or the one-shot timeout from `list_offsets_timeout` / `list_offsets_with_isolation_timeout`).

Kafka 4.0 `validVersions` is `1-10`. Apache 4.3.1 extends that range through v11.

v11 adds EARLIEST_PENDING_UPLOAD, timestamp `-6` (KIP-1023, `OffsetSpec::earliest_pending_upload`). v12+ is not spoken.

### OffsetForLeaderEpoch

Spoken versions: 0–4.

OffsetForLeaderEpoch v0–v3 are classic; v4 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

v1 response adds LeaderEpoch.

v2 adds CurrentLeaderEpoch on the request and ThrottleTimeMs on the response.

ThrottleTimeMs is JSON `2+` (`encode_offset_for_leader_epoch_topics_response_with_throttle`; encode previously always wrote `0` on v2+ and decode discarded; convenience encode still writes `0`; below v2 encode omits it even when the body is non-zero and decode fills `0`).

Empty-Topics v0 and v1 bodies match; v2 and v3 bodies match; v4 is compact.

There is no top-level ErrorCode.

Official Java `getErrorResponse` does not set `throttleTimeMs` (JSON default `0`).

Request ReplicaId is JSON `3+` (INT32 first field; default `-2`; `encode_offset_for_leader_epoch_topics_request_with_replica_id`; encode previously always wrote `CONSUMER_REPLICA_ID` on v3+ and decode discarded; convenience encode still writes `CONSUMER_REPLICA_ID`; below v3 omit even when non-default and decode fills `DEBUGGING_REPLICA_ID`; official Java `OffsetsForLeaderEpochRequest.replicaId()`).

Fetch `FENCED_LEADER_EPOCH` / `UNKNOWN_LEADER_EPOCH` recover with Topics/Partitions of N (one RPC per current leader).

Kafka 4.0 `validVersions` is `2-4` (v0–v1 removed).

v5+ is not spoken.

### AddPartitionsToTxn

Spoken versions: 0–3.

AddPartitionsToTxn v0–v2 are classic; v3 is flexible (compact strings/arrays plus tagged fields on topics / top-level; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`encode_add_partitions_to_txn_topics_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-ResultsByTopicV3AndBelow v0, v1, and v2 bodies match; v3 is compact.

There is no top-level ErrorCode on spoken versions.

v4+ (batched transactions, KIP-890 broker layout) is not spoken.

### AddOffsetsToTxn

Spoken versions: 0–4.

AddOffsetsToTxn v0–v2 are classic; v3–v4 are flexible (compact strings plus tagged fields; request header 2, response header 1).

v4 is TRANSACTION_ABORTABLE (KIP-890; same layout as v3).

ThrottleTimeMs is JSON `0+` (`encode_add_offsets_to_txn_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Request ProducerId is JSON `0+` (INT64 after TransactionalId; decode previously discarded it; encode already takes `producer_id`; official Java `AddOffsetsToTxnRequestData.producerId`; not EndTxn response ProducerId / InitProducerId / TxnOffsetCommit ProducerId / AddPartitionsToTxn ProducerId / WriteTxnMarkers ProducerId / ProducerEpoch).

Request ProducerEpoch is JSON `0+` (INT16 after ProducerId; decode previously discarded it; encode already takes `producer_epoch`; official Java `AddOffsetsToTxnRequestData.producerEpoch`; not EndTxn response ProducerEpoch / InitProducerId / TxnOffsetCommit ProducerEpoch / AddPartitionsToTxn ProducerEpoch / WriteTxnMarkers ProducerEpoch / ProducerId).

Empty-error v0, v1, and v2 bodies match; v3 and v4 bodies match.

Top-level ErrorCode is at bytes 4–5.

`AddOffsetsToTxnResponse.error_counts` is Java `AddOffsetsToTxnResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`).

Kafka 4.0 `validVersions` is `0-4`.

v5+ is not spoken.

### EndTxn

Spoken versions: 0–5.

EndTxn v0–v2 are classic; v3–v5 are flexible (compact strings plus tagged fields; request header 2, response header 1).

v4 is TRANSACTION_ABORTABLE (KIP-890; same request layout as v3).

v5 adds ProducerId / ProducerEpoch on the response (KIP-890 Part 2).

ThrottleTimeMs is JSON `0+` (`encode_end_txn_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-error v0, v1, and v2 bodies match; v3 and v4 bodies match; v5 adds ProducerId / ProducerEpoch.

Top-level ErrorCode is at bytes 4–5.

`EndTxnResponse.error_counts` is Java `EndTxnResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`).

After a successful EndTxn v5 the producer stores those fields when `producer_id >= 0` and clears per-partition sequences if the identity changed.

Kafka 4.0 `validVersions` is `0-5`.

v6+ is not spoken.

### TxnOffsetCommit

Spoken versions: 0–5.

TxnOffsetCommit v0–v2 are classic (v2 adds committed leader epoch).

v3–v5 are flexible (compact strings/arrays plus tagged fields on partitions / topics / top-level; request header 2, response header 1) and add GenerationId / MemberId / GroupInstanceId.

`send_offsets_for_group` sends those fields from `ConsumerGroupMetadata`; `send_offsets_to_transaction` sends `-1` / empty / null.

v4 is TRANSACTION_ABORTABLE (KIP-890; same layout as v3).

v5 is the same layout (KIP-890 Part 2 transaction V2).

When the broker advertises v5, `send_offsets_*` skips AddOffsetsToTxn (the group coordinator performs that work).

ThrottleTimeMs is JSON `0+` (`encode_txn_offset_commit_topics_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Request ProducerId is JSON `0+` (INT64 after GroupId; decode previously discarded it; encode already takes `producer_id`; official Java `TxnOffsetCommitRequestData.producerId`; not AddOffsetsToTxn ProducerId / EndTxn response ProducerId / InitProducerId / AddPartitionsToTxn ProducerId / WriteTxnMarkers ProducerId / ProducerEpoch).

Request ProducerEpoch is JSON `0+` (INT16 after ProducerId; decode previously discarded it; encode already takes `producer_epoch`; official Java `TxnOffsetCommitRequestData.producerEpoch`; not AddOffsetsToTxn ProducerEpoch / EndTxn response ProducerEpoch / InitProducerId / AddPartitionsToTxn ProducerEpoch / WriteTxnMarkers ProducerEpoch / ProducerId).

Empty-Topics v0, v1, and v2 bodies match; v3, v4, and v5 bodies match.

There is no top-level ErrorCode.

Kafka 4.0 `validVersions` is `0-5`.

v6+ is not spoken.

### FindCoordinator

Spoken versions: 0–6.

FindCoordinator v0 is classic and has no KeyType. It accepts only the GROUP coordinator. v1–v2 are classic (Key + KeyType).

v3 is flexible (compact key plus tagged fields; request header 2, response header 1).

v4–v6 replace Key with CoordinatorKeys and the top-level coordinator fields with Coordinators (KIP-699; v5 is TRANSACTION_ABORTABLE; v6 is share groups, KIP-932).

ThrottleTimeMs is JSON `1+` (`encode_find_coordinator_response_coordinators_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-error v1 and v2 bodies match; v3 is compact; empty-Coordinators v4, v5, and v6 bodies match.

Top-level ErrorCode is at bytes 4–5 on v1–v3; v4+ has no top-level ErrorCode.

`FindCoordinatorResponse.has_error` is Java `FindCoordinatorResponse.hasError` (top-level `errorCode` only; v4+ coordinator codes are ignored).

Official Java `getErrorResponse` sets `throttleTimeMs` from the argument on v2+; v1 leaves the JSON default `0`.

`list_consumer_group_offsets_for_groups`, `describe_groups`, `delete_groups`, `consumer_group_describe`, `share_group_describe`, `describe_share_group_offsets`, and `describe_transactions` send CoordinatorKeys of N on v4+ (one FindCoordinator per coordinator-discovery retry) and one subsequent RPC per coordinator.

Brokers that only speak v0–v3 get one FindCoordinator per key.

v7+ is not spoken.

### OffsetCommit

Spoken versions: 2–10.

OffsetCommit v2–v7 are classic; v8–v10 are flexible (compact strings/arrays plus tagged fields on partitions / topics / top-level; request header 2, response header 1).

Official JSON: v3 and v4 match v2 (RetentionTimeMs; this crate sends `-1`).

v5 drops retention.

v6 CommittedLeaderEpoch.

v7 GroupInstanceId.

Request GenerationIdOrMemberEpoch is JSON `1+` (INT32 after GroupId; decode previously discarded it; encode already takes `generation_id`; official Java `OffsetCommitRequestData.generationIdOrMemberEpoch` / `OffsetCommitRequest.DEFAULT_GENERATION_ID`; not SyncGroup GenerationId / Heartbeat GenerationId / JoinGroup response GenerationId / TxnOffsetCommit GenerationId).

v3+ ThrottleTimeMs.

v9 is KIP-848 error codes (same layout as v8).

v10 replaces each topic name with a 16-byte UUID. Name-based commits stay on v2–v9. The group negotiates 2–10 and uses the UUID body when the broker’s maximum is 10.

Kafka 4.0 `validVersions` is `2-9` (v0–v1 removed).

v0–v1 and v11+ are not spoken.

`alter_consumer_group_offsets_timeout` is Java `AlterConsumerGroupOffsetsOptions.timeoutMs` (RPC deadline; OffsetCommit has no TimeoutMs).

### OffsetFetch

Spoken versions: 1–10.

OffsetFetch v1–v5 are classic; v6–v10 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

Official JSON: v3, v4, and v5 match v2 on the request (GroupId, Topics).

v2 nullable Topics and top-level ErrorCode (`None` Topics is every committed partition; `Admin::list_all_consumer_group_offsets`).

v3 ThrottleTimeMs.

v5 CommittedLeaderEpoch (decode fills `-1` below v5).

v7 RequireStable (`true` when `isolation.level` is read-committed; `list_consumer_group_offsets_with` / `list_all_consumer_group_offsets_with` send the Admin flag).

`list_all_consumer_group_offsets_timeout` / `list_consumer_group_offsets_for_groups_timeout` are Java `ListConsumerGroupOffsetsOptions.timeoutMs` (RPC deadline; OffsetFetch has no TimeoutMs).

v8 replaces GroupId / Topics with Groups (KIP-709; `list_consumer_group_offsets_for_groups` sends one Groups array per coordinator).

v9 adds MemberId / MemberEpoch on each group (KIP-848; classic groups send null / `-1`).

v10 replaces each topic name with a 16-byte UUID. The name-only encoder stays on v1–v9. The group negotiates 1–10 and uses the UUID body when the broker’s maximum is 10.

Kafka 4.0 `validVersions` is `1-9` (v0 removed).

v0 and v11+ are not spoken.

### OffsetDelete

Spoken versions: 0.

OffsetDelete v0 is classic (Apache JSON `flexibleVersions: "none"`).

ThrottleTimeMs is JSON `0+` (`encode_offset_delete_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`) but **after** ErrorCode (not first).

Empty-Topics only one version.

Top-level ErrorCode is at bytes 0–1 (throttle occupies bytes 2–5).

Official Java `getErrorResponse` sets `throttleTimeMs` from the argument.

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

### Heartbeat

Spoken versions: 0–4.

Heartbeat v0–v3 are classic; v4 is flexible (compact strings plus tagged fields; request header 2, response header 1).

Official JSON: v1 and v2 match v0.

v1+ ThrottleTimeMs.

v3 GroupInstanceId.

`HeartbeatResponse.error_counts` is Java `HeartbeatResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`).

Kafka 4.0 `validVersions` is `0-4`.

v5+ is not spoken.

### SyncGroup

Spoken versions: 0–5.

SyncGroup v0–v3 are classic; v4–v5 are flexible (compact strings/bytes/arrays plus tagged fields; request header 2, response header 1).

Official JSON: v1 and v2 match v0.

v1+ ThrottleTimeMs.

Request GenerationId is JSON `0+` (INT32 after GroupId; decode previously discarded it; encode already takes `generation_id`; official Java `SyncGroupRequestData.generationId`; not OffsetCommit GenerationId / Heartbeat GenerationId / JoinGroup response GenerationId).

v3 GroupInstanceId.

v5 ProtocolType / ProtocolName (KIP-559; this crate sends `"consumer"` and the selected assignor).

`SyncGroupResponse.error_counts` is Java `SyncGroupResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`).

Kafka 4.0 `validVersions` is `0-5`.

v6+ is not spoken.

### JoinGroup

Spoken versions: 2–9.

JoinGroup v2–v5 are classic; v6–v9 are flexible (compact strings/bytes/arrays plus tagged fields; request header 2, response header 1).

Official JSON: v2 and v3 match v1 (RebalanceTimeoutMs).

Request SessionTimeoutMs is JSON `0+` (decode returns it; encode already takes `session_timeout_ms`).

Request RebalanceTimeoutMs is JSON `1+` (spoken v2–v9 always on the wire; decode returns it; encode writes `rebalance_timeout_ms`; official Java `JoinGroupRequestData.rebalanceTimeoutMs`; classic consumer sends `max.poll.interval.ms`).

Request ProtocolType is JSON `0+` (decode returns it last; encode already takes `protocol_type`; official Java `JoinGroupRequestData.protocolType`).

v4 second join with assigned id.

v5 request GroupInstanceId.

Response member GroupInstanceId is JSON `5+` (nullable STRING after member MemberId; encode previously always wrote null and decode discarded; below v5 encode omits it even when the body has an instance id and decode fills `None`; official Java `JoinGroupResponseData.JoinGroupResponseMember.groupInstanceId`; not JoinGroup request GroupInstanceId / SyncGroup GroupInstanceId / Heartbeat GroupInstanceId / OffsetCommit GroupInstanceId / LeaveGroup GroupInstanceId).

v7 response adds ProtocolType (KIP-559) and nullable ProtocolName.

v8 adds Reason (KIP-800; first join is null; `enforce_rebalance` / `enforce_rebalance_with` send the reason).

v9 adds SkipAssignment; when true the leader does not run the assignor.

SkipAssignment is JSON `9+` (`encode_join_group_response_with_skip_assignment`; encode previously always wrote `false`; below v9 omit even when true and decode fills `false`; convenience encode still writes `false`; official Java `JoinGroupResponseData.skipAssignment`; Java `getErrorResponse` does not set it).

ThrottleTimeMs is JSON `2+` (`encode_join_group_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-Members v2, v3, v4, and v5 bodies match; v6 is compact; v7 and v8 bodies match; v9 adds SkipAssignment.

Top-level ErrorCode is at bytes 4–5.

`JoinGroupResponse.error_counts` is Java `JoinGroupResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`).

Official Java `getErrorResponse` sets `throttleTimeMs` from the argument.

`join()` / `join_sticky` / `join_cooperative_sticky` send Protocols of 1.

`join_with_assignors` sends Protocols of N (Java `partition.assignment.strategy`); the client assigns with the JoinGroup response ProtocolName.

Kafka 4.0 `validVersions` is `2-9` (v0–v1 removed).

v0–v1 and v10+ are not spoken.

### LeaveGroup

Spoken versions: 0–5.

LeaveGroup v0–v3 are classic; v4–v5 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

v0–v2 are GroupId + MemberId (v1 and v2 match v0).

v1+ ThrottleTimeMs.

v3 Members + GroupInstanceId.

v5 Reason (KIP-800).

Kafka 4.0 `validVersions` is `0-5`.

Classic `ConsumerGroup::leave` / `close` send `"the consumer is being closed"`; `unsubscribe` sends `"the consumer unsubscribed from all topics"`; `max.poll.interval.ms` expiry sends `"consumer poll timeout has expired."`.

Admin `removeMembersFromConsumerGroup` stays v3–v5 with `"member was removed by an admin"`.

v6+ is not spoken.

### ConsumerGroupHeartbeat

Spoken versions: 0–1.

ConsumerGroupHeartbeat v0–v1 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

v1 adds SubscribedTopicRegex after SubscribedTopicNames (KIP-848) and requires the consumer to generate its own MemberId (KIP-1082; Kafka `Uuid` URL-safe Base64).

`join_consumer_matching` still expands topics locally (`Fn(&str) -> bool`) and sends SubscribedTopicNames; the regex field is null.

Request RebalanceTimeoutMs is JSON `0+` (INT32 after RackId; encode writes `rebalance_timeout_ms`; decode previously discarded it; JSON default `-1` means unchanged; join sends `max.poll.interval.ms`).

Request ServerAssignor is JSON `0+` (nullable compact STRING after SubscribedTopicRegex on v1 / after SubscribedTopicNames on v0; encode writes `server_assignor`; decode previously discarded it; JSON default null means unused or unchanged; this crate still sends null).

ThrottleTimeMs is JSON `0+` (`ConsumerGroupHeartbeatResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded).

`ConsumerGroupHeartbeatResponse.error_counts` is Java `ConsumerGroupHeartbeatResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; assignment is not counted).

Kafka 4.0 `validVersions` is `0-1`.

v2+ is not spoken.

v1 response matches v0.

### ConsumerGroupDescribe

Spoken versions: 0–1.

ConsumerGroupDescribe v0–v1 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

Request layout is the same on v0 and v1.

Response v1 adds MemberType INT8 (KIP-1099).

ThrottleTimeMs is JSON `0+` (`encode_consumer_group_describe_response_with_throttle`; `encode_consumer_group_describe_response` writes `0`; encode previously always wrote `0` and decode discarded).

`ConsumerGroupDescribeRequest.error_response` is Java `ConsumerGroupDescribeRequest.getErrorResponse` (copies group ids through `error_described_group_list`; ErrorMessage JSON-null; ThrottleTimeMs JSON `0+`; convenience encode writes `0`; official Java sets `throttleTimeMs` from the argument; request IncludeAuthorizedOperations is not copied).

Kafka 4.0 `validVersions` is `0-1`.

v2+ is not spoken.

ErrorCode is per-group.

`consumer_group_describe` sends FindCoordinator v4+ CoordinatorKeys of N (KIP-699) and one ConsumerGroupDescribe RPC per coordinator.

`consumer_group_describe_timeout` is the crate-first RPC deadline (ConsumerGroupDescribe has no TimeoutMs).

Java `describeConsumerGroups` (`Admin::describe_consumer_groups`) tries this API first and falls back to DescribeGroups on `UNSUPPORTED_VERSION` (35) or `GROUP_ID_NOT_FOUND` (69), or when the broker does not advertise api 69.

### ShareGroupHeartbeat

Spoken versions: 0–1.

ShareGroupHeartbeat v0–v1 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

Same fields.

Request RackId is JSON `0+` (nullable compact STRING after MemberEpoch; encode writes `rack_id`; decode previously discarded it; JSON default null means not provided or unchanged; this crate sends `ConsumerConfig::rack`).

ThrottleTimeMs is JSON `0+` (`ShareGroupHeartbeatResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded).

`ShareGroupHeartbeatResponse.error_counts` is Java `ShareGroupHeartbeatResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; assignment is not counted).

Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`).

Kafka 4.1 `validVersions` is `"1"` (v0 removed).

v2+ is not spoken.

v1 response matches v0.

### ShareGroupDescribe

Spoken versions: 0–1.

ShareGroupDescribe v0–v1 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

Same fields.

ThrottleTimeMs is JSON `0+` (`encode_share_group_describe_response_with_throttle`; `encode_share_group_describe_response` writes `0`; encode previously always wrote `0` and decode discarded).

`ShareGroupDescribeRequest.error_response` is Java `ShareGroupDescribeRequest.getErrorResponse` (copies group ids through `error_described_group_list`; ErrorMessage JSON-null; ThrottleTimeMs JSON `0+`; convenience encode writes `0`; official Java sets `throttleTimeMs` from the argument; request IncludeAuthorizedOperations is not copied).

Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`).

Kafka 4.1 `validVersions` is `"1"` (v0 removed).

v2+ is not spoken.

v1 response matches v0.

ErrorCode is per-group.

Official Java `DescribeShareGroupsHandler` uses `CoordinatorType.GROUP`.

`share_group_describe` / `describe_share_groups` send FindCoordinator v4+ CoordinatorKeys of N (KIP-699) and one ShareGroupDescribe RPC per coordinator.

`share_group_describe_timeout` / `describe_share_groups_timeout` are Java `DescribeShareGroupsOptions.timeoutMs` (RPC deadline; ShareGroupDescribe has no TimeoutMs).

### StreamsGroupHeartbeat

Spoken versions: 0.

The body is flexible (request header 2, response header 1). This is the Streams broker protocol, separate from consumer and share groups. `StreamsClient` sends a caller-driven v0 heartbeat. It does not run a Streams execution engine.

v1+ is not spoken.

### StreamsGroupDescribe

Spoken versions: 0.

The body is flexible (request header 2, response header 1). Admin describe uses this v0 body and keeps the typed group, member, and topology fields.

v1+ is not spoken.

### ShareFetch

Spoken versions: 0–2.

ShareFetch v0–v2 are flexible (compact strings/arrays/bytes plus tagged fields; request header 2, response header 1).

Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`).

Kafka 4.1 `validVersions` is `"1"` (v0 removed).

v0 PartitionMaxBytes on each partition.

v1 MaxRecords / BatchSize after MaxBytes (JSON `1+`; `encode_share_fetch_request_with_batch_size`; `encode_share_fetch_request` still writes BatchSize as MaxRecords; v0 omits even when non-zero and decode fills 0; no PartitionMaxBytes).

MaxWaitMs is JSON `0+` (decode returns it; encode already takes `max_wait_ms`).

MinBytes is JSON `0+` (decode returns it; encode already takes `min_bytes`).

MaxBytes is JSON `0+` (decode returns it; encode already takes `max_bytes`; JSON default `0x7fffffff`).

Records is JSON `records` (Kafka 4.0 `nullableVersions` `0+`; Kafka 4.1 `nullableVersions` `0` only; decode accepts compact null as empty on v0; v1 null is `Error::protocol` Java generated `non-nullable field records was serialized as null`; encode still writes `MemoryRecords.EMPTY` not null).

AcquisitionLockTimeoutMs after ErrorMessage (JSON `1+`; `encode_share_fetch_response_with_acquisition_lock_timeout`; `encode_share_fetch_response` still writes 15000; v0 omits even when non-zero and decode fills 0; error-path stays 0).

ForgottenTopicsData is JSON `0+` (`encode_share_fetch_request_with_forgotten`; `encode_share_fetch_request` writes empty; duplicate partition indexes are kept; TopicId UUID, no name).

Response NodeEndpoints is JSON `0+` (untagged compact array of `NodeId` INT32 + `Host` compact STRING + `Port` INT32 + `Rack` compact nullable STRING + nested tagged fields; `encode_share_fetch_response_with_endpoints`; `encode_share_fetch_response` writes empty; not Fetch v16 tagged field 0).

Partition CurrentLeader is JSON `0+` (untagged nested `LeaderIdAndEpoch`: `LeaderId` INT32 + `LeaderEpoch` INT32 + nested tagged fields; encode writes the partition fields; `partition_response` fills 0/0; not Fetch v12+ tagged field 1).

Partition ErrorMessage is JSON `0+` (nullable compact STRING; encode writes the partition field; `partition_response` fills null; not the top-level ErrorMessage).

Partition AcknowledgeErrorCode is JSON `0+` (encode writes the partition field; `partition_response` fills 0; not fetch `ErrorCode`; JSON lists `INVALID_RECORD_STATE` as acknowledge-only).

Partition AcknowledgeErrorMessage is JSON `0+` (nullable compact STRING; encode writes the partition field; `partition_response` fills null; not fetch `ErrorMessage`).

ThrottleTimeMs is JSON `0+` (`encode_share_fetch_response_with_throttle`; `encode_share_fetch_response` writes `0`; `encode_share_fetch_error` still writes `0`; `encode_share_fetch_error_with_throttle` round-trips a non-zero value; Java `ShareFetchRequest.getErrorResponse` / `ShareFetchResponse.of` sets `throttleTimeMs`; empty Responses; v1 AcquisitionLockTimeoutMs stays `0` on that path).

Top-level ErrorCode is JSON `0+` (`encode_share_fetch_response_with_error_code`; `encode_share_fetch_response` still writes `0`; decode returns it and does not fail on a non-zero code).

Top-level ErrorMessage is JSON `0+` (`encode_share_fetch_response_with_error_message`; `encode_share_fetch_response` writes null; not partition ErrorMessage).

v2 adds ShareAcquireMode and acknowledgement type 4 (renew) on the request (KIP-1206 / KIP-1222). The response keeps the v1 fields. The share group negotiates 0–2.

v3+ is not spoken.

### ShareAcknowledge

Spoken versions: 0–2.

ShareAcknowledge v0–v2 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

Same fields on v0 and v1.

Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`).

Kafka 4.1 `validVersions` is `"1"` (v0 removed).

v2 adds IsRenewAck on the request and AcquisitionLockTimeoutMs on the response (KIP-1222). Acknowledgement type 4 renews a lock. The share group negotiates 0–2.

v3+ is not spoken.

v1 response matches v0.

Response NodeEndpoints is JSON `0+` (untagged compact array of `NodeId` INT32 + `Host` compact STRING + `Port` INT32 + `Rack` compact nullable STRING + nested tagged fields; `encode_share_acknowledge_topics_response_with_endpoints`; `encode_share_acknowledge_topics_response` writes empty; not Fetch v16 tagged field 0).

Partition CurrentLeader is JSON `0+` (untagged nested `LeaderIdAndEpoch`: `LeaderId` INT32 + `LeaderEpoch` INT32 + nested tagged fields; encode writes the partition fields; `partition_response` fills 0/0; not Fetch v12+ tagged field 1).

Partition ErrorMessage is JSON `0+` (nullable compact STRING; encode writes the partition field; `partition_response` fills null; not the top-level ErrorMessage).

ThrottleTimeMs is JSON `0+` (`encode_share_acknowledge_topics_response_with_throttle`; `encode_share_acknowledge_topics_response` writes `0`; v0 and v1 bodies match).

Top-level ErrorMessage is JSON `0+` (`encode_share_acknowledge_topics_response_with_error_message`; `encode_share_acknowledge_topics_response` writes null; not partition ErrorMessage; v0 and v1 bodies match).

### DescribeShareGroupOffsets

Spoken versions: 0.

DescribeShareGroupOffsets v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`encode_describe_share_group_offsets_response_with_throttle`; `encode_describe_share_group_offsets_response` writes `0`; encode previously always wrote `0` and decode discarded).

v1+ (Lag, KIP-1226) is not spoken.

Group-level ErrorCode is per-group.

`DescribeShareGroupOffsetsResponse.error_counts` is Java `DescribeShareGroupOffsetsResponse.errorCounts` (group-level non-`NONE` last-wins on GroupId, plus every partition-level code including `NONE`; there is no top-level ErrorCode).

`DescribeShareGroupOffsetsResponse.has_group_error` is Java `DescribeShareGroupOffsetsResponse.hasGroupError` (`true` when any matching `groupId` has a non-`NONE` group-level code; partition-level codes are ignored; Java `groupError` returns `Throwable` and is not mapped).

`DescribeShareGroupOffsetsRequest.error_response` is Java `DescribeShareGroupOffsetsRequest.getErrorResponse` (copies group ids; empty Topics; ErrorMessage JSON-null; ThrottleTimeMs JSON `0+`; convenience encode writes `0`; official Java sets `throttleTimeMs` from the argument; request Topics are not copied).

`DescribeShareGroupOffsetsRequest.error_described_group` is Java `DescribeShareGroupOffsetsRequest.getErrorDescribedGroup` (one GroupId + ErrorCode; empty Topics; ErrorMessage JSON-null; official Java sets English `Errors.message`).

Official Java `ListShareGroupOffsetsHandler` uses `CoordinatorType.GROUP`.

`describe_share_group_offsets` / `list_share_group_offsets` send FindCoordinator v4+ CoordinatorKeys of N (KIP-699) and one DescribeShareGroupOffsets RPC per coordinator.

`describe_share_group_offsets_timeout` / `list_share_group_offsets_timeout` are Java `ListShareGroupOffsetsOptions.timeoutMs` (RPC deadline; DescribeShareGroupOffsets has no TimeoutMs).

`alter_share_group_offsets_timeout` / `delete_share_group_offsets_timeout` are Java `AlterShareGroupOffsetsOptions` / `DeleteShareGroupOffsetsOptions.timeoutMs` (RPC deadline; these RPCs have no TimeoutMs).

### AlterShareGroupOffsets

Spoken versions: 0.

AlterShareGroupOffsets v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`AlteredShareGroupOffsets.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `AlteredShareGroupOffsets::new` fills `0`).

`AlteredShareGroupOffsets.error_counts` is Java `AlterShareGroupOffsetsResponse.errorCounts` (top-level ErrorCode plus each partition-level code, including `NONE`; topics have no ErrorCode).

`AlterShareGroupOffsetsRequest.error_response` is Java `AlterShareGroupOffsetsRequest.getErrorResponse` (empty Responses; ErrorMessage JSON-null; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument and the English `Errors.message`; request GroupId / Topics are not copied).

Kafka 4.1 `validVersions` is `"0"`.

v1+ is not spoken.

ErrorCode is top-level after throttle.

### DeleteShareGroupOffsets

Spoken versions: 0.

DeleteShareGroupOffsets v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`DeletedShareGroupOffsets.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `DeletedShareGroupOffsets::new` fills `0`).

`DeletedShareGroupOffsets.error_counts` is Java `DeleteShareGroupOffsetsResponse.errorCounts` (top-level ErrorCode plus each topic-level code, including `NONE`; there is no partition ErrorCode).

`DeleteShareGroupOffsetsRequest.error_response` is Java `DeleteShareGroupOffsetsRequest.getErrorResponse` (empty Responses; ErrorMessage JSON-null; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument and the English `Errors.message`; request GroupId / Topics are not copied).

Kafka 4.1 `validVersions` is `"0"`.

v1+ is not spoken.

ErrorCode is top-level after throttle.

### DescribeTopicPartitions

Spoken versions: 0.

DescribeTopicPartitions v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`DescribeTopicPartitionsResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `DescribeTopicPartitionsResponse::new` fills `0`).

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

There is no top-level ErrorCode.

### ListConfigResources

Spoken versions: 0–1.

ListConfigResources v0–v1 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`ListConfigResourcesResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `ListConfigResourcesResponse::new` fills `0`).

`ListConfigResourcesResponse.error_counts` is Java `ListConfigResourcesResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; config resources are not counted).

`ListConfigResourcesResponse.error` is Java `ListConfigResourcesResponse.error` (`ApiError` from the top-level ErrorCode; unknown codes become `UNKNOWN_SERVER_ERROR`; JSON has no ErrorMessage; this crate fills a null message).

`ListConfigResourcesRequest.error_response` is Java `ListConfigResourcesRequest.getErrorResponse` (empty ConfigResources; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument).

`ListConfigResourcesRequest.supported_resource_types` is Java `ListConfigResourcesRequest.supportedResourceTypes` (v0 `CLIENT_METRICS` only; v1 `TOPIC` / `BROKER` / `BROKER_LOGGER` / `CLIENT_METRICS` / `GROUP`; `UNKNOWN` is not included).

`ListConfigResourcesRequest.build` is Java `ListConfigResourcesRequest.Builder.build` (v0 ResourceTypes must be exactly `CLIENT_METRICS`; Java `HashSet`; encode still omits ResourceTypes on v0).

`ListConfigResourcesResponse.config_resources` is Java `ListConfigResourcesResponse.configResources` (`ConfigResource` via `Type.forId`; unknown ids are `UNKNOWN`).

Kafka 4.0 `validVersions` is `"0"` (ListClientMetricsResources).

Kafka 4.1 `validVersions` is `"0-1"` (v1 ResourceType, KIP-1142).

v2+ is not spoken.

Empty-resource bodies: v0 == v1.

ErrorCode is top-level after throttle.

### GetTelemetrySubscriptions

Spoken versions: 0.

GetTelemetrySubscriptions v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`GetTelemetrySubscriptionsResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `GetTelemetrySubscriptionsResponse::new` fills `0`).

`GetTelemetrySubscriptionsResponse.error_counts` is Java `GetTelemetrySubscriptionsResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; ClientInstanceId / compression types / requested metrics are not counted).

`GetTelemetrySubscriptionsResponse.has_error` is Java `GetTelemetrySubscriptionsResponse.hasError` (`error() != NONE`).

`GetTelemetrySubscriptionsRequest.error_response` is Java `GetTelemetrySubscriptionsRequest.getErrorResponse` (JSON defaults for ClientInstanceId / subscription fields; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument).

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

ErrorCode is top-level after throttle.

### PushTelemetry

Spoken versions: 0.

PushTelemetry v0 is flexible (compact strings/bytes plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`PushTelemetryResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `PushTelemetryResponse::new` fills `0`).

`PushTelemetryResponse.error_counts` is Java `PushTelemetryResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`).

`PushTelemetryResponse.has_error` is Java `PushTelemetryResponse.hasError` (`error() != NONE`).

`PushTelemetryRequest.error_response` is Java `PushTelemetryRequest.getErrorResponse` (ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java `getErrorResponse` delegates to `errorResponse` and sets `throttleTimeMs` from the argument).

`PushTelemetryRequest.metrics_content_type` is Java `PushTelemetryRequest.metricsContentType` (`OTLP`; the request has no content-type field).

`PushTelemetryRequest.metrics_data` is Java `PushTelemetryRequest.metricsData` (`NONE` returns stored bytes; gzip / snappy / lz4 decompress via `ClientTelemetryUtils.decompress`; zstd requires the optional feature; unknown ids are `IllegalArgumentException`).

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

ErrorCode is top-level after throttle.

### AssignReplicasToDirs

Spoken versions: 0.

AssignReplicasToDirs v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`AssignReplicasToDirsResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `AssignReplicasToDirsResponse::new` fills `0`).

`AssignReplicasToDirsRequest.error_response` is Java `AssignReplicasToDirsRequest.getErrorResponse` (empty Directories; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument; request BrokerId / BrokerEpoch / Directories are not copied).

`AssignReplicasToDirsRequest.MAX_ASSIGNMENTS_PER_REQUEST` is Java `AssignReplicasToDirsRequest.MAX_ASSIGNMENTS_PER_REQUEST` (`2250`; encode does not enforce the cap).

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

ErrorCode is top-level after throttle.

### SaslHandshake

Spoken versions: 0–1.

SaslHandshake v0–v1 are classic (Apache JSON `flexibleVersions: "none"`).

Same fields.

v1 enables SaslAuthenticate.

`SaslHandshakeResponse.error_counts` is Java `SaslHandshakeResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; Mechanisms are not counted).

Kafka 4.0 `validVersions` is `0-1`.

v2+ is not spoken (KAFKA-9577).

### SaslAuthenticate

Spoken versions: 0–2.

SaslAuthenticate v0–v1 are classic.

v2 is flexible (compact bytes/strings plus tagged fields; request header 2, response header 1).

v0 and v1 request match (AuthBytes).

v1+ SessionLifetimeMs.

`SaslAuthenticateResponse.error_counts` is Java `SaslAuthenticateResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; AuthBytes / ErrorMessage / SessionLifetimeMs are not counted).

Kafka 4.0 `validVersions` is `0-2`.

v3+ is not spoken.

### ListTransactions

Spoken versions: 0–2.

ListTransactions v0–v2 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

v1 adds DurationFilter INT64 after ProducerIdFilters (KIP-994; `< 0` means no filter).

ThrottleTimeMs is JSON `0+` (`ListTransactionsResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `::new` fills `0`).

Empty-TransactionStates v0 and v1 bodies match.

Top-level ErrorCode is at bytes 4–5.

`ListTransactionsResponse.error_counts` is Java `ListTransactionsResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `updateErrorCounts`; unknown state filters and listings are not counted).

`ListTransactionsRequest.error_response` is Java `ListTransactionsRequest.getErrorResponse` (empty UnknownStateFilters / TransactionStates; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument; request StateFilters / ProducerIdFilters / DurationFilter are not copied).

Kafka 4.0 `validVersions` is `0-1`.

v2 adds a nullable TransactionalIdPattern. A pattern on v0 or v1 is refused. A nonempty pattern requires v2 and is not dropped on an older broker. Admin negotiates 0–2. v1 and v2 responses match v0.

v3+ is not spoken.

`list_transactions_timeout` / `list_transactions_with_duration_timeout` are Java `ListTransactionsOptions.timeoutMs` (RPC deadline; ListTransactions has no TimeoutMs; `duration_ms` is DurationFilter, not TimeoutMs).

### DescribeTransactions

Spoken versions: 0.

DescribeTransactions v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`encode_describe_transactions_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-TransactionStates only one version.

There is no top-level ErrorCode.

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

ErrorCode is per transactional.id.

`describe_transactions` sends FindCoordinator v4+ CoordinatorKeys of N (`key_type=1`) and one DescribeTransactions RPC per coordinator.

`describe_transactions_timeout` is Java `DescribeTransactionsOptions.timeoutMs` (RPC deadline; DescribeTransactions has no TimeoutMs).

### CreateTopics

Spoken versions: 0–7.

CreateTopics v0–v4 are classic.

v5–v7 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

v5 returns NumPartitions / ReplicationFactor / Configs (KIP-525).

v6 is the same layout (KIP-599 THROTTLING_QUOTA_EXCEEDED).

v7 adds TopicId UUID after Name (KIP-516).

`create_topics_timeout` is Java `CreateTopicsOptions.timeoutMs` (RPC deadline and TimeoutMs).

`create_topics_with_quota_retry` is Java `CreateTopicsOptions.retryOnQuotaViolation` (default `true`; KIP-599; retries only topics that return `THROTTLING_QUOTA_EXCEEDED`).

`NewTopic.with_assignments` is Java `NewTopic(String, Map<Integer, List<Integer>>)` (NumPartitions / ReplicationFactor `-1`; empty Assignments is `NewTopic(String, int, short)` — not null; official JSON is not nullable).

`NewTopic.broker_defaults` is Java `NewTopic(String, Optional.empty(), Optional.empty())` (KIP-464; `-1` / `-1` with empty Assignments; v4+ uses broker `num.partitions` / `default.replication.factor`).

`NewTopic.configs` is Java `NewTopic.configs(Map)` (replaces prior `config` entries).

Kafka 4.0 `validVersions` is `2-7` (v0–v1 removed).

v8+ is not spoken.

### DeleteTopics

Spoken versions: 0–6.

DeleteTopics v0–v3 are classic (TopicNames + TimeoutMs).

v4–v6 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

v5 adds ErrorMessage (KIP-599).

v6 replaces TopicNames with Topics of Name + TopicId (KIP-516; name-based deletes send a zero UUID; `delete_topics_by_id` is Java `deleteTopics(TopicCollection.ofTopicIds)` with null Name).

`delete_topics_timeout` is Java `DeleteTopicsOptions.timeoutMs` (RPC deadline and TimeoutMs).

`delete_topics_with_quota_retry` / `delete_topics_by_id_with_quota_retry` are Java `DeleteTopicsOptions.retryOnQuotaViolation` (default `true`; KIP-599; retries only topics that return `THROTTLING_QUOTA_EXCEEDED`).

Kafka 4.0 `validVersions` is `1-6` (v0 removed).

v7+ is not spoken.

### DescribeConfigs

Spoken versions: 0–4.

DescribeConfigs v0–v3 are classic.

ThrottleTimeMs is JSON `0+` (on the wire for v0–v4).

`shouldClientThrottle` is v2+ (KIP-219).

v1 adds IncludeSynonyms / ConfigSource / Synonyms.

v2 is the same layout as v1.

v3 adds IncludeDocumentation, ConfigType, and Documentation (KIP-226).

v4 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

Kafka 4.0 `validVersions` is `1-4` (v0 removed).

v5+ is not spoken.

`describe_configs_with_documentation` is Java `DescribeConfigsOptions.includeDocumentation`; v0–v2 omit the field.

`describe_configs_timeout` / `describe_configs_with_documentation_timeout` are Java `DescribeConfigsOptions.timeoutMs` (RPC deadline; DescribeConfigs has no TimeoutMs).

### CreatePartitions

Spoken versions: 0–3.

CreatePartitions v0–v1 are classic.

v2–v3 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

v3 is the same layout (KIP-599 THROTTLING_QUOTA_EXCEEDED).

ThrottleTimeMs is JSON `0+` (`encode_create_partitions_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-Results v0 and v1 bodies match; v2 and v3 bodies match.

`create_partitions_timeout` is Java `CreatePartitionsOptions.timeoutMs` (RPC deadline and TimeoutMs).

`create_partitions_with_quota_retry` is Java `CreatePartitionsOptions.retryOnQuotaViolation` (default `true`; KIP-599; retries only topics that return `THROTTLING_QUOTA_EXCEEDED`).

`NewPartitions.with_assignments` is Java `increaseTo(int, List<List<Integer>>)` (null Assignments when omitted; the broker assigns replicas).

Kafka 4.0 `validVersions` is `0-3`.

v4+ is not spoken.

### AlterPartitionReassignments

Spoken versions: 0.

AlterPartitionReassignments v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`AlterPartitionReassignmentsResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `::new` / `::error` fill `0`).

`alter_partition_reassignments_timeout` is Java `AlterPartitionReassignmentsOptions.timeoutMs` (RPC deadline and TimeoutMs).

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

### ListPartitionReassignments

Spoken versions: 0.

ListPartitionReassignments v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`ListPartitionReassignmentsResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `::new` / `::error` fill `0`).

`topics = None` lists every ongoing reassignment.

`list_partition_reassignments_all` is Java `listPartitionReassignments()`.

`list_partition_reassignments_for` is Java `listPartitionReassignments(Set)`.

`list_partition_reassignments_timeout` is Java `ListPartitionReassignmentsOptions.timeoutMs` (RPC deadline and TimeoutMs).

`ListPartitionReassignmentsResponse.error_counts` is Java `ListPartitionReassignmentsResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; topics / partitions are not counted; not `ListPartitionReassignmentsResponse.error` / AlterPartitionReassignments `errorCounts`).

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

### UpdateFeatures

Spoken versions: 0–2.

UpdateFeatures v0–v2 are flexible from v0 (compact strings/arrays plus tagged fields; request header 2, response header 1).

v0 AllowDowngrade.

v1 UpgradeType / ValidateOnly.

v2 omits Results.

ThrottleTimeMs is JSON `0+` (`UpdateFeaturesResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `::new` / `::error` / `create_with_errors` fill `0`).

Empty-Results v0 and v1 bodies match; v2 omits Results.

`update_features_timeout` / `update_features_with_timeout` are Java `UpdateFeaturesOptions.timeoutMs` (RPC deadline and TimeoutMs).

Kafka 4.0 `validVersions` is `0-2`.

v3+ is not spoken.

### AlterUserScramCredentials

Spoken versions: 0.

AlterUserScramCredentials v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`encode_alter_user_scram_credentials_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-Results only one version.

There is no top-level ErrorCode.

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

### DescribeUserScramCredentials

Spoken versions: 0.

DescribeUserScramCredentials v0 is flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`DescribeUserScramCredentialsResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `::new` / `::error` fill `0`).

Empty-Results only one version.

Top-level ErrorCode is at bytes 4–5.

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

### AlterClientQuotas

Spoken versions: 0–1.

AlterClientQuotas v0 is classic.

v1 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`encode_alter_client_quotas_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-Entries v0 and v1 bodies differ.

There is no top-level ErrorCode.

Kafka 4.0 `validVersions` is `0-1`.

v2+ is not spoken.

### DescribeClientQuotas

Spoken versions: 0–1.

DescribeClientQuotas v0 is classic.

v1 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`DescribeClientQuotasResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `::new` / `::error` fill `0`).

Empty-Entries v0 and v1 bodies differ.

Top-level ErrorCode is at bytes 4–5.

`DescribeClientQuotasResponse.error_counts` is Java `DescribeClientQuotasResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; quota entries are not counted; not `DescribeClientQuotasResponse.error` / AlterClientQuotas `errorCounts`).

Kafka 4.0 `validVersions` is `0-1`.

v2+ is not spoken.

### IncrementalAlterConfigs

Spoken versions: 0–1.

IncrementalAlterConfigs v0 is classic.

v1 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`encode_incremental_alter_configs_resource_results_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-Responses v0 and v1 bodies differ.

`incremental_alter_configs_for` is Java `incrementalAlterConfigs(Map)` (Resources of N).

`AlterConfig::append` / `AlterConfig::subtract` are Java `AlterConfigOp.OpType.APPEND` / `SUBTRACT` (LIST configs; op 2 / 3).

`incremental_alter_configs_timeout` is Java `AlterConfigsOptions.timeoutMs` (RPC deadline; IncrementalAlterConfigs has no TimeoutMs).

Kafka 4.0 `validVersions` is `0-1`.

v2+ is not spoken.

### AlterReplicaLogDirs

Spoken versions: 1–2.

AlterReplicaLogDirs v1 is classic.

v2 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

Same fields.

ThrottleTimeMs is JSON `0+` (`AlterReplicaLogDirsResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `AlterReplicaLogDirsResponse::new` fills `0`).

`alter_replica_log_dirs_timeout` is Java `AlterReplicaLogDirsOptions.timeoutMs` (RPC deadline; AlterReplicaLogDirs has no TimeoutMs).

Kafka 4.0 `validVersions` is `1-2` (v0 removed).

v0 and v3+ are not spoken.

### DescribeLogDirs

Spoken versions: 1–5.

DescribeLogDirs v1 is classic.

v2–v5 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

v3 top-level ErrorCode (KIP-784).

v4 TotalBytes / UsableBytes (KIP-827; decode fills `-1` on v1–v3).

ThrottleTimeMs is JSON `0+` (`DescribeLogDirsResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `DescribeLogDirsResponse::new` fills `0`).

`DescribeLogDirsRequest.error_response` is Java `DescribeLogDirsRequest.getErrorResponse` (empty Results; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument; request Topics are not copied; encode omits ErrorCode below v3).

`describe_log_dirs_timeout` / `describe_replica_log_dirs_timeout` / `describe_broker_log_dirs_timeout` are Java `DescribeLogDirsOptions.timeoutMs` (RPC deadline; DescribeLogDirs has no TimeoutMs).

Kafka 4.0 `validVersions` is `1-4` (v0 removed). Kafka 4.3.1 extends the range through v5.

v5 adds IsCordoned on each directory. Older versions omit it and decode false. Admin negotiates 1–5.

v0 and v6+ are not spoken.

### CreateDelegationToken

Spoken versions: 1–3.

CreateDelegationToken v1 is classic.

v2–v3 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

v3 OwnerPrincipalType / OwnerPrincipalName and TokenRequesterPrincipalType / TokenRequesterPrincipalName (decode fills `None` / empty on v1–v2).

ErrorCode is the first field (bytes 0–1); ThrottleTimeMs is last (JSON `0+`; `CreateDelegationTokenResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `CreateDelegationTokenResponse::new` fills `0`).

`CreateDelegationTokenResponse.error_counts` is Java `CreateDelegationTokenResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; token fields are not counted).

`CreateDelegationTokenResponse.has_error` is Java `CreateDelegationTokenResponse.hasError` (`error() != NONE`; Java `error()` is `Errors.forCode` only; crate `error()` stays `getErrorResponse`).

`create_delegation_token_timeout` is Java `CreateDelegationTokenOptions.timeoutMs` (RPC deadline; CreateDelegationToken has no TimeoutMs).

Kafka 4.0 `validVersions` is `1-3` (v0 removed).

v0 and v4+ are not spoken.

Broker-only (`LeastLoadedNodeProvider`); broker-side `forwardToController` is not a client 41 hop.

### RenewDelegationToken

Spoken versions: 1–2.

RenewDelegationToken v1 is classic.

v2 is flexible (compact bytes plus tagged fields; request header 2, response header 1).

Same fields.

ErrorCode is the first field (bytes 0–1); ThrottleTimeMs is last (JSON `0+`; `RenewDelegationTokenResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `RenewDelegationTokenResponse::new` fills `0`).

`RenewDelegationTokenResponse.error_counts` is Java `RenewDelegationTokenResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; expiry is not counted).

`RenewDelegationTokenResponse.has_error` is Java `RenewDelegationTokenResponse.hasError` (`error() != NONE`; Java `error()` is `Errors.forCode` only).

`RenewDelegationTokenRequest.error_response` is Java `RenewDelegationTokenRequest.getErrorResponse` (ExpiryTimestampMs JSON default `0`; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument; request HMAC / RenewPeriodMs are not copied).

`renew_delegation_token_timeout` is Java `RenewDelegationTokenOptions.timeoutMs` (RPC deadline; RenewDelegationToken has no TimeoutMs).

Kafka 4.0 `validVersions` is `1-2` (v0 removed).

v0 and v3+ are not spoken.

Broker-only (`LeastLoadedNodeProvider`); broker-side `forwardToController` is not a client 41 hop.

### ExpireDelegationToken

Spoken versions: 1–2.

ExpireDelegationToken v1 is classic.

v2 is flexible (compact bytes plus tagged fields; request header 2, response header 1).

Same fields.

ErrorCode is the first field (bytes 0–1); ThrottleTimeMs is last (JSON `0+`; `ExpireDelegationTokenResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `ExpireDelegationTokenResponse::new` fills `0`).

`ExpireDelegationTokenResponse.error_counts` is Java `ExpireDelegationTokenResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; expiry is not counted).

`ExpireDelegationTokenResponse.has_error` is Java `ExpireDelegationTokenResponse.hasError` (`error() != NONE`; Java `error()` is `Errors.forCode` only).

`ExpireDelegationTokenRequest.error_response` is Java `ExpireDelegationTokenRequest.getErrorResponse` (ExpiryTimestampMs JSON default `0`; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument; request HMAC / ExpiryTimePeriodMs are not copied).

`expire_delegation_token_timeout` is Java `ExpireDelegationTokenOptions.timeoutMs` (RPC deadline; ExpireDelegationToken has no TimeoutMs).

Kafka 4.0 `validVersions` is `1-2` (v0 removed).

v0 and v3+ are not spoken.

Broker-only (`LeastLoadedNodeProvider`); broker-side `forwardToController` is not a client 41 hop.

### DescribeDelegationToken

Spoken versions: 1–3.

DescribeDelegationToken v1 is classic.

v2–v3 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

Request Owners is the same on v1–v3.

v3 TokenRequesterPrincipalType / TokenRequesterPrincipalName on each token (decode fills empty on v1–v2).

ErrorCode is the first field (bytes 0–1); ThrottleTimeMs is last (JSON `0+`; `DescribeDelegationTokenResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `DescribeDelegationTokenResponse::new` fills `0`).

Empty-Tokens v2 and v3 bodies match.

`DescribeDelegationTokenResponse.error_counts` is Java `DescribeDelegationTokenResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; tokens are not counted).

`DescribeDelegationTokenRequest.error_response` is Java `DescribeDelegationTokenRequest.getErrorResponse` (empty Tokens; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument; request Owners are not copied).

`DescribeDelegationTokenResponse.has_error` is Java `DescribeDelegationTokenResponse.hasError` (`error() != NONE`; Java `error()` is `Errors.forCode` only).

`describe_delegation_token_timeout` is Java `DescribeDelegationTokenOptions.timeoutMs` (RPC deadline; DescribeDelegationToken has no TimeoutMs).

Kafka 4.0 `validVersions` is `1-3` (v0 removed).

v0 and v4+ are not spoken.

Broker-only (`LeastLoadedNodeProvider`); the handler does not `forwardToController`.

### DescribeGroups

Spoken versions: 0–6.

DescribeGroups v0–v4 are classic.

v5–v6 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

v1 ThrottleTimeMs.

v3 IncludeAuthorizedOperations / AuthorizedOperations.

v4 GroupInstanceId.

v6 ErrorMessage and GROUP_ID_NOT_FOUND (KIP-1043).

`describe_groups` / `describe_classic_groups` send FindCoordinator v4+ CoordinatorKeys of N (KIP-699) and one DescribeGroups RPC per coordinator.

`describe_consumer_groups` tries ConsumerGroupDescribe first and uses DescribeGroups for classic groups (same FindCoordinator hop).

`describe_groups_timeout` / `describe_classic_groups_timeout` / `describe_consumer_groups_timeout` are Java `DescribeClassicGroupsOptions` / `DescribeConsumerGroupsOptions.timeoutMs` (RPC deadline; neither RPC has TimeoutMs).

Kafka 4.0 `validVersions` is `0-6`.

v7+ is not spoken.

### ListGroups

Spoken versions: 0–5.

ListGroups v0–v2 are classic.

v3–v5 are flexible (compact strings/arrays plus tagged fields; request header 2, response header 1).

v1 ThrottleTimeMs.

v4 StatesFilter / GroupState (KIP-518).

v5 TypesFilter / GroupType (KIP-848).

`ListGroupsResponse.error_counts` is Java `ListGroupsResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; listed groups are not counted).

`list_groups_timeout` / `list_consumer_groups_timeout` are Java `ListGroupsOptions` / `ListConsumerGroupsOptions.timeoutMs` (RPC deadline; ListGroups has no TimeoutMs).

Kafka 4.0 `validVersions` is `0-5`.

v6+ is not spoken.

### DeleteGroups

Spoken versions: 0–2.

DeleteGroups v0–v1 are classic.

v2 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`encode_delete_groups_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-Results v0 and v1 bodies match; v2 is compact.

There is no top-level ErrorCode.

`DeleteGroupsRequest.error_response` is Java `DeleteGroupsRequest.getErrorResponse` (copies group ids through `error_result_collection`; ThrottleTimeMs JSON `0+`; convenience encode still writes `0`; official Java sets `throttleTimeMs` from the argument).

Kafka 4.0 `validVersions` is `0-2`.

v3+ is not spoken.

### AlterConfigs

Spoken versions: 0–2.

AlterConfigs (legacy api 33) v0–v1 are classic.

ThrottleTimeMs is JSON `0+` (on the wire for v0–v2).

`shouldClientThrottle` is v1+ (KIP-219).

v2 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

`alter_configs_for` is Java `alterConfigs(Map)` (Resources of N).

`alter_configs_timeout` is Java `AlterConfigsOptions.timeoutMs` (RPC deadline; AlterConfigs has no TimeoutMs).

Kafka 4.0 `validVersions` is `0-2`.

v3+ is not spoken.

### DeleteRecords

Spoken versions: 0–2.

DeleteRecords v0–v1 are classic.

ThrottleTimeMs is JSON `0+` (on the wire for v0–v2).

`shouldClientThrottle` is v1+ (KIP-219).

v2 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

`delete_records_for` sends one Topics/Partitions array per leader (Java `deleteRecords(Map)` of `RecordsToDelete`).

`delete_records_timeout` / `delete_records_for_timeout` are Java `DeleteRecordsOptions.timeoutMs` (RPC deadline and TimeoutMs).

Kafka 4.0 `validVersions` is `0-2`.

v3+ is not spoken.

### DescribeProducers

Spoken versions: 0.

DescribeProducers v0 is flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`DescribeProducersResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `::new` fills `0`).

Empty-topics only one version.

There is no top-level ErrorCode.

`describe_producers_for` sends one Topics/PartitionIndexes array per leader (Java `describeProducers(Collection)`).

`describe_producers_for_on_broker` is Java `DescribeProducersOptions.brokerId` (one RPC to that broker; `NOT_LEADER_OR_FOLLOWER` is not retried on the Metadata leader).

`describe_producers_timeout` / `describe_producers_for_timeout` are Java `DescribeProducersOptions.timeoutMs` (RPC deadline; DescribeProducers has no TimeoutMs).

Kafka 4.0 `validVersions` is `0`.

v1+ is not spoken.

Partition-leader hop only (`NOT_LEADER_OR_FOLLOWER`) unless `brokerId` is set; not a controller hop and not a transaction-coordinator hop.

### AllocateProducerIds

Spoken versions: 0.

AllocateProducerIds v0 is flexible (compact tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`AllocateProducerIdsResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `::new` fills `0`).

Top-level ErrorCode is at bytes 4–5.

`AllocateProducerIdsResponse.error_counts` is Java `AllocateProducerIdsResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; ProducerIdStart / ProducerIdLen are not counted).

`AllocateProducerIdsRequest.error_response` is Java `AllocateProducerIdsRequest.getErrorResponse` (ProducerIdStart / ProducerIdLen JSON default `0`; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument; request BrokerId / BrokerEpoch are not copied).

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

### UnregisterBroker

Spoken versions: 0.

UnregisterBroker v0 is flexible (compact strings plus tagged fields; request header 2, response header 1).

ThrottleTimeMs is JSON `0+` (`UnregisterBrokerResponse.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `::new` fills `0`).

Empty-ErrorMessage only one version.

Top-level ErrorCode is at bytes 4–5.

`UnregisterBrokerRequest.error_response` is Java `UnregisterBrokerRequest.getErrorResponse` (ErrorMessage JSON-null; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java sets `throttleTimeMs` from the argument; request BrokerId is not copied).

Kafka 4.0 `validVersions` is `"0"`.

v1+ is not spoken.

### DescribeCluster

Spoken versions: 0–2.

DescribeCluster v0–v2 are flexible from v0 (compact strings/arrays plus tagged fields; request header 2, response header 1).

v1 EndpointType.

v2 IsFenced.

ThrottleTimeMs is JSON `0+` (`ClusterDescription.throttle_time_ms`; encode previously always wrote `0` and decode discarded; `::new` fills `0`).

Empty-Brokers v0 and v1 bodies differ; v1 and v2 bodies match.

Top-level ErrorCode is at bytes 4–5.

`DescribeClusterResponse.error_counts` is Java `DescribeClusterResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; brokers are not counted).

`DescribeClusterRequest.error_response` is Java `DescribeClusterRequest.getErrorResponse` (empty Brokers; ClusterId / ControllerId / EndpointType / ClusterAuthorizedOperations JSON defaults; ErrorMessage JSON-null; ThrottleTimeMs JSON `0+`; convenience fills `0`; official Java does not set `throttleTimeMs` from the argument; request IncludeClusterAuthorizedOperations / EndpointType / IncludeFencedBrokers are not copied).

Kafka 4.0 `validVersions` is `0-2`.

v3+ is not spoken.

### CreateAcls, DescribeAcls, DeleteAcls

Spoken versions: 0–3.

CreateAcls / DescribeAcls / DeleteAcls v0–v1 are classic.

v1 adds ResourcePatternType / PatternTypeFilter (LITERAL on create; ANY on describe/delete filters).

v2–v3 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

v3 is the same layout (user resource type).

CreateAcls ThrottleTimeMs is JSON `0+` (`encode_create_acls_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

Empty-Results v0 and v1 bodies match; v2 and v3 bodies match.

There is no top-level ErrorCode.

DescribeAcls ThrottleTimeMs is JSON `0+` (`encode_describe_acls_response_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

DescribeAcls ErrorCode is JSON `0+` (`encode_describe_acls_response_with_error_code`; encode previously always wrote `0` and decode read it without returning it; convenience encode still writes `0`; not CreateAcls result ErrorCode / DeleteAcls filter ErrorCode / DeleteAcls matching ErrorCode / Metadata ErrorCode).

`DescribeAclsResponse.error_counts` is Java `DescribeAclsResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`).

`DescribeAclsResponse.error` is Java `DescribeAclsResponse.error` (`ApiError` from top-level ErrorCode and ErrorMessage).

DescribeAcls ErrorMessage is JSON `0+` (nullable STRING; `encode_describe_acls_response_with_error_message`; encode previously always wrote null and decode discarded; convenience encode still writes null; not CreateAcls result ErrorMessage / DeleteAcls filter ErrorMessage / DeleteAcls matching ErrorMessage).

Empty-Resources v0 and v1 bodies match; v2 and v3 bodies match.

Top-level ErrorCode is at bytes 4–5.

DeleteAcls ThrottleTimeMs is JSON `0+` (`encode_delete_acls_filter_results_with_throttle`; encode previously always wrote `0` and decode discarded; convenience encode still writes `0`).

DeleteAcls matching ErrorMessage is JSON `0+` (nullable STRING on each MatchingAcl; `DeletedAclsFilterResult.matching` is `DeleteAclsMatchingAcl`; encode previously always wrote null via `ApiError::NONE` and decode discarded; `encode_delete_acls_response` still writes null; not DescribeAcls ErrorMessage / CreateAcls result ErrorMessage / DeleteAcls filter ErrorMessage).

Empty-FilterResults v0 and v1 bodies match; v2 and v3 bodies match.

There is no top-level ErrorCode.

`describe_acls_with` is Java `describeAcls(AclBindingFilter)` (null name/principal/host; Operation and Permission ANY).

`describe_acls_any` is Java `describeAcls(AclBindingFilter.ANY)`.

`delete_acls_with` is Java `deleteAcls(Collection)` (Filters of N).

`create_acls_timeout` / `describe_acls_timeout` / `delete_acls_timeout` are Java `CreateAclsOptions` / `DescribeAclsOptions` / `DeleteAclsOptions.timeoutMs` (RPC deadline; these RPCs have no TimeoutMs).

Kafka 4.0 `validVersions` is `1-3` (v0 removed).

v4+ is not spoken.

### Metadata

Spoken versions: 0–13.

Metadata v0–v8 are classic; v9–v13 are flexible (compact arrays/strings plus tagged fields; request header 2, response header 1).

v0 has no controller id, topic id, or rack. An empty topic array means every topic. A named v0 request requires automatic topic creation. Producer, consumer, and admin negotiate 0–13.

v10 TopicId (name-based describes send a zero UUID; `describe_topics_by_id` is Java `describeTopics(TopicCollection.ofTopicIds)` with null Name).

v12 supports topicId.

ClusterAuthorizedOperations is JSON `8-10` (`MetadataResponse.cluster_authorized_operations`; encode previously always wrote `AUTHORIZED_OPERATIONS_OMITTED` and decode discarded; below v8 and above v10 omit even when non-default and decode fills `AUTHORIZED_OPERATIONS_OMITTED`; `::error_response` fills the JSON default; not TopicAuthorizedOperations / DescribeCluster `cluster_authorized_operations` / IncludeTopicAuthorizedOperations).

IncludeClusterAuthorizedOperations is JSON `8-10` (`encode_metadata_request_topics_with_include_cluster_authorized_operations`; encode previously always wrote `false` and decode discarded; below v8 and above v10 omit even when true and decode fills `false`; convenience encode still writes `false`; official Java `MetadataRequestData.includeClusterAuthorizedOperations`; not IncludeTopicAuthorizedOperations / ClusterAuthorizedOperations / DescribeCluster `cluster_authorized_operations`).

v13 adds top-level ErrorCode INT16 after Topics (and after ClusterAuthorizedOperations on v8–v10) and before tagged fields.

Name-based `describe_topics` / `describe_topics_with` / `describe_topics_with_partition_limit` are Java `describeTopics(TopicCollection.ofTopicNames)` via DescribeTopicPartitions api 75 (KIP-966; `ResponsePartitionLimit` default 2000, `DescribeTopicsOptions.partitionSizeLimitPerResponse`, and `NextCursor` pages); TopicAuthorizedOperations is stored when `describe_topics_with(true)` (DTP has no IncludeTopicAuthorizedOperations request flag).

`describe_topic_partitions_timeout` is the crate-first RPC deadline (DescribeTopicPartitions has no TimeoutMs).

Java `describeTopics` is `describe_topics_timeout`.

Kafka 4.0 `validVersions` is `0-13`.

v14+ is not spoken.

### WriteTxnMarkers

Spoken versions: 0–2.

v0 is classic. v1 and v2 are flexible (request header 2, response header 1).

v2 adds a signed TransactionVersion after CoordinatorEpoch. `Admin::abort_transaction` negotiates 0–2.

v3+ is not spoken.

### ElectLeaders

Spoken versions: 0–2.

v0 and v1 are classic. v2 is flexible (request header 2, response header 1).

v1 adds ElectionType and a top-level response ErrorCode. v0 omits ElectionType even when the request sets one. Admin negotiates 0–2.

v3+ is not spoken.

### DescribeQuorum

Spoken versions: 0–2.

Every version is flexible (request header 2, response header 1). The request fields stay the same.

v1 adds replica fetch and catch-up timestamps. v2 adds error messages, Nodes, and ReplicaDirectoryId. Admin negotiates 0–2.

v3+ is not spoken.

### AddRaftVoter

Spoken versions: 0–1.

Both versions use a flexible header. The default acknowledgement waits for a committed voter set and can use v0. An explicit local append requires v1 and is rejected before I/O on a v0-only broker.

v2+ is not spoken.

### RemoveRaftVoter

Spoken versions: 0.

The request uses a flexible header. Removal stays on v0.

v1+ is not spoken.

### ApiVersions

Spoken versions: 0–4 and sends v4 on connect.

ApiVersions v0–v2 are classic (empty request; v1+ ThrottleTimeMs).

v3–v4 are flexible in the body only (compact strings plus tagged fields; request header 2; response header stays 0, KIP-482).

v3 adds ClientSoftwareName / ClientSoftwareVersion.

v4 is the same request as v3 and allows SupportedFeatures.MinVersion 0 (KAFKA-17011 / KAFKA-17492; v0–v3 omit those features).

Tagged fields: 0 `supportedFeatures` (name, min, max), 1 `finalizedFeaturesEpoch` INT64 (`-1` omitted), 2 `finalizedFeatures` (name, **max** then min), 3 `zkMigrationReady`.

Empty/default tags are omitted.

`ApiVersionsResponse.error_counts` is Java `ApiVersionsResponse.errorCounts` (top-level ErrorCode only, including `NONE`; Java `Collections.singletonMap`; api keys / features are not counted).

`describe_features_timeout` is Java `DescribeFeaturesOptions.timeoutMs` (RPC deadline; ApiVersions has no TimeoutMs).

Kafka 4.0 `validVersions` is `0-4`.

When the broker returns `UNSUPPORTED_VERSION` (KIP-511, brokers 2.4+), the error body is v0 and lists the supported ApiVersions range; the client retries at `pick_version(broker_min, broker_max, 0, 4)`.

v5+ is not spoken.
