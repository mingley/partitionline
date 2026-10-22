# Wire versions

This page is the field notes for each API. The spoken-version index is the table in the [design notes](design.md#wire-format-notes). Rust behavior is authoritative. A Java name is a porting cross-link.

Each section starts with the versions this crate speaks. The notes after that are framing, fields, and what the client does with them.

## Words that repeat

| Phrase | Meaning |
|---|---|
| Classic | Kafka's older arrays and strings. Request header version 1, response header version 0, unless the section says otherwise. |
| Flexible | Compact arrays, strings, and bytes, plus tagged fields. Request header version 2, response header version 1, unless the section says otherwise. |
| JSON `N+` | The Apache schema includes that field from version N. The name in parentheses is the encoder in this crate. |
| Encode previously wrote a constant | An older revision of this crate ignored the field on decode and wrote a fixed value on encode. The convenience encoder may still write that default. |
| Bodies match | Those versions encode to the same bytes when every optional field is at its default. |
| Kafka 4.0 `validVersions` | Apache's advertised range in that release. This crate can speak a wider range. The section says which. |

## Layout rules

- Request `ClientId` is a classic nullable string, including on flexible headers.
- The ApiVersions **response** header is classic. A flexible parse treats the error code as a compact-string length.
- Produce throttle time comes **after** the topic array. Metadata throttle time comes first.
- A magic-2 record batch CRC is CRC32-C from the attributes byte through the end of the batch.
- Record lengths are zigzag varints. A compact protocol length is an unsigned varint of `n + 1`. Zero means null.
- Without `InitProducerId`, producer id, epoch, and sequence are `-1`. Zero is a real id.
- `acks=0` means the broker sends no Produce response. The client does not read one.

## APIs

### InitProducerId

Spoken versions: 0–6. Version 7 and above are not spoken.

v0–v1 are classic. v2–v6 are flexible: a compact transactional id plus tagged fields. Request header version 2, response header version 1. v0 and v1 share one empty-error body. v2–v5 share one empty-error body.

| Version | What changed |
|---|---|
| 3 and above | Request `ProducerId` and `ProducerEpoch` (KIP-360). The first init sends `-1` / `-1`. |
| 4 | `PRODUCER_FENCED`. |
| 5 | `TRANSACTION_ABORTABLE` (KIP-890). |
| 6 | `enable2Pc` and `keepPreparedTxn` (KIP-939). |

Idempotent and ordinary transactional producers negotiate 0–6 and send both v6 flags false. An older peer rejects a true flag before the request is sent. Prepared-transaction lifecycle operations are separate from this initialization.

`ThrottleTimeMs` is JSON `0+` (`encode_init_producer_id_response_with_throttle`). v0 omits it. v1 and above write it, and the convenience encoder writes `0`. Java `getErrorResponse` writes `0` even when its throttle argument is non-zero. The top-level error code is at bytes 4–5. `error_counts` counts that code only, including `NONE` (Java `Collections.singletonMap`).

After `UNKNOWN_PRODUCER_ID`, the idempotent producer bumps the epoch locally and retries. After `UNKNOWN_PRODUCER_ID`, `INVALID_PRODUCER_EPOCH`, or `INVALID_PRODUCER_ID_MAPPING`, a transactional abort still calls EndTxn. A Produce that already finished `send` with that error does not fail the abort. Below EndTxn v5 the client re-inits with the last producer id and epoch. EndTxn v5 already returns the bumped identity. `commit_transaction` still fails `flush` on the Produce error.

Kafka 4.0 `validVersions` is `0-5`. This crate also speaks v6.

### Produce

Spoken versions: 3–13. Version 14 and above are not spoken.

v3–v8 are classic record bytes. v9–v13 are flexible. Request header version 2, response header version 1. v13 uses topic IDs and the identity-aware helpers.

The producer negotiates 3–13. It stays at 12 when Metadata is below v10. A transactional producer stays at 11 unless the cluster has finalized transaction version 2 and EndTxn is v5.

| Versions | Layout |
|---|---|
| 3–8 | One empty-response body. |
| 9–12 | One empty-response body, in the flexible encoding. |
| 10 and above | Partition `CurrentLeader` is tagged field 0 (KIP-951): `LeaderId` INT32, `LeaderEpoch` INT32, then nested tagged fields. Top-level `NodeEndpoints` is tagged field 0: a compact array of `NodeId` INT32, `Host` compact string, `Port` INT32, `Rack` compact nullable string, then nested tagged fields. |
| 11 | `TRANSACTION_ABORTABLE`. Same bytes as v10. |
| 12 | Same bytes as v10. KIP-890 part 2, transaction version 2. |
| 13 | Topic IDs. Still flexible. |

`ThrottleTimeMs` is JSON `1+`, placed after `Responses` (`encode_produce_response_with_throttle`). The convenience encoder writes `0`. There is no top-level error code. Java `getErrorResponse` copies `throttleTimeMs` from its argument.

A retriable Produce error whose `CurrentLeader` names a known broker updates that partition's leader cache and skips a Metadata refresh. An unknown leader is inserted from `NodeEndpoints` first, then applied the same way.

When the broker advertises v12, transactional produce skips `AddPartitionsToTxn`. The partition leader does that work.

Kafka 4.0 removed v0–v2. Its `validVersions` is `3-12`. This crate also speaks v13.

### Fetch

Spoken versions: 4–18. Version 19 and above are not spoken.

v4–v11 are classic. On v11, `RackId` is a non-nullable string. v12–v18 are flexible, with `LastFetchedEpoch` after `FetchOffset`. Request header version 2, response header version 1.

| Versions | What changed |
|---|---|
| 4–6 | One empty-response body. |
| 5 and above | Partition `LogStartOffset` is an INT64 after `LastFetchedEpoch` (Java `FetchRequest.PartitionData.logStartOffset`). Below v5 the encoder omits it and the decoder fills `INVALID_LOG_START_OFFSET`. |
| 7–11 | One empty-response body. `SessionId` and `SessionEpoch` are present (`FetchMetadata`). A session other than `LEGACY` is omitted below v7. `ForgottenTopicsData` is present; the ordinary encoder writes it empty, and a non-empty list is omitted below v7. From v13 those forgotten topics use topic ids. |
| 7 and above | Top-level `ErrorCode` is at bytes 4–5. |
| 12–17 | One empty-response body, in the flexible encoding. |
| 13 | Topic ids replace topic names on request `Topics`, `ForgottenTopics`, and response `Responses` (KIP-516). |
| 14 | Same layout as v13. Adds `OffsetMovedToTieredStorageException` (KIP-405). |
| 15 | Untagged `ReplicaId` is gone. `ReplicaState` is tagged field 1 (KIP-903). This crate omits that tag because the consumer defaults are `-1` / `-1`. `MaxWaitMs` is the first untagged field. |
| 16 | Same consumer request as v15 (KIP-951). |
| 17 | Same consumer request as v16. `ReplicaDirectoryId` tagged field 0 is follower-only and omitted. |
| 18 | Request partition `HighWatermark` is tagged field 1 (KIP-1166). Ordinary consumers omit it. The response matches v17. |

`MaxWaitMs` and `MinBytes` are JSON `0+`. Decode returns them. Encode takes `max_wait_ms` and `min_bytes`. Through v14, untagged `ReplicaId` comes before `MaxWaitMs`.

`ReplicaId` is JSON `0-14`, an untagged INT32 (`encode_fetch_request_with_replica_id`). The convenience encoder writes `CONSUMER_REPLICA_ID`. From v15 the field is omitted even when it is not the default, and decode fills `CONSUMER_REPLICA_ID`. That field is separate from `ReplicaState` tagged field 1.

`ThrottleTimeMs` is JSON `1+` and is the first response field (`encode_fetch_response_with_throttle`). The convenience encoder writes `0`. Java `getErrorResponse` copies `throttleTimeMs` from its argument.

`LastFetchedEpoch` is the leader epoch of the last record batch the consumer read. It is `-1` after assign or seek until a batch arrives, and it comes from OffsetFetch `leader_epoch` on group assign.

From v12, partition `CurrentLeader` tagged field 1 (`LeaderId` INT32, `LeaderEpoch` INT32, nested tags) is decoded. A retriable Fetch error whose leader names a known broker updates the leader cache and retries without a Metadata refresh. Preferred-replica redirects and `DivergingEpoch` seeks retry the same way. An unknown leader is inserted from top-level `NodeEndpoints` tagged field 0 on v16 and above. That array has the same inner layout as Produce.

`DivergingEpoch` is tagged field 0 from v12: `Epoch` INT32, `EndOffset` INT64, then nested tags. When it is present the consumer seeks to that end offset and retries without `retry.backoff.ms`.

`SnapshotId` is tagged field 2 from v12: `EndOffset` INT64, then `Epoch` INT32, then nested tags. The JSON field order is the reverse of `DivergingEpoch`. An omitted value decodes as `-1` / `-1`. Below v12 the encoder omits it even when the body is non-default. This tag is not the FetchSnapshot API, and the client does not send those RPCs.

Kafka 4.0 removed v0–v3. Its `validVersions` is `4-17`. This crate also speaks v18.

### ListOffsets

Spoken versions: 1–11. Version 12 and above are not spoken.

v1–v5 are classic. v6–v11 are flexible. Request header version 2, response header version 1.

From v4 the request puts `current_leader_epoch` before the timestamp, and the response puts `leader_epoch` after the offset.

| Version | Timestamp this crate can ask for |
|---|---|
| 7 | `MAX_TIMESTAMP` `-3` (KIP-734). |
| 8 | `EARLIEST_LOCAL` `-4` (KIP-405). |
| 9 | `LATEST_TIERED` `-5` (KIP-1005). |
| 10 | `TimeoutMs` after `Topics` (KIP-1075). The client sends `request_timeout`, or the one-shot timeout from `list_offsets_timeout` / `list_offsets_with_isolation_timeout`. |
| 11 | `EARLIEST_PENDING_UPLOAD` `-6` (KIP-1023, `OffsetSpec::earliest_pending_upload`). |

`ReplicaId` is JSON `0+`, the first INT32 (`encode_list_offsets_topics_request_with_replica_id`, Java `ListOffsetsRequest.replicaId`). The convenience encoder writes `CONSUMER_REPLICA_ID`.

Kafka 4.0 `validVersions` is `1-10`. Apache 4.3.1 extends that range through v11, which this crate speaks.

### OffsetForLeaderEpoch

Spoken versions: 0–4. Version 5 and above are not spoken.

v0–v3 are classic. v4 is flexible. Request header version 2, response header version 1. v0 and v1 share one empty-topics body. v2 and v3 share another. There is no top-level error code.

| Version | What changed |
|---|---|
| 1 | The response adds `LeaderEpoch`. |
| 2 | The request adds `CurrentLeaderEpoch`. The response adds `ThrottleTimeMs`. |
| 3 | `ReplicaId`, an INT32, is the first field. The default in the schema is `-2`. |
| 4 | Flexible encoding. |

`ThrottleTimeMs` is JSON `2+` (`encode_offset_for_leader_epoch_topics_response_with_throttle`). Below v2 the encoder omits it and the decoder fills `0`. The convenience encoder writes `0`. Java `getErrorResponse` leaves the JSON default `0`.

`ReplicaId` is JSON `3+` (`encode_offset_for_leader_epoch_topics_request_with_replica_id`, Java `OffsetsForLeaderEpochRequest.replicaId`). The convenience encoder writes `CONSUMER_REPLICA_ID`. Below v3 the encoder omits a non-default value and the decoder fills `DEBUGGING_REPLICA_ID`.

Fetch `FENCED_LEADER_EPOCH` and `UNKNOWN_LEADER_EPOCH` recover with one RPC per current leader, each carrying that leader's topics and partitions.

Kafka 4.0 `validVersions` is `2-4`. It removed v0–v1. This crate still speaks v0 and v1.

### AddPartitionsToTxn

Spoken versions: 0–3. Version 4 and above are not spoken. v4 is the KIP-890 batched-transaction broker layout.

v0–v2 are classic and share one empty `ResultsByTopicV3AndBelow` body. v3 is flexible: compact strings and arrays, plus tagged fields on topics and at the top level. Request header version 2, response header version 1. Spoken versions have no top-level error code.

`ThrottleTimeMs` is JSON `0+` (`encode_add_partitions_to_txn_topics_response_with_throttle`). The convenience encoder writes `0`.

### AddOffsetsToTxn

Spoken versions: 0–4. Version 5 and above are not spoken.

v0–v2 are classic and share one empty-error body. v3–v4 are flexible and share one empty-error body. Request header version 2, response header version 1. v4 is `TRANSACTION_ABORTABLE` (KIP-890) with the same layout as v3.

The top-level error code is at bytes 4–5. `error_counts` counts that code only, including `NONE` (Java `Collections.singletonMap`).

`ThrottleTimeMs` is JSON `0+` (`encode_add_offsets_to_txn_response_with_throttle`). The convenience encoder writes `0`.

`ProducerId` is JSON `0+`, an INT64 after `TransactionalId` (Java `AddOffsetsToTxnRequestData.producerId`). `ProducerEpoch` is the INT16 after it (Java `AddOffsetsToTxnRequestData.producerEpoch`). Encode takes both. They are not the producer id or epoch on EndTxn, InitProducerId, TxnOffsetCommit, AddPartitionsToTxn, or WriteTxnMarkers.

Kafka 4.0 `validVersions` is `0-4`.

### EndTxn

Spoken versions: 0–5. Version 6 and above are not spoken.

v0–v2 are classic and share one empty-error body. v3–v5 are flexible. Request header version 2, response header version 1. v3 and v4 share one empty-error body.

| Version | What changed |
|---|---|
| 4 | `TRANSACTION_ABORTABLE` (KIP-890). Same request layout as v3. |
| 5 | The response adds `ProducerId` and `ProducerEpoch` (KIP-890 part 2). |

The top-level error code is at bytes 4–5. `error_counts` counts that code only, including `NONE` (Java `Collections.singletonMap`). `ThrottleTimeMs` is JSON `0+` (`encode_end_txn_response_with_throttle`). The convenience encoder writes `0`.

After a successful EndTxn v5, the producer stores the returned id and epoch when `producer_id >= 0`. If that identity changed, it clears per-partition sequences.

Kafka 4.0 `validVersions` is `0-5`.

### TxnOffsetCommit

Spoken versions: 0–5. Version 6 and above are not spoken.

v0–v2 are classic and share one empty-topics body. v2 adds the committed leader epoch. v3–v5 are flexible and share one empty-topics body. Request header version 2, response header version 1. There is no top-level error code.

v3 adds `GenerationId`, `MemberId`, and `GroupInstanceId`. `send_offsets_for_group` fills those from `ConsumerGroupMetadata`. `send_offsets_to_transaction` sends `-1`, empty, and null. v4 is `TRANSACTION_ABORTABLE` (KIP-890) with the same layout as v3. v5 is the same layout again (KIP-890 part 2, transaction version 2).

When the broker advertises v5, `send_offsets_*` skips `AddOffsetsToTxn`. The group coordinator does that work.

`ThrottleTimeMs` is JSON `0+` (`encode_txn_offset_commit_topics_response_with_throttle`). The convenience encoder writes `0`.

`ProducerId` is JSON `0+`, an INT64 after `GroupId` (Java `TxnOffsetCommitRequestData.producerId`). `ProducerEpoch` is the INT16 after it (Java `TxnOffsetCommitRequestData.producerEpoch`). Encode takes both. They are not the producer id or epoch on AddOffsetsToTxn, the EndTxn response, InitProducerId, AddPartitionsToTxn, or WriteTxnMarkers.

Kafka 4.0 `validVersions` is `0-5`.

### FindCoordinator

Spoken versions: 0–6. Version 7 and above are not spoken.

v0–v2 are classic. v0 has no `KeyType` and accepts only the `GROUP` coordinator. v1–v2 send `Key` and `KeyType` and share one empty-error body. v3–v6 are flexible. Request header version 2, response header version 1. v3 is one compact key. v4, v5, and v6 share one empty-`Coordinators` body.

| Version | What changed |
|---|---|
| 4–6 | `CoordinatorKeys` replaces `Key`, and `Coordinators` replaces the single top-level coordinator (KIP-699). |
| 5 | `TRANSACTION_ABORTABLE`. |
| 6 | Share groups (KIP-932). |

On v1–v3 the top-level error code is at bytes 4–5. v4 and above have no top-level error code. `has_error` reads that top-level code only (Java `FindCoordinatorResponse.hasError`). Coordinator codes inside a v4+ batch are ignored.

`ThrottleTimeMs` is JSON `1+` (`encode_find_coordinator_response_coordinators_with_throttle`). The convenience encoder writes `0`. Java `getErrorResponse` copies the throttle argument on v2 and above. v1 leaves the JSON default `0`.

`list_consumer_group_offsets_for_groups`, `describe_groups`, `delete_groups`, `consumer_group_describe`, `share_group_describe`, `describe_share_group_offsets`, and `describe_transactions` send N `CoordinatorKeys` on v4 and above: one FindCoordinator per discovery retry, then one RPC per coordinator. Brokers that only speak v0–v3 get one FindCoordinator per key.

### OffsetCommit

Spoken versions: 2–10. Versions 0, 1, and 11 and above are not spoken.

v2–v7 are classic. v8–v10 are flexible: compact strings and arrays, plus tagged fields on partitions, topics, and the top level. Request header version 2, response header version 1.

| Version | What changed |
|---|---|
| 2–4 | v3 and v4 match the v2 body, including `RetentionTimeMs`. This crate sends `-1`. |
| 3 and above | `ThrottleTimeMs` is present. |
| 5 | Retention is removed. |
| 6 | `CommittedLeaderEpoch`. |
| 7 | `GroupInstanceId`. |
| 8 | Flexible encoding. |
| 9 | KIP-848 error codes. Same layout as v8. |
| 10 | Each topic name is a 16-byte UUID. |

`GenerationIdOrMemberEpoch` is JSON `1+`, an INT32 after `GroupId`. Encode takes `generation_id`. Java calls it `OffsetCommitRequestData.generationIdOrMemberEpoch` and `OffsetCommitRequest.DEFAULT_GENERATION_ID`. It is a different field from SyncGroup, Heartbeat, the JoinGroup response, and TxnOffsetCommit generation ids.

Name-based commits stay on v2–v9. The group negotiates 2–10 and uses the UUID body when the broker's maximum is 10.

Kafka 4.0 `validVersions` is `2-9`. It removed v0–v1. This crate also speaks v10.

`alter_consumer_group_offsets_timeout` is the RPC deadline (Java `AlterConsumerGroupOffsetsOptions.timeoutMs`). OffsetCommit has no `TimeoutMs` field.

### OffsetFetch

Spoken versions: 1–10. Version 0 and versions 11 and above are not spoken.

v1–v5 are classic. v6–v10 are flexible. Request header version 2, response header version 1.

| Version | What changed |
|---|---|
| 2–5 | The request is `GroupId` and `Topics`. v3, v4, and v5 match that v2 request. |
| 2 | `Topics` is nullable. A null topic list means every committed partition (`Admin::list_all_consumer_group_offsets`). The response has a top-level error code. |
| 3 | `ThrottleTimeMs`. |
| 5 | `CommittedLeaderEpoch`. Below v5, decode fills `-1`. |
| 7 | `RequireStable`. It is true when `isolation.level` is read-committed. `list_consumer_group_offsets_with` and `list_all_consumer_group_offsets_with` send the Admin flag. |
| 8 | `Groups` replaces `GroupId` and `Topics` (KIP-709). `list_consumer_group_offsets_for_groups` sends one `Groups` array per coordinator. |
| 9 | Each group carries `MemberId` and `MemberEpoch` (KIP-848). Classic groups send null and `-1`. |
| 10 | Each topic name is a 16-byte UUID. |

The name-only encoder stays on v1–v9. The group negotiates 1–10 and uses the UUID body when the broker's maximum is 10.

`list_all_consumer_group_offsets_timeout` and `list_consumer_group_offsets_for_groups_timeout` are the RPC deadline (Java `ListConsumerGroupOffsetsOptions.timeoutMs`). OffsetFetch has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `1-9`. It removed v0. This crate also speaks v10.

### OffsetDelete

Spoken versions: 0. Version 1 and above are not spoken.

v0 is classic. Apache lists `flexibleVersions: none`. There is one empty-topics body. Kafka 4.0 `validVersions` is `"0"`.

The top-level error code is at bytes 0–1. `ThrottleTimeMs` is JSON `0+` and comes after that code, at bytes 2–5 (`encode_offset_delete_response_with_throttle`). The convenience encoder writes `0`. Java `getErrorResponse` copies the throttle argument.

### Heartbeat

Spoken versions: 0–4. Version 5 and above are not spoken.

v0–v3 are classic. v1 and v2 match the v0 body. v4 is flexible. Request header version 2, response header version 1.

`ThrottleTimeMs` is present from v1. v3 adds `GroupInstanceId`. `error_counts` counts the top-level error code only, including `NONE` (Java `Collections.singletonMap`).

Kafka 4.0 `validVersions` is `0-4`.

### SyncGroup

Spoken versions: 0–5. Version 6 and above are not spoken.

v0–v3 are classic. v1 and v2 match the v0 body. v4–v5 are flexible: compact strings, bytes, and arrays, plus tagged fields. Request header version 2, response header version 1.

| Version | What changed |
|---|---|
| 1 and above | `ThrottleTimeMs`. |
| 3 | `GroupInstanceId`. |
| 5 | `ProtocolType` and `ProtocolName` (KIP-559). This crate sends `"consumer"` and the selected assignor. |

`GenerationId` is JSON `0+`, an INT32 after `GroupId` (Java `SyncGroupRequestData.generationId`). Encode takes `generation_id`. It is not the generation id on OffsetCommit, Heartbeat, or the JoinGroup response. `error_counts` counts the top-level error code only, including `NONE`.

Kafka 4.0 `validVersions` is `0-5`.

### JoinGroup

Spoken versions: 2–9. Versions 0, 1, and 10 and above are not spoken.

v2–v5 are classic. v2 and v3 match the v1 body, including `RebalanceTimeoutMs`. v6–v9 are flexible. Request header version 2, response header version 1. v2–v5 share one empty-members body. v6 is the compact form of that body. v7 and v8 share one body. v9 adds `SkipAssignment`.

The top-level error code is at bytes 4–5. `error_counts` counts that code only, including `NONE`. `ThrottleTimeMs` is JSON `2+` (`encode_join_group_response_with_throttle`). The convenience encoder writes `0`. Java `getErrorResponse` copies the throttle argument.

| Field | Where it sits |
|---|---|
| `SessionTimeoutMs` | JSON `0+`. Decode returns it. Encode takes `session_timeout_ms`. |
| `RebalanceTimeoutMs` | JSON `1+`. It is on the wire for every spoken version. Encode writes `rebalance_timeout_ms`. The classic consumer sends `max.poll.interval.ms`. |
| `ProtocolType` | JSON `0+`. Decode returns it last. Encode takes `protocol_type` (Java `JoinGroupRequestData.protocolType`). |
| Member `GroupInstanceId` | JSON `5+`, a nullable string after the member's `MemberId` (Java `JoinGroupResponseData.JoinGroupResponseMember.groupInstanceId`). Below v5 the encoder omits it and the decoder fills `None`. This is the response member field, not the request, SyncGroup, Heartbeat, OffsetCommit, or LeaveGroup instance id. |
| `SkipAssignment` | JSON `9+` (`encode_join_group_response_with_skip_assignment`, Java `JoinGroupResponseData.skipAssignment`). Below v9 the encoder omits it and the decoder fills `false`. The convenience encoder writes `false`. Java `getErrorResponse` leaves it unset. |

| Version | What changed |
|---|---|
| 4 | A second join may carry the assigned member id. |
| 5 | The request adds `GroupInstanceId`. |
| 7 | The response adds `ProtocolType` (KIP-559) and a nullable `ProtocolName`. |
| 8 | `Reason` (KIP-800). The first join sends null. `enforce_rebalance` and `enforce_rebalance_with` send the reason. |
| 9 | `SkipAssignment`. When it is true, the leader does not run the assignor. |

`join`, `join_sticky`, and `join_cooperative_sticky` send one protocol. `join_with_assignors` sends N protocols (Java `partition.assignment.strategy`). The client assigns with the `ProtocolName` in the JoinGroup response.

Kafka 4.0 `validVersions` is `2-9`. It removed v0–v1.

### LeaveGroup

Spoken versions: 0–5. Version 6 and above are not spoken.

v0–v3 are classic. v0–v2 are `GroupId` plus `MemberId`, and v1 and v2 match the v0 body. v4–v5 are flexible. Request header version 2, response header version 1.

`ThrottleTimeMs` is present from v1. v3 adds `Members` and `GroupInstanceId`. v5 adds `Reason` (KIP-800).

Classic `ConsumerGroup::leave` and `close` send `"the consumer is being closed"`. `unsubscribe` sends `"the consumer unsubscribed from all topics"`. Expiry of `max.poll.interval.ms` sends `"consumer poll timeout has expired."`. Admin `removeMembersFromConsumerGroup` stays on v3–v5 and sends `"member was removed by an admin"`.

Kafka 4.0 `validVersions` is `0-5`.

### ConsumerGroupHeartbeat

Spoken versions: 0–1. Version 2 and above are not spoken.

v0 and v1 are flexible. Request header version 2, response header version 1. The v1 response matches v0.

v1 adds `SubscribedTopicRegex` after `SubscribedTopicNames` (KIP-848) and requires the consumer to generate its own `MemberId` (KIP-1082, a Kafka `Uuid` in URL-safe Base64). `join_consumer_matching` still expands topics locally with `Fn(&str) -> bool`, sends `SubscribedTopicNames`, and leaves the regex null.

`RebalanceTimeoutMs` is JSON `0+`, an INT32 after `RackId`. Encode writes `rebalance_timeout_ms`. The JSON default `-1` means unchanged. Join sends `max.poll.interval.ms`.

`ServerAssignor` is JSON `0+`, a nullable compact string after `SubscribedTopicRegex` on v1 and after `SubscribedTopicNames` on v0. Encode writes `server_assignor`. The JSON default null means unused or unchanged. This crate sends null.

`ThrottleTimeMs` is JSON `0+` (`ConsumerGroupHeartbeatResponse.throttle_time_ms`). The convenience encoder writes `0`. `error_counts` counts the top-level error code only, including `NONE`. The assignment is not counted.

Kafka 4.0 `validVersions` is `0-1`.

### ConsumerGroupDescribe

Spoken versions: 0–1. Version 2 and above are not spoken.

v0 and v1 are flexible. Request header version 2, response header version 1. The request layout is the same on both. The v1 response adds `MemberType` INT8 (KIP-1099). The error code is per group.

`ThrottleTimeMs` is JSON `0+` (`encode_consumer_group_describe_response_with_throttle`). `encode_consumer_group_describe_response` writes `0`. Java `getErrorResponse` copies the group ids through `error_described_group_list`, sets `ErrorMessage` to JSON null, and copies `throttleTimeMs` from its argument. The request's `IncludeAuthorizedOperations` is not copied onto that error response.

`consumer_group_describe` sends FindCoordinator v4+ with N `CoordinatorKeys` (KIP-699) and one ConsumerGroupDescribe per coordinator. `consumer_group_describe_timeout` is the RPC deadline. The request has no `TimeoutMs` field.

`Admin::describe_consumer_groups` tries this API first (Java `describeConsumerGroups`). It falls back to DescribeGroups on `UNSUPPORTED_VERSION` (35), `GROUP_ID_NOT_FOUND` (69), or when the broker does not advertise api 69.

Kafka 4.0 `validVersions` is `0-1`.

### ShareGroupHeartbeat

Spoken versions: 0–1. Version 2 and above are not spoken.

v0 and v1 are flexible and use the same fields. Request header version 2, response header version 1. The v1 response matches v0.

`RackId` is JSON `0+`, a nullable compact string after `MemberEpoch`. Encode writes `rack_id`. The JSON default null means not provided or unchanged. This crate sends `ConsumerConfig::rack`.

`ThrottleTimeMs` is JSON `0+` (`ShareGroupHeartbeatResponse.throttle_time_ms`). The convenience encoder writes `0`. `error_counts` counts the top-level error code only, including `NONE`. The assignment is not counted.

Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`). Kafka 4.1 `validVersions` is `"1"` and removed v0. This crate speaks both.

### ShareGroupDescribe

Spoken versions: 0–1. Version 2 and above are not spoken.

v0 and v1 are flexible and use the same fields. Request header version 2, response header version 1. The v1 response matches v0. The error code is per group.

`ThrottleTimeMs` is JSON `0+` (`encode_share_group_describe_response_with_throttle`). `encode_share_group_describe_response` writes `0`. Java `getErrorResponse` copies the group ids through `error_described_group_list`, sets `ErrorMessage` to JSON null, and copies `throttleTimeMs` from its argument. The request's `IncludeAuthorizedOperations` is not copied onto that error response.

Java `DescribeShareGroupsHandler` uses `CoordinatorType.GROUP`. `share_group_describe` and `describe_share_groups` send FindCoordinator v4+ with N `CoordinatorKeys` (KIP-699) and one ShareGroupDescribe per coordinator. `share_group_describe_timeout` and `describe_share_groups_timeout` are the RPC deadline (Java `DescribeShareGroupsOptions.timeoutMs`). The request has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`). Kafka 4.1 `validVersions` is `"1"` and removed v0. This crate speaks both.

### StreamsGroupHeartbeat

Spoken versions: 0.

The body is flexible (request header 2, response header 1). This is the Streams broker protocol, separate from consumer and share groups. `StreamsClient` sends a caller-driven v0 heartbeat. It does not run a Streams execution engine.

v1+ is not spoken.

### StreamsGroupDescribe

Spoken versions: 0.

The body is flexible (request header 2, response header 1). Admin describe uses this v0 body and keeps the typed group, member, and topology fields.

v1+ is not spoken.

### ShareFetch

Spoken versions: 0–2. Version 3 and above are not spoken.

v0–v2 are flexible. Request header version 2, response header version 1. The share group negotiates 0–2.

| Version | What changed |
|---|---|
| 0 | Each partition has `PartitionMaxBytes`. |
| 1 | `MaxRecords` / `BatchSize` follow `MaxBytes` (JSON `1+`). There is no `PartitionMaxBytes`. v0 omits a non-zero batch size and decode fills 0. `encode_share_fetch_request` writes `BatchSize` as `MaxRecords`. `encode_share_fetch_request_with_batch_size` writes the separate field. `AcquisitionLockTimeoutMs` follows `ErrorMessage`. |
| 2 | The request adds `ShareAcquireMode` and acknowledgement type 4, renew (KIP-1206 / KIP-1222). The response keeps the v1 fields. |

`MaxWaitMs`, `MinBytes`, and `MaxBytes` are JSON `0+`. Decode returns them. Encode takes `max_wait_ms`, `min_bytes`, and `max_bytes`. The JSON default for `MaxBytes` is `0x7fffffff`.

`Records` is the JSON `records` field. Kafka 4.0 marks it nullable on every version. Kafka 4.1 marks it nullable on v0 only. Decode accepts a compact null as empty on v0. A v1 null is `Error::protocol` (Java: `non-nullable field records was serialized as null`). Encode writes `MemoryRecords.EMPTY`, not null.

`AcquisitionLockTimeoutMs` is JSON `1+` (`encode_share_fetch_response_with_acquisition_lock_timeout`). The ordinary response encoder writes 15000. v0 omits a non-zero value and decode fills 0. The error path stays at 0.

`ForgottenTopicsData` is JSON `0+`. The ordinary encoder writes it empty (`encode_share_fetch_request_with_forgotten` writes the caller's list). Duplicate partition indexes are kept. The topic is a UUID, with no name.

These response fields are untagged, not the Fetch tagged fields with the same names:

| Field | Default the ordinary encoder writes |
|---|---|
| `NodeEndpoints` | Empty. Compact array of `NodeId` INT32, `Host` compact string, `Port` INT32, `Rack` compact nullable string, then nested tags. |
| Partition `CurrentLeader` | `0` / `0`. Nested `LeaderId` INT32, `LeaderEpoch` INT32, then nested tags. |
| Partition `ErrorMessage` | Null. Separate from the top-level `ErrorMessage`. |
| Partition `AcknowledgeErrorCode` | `0`. Separate from the fetch `ErrorCode`. The schema lists `INVALID_RECORD_STATE` as acknowledge-only. |
| Partition `AcknowledgeErrorMessage` | Null. Separate from the fetch `ErrorMessage`. |

`ThrottleTimeMs` and the top-level `ErrorCode` and `ErrorMessage` are JSON `0+`. The ordinary encoder writes throttle `0`, error code `0`, and a null message. Decode returns a non-zero error code and does not fail the response. `encode_share_fetch_error` writes throttle `0`. `encode_share_fetch_error_with_throttle` round-trips a non-zero value. Java `getErrorResponse` and `ShareFetchResponse.of` set `throttleTimeMs`, leave `Responses` empty, and leave v1 `AcquisitionLockTimeoutMs` at `0`.

Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`). Kafka 4.1 `validVersions` is `"1"` and removed v0. This crate speaks 0–2.

### ShareAcknowledge

Spoken versions: 0–2. Version 3 and above are not spoken.

v0–v2 are flexible. Request header version 2, response header version 1. v0 and v1 use the same fields, and the v1 response matches v0. The share group negotiates 0–2.

v2 adds `IsRenewAck` on the request and `AcquisitionLockTimeoutMs` on the response (KIP-1222). Acknowledgement type 4 renews a lock.

`NodeEndpoints`, partition `CurrentLeader`, and partition `ErrorMessage` have the same untagged layouts as ShareFetch. The ordinary encoder writes an empty endpoint list, leader `0` / `0`, and a null partition message. Those are not the Fetch tagged fields, and the partition message is not the top-level `ErrorMessage`.

`ThrottleTimeMs` and the top-level `ErrorMessage` are JSON `0+`. The ordinary encoder writes throttle `0` and a null message. v0 and v1 bodies match.

Kafka 4.0 `validVersions` is `"0"` (`latestVersionUnstable`). Kafka 4.1 `validVersions` is `"1"` and removed v0. This crate speaks 0–2.

### DescribeShareGroupOffsets

Spoken versions: 0. Version 1 and above are not spoken. v1 would add lag (KIP-1226).

v0 is flexible. Request header version 2, response header version 1. There is no top-level error code. Each group has its own error code.

`ThrottleTimeMs` is JSON `0+` (`encode_describe_share_group_offsets_response_with_throttle`). The ordinary encoder writes `0`.

`error_counts` keeps the last non-`NONE` group code for each `GroupId`, then every partition code, including `NONE`. `has_group_error` is true when any matching group id has a non-`NONE` group code. Partition codes are ignored. Java `groupError` returns a `Throwable` and is not mapped.

Java `getErrorResponse` copies the group ids, leaves `Topics` empty, sets `ErrorMessage` to JSON null, and copies `throttleTimeMs` from its argument. The request topics are not copied. `error_described_group` is one group id plus an error code, empty topics, and a null message. Java sets the English `Errors.message` there.

Java `ListShareGroupOffsetsHandler` uses `CoordinatorType.GROUP`. `describe_share_group_offsets` and `list_share_group_offsets` send FindCoordinator v4+ with N `CoordinatorKeys` and one DescribeShareGroupOffsets per coordinator. The list, alter, and delete timeouts are RPC deadlines. None of these three requests has a `TimeoutMs` field.

### AlterShareGroupOffsets

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. The error code is top-level, after throttle. Topics have no error code. `ThrottleTimeMs` is JSON `0+`. `AlteredShareGroupOffsets::new` fills `0`.

`error_counts` counts the top-level code and every partition code, including `NONE`. Java `getErrorResponse` leaves `Responses` empty, sets `ErrorMessage` to JSON null, copies `throttleTimeMs` from its argument, and sets the English `Errors.message`. It does not copy `GroupId` or `Topics`.

Kafka 4.1 `validVersions` is `"0"`.

### DeleteShareGroupOffsets

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. The error code is top-level, after throttle. There is no partition error code. `ThrottleTimeMs` is JSON `0+`. `DeletedShareGroupOffsets::new` fills `0`.

`error_counts` counts the top-level code and every topic code, including `NONE`. Java `getErrorResponse` leaves `Responses` empty, sets `ErrorMessage` to JSON null, copies `throttleTimeMs` from its argument, and sets the English `Errors.message`. It does not copy `GroupId` or `Topics`.

Kafka 4.1 `validVersions` is `"0"`.

### DescribeTopicPartitions

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. There is no top-level error code. `ThrottleTimeMs` is JSON `0+`. `DescribeTopicPartitionsResponse::new` fills `0`.

Kafka 4.0 `validVersions` is `"0"`.

### ListConfigResources

Spoken versions: 0–1. Version 2 and above are not spoken.

v0 and v1 are flexible and share one empty-resource body. Request header version 2, response header version 1. The error code is top-level, after throttle. `ThrottleTimeMs` is JSON `0+`. `ListConfigResourcesResponse::new` fills `0`.

`error_counts` counts that top-level code only, including `NONE`. Config resources are not counted. `error` is an `ApiError` from the code. An unknown code becomes `UNKNOWN_SERVER_ERROR`. The JSON schema has no `ErrorMessage`, and this crate fills a null message. Java `getErrorResponse` leaves `ConfigResources` empty and copies `throttleTimeMs` from its argument.

`supported_resource_types` is `CLIENT_METRICS` only on v0. v1 adds `TOPIC`, `BROKER`, `BROKER_LOGGER`, and `GROUP`. `UNKNOWN` is not included. `build` requires v0 `ResourceTypes` to be exactly `CLIENT_METRICS` (Java `HashSet`). Encode still omits `ResourceTypes` on v0. `config_resources` maps each id with `Type.forId`. An unknown id is `UNKNOWN`.

Kafka 4.0 `validVersions` is `"0"` (ListClientMetricsResources). Kafka 4.1 `validVersions` is `"0-1"`. v1 adds `ResourceType` (KIP-1142).

### GetTelemetrySubscriptions

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. The error code is top-level, after throttle. `ThrottleTimeMs` is JSON `0+`. `GetTelemetrySubscriptionsResponse::new` fills `0`.

`error_counts` counts that code only, including `NONE`. `ClientInstanceId`, compression types, and requested metrics are not counted. `has_error` is `error() != NONE`. Java `getErrorResponse` uses the JSON defaults for `ClientInstanceId` and the subscription fields, and copies `throttleTimeMs` from its argument.

Kafka 4.0 `validVersions` is `"0"`.

### PushTelemetry

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible: compact strings and bytes, plus tagged fields. Request header version 2, response header version 1. The error code is top-level, after throttle. `ThrottleTimeMs` is JSON `0+`. `PushTelemetryResponse::new` fills `0`.

`error_counts` counts that code only, including `NONE`. `has_error` is `error() != NONE`. Java `getErrorResponse` delegates to `errorResponse` and copies `throttleTimeMs` from its argument.

`metrics_content_type` is `OTLP`. The request has no content-type field. `metrics_data` returns the stored bytes for `NONE`. Gzip, Snappy, and LZ4 decompress through `ClientTelemetryUtils.decompress`. Zstd needs the optional feature. An unknown id is `IllegalArgumentException`.

Kafka 4.0 `validVersions` is `"0"`.

### AssignReplicasToDirs

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. The error code is top-level, after throttle. `ThrottleTimeMs` is JSON `0+`. `AssignReplicasToDirsResponse::new` fills `0`.

Java `getErrorResponse` leaves `Directories` empty and does not copy `BrokerId`, `BrokerEpoch`, or `Directories`. `MAX_ASSIGNMENTS_PER_REQUEST` is 2250. Encode does not enforce that cap.

Kafka 4.0 `validVersions` is `"0"`.

ErrorCode is top-level after throttle.

### SaslHandshake

Spoken versions: 0–1. Version 2 and above are not spoken (KAFKA-9577).

v0 and v1 are classic and use the same fields. Apache lists `flexibleVersions: none`. v1 is what enables SaslAuthenticate.

`error_counts` counts the top-level error code only, including `NONE`. Mechanism names are not counted.

Kafka 4.0 `validVersions` is `0-1`.

### SaslAuthenticate

Spoken versions: 0–2. Version 3 and above are not spoken.

v0 and v1 are classic and share one `AuthBytes` request. v2 is flexible. Request header version 2, response header version 1.

v1 and v2 add `SessionLifetimeMs`. `error_counts` counts the top-level error code only, including `NONE`. `AuthBytes`, `ErrorMessage`, and `SessionLifetimeMs` are not counted.

Kafka 4.0 `validVersions` is `0-2`.

### ListTransactions

Spoken versions: 0–2. Version 3 and above are not spoken.

v0–v2 are flexible. Request header version 2, response header version 1. v0 and v1 share one empty-`TransactionStates` body. The v1 and v2 responses match v0. The top-level error code is at bytes 4–5.

| Version | What changed |
|---|---|
| 1 | `DurationFilter`, an INT64 after `ProducerIdFilters` (KIP-994). A negative value means no filter. |
| 2 | Nullable `TransactionalIdPattern`. |

A pattern on v0 or v1 is refused. A nonempty pattern requires v2 and is not dropped when the broker is older. Admin negotiates 0–2.

`ThrottleTimeMs` is JSON `0+`. `ListTransactionsResponse::new` fills `0`. `error_counts` counts the top-level error code only, including `NONE`. Unknown state filters and transaction listings are not counted. Java `getErrorResponse` leaves `UnknownStateFilters` and `TransactionStates` empty and does not copy the request's `StateFilters`, `ProducerIdFilters`, or `DurationFilter`.

Kafka 4.0 `validVersions` is `0-1`. This crate also speaks v2.

`list_transactions_timeout` and `list_transactions_with_duration_timeout` are the RPC deadline (Java `ListTransactionsOptions.timeoutMs`). The request has no `TimeoutMs` field. `duration_ms` is `DurationFilter`.

### DescribeTransactions

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. There is one empty-`TransactionStates` body and no top-level error code. The error code is per transactional id.

`ThrottleTimeMs` is JSON `0+` (`encode_describe_transactions_response_with_throttle`). The convenience encoder writes `0`.

`describe_transactions` sends FindCoordinator v4+ with N `CoordinatorKeys` and `key_type` 1, then one DescribeTransactions per coordinator. `describe_transactions_timeout` is the RPC deadline (Java `DescribeTransactionsOptions.timeoutMs`). The request has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `"0"`.

### CreateTopics

Spoken versions: 0–7. Version 8 and above are not spoken.

v0–v4 are classic. v5–v7 are flexible. Request header version 2, response header version 1.

| Version | What changed |
|---|---|
| 5 | The response returns `NumPartitions`, `ReplicationFactor`, and `Configs` (KIP-525). |
| 6 | Same layout. Adds `THROTTLING_QUOTA_EXCEEDED` (KIP-599). |
| 7 | `TopicId` UUID after `Name` (KIP-516). |

`create_topics_timeout` is both the RPC deadline and the `TimeoutMs` field (Java `CreateTopicsOptions.timeoutMs`). `create_topics_with_quota_retry` defaults to true (Java `retryOnQuotaViolation`) and retries only topics that return `THROTTLING_QUOTA_EXCEEDED`.

`NewTopic.with_assignments` is Java `NewTopic(String, Map<Integer, List<Integer>>)`. `NumPartitions` and `ReplicationFactor` are `-1`. An empty assignment list is `NewTopic(String, int, short)`, and the JSON field is not nullable, so it is not null. `NewTopic.broker_defaults` is Java `NewTopic(String, Optional.empty(), Optional.empty())` (KIP-464): `-1` / `-1` and empty assignments. From v4 the broker uses `num.partitions` and `default.replication.factor`. `NewTopic.configs` replaces the previous config entries (Java `NewTopic.configs(Map)`).

Kafka 4.0 `validVersions` is `2-7`. It removed v0–v1. This crate still speaks v0 and v1.

### DeleteTopics

Spoken versions: 0–6. Version 7 and above are not spoken.

v0–v3 are classic: `TopicNames` and `TimeoutMs`. v4–v6 are flexible. Request header version 2, response header version 1.

v5 adds `ErrorMessage` (KIP-599). v6 replaces `TopicNames` with `Topics` of `Name` plus `TopicId` (KIP-516). A name-based delete sends a zero UUID. `delete_topics_by_id` sends a null name (Java `deleteTopics(TopicCollection.ofTopicIds)`).

`delete_topics_timeout` is both the RPC deadline and `TimeoutMs`. `delete_topics_with_quota_retry` and `delete_topics_by_id_with_quota_retry` default to true and retry only topics that return `THROTTLING_QUOTA_EXCEEDED`.

Kafka 4.0 `validVersions` is `1-6`. It removed v0. This crate still speaks v0.

### DescribeConfigs

Spoken versions: 0–4. Version 5 and above are not spoken.

v0–v3 are classic. v4 is flexible. Request header version 2, response header version 1. `ThrottleTimeMs` is JSON `0+` and is on the wire for every spoken version.

| Version | What changed |
|---|---|
| 1 | `IncludeSynonyms`, `ConfigSource`, and `Synonyms`. |
| 2 | Same layout as v1. `shouldClientThrottle` applies from here (KIP-219). |
| 3 | `IncludeDocumentation`, `ConfigType`, and `Documentation` (KIP-226). |
| 4 | Flexible encoding. |

`describe_configs_with_documentation` is Java `DescribeConfigsOptions.includeDocumentation`. v0–v2 omit that field. `describe_configs_timeout` and `describe_configs_with_documentation_timeout` are the RPC deadline. The request has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `1-4`. It removed v0. This crate still speaks v0.

### CreatePartitions

Spoken versions: 0–3. Version 4 and above are not spoken.

v0–v1 are classic and share one empty-results body. v2–v3 are flexible and share one empty-results body. Request header version 2, response header version 1. v3 is the same layout as v2 and adds `THROTTLING_QUOTA_EXCEEDED` (KIP-599).

`ThrottleTimeMs` is JSON `0+` (`encode_create_partitions_response_with_throttle`). The convenience encoder writes `0`.

`create_partitions_timeout` is both the RPC deadline and `TimeoutMs`. `create_partitions_with_quota_retry` defaults to true and retries only topics that return `THROTTLING_QUOTA_EXCEEDED`. `NewPartitions.with_assignments` is Java `increaseTo(int, List<List<Integer>>)`. Omitted assignments are null, and the broker places the replicas.

Kafka 4.0 `validVersions` is `0-3`.

### AlterPartitionReassignments

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. `ThrottleTimeMs` is JSON `0+`. `::new` and `::error` fill `0`. `alter_partition_reassignments_timeout` is both the RPC deadline and `TimeoutMs` (Java `AlterPartitionReassignmentsOptions.timeoutMs`).

Kafka 4.0 `validVersions` is `"0"`.

### ListPartitionReassignments

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. `topics = None` lists every ongoing reassignment. `list_partition_reassignments_all` is Java `listPartitionReassignments()`. `list_partition_reassignments_for` is Java `listPartitionReassignments(Set)`.

`ThrottleTimeMs` is JSON `0+`. `::new` and `::error` fill `0`. `error_counts` counts the top-level error code only, including `NONE`. Topics and partitions are not counted. This is not `ListPartitionReassignmentsResponse.error` and not the AlterPartitionReassignments counts. The timeout is both the RPC deadline and `TimeoutMs`.

Kafka 4.0 `validVersions` is `"0"`.

### UpdateFeatures

Spoken versions: 0–2. Version 3 and above are not spoken.

v0–v2 are flexible from v0. Request header version 2, response header version 1. v0 and v1 share one empty-`Results` body. v2 omits `Results`.

| Version | What changed |
|---|---|
| 0 | `AllowDowngrade`. |
| 1 | `UpgradeType` and `ValidateOnly`. |
| 2 | `Results` is omitted. |

`ThrottleTimeMs` is JSON `0+`. `::new`, `::error`, and `create_with_errors` fill `0`. `update_features_timeout` and `update_features_with_timeout` are both the RPC deadline and `TimeoutMs`.

Kafka 4.0 `validVersions` is `0-2`.

### AlterUserScramCredentials

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. There is one empty-results body and no top-level error code. `ThrottleTimeMs` is JSON `0+` (`encode_alter_user_scram_credentials_response_with_throttle`). The convenience encoder writes `0`.

Kafka 4.0 `validVersions` is `"0"`.

### DescribeUserScramCredentials

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. There is one empty-results body. The top-level error code is at bytes 4–5. `ThrottleTimeMs` is JSON `0+`. `::new` and `::error` fill `0`.

Kafka 4.0 `validVersions` is `"0"`.

### AlterClientQuotas

Spoken versions: 0–1. Version 2 and above are not spoken.

v0 is classic. v1 is flexible. Request header version 2, response header version 1. The empty-entries bodies differ. There is no top-level error code. `ThrottleTimeMs` is JSON `0+` (`encode_alter_client_quotas_response_with_throttle`). The convenience encoder writes `0`.

Kafka 4.0 `validVersions` is `0-1`.

### DescribeClientQuotas

Spoken versions: 0–1. Version 2 and above are not spoken.

v0 is classic. v1 is flexible. Request header version 2, response header version 1. The empty-entries bodies differ. The top-level error code is at bytes 4–5. `error_counts` counts that code only, including `NONE`. Quota entries are not counted. This is not `DescribeClientQuotasResponse.error` and not the AlterClientQuotas counts.

`ThrottleTimeMs` is JSON `0+`. `::new` and `::error` fill `0`.

Kafka 4.0 `validVersions` is `0-1`.

### IncrementalAlterConfigs

Spoken versions: 0–1. Version 2 and above are not spoken.

v0 is classic. v1 is flexible. Request header version 2, response header version 1. The empty-responses bodies differ. `ThrottleTimeMs` is JSON `0+` (`encode_incremental_alter_configs_resource_results_with_throttle`). The convenience encoder writes `0`.

`incremental_alter_configs_for` sends N resources (Java `incrementalAlterConfigs(Map)`). `AlterConfig::append` and `AlterConfig::subtract` are Java `APPEND` and `SUBTRACT` for LIST configs, op 2 and op 3. `incremental_alter_configs_timeout` is the RPC deadline (Java `AlterConfigsOptions.timeoutMs`). The request has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `0-1`.

### AlterReplicaLogDirs

Spoken versions: 1–2. Version 0 and versions 3 and above are not spoken.

v1 is classic. v2 is flexible and uses the same fields. Request header version 2, response header version 1. `ThrottleTimeMs` is JSON `0+`. `AlterReplicaLogDirsResponse::new` fills `0`. `alter_replica_log_dirs_timeout` is the RPC deadline. The request has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `1-2`. It removed v0.

### DescribeLogDirs

Spoken versions: 1–5. Version 0 and versions 6 and above are not spoken.

v1 is classic. v2–v5 are flexible. Request header version 2, response header version 1. Admin negotiates 1–5.

| Version | What changed |
|---|---|
| 3 | Top-level error code (KIP-784). Below v3 the error-response encoder omits it. |
| 4 | `TotalBytes` and `UsableBytes` (KIP-827). v1–v3 decode fills `-1`. |
| 5 | `IsCordoned` on each directory. Older versions omit it and decode `false`. |

`ThrottleTimeMs` is JSON `0+`. `DescribeLogDirsResponse::new` fills `0`. Java `getErrorResponse` leaves `Results` empty, does not copy the request topics, and copies `throttleTimeMs` from its argument. The three describe timeouts are the RPC deadline. The request has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `1-4`. It removed v0. Kafka 4.3.1 extends the range through v5, which this crate speaks.

### Delegation tokens

Create, renew, expire, and describe are broker RPCs. The client picks the least-loaded node. A broker that forwards to the controller does that itself. The client does not send an api 41 hop for create, renew, or expire. Describe does not forward to the controller.

On every one of these responses the error code is the first field, bytes 0–1, and `ThrottleTimeMs` is the last field. It is JSON `0+`, and `::new` fills `0`. `error_counts` counts that top-level code only, including `NONE`. Token bytes and expiry are not counted. `has_error` is `error() != NONE`. Java `error()` is `Errors.forCode`. The crate `error()` on create stays `getErrorResponse`. Each timeout is the RPC deadline. None of the requests has a `TimeoutMs` field.

| API | Spoken | Framing | What changes |
|---|---|---|---|
| CreateDelegationToken | 1–3. v0 and v4+ are not spoken. | v1 classic. v2–v3 flexible. | v3 adds owner and token-requester principal type and name. v1–v2 decode fills `None` and empty. |
| RenewDelegationToken | 1–2. v0 and v3+ are not spoken. | v1 classic. v2 flexible. Same fields. | Java `getErrorResponse` leaves `ExpiryTimestampMs` at 0 and does not copy the HMAC or `RenewPeriodMs`. |
| ExpireDelegationToken | 1–2. v0 and v3+ are not spoken. | v1 classic. v2 flexible. Same fields. | Java `getErrorResponse` leaves `ExpiryTimestampMs` at 0 and does not copy the HMAC or `ExpiryTimePeriodMs`. |
| DescribeDelegationToken | 1–3. v0 and v4+ are not spoken. | v1 classic. v2–v3 flexible. v2 and v3 share one empty-tokens body. | Request `Owners` is the same on v1–v3. v3 adds token-requester principal type and name. v1–v2 decode fills empty. Java `getErrorResponse` leaves `Tokens` empty and does not copy `Owners`. |

Kafka 4.0 removed v0 for all four. Its ranges are create `1-3`, renew `1-2`, expire `1-2`, and describe `1-3`. Request header version 2 and response header version 1 apply to the flexible versions.

### DescribeGroups

Spoken versions: 0–6. Version 7 and above are not spoken.

v0–v4 are classic. v5–v6 are flexible. Request header version 2, response header version 1.

| Version | What changed |
|---|---|
| 1 | `ThrottleTimeMs`. |
| 3 | `IncludeAuthorizedOperations` and `AuthorizedOperations`. |
| 4 | `GroupInstanceId`. |
| 6 | `ErrorMessage` and `GROUP_ID_NOT_FOUND` (KIP-1043). |

`describe_groups` and `describe_classic_groups` send FindCoordinator v4+ with N `CoordinatorKeys` and one DescribeGroups per coordinator. `describe_consumer_groups` tries ConsumerGroupDescribe first and uses DescribeGroups for classic groups, on the same FindCoordinator hop. The three timeouts are the RPC deadline. Neither RPC has a `TimeoutMs` field.

Kafka 4.0 `validVersions` is `0-6`.

### ListGroups

Spoken versions: 0–5. Version 6 and above are not spoken.

v0–v2 are classic. v3–v5 are flexible. Request header version 2, response header version 1.

v1 adds `ThrottleTimeMs`. v4 adds `StatesFilter` and `GroupState` (KIP-518). v5 adds `TypesFilter` and `GroupType` (KIP-848). `error_counts` counts the top-level error code only, including `NONE`. Listed groups are not counted. `list_groups_timeout` and `list_consumer_groups_timeout` are the RPC deadline. The request has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `0-5`.

### DeleteGroups

Spoken versions: 0–2. Version 3 and above are not spoken.

v0–v1 are classic and share one empty-results body. v2 is flexible. Request header version 2, response header version 1. There is no top-level error code. `ThrottleTimeMs` is JSON `0+` (`encode_delete_groups_response_with_throttle`). The convenience encoder writes `0`.

Java `getErrorResponse` copies the group ids through `error_result_collection` and copies `throttleTimeMs` from its argument.

Kafka 4.0 `validVersions` is `0-2`.

### AlterConfigs

Spoken versions: 0–2. Version 3 and above are not spoken.

This is legacy api 33. v0–v1 are classic. v2 is flexible. Request header version 2, response header version 1. `ThrottleTimeMs` is JSON `0+` on every spoken version. `shouldClientThrottle` applies from v1 (KIP-219).

`alter_configs_for` sends N resources (Java `alterConfigs(Map)`). `alter_configs_timeout` is the RPC deadline. The request has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `0-2`.

### DeleteRecords

Spoken versions: 0–2. Version 3 and above are not spoken.

v0–v1 are classic. v2 is flexible. Request header version 2, response header version 1. `ThrottleTimeMs` is JSON `0+` on every spoken version. `shouldClientThrottle` applies from v1 (KIP-219).

`delete_records_for` sends one topics-and-partitions array per leader (Java `deleteRecords(Map)` of `RecordsToDelete`). `delete_records_timeout` and `delete_records_for_timeout` are both the RPC deadline and `TimeoutMs`.

Kafka 4.0 `validVersions` is `0-2`.

### DescribeProducers

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. There is one empty-topics body and no top-level error code. `ThrottleTimeMs` is JSON `0+`. `::new` fills `0`.

`describe_producers_for` sends one topics-and-partition-indexes array per leader (Java `describeProducers(Collection)`). Without `brokerId`, a `NOT_LEADER_OR_FOLLOWER` answer is the end of the attempt. The call does not move to the metadata leader, the controller, or the transaction coordinator. `describe_producers_for_on_broker` sends one RPC to that broker (Java `DescribeProducersOptions.brokerId`) and does not retry `NOT_LEADER_OR_FOLLOWER` against the metadata leader. The timeouts are the RPC deadline. The request has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `0`.

### AllocateProducerIds

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible: compact tagged fields. Request header version 2, response header version 1. The top-level error code is at bytes 4–5. `ThrottleTimeMs` is JSON `0+`. `::new` fills `0`.

`error_counts` counts that code only, including `NONE`. `ProducerIdStart` and `ProducerIdLen` are not counted. Java `getErrorResponse` leaves both at the JSON default `0` and does not copy `BrokerId` or `BrokerEpoch`.

Kafka 4.0 `validVersions` is `"0"`.

### UnregisterBroker

Spoken versions: 0. Version 1 and above are not spoken.

v0 is flexible. Request header version 2, response header version 1. There is one empty-`ErrorMessage` body. The top-level error code is at bytes 4–5. `ThrottleTimeMs` is JSON `0+`. `::new` fills `0`.

Java `getErrorResponse` sets `ErrorMessage` to JSON null and does not copy `BrokerId`.

Kafka 4.0 `validVersions` is `"0"`.

### DescribeCluster

Spoken versions: 0–2. Version 3 and above are not spoken.

v0–v2 are flexible from v0. Request header version 2, response header version 1. The empty-brokers body differs between v0 and v1. v1 and v2 share one body. The top-level error code is at bytes 4–5.

v1 adds `EndpointType`. v2 adds `IsFenced`. `ThrottleTimeMs` is JSON `0+`. `ClusterDescription::new` fills `0`. `error_counts` counts the top-level error code only, including `NONE`. Brokers are not counted.

Java `getErrorResponse` leaves `Brokers` empty, uses the JSON defaults for `ClusterId`, `ControllerId`, `EndpointType`, and `ClusterAuthorizedOperations`, sets `ErrorMessage` to JSON null, and does not copy `throttleTimeMs` from its argument. It also does not copy `IncludeClusterAuthorizedOperations`, `EndpointType`, or `IncludeFencedBrokers`.

Kafka 4.0 `validVersions` is `0-2`.

### CreateAcls, DescribeAcls, DeleteAcls

Spoken versions: 0–3. Version 4 and above are not spoken.

v0–v1 are classic. v2–v3 are flexible. Request header version 2, response header version 1. v3 is the same layout as v2 and adds the user resource type. v1 adds `ResourcePatternType` and `PatternTypeFilter`. Create uses `LITERAL`. Describe and delete filters use `ANY`.

| API | Throttle and errors |
|---|---|
| CreateAcls | `ThrottleTimeMs` is JSON `0+` (`encode_create_acls_response_with_throttle`). The convenience encoder writes `0`. v0 and v1 share one empty-results body. v2 and v3 share another. There is no top-level error code. |
| DescribeAcls | `ThrottleTimeMs` and the top-level error code are JSON `0+`. The convenience encoder writes `0` and a null `ErrorMessage`. v0 and v1 share one empty-resources body. v2 and v3 share another. The error code is at bytes 4–5. `error_counts` counts that code only, including `NONE`. `error` is an `ApiError` from the code and the message. This error code is not a CreateAcls result code, a DeleteAcls filter code, a DeleteAcls matching code, or the Metadata error code. |
| DeleteAcls | `ThrottleTimeMs` is JSON `0+` (`encode_delete_acls_filter_results_with_throttle`). The convenience encoder writes `0`. v0 and v1 share one empty-filter-results body. v2 and v3 share another. There is no top-level error code. Each matching ACL has its own nullable `ErrorMessage` (`DeletedAclsFilterResult.matching` is `DeleteAclsMatchingAcl`). `encode_delete_acls_response` writes that message null. It is not the DescribeAcls message, the CreateAcls result message, or the DeleteAcls filter message. |

`describe_acls_with` is Java `describeAcls(AclBindingFilter)`, with a null name, principal, and host, and `ANY` operation and permission. `describe_acls_any` is `AclBindingFilter.ANY`. `delete_acls_with` sends N filters (Java `deleteAcls(Collection)`). The three timeouts are RPC deadlines. None of these requests has a `TimeoutMs` field.

Kafka 4.0 `validVersions` is `1-3`. It removed v0. This crate still speaks v0.

### Metadata

Spoken versions: 0–13. Version 14 and above are not spoken.

v0–v8 are classic. v9–v13 are flexible. Request header version 2, response header version 1. Producer, consumer, and admin negotiate 0–13.

v0 has no controller id, topic id, or rack. An empty topic array means every topic. A named v0 request requires automatic topic creation.

| Version | What changed |
|---|---|
| 8–10 | `ClusterAuthorizedOperations` and `IncludeClusterAuthorizedOperations`. Outside that range the encoder omits them and the decoder fills `AUTHORIZED_OPERATIONS_OMITTED` and `false`. The convenience encoder writes `false` for the include flag. These are not topic authorized operations and not the DescribeCluster fields of the same names. |
| 10 | `TopicId`. A name-based describe sends a zero UUID. `describe_topics_by_id` sends a null name (Java `describeTopics(TopicCollection.ofTopicIds)`). |
| 12 | Topic id is supported on the describe path. |
| 13 | Top-level error code, an INT16 after `Topics` and, on v8–v10, after `ClusterAuthorizedOperations`, before the tagged fields. |

Name-based `describe_topics`, `describe_topics_with`, and `describe_topics_with_partition_limit` go through DescribeTopicPartitions, api 75 (KIP-966, Java `describeTopics(TopicCollection.ofTopicNames)`). `ResponsePartitionLimit` defaults to 2000 (`DescribeTopicsOptions.partitionSizeLimitPerResponse`), and `NextCursor` pages the rest. `describe_topics_with(true)` stores `TopicAuthorizedOperations`. DescribeTopicPartitions has no `IncludeTopicAuthorizedOperations` request flag. `describe_topic_partitions_timeout` is the RPC deadline. Java `describeTopics` is `describe_topics_timeout`.

Kafka 4.0 `validVersions` is `0-13`.

### WriteTxnMarkers

Spoken versions: 0–2. Version 3 and above are not spoken.

v0 is classic. v1 and v2 are flexible. Request header version 2, response header version 1. v2 adds a signed `TransactionVersion` after `CoordinatorEpoch`. `Admin::abort_transaction` negotiates 0–2.

### ElectLeaders

Spoken versions: 0–2. Version 3 and above are not spoken.

v0 and v1 are classic. v2 is flexible. Request header version 2, response header version 1. v1 adds `ElectionType` and a top-level response error code. v0 omits `ElectionType` even when the request sets one. Admin negotiates 0–2.

### DescribeQuorum

Spoken versions: 0–2. Version 3 and above are not spoken.

Every version is flexible. Request header version 2, response header version 1. The request fields stay the same. v1 adds replica fetch and catch-up timestamps. v2 adds error messages, `Nodes`, and `ReplicaDirectoryId`. Admin negotiates 0–2.

### AddRaftVoter

Spoken versions: 0–1. Version 2 and above are not spoken.

Both versions use a flexible header. The default acknowledgement waits for a committed voter set and can use v0. An explicit local append requires v1 and is rejected before I/O on a broker that only speaks v0.

### RemoveRaftVoter

Spoken versions: 0. Version 1 and above are not spoken.

The request uses a flexible header. Removal stays on v0.

### ApiVersions

Spoken versions: 0–4. Version 5 and above are not spoken. Connect sends v4.

v0–v2 are classic. The request is empty. `ThrottleTimeMs` is present from v1. v3–v4 are flexible in the body only: compact strings and tagged fields, request header version 2. The response header stays version 0 (KIP-482).

v3 adds `ClientSoftwareName` and `ClientSoftwareVersion`. v4 is the same request and allows `SupportedFeatures.MinVersion` 0 (KAFKA-17011 / KAFKA-17492). v0–v3 omit those features.

Tagged fields, in order:

| Tag | Field |
|---|---|
| 0 | `supportedFeatures`: name, min, max. |
| 1 | `finalizedFeaturesEpoch`, INT64. `-1` is omitted. |
| 2 | `finalizedFeatures`: name, then **max**, then min. |
| 3 | `zkMigrationReady`. |

Empty and default tags are omitted. `error_counts` counts the top-level error code only, including `NONE`. Api keys and features are not counted. `describe_features_timeout` is the RPC deadline (Java `DescribeFeaturesOptions.timeoutMs`). The request has no `TimeoutMs` field.

Kafka 4.0 `validVersions` is `0-4`.

When the broker returns `UNSUPPORTED_VERSION` (KIP-511, brokers 2.4 and newer), the error body is v0 and lists the supported ApiVersions range. The client retries at `pick_version(broker_min, broker_max, 0, 4)`.
