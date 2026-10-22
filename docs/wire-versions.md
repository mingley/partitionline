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
