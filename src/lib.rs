//! A Kafka client written in Rust. No C, no librdkafka.
//!
//! # Produce
//!
//! ```no_run
//! # async fn example() -> partitionline::Result<()> {
//! use partitionline::{ProduceRecord, Producer};
//!
//! let producer = Producer::connect("127.0.0.1:9092").await?;
//! let md = producer
//!     .send(ProduceRecord::to("events").value(&b"hello"[..]))
//!     .await?;
//! println!("{md}");
//! producer.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! For many records, [`Producer::send_all`] waits for every offset after
//! queuing, and [`Producer::try_send`] plus [`Producer::flush`] is the
//! throughput path (see `examples/bench_produce.rs`).
//! The producer negotiates Produce v3–v12 (v3–v8 classic; v9+ flexible;
//! v10+ KIP-951 CurrentLeader / NodeEndpoints; v11 TRANSACTION_ABORTABLE; v12 KIP-890
//! Part 2 transaction V2, skipping AddPartitionsToTxn). v13+ (topic IDs)
//! is not spoken. [`protocol::api::ProduceRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`]
//! / [`protocol::api::ProduceRequest::is_transaction_v2_requested`] are Java
//! `ProduceRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2` /
//! `isTransactionV2Requested`.
//! [`protocol::api::ProduceRequest::has_transactional_records`] is Java
//! `RequestUtils.hasTransactionalRecords` (first batch of each partition
//! only).
//! [`protocol::api::ProduceRequest::partition_sizes`] is Java
//! `ProduceRequest.partitionSizes` (`(topic, partition)` to encoded
//! records size; a later pair adds).
//! [`protocol::api::ProduceRequest::error_response`] is Java
//! `ProduceRequest.getErrorResponse` (`acks` `0` is `None`; unique
//! `partitionSizes` keys otherwise).
//! [`protocol::api::ProduceRequest::error_counts`] is Java
//! `ProduceRequest.errorCounts(Throwable)` (unique `partitionSizes` keys;
//! empty is `{error: 0}`, not an empty map; does not look at `acks`).
//! InitProducerId is v0–v5 (v2+ flexible; v3+ KIP-360 ProducerId;
//! first init [`RecordBatch::NO_PRODUCER_ID`] /
//! [`RecordBatch::NO_PRODUCER_EPOCH`], epoch-bump resume sends the last
//! id/epoch). Java `InitProducerIdRequest.getErrorResponse` writes those
//! sentinels ([`protocol::idem::InitProducerIdRequest::error_response`];
//! throttle `0` even when the Java `throttleTimeMs` argument is non-zero).
//! Java `InitProducerIdRequest.Builder.build` rejects a
//! non-positive `transaction.timeout.ms` and an empty (non-null)
//! transactional id.
//! [`protocol::idem::InitProducerIdResponse::should_client_throttle`] is Java
//! `InitProducerIdResponse.shouldClientThrottle` (v1+).
//! Metadata negotiates v1–v13 (v9+ flexible; v13 top-level ErrorCode;
//! v8+ IncludeTopicAuthorizedOperations on [`Admin::describe_topics_by_id_with`];
//! v12+ TopicId on [`Admin::describe_topics_by_id`]).
//! [`protocol::api::MetadataResponse::NO_CONTROLLER_ID`] /
//! [`protocol::api::MetadataResponse::NO_LEADER_ID`] /
//! [`protocol::api::MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED`] /
//! [`protocol::api::MetadataResponse::has_reliable_leader_epochs`] /
//! [`protocol::api::MetadataResponse::should_client_throttle`] are Java
//! `MetadataResponse.NO_CONTROLLER_ID` / `NO_LEADER_ID` /
//! `AUTHORIZED_OPERATIONS_OMITTED` / `hasReliableLeaderEpochs` /
//! `shouldClientThrottle`
//! (Metadata versions before 9 do not retain leader epochs for Fetch,
//! ListOffsets, or OffsetsForLeaderEpoch; the client cache fills missing
//! partition leaders with `NO_LEADER_ID` and missing epochs with
//! [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]).
//! [`protocol::api::PartitionMetadata::without_leader_epoch`] is Java
//! `MetadataResponse.PartitionMetadata.withoutLeaderEpoch`.
//! [`protocol::api::TopicMetadata`] `Display` is Java
//! `MetadataResponse.TopicMetadata.toString` (nested
//! `PartitionMetadata.toString` uses the topic name).
//! [`protocol::api::MetadataResponse::errors`] /
//! [`protocol::api::MetadataResponse::errors_by_topic_id`] /
//! [`protocol::api::MetadataResponse::topics_by_error`] /
//! [`protocol::api::MetadataResponse::error_counts`] /
//! [`protocol::api::MetadataResponse::topic_authorized_operations`] /
//! [`protocol::api::MetadataResponse::brokers_by_id`] are Java
//! `MetadataResponse.errors` / `errorsByTopicId` / `topicsByError` /
//! `errorCounts` / `topicAuthorizedOperations` / `brokersById`
//! (map values are Kafka error codes; `errors` throws when any topic name is
//! `None`; `errors_by_topic_id` throws when any topic id is zeros;
//! `errorCounts` counts topic and partition codes, not the top-level error).
//! [`PartitionInfo::from_partition_metadata`] is Java
//! `MetadataResponse.toPartitionInfo` (broker ids, not `Node`).
//! [`protocol::api::MetadataRequest::is_all_topics`] /
//! [`protocol::api::MetadataRequest::topic_ids`] /
//! [`protocol::api::MetadataRequest::topics`] are Java
//! `MetadataRequest.isAllTopics` / `topicIds` / `topics` (null Topics is all
//! topics; empty Topics is all topics only on v0; topic IDs are empty when all
//! topics or below v10; `topics` is null when all topics, else each Name);
//! [`protocol::api::MetadataRequestTopic::convert_from_names`] /
//! [`protocol::api::MetadataRequestTopic::convert_from_ids`] are Java
//! `MetadataRequest.convertToMetadataRequestTopic` /
//! `convertTopicIdsToMetadataRequestTopic`.
//! [`protocol::api::TopicMetadata::error`] /
//! [`protocol::api::MetadataRequestTopic::error_result`] /
//! [`protocol::api::MetadataRequest::error_response`] are Java
//! `MetadataRequest.getErrorResponse` (one topic / request: null Topics is
//! empty Topics, not all-topics; duplicate names are kept; Brokers stay
//! empty; top-level ErrorCode is the same code; Java
//! `hasReliableLeaderEpochs` is `true` even below Metadata v9).
//! Name-based [`Admin::describe_topics`] uses DescribeTopicPartitions (api 75).
//! Groups and transactions negotiate FindCoordinator v1–v6 (v3+ flexible;
//! v4+ KIP-699 CoordinatorKeys; v5 TRANSACTION_ABORTABLE; v6 share groups).
//! [`CoordinatorType`] is Java `FindCoordinatorRequest.CoordinatorType`
//! (`id` / `forId`; unknown is `None`). [`protocol::group::MIN_BATCHED_VERSION`]
//! is Java `FindCoordinatorRequest.MIN_BATCHED_VERSION`.
//! [`protocol::group::FindCoordinatorResponse::should_client_throttle`] is Java
//! `FindCoordinatorResponse.shouldClientThrottle` (v2+);
//! [`protocol::group::FindCoordinatorResponse::error_counts`] is Java
//! `FindCoordinatorResponse.errorCounts` (each `Coordinators[]` code,
//! including `NONE`; empty Coordinators falls back to top-level `NONE`);
//! [`protocol::group::FindCoordinatorResponse::coordinator_by_key`] is Java
//! `FindCoordinatorResponse.coordinatorByKey` (v4+ first matching `Key`;
//! v0–v3 stuffs `key` into the folded top-level coordinator; empty
//! Coordinators synthesizes JSON defaults with that `Key`);
//! [`protocol::group::CoordinatorResult::error`] /
//! [`protocol::group::CoordinatorResult::error_for_key`] /
//! [`protocol::group::FindCoordinatorResponse::prepare_error_response`] /
//! [`protocol::group::FindCoordinatorResponse::error_results`] are Java
//! `FindCoordinatorResponse.prepareOldResponse` /
//! `prepareCoordinatorResponse` / `prepareErrorResponse` /
//! `FindCoordinatorRequest.getErrorResponse` (`Node.noNode`; empty Key
//! below v4). `ErrorMessage` stays the JSON default (null); official Java
//! also sets the English `Errors.message` string. Throttle is the JSON
//! default (`0`).
//! OffsetCommit v2–v9 (v2–v4 RetentionTimeMs round-trips, including a
//! non-default value; v5+ omits the field even when the body is non-default
//! and decode fills [`protocol::group::DEFAULT_RETENTION_TIME`]; v6+ epoch;
//! decode below v6 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]; v7 GroupInstanceId;
//! v7+ round-trips GroupInstanceId; below v7 encode omits it even when
//! the body has an instance id and decode fills `None`;
//! v3+ round-trips ThrottleTimeMs; below v3 encode omits it even when
//! the body has a non-zero value and decode fills `0`;
//! [`protocol::group::encode_offset_commit_topics_response`] still writes `0`;
//! v8+ flexible; v9 KIP-848 errors;
//! [`protocol::group::OffsetCommitResponse::should_client_throttle`] is Java
//! `OffsetCommitResponse.shouldClientThrottle` (v4+);
//! [`protocol::group::OffsetCommitResponse::error_counts`] is Java
//! `OffsetCommitResponse.errorCounts` (partition-level codes, including `NONE`);
//! [`protocol::group::OffsetCommitResponse::from_errors`] is Java
//! `OffsetCommitResponse` constructor from an errors map (group by
//! topic name; a later entry for the same topic appends; first-seen
//! topic order);
//! [`protocol::group::OffsetCommitResponse::merge`] is Java
//! `OffsetCommitResponse.Builder.merge` (replace when current Topics are
//! empty; otherwise append topics / partitions; overlapping partitions are
//! not checked);
//! [`protocol::group::OffsetTopic::error_result`] /
//! [`protocol::group::OffsetTopic::error_results`] /
//! [`protocol::group::OffsetCommitResponsePartition::error`] are Java
//! `OffsetCommitRequest.getErrorResponse` (one topic / Topics / partition
//! body). Nested body is PartitionIndex + ErrorCode;
//! [`protocol::group::OffsetCommitRequest::error_response`] is Java
//! `OffsetCommitRequest.getErrorResponse` (v3+ writes the `throttleTimeMs`
//! argument; below v3 omits it);
//! [`protocol::group::OffsetCommitRequest::offsets`] is Java
//! `OffsetCommitRequest.offsets` (`(topic, partition)` to committed offset;
//! a later partition overwrites);
//! [`protocol::group::OffsetCommitRequest::build`] is Java
//! `OffsetCommitRequest.Builder.build` (a present `group.instance.id`
//! below v7 is `UnsupportedVersionException`; encode still omits)),
//! OffsetFetch v1–v9 (v2 top-level error; v3 throttle; v5 epoch; v6+ flexible; v7 RequireStable; v8 Groups; v9 MemberId;
//! [`protocol::group::OffsetFetchGroup::is_all_partitions`] is Java
//! `OffsetFetchRequest.isAllPartitions` (`None` Topics is every committed
//! partition; `Some` empty is not);
//! [`protocol::group::OffsetFetchRequest::is_all_partitions_for_group`] is Java
//! `OffsetFetchRequest.isAllPartitionsForGroup` (first matching GroupId;
//! missing group is [`Error::protocol`]; `None` Topics is every committed
//! partition);
//! [`protocol::group::OffsetFetchRequest::group_ids_to_partitions`] is Java
//! `OffsetFetchRequest.groupIdsToPartitions` (group id to
//! `(topic, partition)` list; `None` Topics is `None`; a later group
//! overwrites);
//! [`protocol::group::OffsetFetchRequest::group_ids_to_topics`] is Java
//! `OffsetFetchRequest.groupIdsToTopics` (group id to Topics as-is;
//! `None` Topics is `None`; a later group overwrites);
//! [`protocol::group::OffsetFetchRequest::group_ids`] is Java
//! `OffsetFetchRequest.groupIds` (request order; duplicate ids kept);
//! [`protocol::group::OffsetFetchRequest::groups`] is Java
//! `OffsetFetchRequest.groups` (v8+ as-is; below v8 a singleton from the
//! first group's GroupId / Topics; empty input below v8 is still a
//! singleton; extra groups below v8 are dropped);
//! [`protocol::group::OffsetFetchRequest::partitions`] is Java
//! `OffsetFetchRequest.partitions` (`None` Topics is `None`; otherwise
//! each `(topic, partition)` in request order);
//! [`protocol::group::OffsetFetchRequest::from_partitions`] is Java
//! `OffsetFetchRequest.Builder` Topics from a partition list (`None` is
//! all partitions; a later entry for the same topic appends; first-seen
//! topic order; duplicate pairs kept);
//! [`protocol::group::OffsetFetchRequest::build`] is Java
//! `OffsetFetchRequest.Builder.build` (`requireStable` below v7 with
//! `throwOnFetchStableOffsetsUnsupported` is `UnsupportedVersionException`;
//! otherwise Java falls back to false; encode still omits);
//! [`protocol::group::OffsetFetchResponse::error_counts`] is Java
//! `OffsetFetchResponse.errorCounts` (v8+ group-level plus partitions;
//! v2–v7 top-level plus partitions; v1 first non-partition error plus
//! partitions; including `NONE`);
//! [`protocol::group::OffsetFetchResponse::group_has_error`] /
//! [`protocol::group::OffsetFetchResponse::group_level_error`] /
//! [`protocol::group::OffsetFetchResponse::error`] are Java
//! `OffsetFetchResponse.groupHasError` / `groupLevelError` / `error`
//! (v8+ named group's `errorCode`; missing group is false / `None`;
//! v1–v7 ignore `group_id` and use the top-level code, including `NONE`;
//! `error` is always `None` on v8+ even when groups have errors);
//! [`protocol::group::OffsetFetchResponse::partition_data_map`] is Java
//! `OffsetFetchResponse.partitionDataMap` (v1–v7 ignore `group_id`; v8+
//! first matching group; missing group is [`Error::protocol`]; a later
//! partition overwrites);
//! [`protocol::group::OffsetFetchResponse::from_partition_data`] is Java
//! `OffsetFetchResponse` constructor from a partition map (group by name;
//! a later entry for the same topic appends; first-seen topic order;
//! duplicate pairs kept);
//! [`protocol::group::OffsetFetchResponse::from_groups_partition_data`] is Java
//! `OffsetFetchResponse` constructor from group errors and partition maps
//! (v8+; a group missing from `errors` is [`Error::protocol`]; a group
//! only in `errors` is omitted);
//! [`protocol::group::OffsetFetchResponse::from_groups`] is Java
//! `OffsetFetchResponse` constructor from a group list (v8+ as-is; below
//! v8 exactly one group; v1 rewrites partitions when the group has an
//! error);
//! [`protocol::group::OffsetFetchGroup::error_result`] /
//! [`protocol::group::OffsetFetchGroup::error_results`] /
//! [`protocol::group::OffsetFetchGroupResult::error`] are Java
//! `OffsetFetchRequest.getErrorResponse` one group / Groups on v8+
//! (empty Topics; request partitions are not copied);
//! [`protocol::group::OffsetFetchRequest::error_response`] is Java
//! `OffsetFetchRequest.getErrorResponse` (v1 fills unique partitions;
//! null Topics is [`Error::protocol`]; v2–v7 omit partitions; below v8
//! is the `groups` singleton; v8+ unique GroupId; `error_results` keeps
//! duplicate ids);
//! v3+ round-trips ThrottleTimeMs; below v3 encode omits it even when
//! the body has a non-zero value and decode fills `0`;
//! [`protocol::group::encode_offset_fetch_groups_response`] still writes `0`),
//! Heartbeat v0–v4 (v1+ throttle; v3 GroupInstanceId; v4 flexible;
//! [`protocol::group::HeartbeatRequest::build`] is Java
//! `HeartbeatRequest.Builder.build` (a present `group.instance.id`
//! below v3 is `UnsupportedVersionException`; encode still omits);
//! v3+ round-trips GroupInstanceId; below v3 encode omits it even when
//! the body has an instance id and decode fills `None`;
//! v1+ round-trips ThrottleTimeMs; below v1 encode omits it even when
//! the body has a non-zero value and decode fills `0`;
//! [`protocol::group::encode_heartbeat_response`] still writes `0`;
//! [`protocol::group::HeartbeatRequest::error_response`] is Java
//! `HeartbeatRequest.getErrorResponse` (v1+ writes the `throttleTimeMs`
//! argument; below v1 omits it);
//! [`protocol::group::HeartbeatResponse::should_client_throttle`] is Java
//! `HeartbeatResponse.shouldClientThrottle` (v2+)),
//! SyncGroup v0–v5 (v1+ throttle; v3 GroupInstanceId; v4+ flexible; v5 ProtocolType / ProtocolName;
//! v3+ round-trips GroupInstanceId; below v3 encode omits it even when
//! the body has an instance id and decode fills `None`;
//! v5+ round-trips ProtocolType / ProtocolName; below v5 encode omits
//! them even when the body has values and decode fills `None`;
//! v1+ round-trips ThrottleTimeMs; below v1 encode omits it even when
//! the body has a non-zero value and decode fills `0`;
//! [`protocol::group::encode_sync_group_response`] still writes ThrottleTimeMs `0` and null ProtocolType / ProtocolName;
//! [`protocol::group::SyncGroupRequest::are_mandatory_protocol_type_and_name_present`] is Java
//! `SyncGroupRequest.areMandatoryProtocolTypeAndNamePresent` (v5+ both ProtocolType and
//! ProtocolName present; empty string is present; below v5 always true);
//! [`protocol::group::SyncGroupRequest::error_response`] is Java
//! `SyncGroupRequest.getErrorResponse` (empty assignment; ProtocolType /
//! ProtocolName JSON default (null) on v5+; v1+ writes the `throttleTimeMs`
//! argument; below v1 omits it);
//! [`protocol::group::SyncGroupRequest::group_assignments`] is Java
//! `SyncGroupRequest.groupAssignments` (a later member overwrites);
//! [`protocol::group::SyncGroupRequest::build`] is Java
//! `SyncGroupRequest.Builder.build` (a present `group.instance.id`
//! below v3 is `UnsupportedVersionException`; encode still omits);
//! v5+ response round-trips ProtocolType / ProtocolName; below v5 encode
//! omits them even when the body has values and decode fills `None`;
//! [`protocol::group::SyncGroupResponse::should_client_throttle`] is Java
//! `SyncGroupResponse.shouldClientThrottle` (v2+)),
//! JoinGroup v2–v9 (v5 GroupInstanceId; v6+ flexible; v8 Reason; v9 SkipAssignment;
//! Protocols of N via [`ConsumerGroup::join_with_assignors`];
//! [`protocol::group::ConsumerProtocol::PROTOCOL_TYPE`] is Java
//! `ConsumerProtocol.PROTOCOL_TYPE`;
//! [`protocol::group::ConsumerProtocol::serialize_subscription`] is Java
//! `ConsumerProtocol.serializeSubscription` (v3 `GenerationId` / `RackId`;
//! [`protocol::group::ConsumerProtocolSubscription::DEFAULT_GENERATION`];
//! [`protocol::group::ConsumerProtocolSubscription`] `Display` is Java
//! `ConsumerPartitionAssignor.Subscription.toString`);
//! [`protocol::group::ConsumerProtocol::serialize_assignment`] is Java
//! `ConsumerProtocol.serializeAssignment` (does not sort partitions;
//! [`protocol::group::ConsumerProtocolAssignment`] `Display` is Java
//! `ConsumerPartitionAssignor.Assignment.toString`);
//! [`group::resolve_sticky_owned_partitions`] is Java
//! `AbstractStickyAssignor.allSubscriptionsEqual` owned-partition
//! generation resolution (higher generation keeps the partition; the same
//! generation revokes it from both);
//! [`protocol::group::JoinGroupResponse::is_leader`] /
//! [`protocol::group::JoinGroupResponse::should_client_throttle`] /
//! [`protocol::group::JoinGroupResponse::protocol_name`] are Java
//! `JoinGroupResponse.isLeader` / `shouldClientThrottle` /
//! `JoinGroupResponse(JoinGroupResponseData, short)` ProtocolName
//! (below v7 null becomes empty; v7+ empty becomes null);
//! v7+ round-trips ProtocolType; below v7 encode omits it even when
//! the body has a value and decode fills `None`;
//! [`protocol::group::encode_join_group_response`] still writes null;
//! [`protocol::group::JoinGroupRequest::error_response`] is Java
//! `JoinGroupRequest.getErrorResponse` ([`protocol::group::JoinGroupRequest::UNKNOWN_GENERATION_ID`] /
//! [`protocol::group::JoinGroupRequest::UNKNOWN_PROTOCOL_NAME`] / [`protocol::group::JoinGroupRequest::UNKNOWN_MEMBER_ID`];
//! empty members; ProtocolName null on v7+; ProtocolType stays null);
//! [`protocol::group::JoinGroupRequest::build`] is Java
//! `JoinGroupRequest.Builder.build` (a present `group.instance.id`
//! below v5 is `UnsupportedVersionException`; encode still omits)),
//! LeaveGroup v0–v5 (v3 Members / GroupInstanceId; v4 flexible; v5 Reason;
//! v1+ round-trips ThrottleTimeMs; below v1 encode omits it even when
//! the body has a non-zero value and decode fills `0`;
//! [`protocol::group::encode_leave_group_response_version`] still writes `0`;
//! [`protocol::group::LeaveGroupRequest::error_response`] is Java
//! `LeaveGroupRequest.getErrorResponse` (empty Members; request members are
//! not copied; v1+ writes the `throttleTimeMs` argument; below v1 omits it);
//! [`protocol::group::LeaveGroupRequest::members`] is Java
//! `LeaveGroupRequest.members` (v0–v2 singleton `member_id`; v3+ Members);
//! [`protocol::group::LeaveGroupResponse::should_client_throttle`] is Java
//! `LeaveGroupResponse.shouldClientThrottle` (v2+);
//! [`protocol::group::LeaveGroupResponse::error_counts`] is Java
//! `LeaveGroupResponse.errorCounts` (top-level `errorCode` plus each
//! member-level code, including `NONE`);
//! [`protocol::group::LeaveGroupResponse::error`] is Java
//! `LeaveGroupResponse.error` (top-level when not `NONE`, else first
//! member-level non-`NONE`);
//! [`protocol::group::LeaveGroupResponse::for_version`] is Java
//! `LeaveGroupResponse(LeaveGroupResponseData, short)` (v3+ identity;
//! below v3 a non-`NONE` top-level drops members; `NONE` requires one
//! member and copies that `errorCode`);
//! [`protocol::group::LeaveGroupResponse::from_members`] is Java
//! `LeaveGroupResponse(List, Errors, int, short)` (v3+ identity; v0–v2
//! fold `error()` and drop members; zero or many members are allowed);
//! [`LEAVE_GROUP_REASON_CLOSED`] on leave / close, [`LEAVE_GROUP_REASON_UNSUBSCRIBED`]
//! on unsubscribe, [`LEAVE_GROUP_REASON_POLL_TIMEOUT`] on `max.poll.interval.ms`),
//! SaslHandshake v0–v1 (never flexible; v1 enables SaslAuthenticate;
//! [`protocol::sasl::SaslHandshakeRequest::error_response`] is Java
//! `SaslHandshakeRequest.getErrorResponse` (empty Mechanisms)),
//! SaslAuthenticate v0–v2 (v1 SessionLifetimeMs; v2 flexible;
//! [`protocol::sasl::SaslAuthenticateRequest::error_response`] is Java
//! `SaslAuthenticateRequest.getErrorResponse` (empty AuthBytes; the
//! request bytes are not copied; SessionLifetimeMs is `0`; the Java
//! `throttleTimeMs` argument is unused; `ErrorMessage` stays the JSON
//! default, null); v1+ round-trips SessionLifetimeMs; below v1 decode
//! fills `0` and encode omits the field even when the body has a
//! non-zero value),
//! [`protocol::scram::sasl_name`] / [`protocol::scram::username`] /
//! [`protocol::scram::xor`] / [`protocol::scram::auth_message`] /
//! [`protocol::scram::to_bytes`] / [`protocol::scram::normalize`] are Java
//! `ScramFormatter.saslName` / `username` / `xor` / `authMessage` /
//! `toBytes` / `normalize` (`=` then `,`; leftover `=` is [`Error::protocol`];
//! length mismatch is Java `Argument arrays must be of the same length`;
//! `authMessage` is `a,b,c`),
//! [`protocol::scram::ScramAlg::hmac`] / [`protocol::scram::ScramAlg::hash`] /
//! [`protocol::scram::ScramAlg::hi`] /
//! [`protocol::scram::ScramAlg::salted_password`] /
//! [`protocol::scram::ScramAlg::client_key`] /
//! [`protocol::scram::ScramAlg::stored_key`] /
//! [`protocol::scram::ScramAlg::stored_key_from_proof`] /
//! [`protocol::scram::ScramAlg::server_key`] /
//! [`protocol::scram::ScramAlg::client_signature`] /
//! [`protocol::scram::ScramAlg::client_proof`] /
//! [`protocol::scram::ScramAlg::server_signature`] are Java
//! `ScramFormatter.hmac` / `hash` / `hi` / `saltedPassword` / `clientKey` /
//! `storedKey` / `serverKey` / `clientSignature` / `clientProof` /
//! `serverSignature`,
//! [`protocol::scram::ScramAlg::hash_algorithm`] /
//! [`protocol::scram::ScramAlg::mac_algorithm`] /
//! [`protocol::scram::ScramAlg::min_iterations`] /
//! [`protocol::scram::ScramAlg::max_iterations`] /
//! [`protocol::scram::ScramAlg::from_mechanism_name`] /
//! [`protocol::scram::ScramAlg::mechanism_names`] /
//! [`protocol::scram::ScramAlg::is_scram`] are Java internals
//! `ScramMechanism.hashAlgorithm` / `macAlgorithm` / `minIterations` /
//! `maxIterations` / `forMechanismName` / `mechanismNames` / `isScram`
//! (unknown name is `None`; admin `ScramMechanism.fromMechanismName` returns
//! `UNKNOWN` instead),
//! ApiVersions v0–v4 (v3+ ClientSoftwareName; v4 SupportedFeatures.MinVersion 0; KIP-511 retry;
//! [`protocol::api::ApiVersionsRequest::is_valid`] is Java `ApiVersionsRequest.isValid`;
//! [`protocol::api::ApiVersionsRequest::error_response`] is Java
//! `ApiVersionsRequest.getErrorResponse` (`UNSUPPORTED_VERSION` fills
//! ApiKeys with `toApiVersion(API_VERSIONS)` min 0 max 4; any other
//! error leaves ApiKeys empty; encode still writes the caller's
//! `api_keys` as-is);
//! [`protocol::api::ApiVersionsResponse::api_version`] /
//! [`protocol::api::ApiVersionsResponse::UNKNOWN_FINALIZED_FEATURES_EPOCH`] /
//! [`protocol::api::ApiVersionsResponse::should_client_throttle`] /
//! [`protocol::api::ApiVersionsResponse::intersect`] /
//! [`protocol::api::ApiVersionsResponse::create_finalized_feature_keys`] /
//! [`protocol::api::ApiVersionsResponse::maybe_filter_supported_feature_keys`] are Java
//! `ApiVersionsResponse.apiVersion` / `UNKNOWN_FINALIZED_FEATURES_EPOCH` /
//! `shouldClientThrottle` / `intersect` (`null` is `None`; mismatched api
//! keys are `IllegalArgumentException`) / `createFinalizedFeatureKeys`
//! (level `0` is omitted; last-wins name; first-seen order; encode still
//! writes FinalizedFeatures as-is) / `maybeFilterSupportedFeatureKeys`
//! (`alterFeatureLevel0` omits `minVersion` `0`; encode already filters
//! the same way on v3);
//! [`protocol::api::SupportedFeatureKey::name`] /
//! [`protocol::api::SupportedFeatureKey::min_version`] /
//! [`protocol::api::SupportedFeatureKey::max_version`] /
//! [`protocol::api::FinalizedFeatureKey::name`] /
//! [`protocol::api::FinalizedFeatureKey::max_version_level`] /
//! [`protocol::api::FinalizedFeatureKey::min_version_level`] are Java
//! `ApiVersionsResponseData.SupportedFeatureKey` / `FinalizedFeatureKey`
//! getters),
//! ConsumerGroupHeartbeat v0–v1 (v1 SubscribedTopicRegex / KIP-1082 member id;
//! [`protocol::cgheartbeat::ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH`] /
//! [`protocol::cgheartbeat::ConsumerGroupHeartbeatRequest::LEAVE_GROUP_STATIC_MEMBER_EPOCH`] /
//! [`protocol::cgheartbeat::ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH`] /
//! [`protocol::cgheartbeat::ConsumerGroupHeartbeatRequest::leave_group_epoch`] /
//! [`protocol::cgheartbeat::ConsumerGroupHeartbeatRequest::CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION`] /
//! [`protocol::cgheartbeat::ConsumerGroupHeartbeatRequest::REGEX_RESOLUTION_NOT_SUPPORTED_MSG`]
//! are Java `ConsumerGroupHeartbeatRequest` join/leave epochs, KIP-1082,
//! and regex-on-v0 (`leave_group_epoch` is Java `ConsumerMembershipManager.leaveGroupEpoch`;
//! static members send `-2`)),
//! ShareGroupHeartbeat v0–v1 (v0 Kafka 4.0 early access; v1 Kafka 4.1 stable; same fields;
//! [`protocol::share::ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH`] /
//! [`protocol::share::ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH`]
//! are Java `ShareGroupHeartbeatRequest` join/leave epochs),
//! ShareGroupDescribe v0–v1 (v0 Kafka 4.0 early access; v1 Kafka 4.1 stable; same fields; FindCoordinator v4+ CoordinatorKeys of N;
//! [`protocol::admin::ShareGroupDescribeRequest::error_described_group_list`] is Java
//! `ShareGroupDescribeRequest.getErrorDescribedGroupList` (each id through
//! [`DescribedShareGroup::new`]);
//! [`protocol::admin::ShareGroupDescribeResponse::error_counts`] is Java
//! `ShareGroupDescribeResponse.errorCounts` (per-group codes, including `NONE`)),
//! ShareFetch v0–v1 (v0 PartitionMaxBytes; v1 MaxRecords / BatchSize / AcquisitionLockTimeoutMs;
//! [`ShareRequestMetadata`] is Java `ShareRequestMetadata`
//! ([`ShareRequestMetadata::INITIAL_EPOCH`] / [`ShareRequestMetadata::FINAL_EPOCH`]
//! / [`ShareRequestMetadata::next_epoch`]; `nextEpoch` wraps `i32::MAX` to `1`.
//! [`ShareGroup`] uses those epochs on ShareFetch / ShareAcknowledge);
//! [`protocol::share::ShareFetchedPartition::partition_response`] is Java
//! `ShareFetchResponse.partitionResponse` (`PartitionIndex` and `ErrorCode`).
//! Records and acquired ranges stay empty. Official Java leaves ErrorMessage,
//! AcknowledgeErrorCode, AcknowledgeErrorMessage, CurrentLeader, and Records
//! at JSON defaults (null / 0 / 0/0 / null). Crate encode writes ErrorMessage
//! null, AcknowledgeErrorCode 0, AcknowledgeErrorMessage null, CurrentLeader
//! id 1 epoch 0, empty Records, empty AcquiredRecords, empty NodeEndpoints.
//! v1 AcquisitionLockTimeoutMs is 15000. Top-level ErrorCode stays 0
//! (crate encode). Throttle is the JSON default (`0`);
//! [`protocol::share::ShareFetchResponse::error_counts`] is Java
//! `ShareFetchResponse.errorCounts` (top-level `errorCode` plus each
//! partition-level code, including `NONE`). Crate decode currently fails
//! on a non-zero top-level code and does not return it;
//! [`protocol::share::ShareFetchResponse::response_data`] is Java
//! `ShareFetchResponse.responseData` (looks up `topic_id`; skips a missing
//! name; a later partition overwrites);
//! [`protocol::share::ShareFetchRequest::forgotten_topics`] is Java
//! `ShareFetchRequest.forgottenTopics` (looks up `topic_id` and keeps a
//! missing name as `None`; duplicates are kept; encode still writes empty
//! ForgottenTopicsData);
//! [`protocol::share::ShareFetchRequest::update_forgotten_data`] is Java
//! `ShareFetchRequest.Builder.updateForgottenData` (group by topic id;
//! first-seen id order; later partitions append; grouped entries are
//! appended to the existing list, including a second entry for the same
//! id; encode still writes empty ForgottenTopicsData);
//! [`protocol::share::ShareFetchRequest::share_fetch_data`] is Java
//! `ShareFetchRequest.shareFetchData` (looks up `topic_id` and keeps a
//! missing name as `None`; values are `PartitionMaxBytes`; a later
//! partition overwrites);
//! [`protocol::share::ShareFetchRequest::for_consumer`] is Java
//! `ShareFetchRequest.Builder.forConsumer` Topics (group by topic id;
//! first-seen id and partition order; send last-wins the partition body;
//! acks replace batches on an existing partition; closing skips send and
//! zeros ack-only `PartitionMaxBytes`);
//! [`protocol::share::ShareFetchResponse::to_message`] is Java
//! `ShareFetchResponse.toMessage` Responses (group by `topic_id` in
//! first-seen order; key partition overwrites the body);
//! [`protocol::share::ShareFetchedPartition::records_size`] is Java
//! `ShareFetchResponse.recordsSize` (`0` when records are empty)),
//! ShareAcknowledge v0–v1 (v0 Kafka 4.0 early access; v1 Kafka 4.1 stable; same fields;
//! [`protocol::share::ShareAcknowledgeResponsePartition::partition_response`] is Java
//! `ShareAcknowledgeResponse.partitionResponse` (`PartitionIndex` and `ErrorCode`).
//! Official Java leaves ErrorMessage and CurrentLeader at JSON defaults
//! (null / 0/0). Crate encode writes ErrorMessage null, CurrentLeader id 0
//! epoch 0, empty NodeEndpoints. Top-level ErrorCode stays 0 (crate encode
//! of this factory). Throttle is the JSON default (`0`). Official Java
//! `ShareAcknowledgeRequest.getErrorResponse` writes only the top-level
//! ErrorCode (empty Responses);
//! [`protocol::share::ShareAcknowledgeResponse::error_counts`] is Java
//! `ShareAcknowledgeResponse.errorCounts` (top-level `errorCode` plus each
//! partition-level code, including `NONE`);
//! [`protocol::share::ShareAcknowledgeResponse::to_message`] is Java
//! `ShareAcknowledgeResponse.toMessage` Responses (group by `topic_id` in
//! first-seen order; key partition overwrites the body);
//! [`protocol::share::ShareAcknowledgeRequest::for_consumer`] is Java
//! `ShareAcknowledgeRequest.Builder.forConsumer` Topics (group by topic
//! id; first-seen id and partition order; duplicate `(id, partition)`
//! replaces the batches)),
//! ConsumerGroupDescribe v0–v1 (v1 MemberType; FindCoordinator v4+ CoordinatorKeys of N;
//! [`protocol::admin::ConsumerGroupDescribeRequest::error_described_group_list`] is Java
//! `ConsumerGroupDescribeRequest.getErrorDescribedGroupList` (each id through
//! [`DescribedConsumerGroup::new`]);
//! [`protocol::admin::ConsumerGroupDescribeResponse::error_counts`] is Java
//! `ConsumerGroupDescribeResponse.errorCounts` (per-group codes, including `NONE`)),
//! ListTransactions v0–v1 (v1 DurationFilter, KIP-994;
//! Java `ListTransactionsRequest.Builder.build` rejects a non-negative
//! DurationFilter on v0),
//! CreateTopics v0–v7 (v5+ flexible; v5 KIP-525 configs; v7 TopicId;
//! [`protocol::admin::CreateTopicsResponse::should_client_throttle`] is Java
//! `CreateTopicsResponse.shouldClientThrottle` (v3+);
//! [`protocol::admin::CreateTopicsResponse::error_counts`] is Java
//! `CreateTopicsResponse.errorCounts` (per-topic codes, including `NONE`);
//! [`protocol::admin::CreatableTopic::error_result`] /
//! [`protocol::admin::CreateTopicsRequest::error_results`] are Java
//! `CreateTopicsRequest.getErrorResponse` (one topic / Topics). v5+
//! NumPartitions / ReplicationFactor stay `-1`, Configs empty, TopicId
//! zero. `ErrorMessage` stays the JSON default (null); official Java
//! also sets the English `Errors.message` string;
//! [`protocol::admin::CreateTopicsRequest::error_response`] is Java
//! `CreateTopicsRequest.getErrorResponse` (copies names; `ErrorMessage`
//! stays JSON-null; v2+ writes the `throttleTimeMs` argument; below v2
//! omits it);
//! v2+ round-trips ThrottleTimeMs; below v2 encode omits it even when
//! the body has a non-zero value and decode fills `0`;
//! [`protocol::admin::encode_create_topics_response`] still writes `0`),
//! DeleteTopics v0–v6 (v4+ flexible; v5 ErrorMessage; v6 TopicId, `delete_topics_by_id`;
//! [`protocol::admin::DeleteTopicsResponse::should_client_throttle`] is Java
//! `DeleteTopicsResponse.shouldClientThrottle` (v2+);
//! [`protocol::admin::DeleteTopicsResponse::error_counts`] is Java
//! `DeleteTopicsResponse.errorCounts` (per-topic codes, including `NONE`);
//! [`protocol::admin::TopicResult::error`] /
//! [`protocol::admin::DeleteTopicState::error_result`] are Java
//! `DeleteTopicsRequest.getErrorResponse` (one topic);
//! [`protocol::admin::DeleteTopicsRequest::error_response`] is Java
//! `DeleteTopicsRequest.getErrorResponse` (copies names / TopicIds;
//! `ErrorMessage` stays JSON-null; v1+ writes the `throttleTimeMs`
//! argument; below v1 omits it);
//! v1+ round-trips ThrottleTimeMs; below v1 encode omits it even when
//! the body has a non-zero value and decode fills `0`;
//! [`protocol::admin::encode_delete_topics_response`] still writes `0`;
//! [`protocol::admin::DeleteTopicsRequest::topic_ids`] /
//! [`protocol::admin::DeleteTopicsRequest::topic_names`] /
//! [`protocol::admin::DeleteTopicsRequest::topics`] are Java
//! `DeleteTopicsRequest.topicIds` / `topicNames` / `topics` (topic IDs empty
//! below v6; v6+ names include null when deleting by TopicId; below v6
//! id-only entries are omitted from `topicNames`; `topics` below v6 keeps
//! named entries with TopicId zeros and drops id-only; v6+ `topics` is
//! as-is);
//! [`protocol::admin::DeleteTopicsRequest::build`] is Java
//! `DeleteTopicsRequest.Builder.build` (v6+ non-empty TopicNames replaces
//! Topics; empty TopicNames leaves Topics as-is, including id-only; a
//! list of empty strings is still present; below v6 Topics is not
//! rewritten; encode still has separate name and state paths)),
//! DescribeGroups v0–v6 (v3 IncludeAuthorizedOperations; v4 GroupInstanceId; v5 flexible; v6 ErrorMessage; FindCoordinator v4+ CoordinatorKeys of N;
//! [`protocol::admin::DescribeGroupsResponse::UNKNOWN_STATE`] /
//! [`protocol::admin::DescribeGroupsResponse::UNKNOWN_PROTOCOL_TYPE`] /
//! [`protocol::admin::DescribeGroupsResponse::UNKNOWN_PROTOCOL`] /
//! [`protocol::admin::DescribeGroupsResponse::AUTHORIZED_OPERATIONS_OMITTED`] /
//! [`protocol::admin::DescribeGroupsResponse::should_client_throttle`] are Java
//! `DescribeGroupsResponse` error sentinels / `shouldClientThrottle` (v2+);
//! [`DescribedGroup::new`] is Java `groupError`;
//! [`protocol::admin::DescribeGroupsResponse::group_member`] /
//! [`protocol::admin::DescribeGroupsResponse::group_metadata`] /
//! [`protocol::admin::DescribeGroupsResponse::group_error`] are Java
//! `groupMember` / `groupMetadata` / `groupError` with `ErrorMessage`
//! (`ErrorMessage` stays JSON default (null) on `groupMetadata`);
//! [`protocol::admin::DescribeGroupsRequest::error_described_group_list`] is Java
//! `DescribeGroupsRequest.getErrorDescribedGroupList` (each id through
//! [`DescribedGroup::new`]);
//! [`protocol::admin::DescribeGroupsRequest::error_response`] is Java
//! `DescribeGroupsRequest.getErrorResponse` (each id through
//! [`DescribedGroup::new`]; v1+ writes the `throttleTimeMs` argument;
//! below v1 omits it);
//! v1+ round-trips ThrottleTimeMs; below v1 encode omits it even when
//! the body has a non-zero value and decode fills `0`;
//! [`protocol::admin::encode_describe_groups_response`] still writes `0`;
//! [`protocol::admin::DescribeGroupsResponse::error_counts`] is Java
//! `DescribeGroupsResponse.errorCounts` (per-group codes, including `NONE`)),
//! ListGroups v0–v5 (v3 flexible; v4 StatesFilter / GroupState; v5 TypesFilter / GroupType;
//! [`protocol::admin::ListGroupsResponse::should_client_throttle`] is Java
//! `ListGroupsResponse.shouldClientThrottle` (v2+);
//! [`protocol::admin::ListGroupsRequest::error_response`] is Java
//! `ListGroupsRequest.getErrorResponse` (empty Groups; request filters are
//! not copied; v1+ writes the `throttleTimeMs` argument; below v1 omits it);
//! v1+ round-trips ThrottleTimeMs; below v1 encode omits it even when
//! the body has a non-zero value and decode fills `0`;
//! [`protocol::admin::encode_list_groups_response`] still writes `0`;
//! [`protocol::admin::ListGroupsRequest::build`] is Java
//! `ListGroupsRequest.Builder.build` (a non-empty StatesFilter below v4,
//! or a non-empty TypesFilter below v5, is `UnsupportedVersionException`;
//! encode still omits)),
//! DeleteGroups v0–v2 (v0–v1 classic; v2 flexible; FindCoordinator v4+ CoordinatorKeys of N;
//! [`protocol::admin::DeleteGroupsResponse::should_client_throttle`] is Java
//! `DeleteGroupsResponse.shouldClientThrottle` (v1+);
//! [`protocol::admin::DeleteGroupsResponse::error_counts`] is Java
//! `DeleteGroupsResponse.errorCounts` (per-group codes, including `NONE`);
//! [`protocol::admin::DeleteGroupsResponse::errors`] /
//! [`protocol::admin::DeleteGroupsResponse::get`] are Java
//! `DeleteGroupsResponse.errors` / `get` (group id to `errorCode`; missing
//! id is [`Error::protocol`]);
//! [`protocol::admin::DeleteGroupsRequest::error_result_collection`] is Java
//! `DeleteGroupsRequest.getErrorResultCollection` (each id through
//! [`DeletableGroupResult::new`])),
//! DescribeClientQuotas / AlterClientQuotas v0–v1 (v1 flexible;
//! [`protocol::admin::DescribeClientQuotasRequest::MATCH_TYPE_EXACT`] /
//! [`protocol::admin::DescribeClientQuotasRequest::MATCH_TYPE_DEFAULT`] /
//! [`protocol::admin::DescribeClientQuotasRequest::MATCH_TYPE_SPECIFIED`]
//! are Java `DescribeClientQuotasRequest` MatchType constants;
//! [`protocol::admin::DescribeClientQuotasRequest::filter`] is Java
//! `DescribeClientQuotasRequest.filter` (`ofEntity` / `ofDefaultEntity` /
//! `ofEntityType`, then `containsOnly` or `contains`; unknown MatchType
//! is [`Error::protocol`]);
//! [`protocol::admin::DescribeClientQuotasRequest::from_filter`] is Java
//! `DescribeClientQuotasRequest.Builder` from a filter (MatchType from
//! [`ClientQuotaFilterComponent::matched`]; leftover Match on
//! default/specified is null);
//! [`protocol::admin::DescribeClientQuotasResponse::error`] is Java
//! `DescribeClientQuotasRequest.getErrorResponse` (`Entries` null, not
//! empty). `ErrorMessage` stays the JSON default (null); official Java
//! also sets the English `Errors.message` string. Throttle is the JSON
//! default (`0`);
//! [`protocol::admin::DescribeClientQuotasResponse::from_quota_entities`] is Java
//! `DescribeClientQuotasResponse.fromQuotaEntities` (type/name pairs
//! plus values into `Entries`; `ErrorCode` `0`; `ErrorMessage` null;
//! empty input is empty `Entries`, not null; throttle unused);
//! [`protocol::admin::AlterClientQuotasResponse::error_counts`] is Java
//! `AlterClientQuotasResponse.errorCounts` (per-entry codes, including `NONE`);
//! [`protocol::admin::AlterClientQuotasResponse::from_quota_entities`] is Java
//! `AlterClientQuotasResponse.fromQuotaEntities` (type/name pairs plus
//! [`ApiError`] into `Entries`; `ErrorMessage` is copied; throttle unused);
//! [`protocol::admin::AlterClientQuotasRequest::entries`] is Java
//! `AlterClientQuotasRequest.entries` (duplicate EntityType last-wins;
//! leftover Value on remove is ignored)),
//! ListConfigResources v0–v1 (v0 ListClientMetricsResources; v1 ResourceTypes),
//! AlterReplicaLogDirs v1–v2 (v1 classic; v2 flexible;
//! [`protocol::admin::AlterReplicaLogDirsResponse::should_client_throttle`] is Java
//! `AlterReplicaLogDirsResponse.shouldClientThrottle` (v1+);
//! [`protocol::admin::AlterReplicaLogDirsResponse::error_counts`] is Java
//! `AlterReplicaLogDirsResponse.errorCounts` (partition-level codes, including `NONE`);
//! [`AlterReplicaLogDirsTopic::error_result`] /
//! [`AlterReplicaLogDirsRequest::error_result`] are Java
//! `AlterReplicaLogDirsRequest.getErrorResponse` (one topic / flatten dirs);
//! [`AlterReplicaLogDirsRequest::partition_dirs`] is Java
//! `AlterReplicaLogDirsRequest.partitionDirs` (`(topic, partition)` to path;
//! a later directory overwrites)),
//! DescribeLogDirs v1–v4 (v1 classic; v2+ flexible; v3 ErrorCode; v4 TotalBytes;
//! [`protocol::admin::DescribeLogDirsResponse::UNKNOWN_VOLUME_BYTES`] /
//! [`protocol::admin::DescribeLogDirsResponse::INVALID_OFFSET_LAG`] /
//! [`protocol::admin::DescribeLogDirsResponse::should_client_throttle`] are Java
//! `DescribeLogDirsResponse` sentinels / `shouldClientThrottle` (v1+);
//! [`protocol::admin::DescribeLogDirsResponse::error_counts`] is Java
//! `DescribeLogDirsResponse.errorCounts` (top-level `errorCode` plus each
//! directory-level code, including `NONE`);
//! [`DescribeLogDirsRequest::is_all_topic_partitions`] is Java
//! `DescribeLogDirsRequest.isAllTopicPartitions`),
//! CreateDelegationToken v1–v3 (v1 classic; v2+ flexible; v3 owner/requester;
//! [`protocol::admin::CreateDelegationTokenResponse::should_client_throttle`] is Java
//! `CreateDelegationTokenResponse.shouldClientThrottle` (v1+);
//! [`CreateDelegationTokenResponse::error`] /
//! [`CreateDelegationTokenResponse::prepare_response`] are Java
//! `CreateDelegationTokenRequest.getErrorResponse` /
//! `CreateDelegationTokenResponse.prepareResponse` (`KafkaPrincipal.ANONYMOUS`
//! owner and requester; timestamps `-1`; empty TokenId / Hmac).
//! TokenRequester fields are v3+; throttle stays the JSON default (`0`)),
//! RenewDelegationToken v1–v2 (v1 classic; v2 flexible;
//! [`protocol::admin::RenewDelegationTokenResponse::should_client_throttle`] is Java
//! `RenewDelegationTokenResponse.shouldClientThrottle` (v1+)),
//! ExpireDelegationToken v1–v2 (v1 classic; v2 flexible;
//! [`protocol::admin::ExpireDelegationTokenResponse::should_client_throttle`] is Java
//! `ExpireDelegationTokenResponse.shouldClientThrottle` (v1+)),
//! DescribeDelegationToken v1–v3 (v1 classic; v2+ flexible; v3 TokenRequester;
//! [`protocol::admin::DescribeDelegationTokenResponse::should_client_throttle`] is Java
//! `DescribeDelegationTokenResponse.shouldClientThrottle` (v1+);
//! [`DescribeDelegationTokenRequest::owners_list_empty`] is Java
//! `DescribeDelegationTokenRequest.ownersListEmpty` (`Some` empty is true;
//! `None` is every visible token)),
//! DescribeConfigs v0–v4 (v1 synonyms; v3 IncludeDocumentation / ConfigType; v4 flexible;
//! [`protocol::admin::DescribeConfigsResponse::should_client_throttle`] is Java
//! `DescribeConfigsResponse.shouldClientThrottle` (v2+);
//! [`protocol::admin::DescribeConfigsResponse::error_counts`] is Java
//! `DescribeConfigsResponse.errorCounts` (per-resource codes, including `NONE`);
//! [`protocol::admin::DescribeConfigsResponse::result_map`] is Java
//! `DescribeConfigsResponse.resultMap` (ConfigResource to each result;
//! unknown resource types are UNKNOWN);
//! [`protocol::admin::DescribeConfigsRequest::error_response`] is Java
//! `DescribeConfigsRequest.getErrorResponse` (copies names / types;
//! `ErrorMessage` stays JSON-null; always writes the `throttleTimeMs`
//! argument);
//! ThrottleTimeMs is JSON `0+` (on the wire for every spoken version,
//! including v0); round-trips a non-zero value;
//! [`protocol::admin::encode_describe_configs_response`] still writes `0`),
//! CreatePartitions v0–v3 (v2+ flexible; v3 KIP-599;
//! [`protocol::admin::CreatePartitionsResponse::should_client_throttle`] is Java
//! `CreatePartitionsResponse.shouldClientThrottle` (v1+);
//! [`protocol::admin::CreatePartitionsResponse::error_counts`] is Java
//! `CreatePartitionsResponse.errorCounts` (per-topic codes, including `NONE`);
//! [`protocol::admin::CreatePartitionsTopic::error_result`] /
//! [`protocol::admin::CreatePartitionsTopic::error_results`] are Java
//! `CreatePartitionsRequest.getErrorResponse` (one topic / Results).
//! `ErrorMessage` stays the JSON default (null); official Java also
//! sets the English `Errors.message` string),
//! IncrementalAlterConfigs v0–v1 (v1 flexible; Resources of N;
//! [`protocol::admin::IncrementalAlterConfigsResponse::should_client_throttle`] is Java
//! `IncrementalAlterConfigsResponse.shouldClientThrottle` (v0+);
//! [`protocol::admin::IncrementalAlterConfigsResponse::error_counts`] is Java
//! `IncrementalAlterConfigsResponse.errorCounts` (per-resource codes, including `NONE`);
//! [`protocol::admin::IncrementalAlterConfigsResponse::from_response_data`] is Java
//! `IncrementalAlterConfigsResponse.fromResponseData` (ConfigResource to
//! [`ApiError`]; unknown resource types are UNKNOWN);
//! [`protocol::admin::IncrementalAlterConfigsResponse::from_errors`] is Java
//! `IncrementalAlterConfigsResponse` constructed from a result map (type
//! id + name plus [`ApiError`] into Responses; `ErrorMessage` is copied;
//! throttle unused);
//! [`protocol::admin::IncrementalAlterConfigsRequest::from_configs`] is Java
//! `IncrementalAlterConfigsRequest.Builder` from a resource list and
//! configs map (missing `Map.get` is [`Error::protocol`]; mapKey first
//! stays; extra map entries omitted)),
//! AlterConfigs v0–v2 (v2 flexible; Resources of N;
//! [`protocol::admin::AlterConfigsResponse::should_client_throttle`] is Java
//! `AlterConfigsResponse.shouldClientThrottle` (v1+);
//! [`protocol::admin::AlterConfigsResponse::error_counts`] is Java
//! `AlterConfigsResponse.errorCounts` (per-resource codes, including `NONE`);
//! [`protocol::admin::AlterConfigsResponse::errors`] is Java
//! `AlterConfigsResponse.errors` (ConfigResource to [`ApiError`]; unknown
//! resource types are UNKNOWN);
//! [`protocol::admin::AlterConfigsRequest::configs`] is Java
//! `AlterConfigsRequest.configs` (ConfigResource to [`Config`]; unknown
//! resource types are UNKNOWN; each value is [`ConfigEntry::new`]);
//! [`protocol::admin::AlterConfigsRequest::from_configs`] is Java
//! `AlterConfigsRequest.Builder` from a configs map (null Value is
//! [`Error::protocol`]; mapKey first stays);
//! [`protocol::admin::AlterConfigsRequest::error_response`] is Java
//! `AlterConfigsRequest.getErrorResponse` (copies names / types;
//! `ErrorMessage` stays JSON-null; always writes the `throttleTimeMs`
//! argument);
//! ThrottleTimeMs is JSON `0+` (on the wire for every spoken version,
//! including v0); round-trips a non-zero value;
//! [`protocol::admin::encode_alter_configs_resource_results`] still writes `0`),
//! DeleteRecords v0–v2 (v2 flexible;
//! [`protocol::admin::DeleteRecordsRequest::HIGH_WATERMARK`];
//! [`DeletedRecords::INVALID_LOW_WATERMARK`];
//! [`protocol::admin::DeleteRecordsResponse::should_client_throttle`] is Java
//! `DeleteRecordsResponse.shouldClientThrottle` (v1+);
//! [`protocol::admin::DeleteRecordsResponse::error_counts`] is Java
//! `DeleteRecordsResponse.errorCounts` (partition-level codes, including `NONE`);
//! [`protocol::admin::DeletedRecordsPartition::error`] /
//! [`protocol::admin::DeleteRecordsTopic::error_result`] are Java
//! `DeleteRecordsRequest.getErrorResponse` (partition body / one topic);
//! [`protocol::admin::DeleteRecordsRequest::error_response`] is Java
//! `DeleteRecordsRequest.getErrorResponse` (copies names / indexes;
//! `INVALID_LOW_WATERMARK`; always writes the `throttleTimeMs` argument);
//! ThrottleTimeMs is JSON `0+` (on the wire for every spoken version,
//! including v0); round-trips a non-zero value;
//! [`protocol::admin::encode_delete_records_topics_response`] still writes `0`),
//! CreateAcls / DescribeAcls / DeleteAcls v0–v3 (v1 ResourcePatternType; v2+ flexible;
//! [`protocol::acl::CreateAclsResponse::should_client_throttle`] /
//! [`protocol::acl::DescribeAclsResponse::should_client_throttle`] /
//! [`protocol::acl::DeleteAclsResponse::should_client_throttle`] are Java
//! `shouldClientThrottle` (v1+);
//! [`protocol::acl::CreateAclsResponse::error_counts`] is Java
//! `CreateAclsResponse.errorCounts` (per-creation codes, including `NONE`);
//! [`protocol::acl::DeleteAclsResponse::error_counts`] is Java
//! `DeleteAclsResponse.errorCounts` (filter-level codes, including `NONE`;
//! matching-ACL codes are not counted);
//! [`AclCreationResult::error`] / [`AclCreationResult::error_results`] are Java
//! `CreateAclsRequest.getErrorResponse` (one result / `nCopies`). Request
//! bindings are not copied; `ErrorMessage` stays the JSON default (null);
//! official Java also sets the English `Errors.message` string. Throttle
//! is the JSON default (`0`). Java `CreateAclsRequest.validate` rejects
//! UNKNOWN resource / pattern / operation / permission;
//! `DescribeAclsRequest.normalizeAndValidate` /
//! `DeleteAclsRequest.normalizeAndValidate` do the same on filters
//! (`DescribeAclsRequest contains UNKNOWN elements` /
//! `Filters contain UNKNOWN elements`). Java `DescribeAclsResponse.validate` /
//! `DeleteAclsResponse.validate` reject UNKNOWN on response resources /
//! MatchingAcls (`Contain UNKNOWN elements` /
//! `DeleteAclsMatchingAcls contain UNKNOWN elements`);
//! [`protocol::acl::DescribeAclsResponse::acls_resources`] /
//! [`protocol::acl::DescribeAclsResponse::acl_bindings`] are Java
//! `aclsResources` / `aclBindings` (group by [`ResourcePattern`]);
//! [`protocol::acl::DeleteAclsResponse::matching_acl`] /
//! [`protocol::acl::DeleteAclsResponse::acl_binding`] are Java
//! `matchingAcl` / `aclBinding` ([`protocol::acl::DeleteAclsMatchingAcl`];
//! unknown resource / pattern / operation / permission codes become
//! UNKNOWN; encode of [`DeletedAclsFilterResult`] matching ACEs is
//! [`ApiError::NONE`]);
//! [`DeletedAclsFilterResult::error`] / [`DeletedAclsFilterResult::error_results`]
//! are Java `DeleteAclsRequest.getErrorResponse` (one FilterResult /
//! `nCopies`). MatchingAcls stay the JSON default (empty); `ErrorMessage`
//! stays the JSON default (null); official Java also sets the English
//! `Errors.message` string. Throttle is the JSON default (`0`)),
//! AddPartitionsToTxn v0–v3 (v3 flexible;
//! [`protocol::txn::AddPartitionsToTxnResponse::should_client_throttle`] is Java
//! `AddPartitionsToTxnResponse.shouldClientThrottle` (v1+);
//! [`protocol::txn::AddPartitionsToTxnResponse::error_counts`] is Java
//! `AddPartitionsToTxnResponse.errorCounts` for v0–v3 (partition-level codes,
//! including `NONE`);
//! [`protocol::txn::AddPartitionsToTxnResponse::errors`] /
//! [`protocol::txn::AddPartitionsToTxnResponse::errors_for_transaction`] are
//! Java `AddPartitionsToTxnResponse.errors` /
//! `errorsForTransaction` (v0–v3 key
//! [`protocol::txn::AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID`]; a later
//! partition overwrites);
//! [`protocol::txn::AddPartitionsToTxnResponse::from_errors`] is Java
//! `AddPartitionsToTxnResponse.topicCollectionForErrors` / topic results
//! of `resultForTransaction` (group by name; a later entry for the same
//! topic appends; a later partition with the same index is ignored);
//! [`protocol::txn::TxnPartitionsTopic::error_result`] /
//! [`protocol::txn::TxnPartitionsTopic::error_results`] /
//! [`protocol::txn::AddPartitionsToTxnPartitionResult::error`] are Java
//! `AddPartitionsToTxnRequest.getErrorResponse` / `errorResponseForTopics`
//! (one topic / Topics / partition body). Nested body is PartitionIndex
//! and PartitionErrorCode (`ResultsByTopicV3AndBelow`). Throttle is the
//! JSON default (`0`);
//! [`protocol::txn::AddPartitionsToTxnRequest::partitions`] is Java
//! `AddPartitionsToTxnRequest.getPartitions` (each `(topic, partition)`
//! in request order);
//! [`protocol::txn::AddPartitionsToTxnRequest::from_partitions`] is Java
//! `AddPartitionsToTxnRequest.buildTxnTopicCollection` (group by name;
//! a later entry for the same topic appends; first-seen topic order;
//! duplicate pairs kept)), AddOffsetsToTxn v0–v4
//! (v3+ flexible; v4 TRANSACTION_ABORTABLE;
//! [`protocol::txn::AddOffsetsToTxnResponse::should_client_throttle`] is Java
//! `AddOffsetsToTxnResponse.shouldClientThrottle` (v1+)), EndTxn v0–v5
//! (v3+ flexible; v4 TRANSACTION_ABORTABLE; v5 ProducerId / ProducerEpoch;
//! [`protocol::txn::EndTxnRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`];
//! [`protocol::txn::EndTxnRequest::error_response`] is Java
//! `EndTxnRequest.getErrorResponse` ([`RecordBatch::NO_PRODUCER_ID`] /
//! [`RecordBatch::NO_PRODUCER_EPOCH`] on v5+; throttle JSON default 0);
//! [`protocol::txn::EndTxnResponse::should_client_throttle`] is Java
//! `EndTxnResponse.shouldClientThrottle` (v1+);
//! EndTxn decode below v5 fills [`RecordBatch::NO_PRODUCER_ID`] /
//! [`RecordBatch::NO_PRODUCER_EPOCH`] (JSON default `-1`);
//! [`TransactionResult`] is Java `TransactionResult` (`ABORT` / `COMMIT`)),
//! and TxnOffsetCommit v0–v5
//! (v3+ flexible; GenerationId / MemberId / GroupInstanceId;
//! decode below v2 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`];
//! v5 skips AddOffsetsToTxn, KIP-890 Part 2;
//! [`protocol::txn::TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`];
//! [`protocol::txn::TxnOffsetCommitResponse::should_client_throttle`] is Java
//! `TxnOffsetCommitResponse.shouldClientThrottle` (v1+);
//! [`protocol::txn::TxnOffsetCommitResponse::error_counts`] is Java
//! `TxnOffsetCommitResponse.errorCounts` (partition-level codes, including `NONE`);
//! [`protocol::txn::TxnOffsetCommitResponse::errors`] is Java
//! `TxnOffsetCommitResponse.errors` (`(topic, partition)` codes; a later
//! partition overwrites);
//! [`protocol::txn::TxnOffsetCommitResponse::from_errors`] is Java
//! `TxnOffsetCommitResponse` constructor from an errors map (group by
//! topic name; a later entry for the same topic appends; first-seen
//! topic order);
//! [`protocol::txn::TxnOffsetCommitResponse::merge`] is Java
//! `TxnOffsetCommitResponse.Builder.merge` (replace when current Topics are
//! empty; otherwise append topics / partitions; overlapping partitions are
//! not checked);
//! [`protocol::txn::TxnOffsetTopic::error_result`] /
//! [`protocol::txn::TxnOffsetTopic::error_results`] /
//! [`protocol::txn::TxnOffsetCommitResponsePartition::error`] are Java
//! `TxnOffsetCommitRequest.getErrorResponse` / `getErrorResponseTopics`
//! (one topic / Topics / partition body). Nested body is PartitionIndex
//! and ErrorCode. Throttle is the JSON default (`0`);
//! [`protocol::txn::TxnOffsetCommitMember::unknown`] /
//! [`protocol::txn::TxnOffsetCommitMember::group_metadata_set`] are Java
//! `TxnOffsetCommitRequest.Builder` without group metadata /
//! `groupMetadataSet`;
//! [`protocol::txn::TxnOffsetPartition`] getters / `Display` match Java
//! `TxnOffsetCommitRequest.CommittedOffset`;
//! [`protocol::txn::TxnOffsetCommitRequest::offsets`] is Java
//! `TxnOffsetCommitRequest.offsets` (`(topic, partition)` to
//! [`protocol::txn::TxnOffsetPartition`]; a later partition overwrites);
//! [`protocol::txn::TxnOffsetCommitRequest::from_offsets`] is Java
//! `TxnOffsetCommitRequest.getTopics` (group by name; a later entry for
//! the same topic appends; first-seen topic order; duplicate pairs
//! kept)).
//! [`Producer::metrics`] is a snapshot of queued / acked / error counts
//! plus produce-ack latency min/mean/max and p50/p99 (last 1024 samples),
//! with per-topic rows on [`ProducerMetrics::topics`].
//! [`metrics::format_bytes`] is Java `Utils.formatBytes` (English `0.##`
//! scale; `-1` is `-1`; `1024` is `1 KB`). [`Quota`] is Java
//! `org.apache.kafka.common.metrics.Quota` (`upper=1.0` / `lower=1.0`;
//! `acceptable` is at or below the bound for an upper bound and at or
//! above for a lower bound). [`partitioner::abs`] is Java
//! `Utils.abs` ([`i32::MIN`] is `0`). [`partitioner::to_positive`] is Java
//! `Utils.toPositive`.
//! [`Admin::metrics`] is the same snapshot pattern for Admin RPCs
//! ([`AdminMetrics`]; Java `Admin.metrics()`).
//! [`Producer::client_instance_id`] is Java `clientInstanceId` (KIP-714;
//! returns [`Uuid`]).
//! [`Producer::client_instance_id_timeout`] is Java `clientInstanceId(Duration)`.
//! [`RecordMetadata::new`] is Java
//! `RecordMetadata(TopicPartition, long, int, long, int, int)` (`baseOffset`
//! [`RecordMetadata::INVALID_OFFSET`] keeps offset `-1` and ignores
//! `batchIndex`; otherwise offset is `baseOffset + batchIndex`).
//! [`RecordMetadata::timestamp`] / [`RecordMetadata::has_timestamp`] /
//! [`RecordMetadata::serialized_key_size`] / [`RecordMetadata::serialized_value_size`]
//! match Java `RecordMetadata`. [`RecordMetadata::UNKNOWN_PARTITION`] is Java
//! `RecordMetadata.UNKNOWN_PARTITION`. [`protocol::api::ProducePartitionResponse::INVALID_OFFSET`]
//! is Java `ProduceResponse.INVALID_OFFSET`.
//! [`protocol::api::ProducePartitionResponse::partition_response`] is Java
//! `ProduceResponse.PartitionResponse(Errors)`.
//! [`protocol::api::ProduceTopicData::error_result`] is Java
//! `ProduceRequest.getErrorResponse` (one topic).
//! [`protocol::api::ProduceRequest::error_response`] is Java
//! `ProduceRequest.getErrorResponse` (`acks` `0` is `None`; unique
//! `partitionSizes` keys otherwise).
//! [`protocol::api::ProduceRequest::error_counts`] is Java
//! `ProduceRequest.errorCounts(Throwable)` (unique `partitionSizes` keys;
//! empty is `{error: 0}`, not an empty map; does not look at `acks`).
//! [`protocol::api::ProduceResponse::should_client_throttle`] is Java
//! `ProduceResponse.shouldClientThrottle` (v6+).
//! [`protocol::api::ProduceResponse::error_counts`] is Java
//! `ProduceResponse.errorCounts` (partition-level codes, including `NONE`).
//! [`protocol::api::ProduceResponse::to_data`] is Java
//! `ProduceResponse.toData` Responses (group by name in first-seen order;
//! a later partition for the same topic appends, including after another
//! topic; duplicates are kept).
//! [`protocol::api::ProduceRecordError`] is Java
//! `ProduceResponse.RecordError` (`Display` is `RecordError.toString`:
//! `message=null` when the message is `None`; otherwise the text is
//! single-quoted; duplicate `batchIndex` values are kept). Produce v8+
//! round-trips `RecordErrors` / `ErrorMessage`; below v8 decode fills
//! empty / null.
//! Produce decode below v5 fills
//! that sentinel; Java `PartitionResponse(Errors)` writes it for
//! `baseOffset` / `logStartOffset`. Omitted Produce v10+ CurrentLeader fills
//! [`protocol::api::MetadataResponse::NO_LEADER_ID`] /
//! [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]. [`RecordMetadata::INVALID_OFFSET`] is
//! the client-type copy (`hasOffset` is false when the offset is that
//! value). [`RecordBatch::NO_TIMESTAMP`] is Java
//! `RecordBatch.NO_TIMESTAMP`. [`RecordBatch::MAGIC_VALUE_V0`] /
//! [`RecordBatch::MAGIC_VALUE_V1`] / [`RecordBatch::MAGIC_VALUE_V2`] /
//! [`RecordBatch::CURRENT_MAGIC_VALUE`] are Java `MAGIC_VALUE_V0` /
//! `MAGIC_VALUE_V1` / `MAGIC_VALUE_V2` / `CURRENT_MAGIC_VALUE` (this crate
//! encodes magic-v2 only). [`RecordBatch::RECORD_BATCH_OVERHEAD`] is Java
//! `DefaultRecordBatch.RECORD_BATCH_OVERHEAD` (`61`).
//! [`RecordBatch::CRC_OFFSET`] / [`RecordBatch::LAST_OFFSET_DELTA_OFFSET`] /
//! [`RecordBatch::RECORDS_COUNT_OFFSET`] are Java `DefaultRecordBatch`
//! layout offsets.
//! [`Record::MAX_RECORD_OVERHEAD`] is Java `DefaultRecord.MAX_RECORD_OVERHEAD`
//! (`21`).
//! [`Admin::get_telemetry_subscriptions`] / [`Admin::push_telemetry`] take
//! [`Uuid`] or `[u8; 16]`.
//!
//! # Fetch
//!
//! ```no_run
//! # async fn example() -> partitionline::Result<()> {
//! use partitionline::Consumer;
//!
//! let mut consumer = Consumer::connect("127.0.0.1:9092").await?;
//! consumer.assign("events", 0, 0).await?;
//! let recs = consumer.fetch().await?;
//! # let _ = recs;
//! # Ok(())
//! # }
//! ```
//!
//! [`Consumer::assign_topic`] assigns every partition. [`Consumer::seek`] /
//! [`Consumer::seek_to`] / [`Consumer::seek_with_metadata`] /
//! [`Consumer::seek_to_beginning`] /
//! [`Consumer::seek_to_end`] / [`Consumer::seek_to_beginning_of`] /
//! [`Consumer::seek_to_end_of`] move the
//! next fetch offset ([`Consumer::seek_with_metadata`] is Java
//! `seek(TopicPartition, OffsetAndMetadata)` and sends the leader epoch as
//! Fetch `LastFetchedEpoch`; a negative offset is Java
//! `seek offset must not be a negative number`, and an unassigned partition
//! is Java `No current assignment for partition`; [`Consumer::current_lag`]
//! uses that same message). [`Consumer::position`] /
//! [`Consumer::position_of`] for an unassigned partition is Java
//! `You can only check the position for partitions assigned to this consumer.`.
//! [`Consumer::pause`] / [`Consumer::resume`] skip
//! partitions without dropping the assignment. [`Consumer::fetch`] talks to
//! every partition leader in parallel. Nothing assigned is Java
//! `Consumer is not subscribed to any topics or assigned any partitions`
//! ([`ConsumerGroup::poll`] uses the same check; [`ShareGroup::poll`] is
//! `Consumer is not subscribed to any topics.`). Fetch negotiates v4–v17 (v12+ is
//! flexible; v13+ topic IDs, KIP-516; v15 omits untagged ReplicaId, KIP-903;
//! v16 CurrentLeader / NodeEndpoints, KIP-951; v17 omits ReplicaDirectoryId, KIP-853;
//! v12+ LastFetchedEpoch from the last consumed batch, KIP-320;
//! decode below v12 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`];
//! SessionId / SessionEpoch / ForgottenTopicsData are v7+;
//! [`protocol::fetch::encode_fetch_request_with_session`] round-trips a
//! non-LEGACY session on v7+; below v7 encode omits SessionId / SessionEpoch
//! even when the body is non-LEGACY and decode fills
//! [`protocol::fetch::FetchMetadata::LEGACY`];
//! [`protocol::fetch::encode_fetch_request`] still writes LEGACY and empty
//! ForgottenTopicsData;
//! [`protocol::fetch::encode_fetch_request_with_forgotten`] round-trips
//! ForgottenTopicsData on v7+ (including duplicate partition indexes);
//! below v7 encode omits it even when the body is non-empty and decode
//! fills empty; v13+ uses TopicId;
//! request LogStartOffset is v5+;
//! CurrentLeaderEpoch is v9+; RackId and response PreferredReadReplica are
//! v11+; response LogStartOffset is v5+; below those versions encode omits
//! the field even when the body has a value and decode fills the JSON
//! default; response SnapshotId tagged field 2 is v12+ (`EndOffset`
//! INT64 then `Epoch` INT32; the reverse of DivergingEpoch); below v12
//! encode omits it even when the body is non-default and decode fills
//! [`protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH_OFFSET`] /
//! [`protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH`]; this is not the
//! FetchSnapshot API and does not start those RPCs). v18+
//! is not spoken. [`protocol::fetch::FetchedPartition::INVALID_HIGH_WATERMARK`] /
//! [`protocol::fetch::FetchedPartition::INVALID_LAST_STABLE_OFFSET`] /
//! [`protocol::fetch::FetchedPartition::INVALID_LOG_START_OFFSET`] /
//! [`protocol::fetch::FetchedPartition::INVALID_PREFERRED_REPLICA_ID`] are Java
//! `FetchResponse` sentinels (`-1`).
//! [`protocol::fetch::FetchedPartition::partition_response`] is Java
//! `FetchResponse.partitionResponse`.
//! [`protocol::fetch::FetchTopic::error_result`] is Java
//! `FetchRequest.getErrorResponse` (one topic; v13 and later omit partitions).
//! [`protocol::fetch::FetchRequest::error_response`] is Java
//! `FetchRequest.getErrorResponse` (below v13 each topic through
//! [`protocol::fetch::FetchTopic::error_result`]; v13+ Responses is empty;
//! [`protocol::fetch::encode_fetch_response`] writes top-level `ErrorCode` 0 /
//! `SessionId` 0; v7+ round-trips those fields; below v7 encode omits them
//! even when the body is non-zero and decode fills `0`).
//! [`protocol::fetch::FetchRequest::fetch_data`] is Java
//! `FetchRequest.fetchData` (v4–v12 use the topic name; v13+ looks up
//! `topic_id` and keeps a missing name as `None`; a later partition
//! overwrites).
//! [`protocol::fetch::FetchRequest::forgotten_topics`] is Java
//! `FetchRequest.forgottenTopics` (v4–v12 use the topic name; v13+ looks
//! up `topic_id` and keeps a missing name as `None`; duplicates are kept;
//! [`protocol::fetch::encode_fetch_request_with_forgotten`] writes the list
//! on v7+; [`protocol::fetch::encode_fetch_request`] still writes empty
//! ForgottenTopicsData).
//! [`protocol::fetch::FetchRequest::forgotten_from_removed`] is Java
//! `FetchRequest.Builder.build` ForgottenTopicsData from removed and
//! replaced (group by name; first topic id for a name is kept; later
//! partitions append; replaced only on v13+;
//! [`protocol::fetch::encode_fetch_request_with_forgotten`] writes the list
//! on v7+; [`protocol::fetch::encode_fetch_request`] still writes empty
//! ForgottenTopicsData).
//! [`protocol::fetch::FetchRequest::topics_from_fetch_data`] is Java
//! `FetchRequest.Builder.build` Topics from fetchData (consecutive same
//! name share one topic; first topic id is kept; intervening names stay
//! split; encode still writes the caller's Topics as-is).
//! [`protocol::fetch::FetchedPartition::preferred_read_replica()`] /
//! [`protocol::fetch::FetchedPartition::is_preferred_replica`] /
//! [`protocol::fetch::FetchedPartition::diverging_epoch()`] /
//! [`protocol::fetch::FetchedPartition::is_diverging_epoch`] are Java
//! `FetchResponse.preferredReadReplica` / `isPreferredReplica` /
//! `divergingEpoch` / `isDivergingEpoch` (`None` is empty `Optional`;
//! epoch `< 0` is empty);
//! [`protocol::fetch::FetchedPartition::snapshot_id()`] /
//! [`protocol::fetch::FetchedPartition::is_snapshot_id`] are JSON
//! `SnapshotId` tagged field 2 (`None` when both fields are the JSON
//! defaults; the pair is `(end_offset, epoch)`; Apache
//! `FetchResponse.java` has no `snapshotId` helper; this is not the
//! FetchSnapshot API);
//! [`protocol::fetch::FetchedPartition::records_size`] is Java
//! `FetchResponse.recordsSize` (`0` when records are empty);
//! [`protocol::fetch::FetchResponse::should_client_throttle`] is Java
//! `FetchResponse.shouldClientThrottle` (v8+).
//! [`protocol::fetch::FetchResponse::topic_ids`] is Java
//! `FetchResponse.topicIds` (skips zeros).
//! [`protocol::fetch::FetchResponse::response_data`] is Java
//! `FetchResponse.responseData` (v4–v12 use the topic name; v13+ looks up
//! `topic_id` and skips a missing name; a later partition overwrites).
//! [`protocol::fetch::FetchResponse::error_counts`] is Java
//! `FetchResponse.errorCounts` (top-level `errorCode` plus each
//! partition-level code, including `NONE`). Decode returns the top-level
//! code; [`protocol::fetch::encode_fetch_response`] writes `0`.
//! [`protocol::fetch::FetchResponse::to_message`] is Java
//! `FetchResponse.toMessage` Responses (consecutive `matchingTopic`:
//! non-zero `topic_id` matches by id, else by name; key partition
//! overwrites the body).
//! [`protocol::fetch::DEFAULT_RESPONSE_MAX_BYTES`] /
//! [`protocol::fetch::is_from_follower`] are Java
//! `FetchRequest.DEFAULT_RESPONSE_MAX_BYTES` / `isFromFollower`.
//! Omitted Fetch
//! v12+ CurrentLeader fills [`protocol::api::MetadataResponse::NO_LEADER_ID`] /
//! [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]; omitted DivergingEpoch fills
//! [`protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH`] /
//! [`protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH_OFFSET`]; omitted
//! SnapshotId fills
//! [`protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH_OFFSET`] /
//! [`protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH`].
//! [`protocol::fetch::CONSUMER_REPLICA_ID`] is Java
//! `FetchRequest.CONSUMER_REPLICA_ID` (written through v14).
//! [`protocol::offsets::CONSUMER_REPLICA_ID`] /
//! [`protocol::epoch::CONSUMER_REPLICA_ID`] are Java `ListOffsetsRequest` /
//! `OffsetsForLeaderEpochRequest` consumer replica ids.
//! [`protocol::fetch::is_consumer`] / [`protocol::fetch::is_valid_broker_id`] /
//! [`protocol::fetch::describe_replica_id`] are Java `FetchRequest.isConsumer` /
//! `isValidBrokerId` / `describeReplicaId`.
//! [`protocol::fetch::replica_id`] / [`protocol::fetch::replica_id_from_data`]
//! are Java `FetchRequest.replicaId()` / `replicaId(FetchRequestData)` (below
//! v15 untagged ReplicaId; v15+ ReplicaState; static uses untagged when it
//! is not `-1`; encode still writes [`protocol::fetch::CONSUMER_REPLICA_ID`]).
//! [`protocol::fetch::FetchMetadata`] is Java `FetchMetadata`
//! (v7+ round-trips SessionId / SessionEpoch, including a non-LEGACY
//! value; below v7 encode omits them even when the body is non-LEGACY
//! and decode fills [`protocol::fetch::FetchMetadata::LEGACY`];
//! [`protocol::fetch::encode_fetch_request`] still writes LEGACY).
//! [`protocol::header::RequestHeader`] `Display` is Java
//! `RequestHeader.toString` (`apiKey` is the Kafka 4.0 `ApiKeys` enum
//! name; null `clientId` prints `null`). [`protocol::header::RequestHeader::size`]
//! is Java `RequestHeader.size`. [`protocol::header::RequestHeader::to_response_header`]
//! is Java `RequestHeader.toResponseHeader`.
//! [`protocol::header::RequestHeader::check_correlation`] is Java
//! `AbstractResponse.parseResponse` (`CorrelationIdMismatchException`).
//! [`protocol::header::response_header_size`]
//! is Java `ResponseHeader.size` for a header version (this crate's
//! [`protocol::header::ResponseHeader`] stores only `correlationId`).
//! [`protocol::api_keys::name`] is
//! that enum name for an id. [`protocol::api_keys::has_id`] /
//! [`protocol::api_keys::for_id`] are Java `ApiKeys.hasId` / `forId`
//! (`Unexpected api key: {id}`). [`protocol::api_keys::cluster_action`] /
//! [`protocol::api_keys::forwardable`] /
//! [`protocol::api_keys::min_required_inter_broker_magic`] are Java
//! `ApiKeys.clusterAction` / `forwardable` / `minRequiredInterBrokerMagic`
//! (txn APIs are [`RecordBatch::MAGIC_VALUE_V2`]; others are
//! [`RecordBatch::MAGIC_VALUE_V0`]).
//! [`ShareRequestMetadata`] is Java `ShareRequestMetadata`
//! ([`ShareRequestMetadata::INITIAL_EPOCH`] / [`ShareRequestMetadata::FINAL_EPOCH`]
//! on ShareFetch / ShareAcknowledge).
//! OffsetForLeaderEpoch negotiates v0–v4 (v2 CurrentLeaderEpoch;
//! decode below v2 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`];
//! v3 ReplicaId; v4 flexible; Topics/Partitions of N). v5+ is not spoken.
//! [`protocol::epoch::supports_topic_permission`] is Java
//! `OffsetsForLeaderEpochRequest.supportsTopicPermission` (v3+ uses topic
//! Describe instead of Cluster permission).
//! [`protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH`] /
//! [`protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH_OFFSET`] are Java
//! `OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH` / `UNDEFINED_EPOCH_OFFSET`.
//! [`protocol::epoch::OffsetsForLeaderEpochResponse::error_counts`] is Java
//! `OffsetsForLeaderEpochResponse.errorCounts` (partition-level codes,
//! including `NONE`).
//! [`protocol::epoch::EpochEndOffset::error`] /
//! [`protocol::epoch::OffsetForLeaderTopic::error_result`] are Java
//! `OffsetsForLeaderEpochRequest.getErrorResponse` (partition body /
//! one topic; throttle stays JSON default `0`).
//! [`ConsumerConfig::max_bytes`] sets
//! both `fetch.max.bytes` and `max.partition.fetch.bytes`;
//! [`ConsumerConfig::fetch_max_bytes`] /
//! [`ConsumerConfig::max_partition_fetch_bytes`] set them independently.
//! [`Consumer::partitions_for`] /
//! [`Producer::partitions_for`] return Metadata (leader, replicas, ISR,
//! [`PartitionInfo::offline_replicas`], [`PartitionInfo::leader_epoch`];
//! unknown leader is [`protocol::api::MetadataResponse::NO_LEADER_ID`] /
//! [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]).
//! [`Consumer::wakeup`] interrupts fetch
//! (clone [`WakeupHandle`] for another task).
//! [`Consumer::client_instance_id`] is Java `clientInstanceId` (KIP-714;
//! returns [`Uuid`]).
//! [`Consumer::client_instance_id_timeout`] /
//! [`ConsumerGroup::client_instance_id_timeout`] /
//! [`ShareGroup::client_instance_id_timeout`] /
//! [`Admin::client_instance_id_timeout`] are Java `clientInstanceId(Duration)`.
//! [`Consumer::offsets_for_times`] is Java `offsetsForTimes`
//! ([`OffsetAndTimestamp::leader_epoch`] is Java `getLeaderEpoch`;
//! a negative timestamp is `The target time cannot be negative`).
//! [`FetchedRecord::leader_epoch`] is the record-batch partition leader epoch.
//! [`FetchedRecord::timestamp_type`] is Java `timestampType` ([`TimestampType`]).
//! [`FetchedRecord::last_header`] / [`FetchedRecord::headers_for_key`] are Java
//! `Headers.lastHeader` / `headers(String)`.
//! [`Header`] `Display` is Java `RecordHeader.toString`.
//! [`FetchedRecord`] / [`ShareRecord`] `Display` is Java `ConsumerRecord.toString`.
//! [`FetchedRecord::NO_TIMESTAMP`] / [`FetchedRecord::NULL_SIZE`] /
//! [`ShareRecord::NO_TIMESTAMP`] / [`ShareRecord::NULL_SIZE`] are Java
//! `ConsumerRecord.NO_TIMESTAMP` / `NULL_SIZE`.
//! [`ProduceRecord`] `Display` is Java `ProducerRecord.toString`.
//! [`Producer::send`] / [`Producer::try_send`] reject a negative partition
//! or timestamp with Java `ProducerRecord` constructor messages
//! (`Invalid partition` / `Invalid timestamp`), and reject an invalid topic
//! name with Java `Topic.validate` (`Topic name is invalid`).
//! [`ConsumerGroup::subscribe`] / [`ShareGroup::subscribe`] /
//! [`Consumer::assign`] / [`Consumer::assign_partitions`] /
//! [`Consumer::assign_topic`] use the same `Topic.validate` check.
//! [`OffsetAndMetadata`] / [`OffsetAndTimestamp`] / [`PartitionInfo`] `Display`
//! match Java `toString`. [`protocol::group::FetchedOffset::INVALID_OFFSET`] /
//! [`protocol::group::FetchedOffset::NO_METADATA`] /
//! [`protocol::group::FetchedOffset::has_error`] /
//! [`protocol::group::FetchedOffset::unknown_partition`] /
//! [`protocol::group::FetchedOffset::unauthorized_partition`] /
//! [`protocol::group::FetchedOffset::error`] /
//! [`protocol::group::OffsetFetchTopic::error_result`] /
//! [`protocol::group::OffsetFetchGroup::error_result`] /
//! [`protocol::group::OffsetFetchGroup::error_results`] /
//! [`protocol::group::OffsetFetchGroupResult::error`] are Java
//! `OffsetFetchResponse.INVALID_OFFSET` / `NO_METADATA` /
//! `PartitionData.hasError` / `UNKNOWN_PARTITION` / `UNAUTHORIZED_PARTITION` /
//! `OffsetFetchRequest.getErrorResponse` (partition body / one topic on
//! v1–v7; one group / Groups on v8+). v1 fills request partitions; v2–v7
//! omit partitions; v8+ copies GroupId with empty Topics.
//! (`FetchedOffset` `Display` is `PartitionData.toString`).
//! [`error::for_code`] / [`error::UNKNOWN_SERVER_ERROR`] are Java
//! `Errors.forCode` (Kafka 4.0.0 enum name; unknown is `UNKNOWN_SERVER_ERROR`) / code `-1`.
//! [`ApiError`] is Java `org.apache.kafka.common.requests.ApiError`
//! (`NONE` is code 0 and a null message; unknown codes become
//! [`error::UNKNOWN_SERVER_ERROR`]; `Display` is
//! `ApiError(error=NONE, message=null)`).
//! [`protocol::group::OffsetFetchResponse::should_client_throttle`]
//! is Java `OffsetFetchResponse.shouldClientThrottle` (v4+).
//! [`protocol::group::OffsetFetchResponse::error_counts`] is Java
//! `OffsetFetchResponse.errorCounts` (v8+ group-level plus partitions;
//! v2–v7 top-level plus partitions; v1 first non-partition error plus
//! partitions; including `NONE`).
//! [`protocol::group::OffsetFetchResponse::group_has_error`] /
//! [`protocol::group::OffsetFetchResponse::group_level_error`] /
//! [`protocol::group::OffsetFetchResponse::error`] are Java
//! `OffsetFetchResponse.groupHasError` / `groupLevelError` / `error`
//! (v8+ named group's `errorCode`; missing group is false / `None`;
//! v1–v7 ignore `group_id` and use the top-level code, including `NONE`;
//! `error` is always `None` on v8+ even when groups have errors);
//! [`protocol::group::OffsetFetchResponse::partition_data_map`] is Java
//! `OffsetFetchResponse.partitionDataMap` (v1–v7 ignore `group_id`; v8+
//! first matching group; missing group is [`Error::protocol`]; a later
//! partition overwrites).
//! [`protocol::group::OffsetFetchResponse::from_partition_data`] is Java
//! `OffsetFetchResponse` constructor from a partition map (group by name;
//! a later entry for the same topic appends; first-seen topic order;
//! duplicate pairs kept).
//! [`protocol::group::OffsetFetchResponse::from_groups_partition_data`] is Java
//! `OffsetFetchResponse` constructor from group errors and partition maps
//! (v8+; a group missing from `errors` is [`Error::protocol`]; a group
//! only in `errors` is omitted).
//! [`protocol::group::OffsetFetchResponse::from_groups`] is Java
//! `OffsetFetchResponse` constructor from a group list (v8+ as-is; below
//! v8 exactly one group; v1 rewrites partitions when the group has an
//! error).
//! [`OffsetAndMetadata::NO_METADATA`] /
//! [`OffsetAndMetadata::INVALID_OFFSET`] are the client-type copies (assign
//! uses that sentinel when OffsetFetch omits a partition, then
//! `auto.offset.reset`). [`OffsetAndTimestamp::UNKNOWN_OFFSET`] /
//! [`OffsetAndTimestamp::UNKNOWN_TIMESTAMP`] are Java
//! `ListOffsetsResponse.UNKNOWN_OFFSET` / `UNKNOWN_TIMESTAMP`.
//! [`TopicIdPartition`] `Display` is Java
//! `TopicIdPartition.toString`. [`TopicPartition::cluster_metadata`] is Java
//! `Topic.CLUSTER_METADATA_TOPIC_PARTITION`. [`TopicListing`] / [`TopicPartitionReplica`] /
//! [`ReplicaLogDirInfo`] `Display` match Java `toString`. [`Uuid::from_string`]
//! is Java `Uuid.fromString` (`Input string` length errors; invalid base64 is
//! crate-specific). [`Uuid::random_uuid`] is Java `Uuid.randomUuid`.
//! [`Uuid::ZERO_UUID`] / [`Uuid::ONE_UUID`] are Java
//! `ZERO_UUID` / `ONE_UUID`. [`Config`] / [`ConfigEntry`] / [`ConfigResource`] /
//! [`CreatedTopicConfig`] / [`ListedConfigResource`] `Display` match Java
//! `toString` (`ConfigEntry.toString` on [`CreatedTopicConfig`];
//! `ConfigResource.toString` on [`ListedConfigResource`]). [`AclBinding`] /
//! [`ResourcePattern`] / [`AccessControlEntry`] / [`AclBindingFilter`]
//! `Display` match Java `toString`. Java `ResourcePattern` constructor
//! rejects resource type ANY and pattern type ANY/MATCH; Java
//! `AccessControlEntry` constructor rejects operation/permission ANY
//! (checked at CreateAcls encode). Java `CreateAclsRequest.validate` /
//! `DescribeAclsRequest.normalizeAndValidate` /
//! `DeleteAclsRequest.normalizeAndValidate` reject UNKNOWN resource /
//! pattern / operation / permission (`CreatableAcls contain unknown
//! elements` / `DescribeAclsRequest contains UNKNOWN elements` /
//! `Filters contain UNKNOWN elements`). Java `DescribeAclsResponse.validate` /
//! `DeleteAclsResponse.validate` reject UNKNOWN on response resources /
//! MatchingAcls (`Contain UNKNOWN elements` /
//! `DeleteAclsMatchingAcls contain UNKNOWN elements`).
//! [`protocol::acl::DescribeAclsResponse::acls_resources`] /
//! [`protocol::acl::DescribeAclsResponse::acl_bindings`] are Java
//! `aclsResources` / `aclBindings` (group by [`ResourcePattern`]).
//! [`protocol::acl::DeleteAclsResponse::matching_acl`] /
//! [`protocol::acl::DeleteAclsResponse::acl_binding`] are Java
//! `matchingAcl` / `aclBinding` ([`protocol::acl::DeleteAclsMatchingAcl`];
//! unknown resource / pattern / operation / permission codes become
//! UNKNOWN). [`NewTopic`] / [`NewPartitions`] / [`ListedGroup`] `Display`
//! match Java `toString` (`GroupListing.toString` on [`ListedGroup`]).
//! [`ClientQuotaEntity`] / [`ClientQuotaFilter`] /
//! [`ClientQuotaFilterComponent`] / [`ClientQuotaAlteration`] `Display`
//! match Java `toString`. [`FeatureUpdate`] / [`UpgradeType`] /
//! [`RecordsToDelete`] / [`SupportedVersionRange`] /
//! [`FinalizedVersionRange`] / [`FeatureMetadata`] `Display` match Java
//! `toString`. [`UpgradeType::code`] / [`UpgradeType::from_code`] are Java
//! `FeatureUpdate.UpgradeType.code` / `fromCode` (Java `UNKNOWN` is `None`).
//! Java `FeatureUpdate` constructor rejects maxVersionLevel 0 with
//! `UpgradeType.UPGRADE` and a negative maxVersionLevel (checked at
//! UpdateFeatures encode). [`FeatureUpdate::is_delete_request`] is Java
//! `UpdateFeaturesRequest.FeatureUpdateItem.isDeleteRequest`.
//! [`protocol::admin::UpdateFeaturesRequest::get_feature`] /
//! [`protocol::admin::UpdateFeaturesRequest::feature_updates`] are Java
//! `getFeature` / `featureUpdates`. Java `SupportedVersionRange` /
//! `FinalizedVersionRange` constructors reject a negative min or max, or
//! max below min.
//! [`ScramMechanism`] / [`ScramCredentialInfo`] /
//! [`DescribeUserScramCredentialsResult`] `Display` match Java `toString`
//! (`UserScramCredentialsDescription.toString` on
//! [`DescribeUserScramCredentialsResult`]).
//! [`DescribeUserScramCredentialsResult::error`] /
//! [`DescribeUserScramCredentialsResult::error_results`] /
//! [`protocol::admin::DescribeUserScramCredentialsResponse::error`] are Java
//! `DescribeUserScramCredentialsRequest.getErrorResponse` (one result /
//! `nCopies` / top-level plus Results). Request user names are not
//! copied (`User` stays the JSON default, empty). `ErrorMessage` stays
//! the JSON default (null); official Java also sets the English
//! `Errors.message` string. Throttle is the JSON default (`0`).
//! [`protocol::admin::DescribeUserScramCredentialsResponse::error_counts`] /
//! [`protocol::admin::DescribeUserScramCredentialsResponse::should_client_throttle`]
//! are Java `DescribeUserScramCredentialsResponse.errorCounts` (per-user
//! codes, including `NONE`; the top-level `errorCode` is not counted) /
//! `shouldClientThrottle` (always).
//! [`ScramMechanism::id`] is Java
//! `ScramMechanism.type`. [`ActiveProducer`] `Display`
//! is Java `ProducerState.toString`. [`DescribeProducersPartition`]
//! `Display` is Java `PartitionProducerState.toString`.
//! [`DescribeProducersPartition::error`] /
//! [`protocol::admin::DescribeProducersTopicRequest::error_result`] are Java
//! `DescribeProducersRequest.getErrorResponse` (partition body / one topic).
//! [`protocol::admin::DescribeProducersResponse::error_counts`] is Java
//! `DescribeProducersResponse.errorCounts` (partition-level codes, including `NONE`).
//! [`OngoingReassignment`] `Display`
//! is Java `PartitionReassignment.toString`. [`TransactionListing`]
//! `Display` is Java `TransactionListing.toString`. [`AbortTransactionSpec`]
//! `Display` is Java `AbortTransactionSpec.toString`.
//! [`ConsumerGroupAssignment`] `Display` is Java `MemberAssignment.toString`.
//! [`ConsumerGroupMember`] `Display` is Java `MemberDescription.toString`.
//! [`ShareGroupAssignment`] `Display` is Java `ShareMemberAssignment.toString`.
//! [`ShareGroupMember`] `Display` is Java `ShareMemberDescription.toString`.
//! [`DescribeLogDirsPartition`] `Display` is Java `ReplicaInfo.toString`.
//! [`AlterConfigOpType`] / [`AlterConfig`] `Display` match Java
//! `AlterConfigOp.OpType.toString` / `AlterConfigOp.toString`.
//! [`ConfigResourceType::id`] / [`AlterConfigOpType::id`] are Java
//! `ConfigResource.Type.id` / `AlterConfigOp.OpType.id`.
//! [`IsolationLevel`] / [`Compression`] `Display` match Java
//! `IsolationLevel.toString` / `CompressionType.toString`.
//! [`IsolationLevel::id`] / [`IsolationLevel::from_id`] are Java
//! `IsolationLevel.id` / `forId`. [`SecurityProtocol::id`] /
//! [`SecurityProtocol::from_id`] / [`SecurityProtocol::from_name`] /
//! [`SecurityProtocol::names`] are Java `SecurityProtocol.id` / `forId` /
//! `forName` / `names` (unknown id is `None`; unknown name is
//! [`Error::protocol`]). [`SecurityProtocol`] `Display` is Java
//! `SecurityProtocol.toString` (`PLAINTEXT`). [`ListenerName::new`] /
//! [`ListenerName::for_security_protocol`] / [`ListenerName::normalised`] /
//! [`ListenerName::value`] / [`ListenerName::config_prefix`] /
//! [`ListenerName::sasl_mechanism_config_prefix`] /
//! [`ListenerName::sasl_mechanism_prefix`] are Java `ListenerName`
//! (`toUpperCase`; blank is [`Error::protocol`]). [`ListenerName`] `Display`
//! is Java `ListenerName.toString` (`ListenerName(PLAINTEXT)`).
//! [`Endpoint::new`] / [`Endpoint::listener_name`] /
//! [`Endpoint::security_protocol`] / [`Endpoint::host`] / [`Endpoint::port`]
//! are Java `Endpoint` (`None` is null; `listenerName` is
//! `Optional.ofNullable`). [`Endpoint`] `Display` is Java `Endpoint.toString`.
//! [`Compression::id`] /
//! [`Compression::from_id`] / [`Compression::from_name`] are Java
//! `CompressionType.id` / `forId` / `forName`
//! (zstd `4` is `None`; this crate does not speak zstd).
//! [`Compression::default_level`] / [`Compression::min_level`] /
//! [`Compression::max_level`] are Java `CompressionType.defaultLevel` /
//! `minLevel` / `maxLevel` (`gzip` / `lz4`; [`Error::Unsupported`] for
//! `none` / `snappy`).
//! [`TimestampType::id`] / [`TimestampType::from_name`] are Java
//! `TimestampType.id` / `forName`.
//! [`AcknowledgeType`] `Display` is Java `AcknowledgeType.toString`
//! (`accept`). [`AcknowledgeType::id`] / [`AcknowledgeType::from_id`] are
//! Java `AcknowledgeType.id` / `forId` (gap `0` is `None`).
//! [`ShareRequestMetadata`] `Display` is Java `ShareRequestMetadata.toString`
//! (`(memberId=..., epoch=INITIAL)`).
//! [`AutoOffsetReset`] `Display` is Java `OffsetResetStrategy.toString`.
//! [`Record`] `Display` is Java `DefaultRecord.toString`.
//! [`Record::size_of_body_in_bytes`] / [`Record::size_in_bytes`] are Java
//! `DefaultRecord.sizeOfBodyInBytes` / `sizeInBytes`.
//! [`protocol::buf::size_of_unsigned_varint`] / [`protocol::buf::size_of_varint`] /
//! [`protocol::buf::size_of_unsigned_varlong`] / [`protocol::buf::size_of_varlong`]
//! are Java `ByteUtils.sizeOfUnsignedVarint` / `sizeOfVarint` /
//! `sizeOfUnsignedVarlong` / `sizeOfVarlong` (unsigned helpers reinterpret
//! signed bits; `-1` is five bytes / ten bytes).
//! [`protocol::buf::utf8_length`] is Java `Utils.utf8Length` (UTF-8 byte
//! length; `DefaultRecord` header-key size).
//! [`protocol::buf::to_32_bit_field`] / [`protocol::buf::from_32_bit_field`]
//! are Java `Utils.to32BitField` / `from32BitField` (bits `0..=31`;
//! out of range is [`Error::protocol`]).
//! [`protocol::buf::is_blank`] / [`protocol::buf::replace_suffix`] are Java
//! `Utils.isBlank` / `replaceSuffix` (`None` is null; trim is code units at
//! or below U+0020; missing suffix is [`Error::protocol`]).
//! [`protocol::buf::entries_with_prefix`] /
//! [`protocol::buf::entries_with_prefix_matching`] are Java
//! `Utils.entriesWithPrefix` (two-argument form strips the prefix and omits
//! keys equal to it).
//! [`protocol::buf::parse_map`] / [`protocol::buf::mk_string`] are Java
//! `Utils.parseMap` / `mkString` (empty is an empty map; trailing empty
//! elements are discarded; later `=` stays in the value; duplicate keys
//! last-win; a missing `=` is [`Error::protocol`]; empty `mkString` is
//! begin then end).
//! [`protocol::buf::union`] / [`protocol::buf::intersection`] /
//! [`protocol::buf::diff`] are Java `Utils.union` / `intersection` / `diff`
//! (empty union is empty; intersection of first only is a copy; a later
//! disjoint set makes intersection empty; diff is left minus right).
//! [`protocol::buf::is_equal_constant_time`] is Java
//! `Utils.isEqualConstantTime` (`None` is null; both null is true; empty
//! `second` returns whether `first` is empty; otherwise every element of
//! `first` is compared and timing depends only on its length).
//! [`protocol::buf::require`] / [`protocol::buf::require_message`] are Java
//! `Utils.require` (failure is [`Error::protocol`]; the one-argument form
//! is `requirement failed`).
//! [`protocol::buf::min`] / [`protocol::buf::max`] / [`protocol::buf::min_i16`]
//! are Java `Utils.min(long, long...)` / `Utils.max(long, long...)` /
//! `Utils.min(short, short)` (empty rest returns first).
//! [`protocol::buf::deep_to_string`] is Java `MessageUtil.deepToString`
//! (comma-space inside square brackets; empty is `[]`).
//! [`protocol::buf::compare_raw_tagged_fields`] is Java
//! `MessageUtil.compareRawTaggedFields` (`None` is null; a null list equals
//! null or empty).
//! [`protocol::buf::read_unsigned_int`] / [`protocol::buf::write_unsigned_int`] /
//! [`protocol::buf::read_unsigned_int_at`] / [`protocol::buf::write_unsigned_int_at`] /
//! [`protocol::buf::read_int_be`] / [`protocol::buf::read_unsigned_int_le`] /
//! [`protocol::buf::write_unsigned_int_le`] are Java `ByteUtils.readUnsignedInt`
//! / `writeUnsignedInt` (sequential and indexed Buffer forms) / `readIntBE` /
//! `readUnsignedIntLE` / `writeUnsignedIntLE` (offset forms; short buffer is
//! [`Error::protocol`] `need 4 bytes`).
//! [`protocol::buf::read_bytes`] / [`protocol::buf::read_bytes_at`] are Java
//! `Utils.readBytes` (sequential `ByteBuffer` form: negative length is `None`;
//! offset form is absolute; short buffer is [`Error::protocol`] `need N bytes`).
//! [`protocol::buf::size_delimited`] is Java `Utils.sizeDelimited` (negative
//! size is `None`; short buffer is [`Error::protocol`] `need N bytes`).
//! [`RecordBatch::size_in_bytes`] encodes this batch (including compression).
//! [`RecordBatch::encoded_size_in_bytes`] is Java
//! `DefaultRecordBatch.sizeInBytes()` on a buffer (`LOG_OVERHEAD` plus the
//! length field; wrapping add; short size field is [`Error::protocol`]
//! `need 4 bytes`). [`RecordBatch::encoded_last_offset`] /
//! [`RecordBatch::encoded_next_offset`] are Java
//! `DefaultRecordBatch.lastOffset` / `nextOffset` on a buffer (`baseOffset`
//! plus `lastOffsetDelta`; wrapping add; short fields are
//! [`Error::protocol`] `need N bytes`). [`RecordBatch::encoded_last_sequence`]
//! is Java `DefaultRecordBatch.lastSequence` on a buffer (`NO_SEQUENCE` skips
//! the delta; otherwise `incrementSequence` of the stored base and
//! `lastOffsetDelta`). [`RecordBatch::encoded_delete_horizon_ms`] is Java
//! `DefaultRecordBatch.deleteHorizonMs` on a buffer (unset flag is `None`
//! without reading the base timestamp). [`RecordBatch::encoded_is_transactional`]
//! / [`RecordBatch::encoded_is_control_batch`] /
//! [`RecordBatch::encoded_timestamp_type`] are Java
//! `DefaultRecordBatch.isTransactional` / `isControlBatch` / `timestampType`
//! on a buffer (short attributes field is [`Error::protocol`] `need 2 bytes`).
//! [`RecordBatch::encoded_has_producer_id`] is Java
//! `AbstractRecordBatch.hasProducerId` on a buffer (producer id greater than
//! [`RecordBatch::NO_PRODUCER_ID`]; short field is [`Error::protocol`]
//! `need 8 bytes`).
//! [`RecordBatch::encoded_count_or_null`] is Java
//! `DefaultRecordBatch.countOrNull` on a buffer (header records count;
//! magic-v2 is always `Some`).
//! [`RecordBatch::set_last_offset`] is Java `DefaultRecordBatch.setLastOffset`
//! on a buffer (`baseOffset` is `lastOffset` minus `lastOffsetDelta`;
//! wrapping subtract; CRC is unchanged).
//! [`RecordBatch::size_in_bytes_of`] and
//! [`RecordBatch::size_in_bytes_from`] are the static helpers (empty is
//! `0`). [`RecordBatch::checksum`] is Java `DefaultRecordBatch.checksum`.
//! [`RecordBatch::is_valid`] is Java `DefaultRecordBatch.isValid` (declared
//! size below overhead is `false`; otherwise stored CRC32-C must match bytes
//! from [`RecordBatch::ATTRIBUTES_OFFSET`]; short size/CRC fields are
//! [`Error::protocol`] `need 4 bytes`).
//! [`RecordBatch::ensure_valid`] is Java `DefaultRecordBatch.ensureValid` on
//! a buffer (size below overhead is `Record batch is corrupt`; CRC of bytes
//! from [`RecordBatch::ATTRIBUTES_OFFSET`] to the slice end; not used by
//! [`protocol::records::decode_record_batch`], which CRC-checks the declared
//! body).
//! [`RecordBatch`] `Display` is Java `DefaultRecordBatch.toString`.
//! [`Record::record_size_upper_bound`] /
//! [`RecordBatch::estimate_batch_size_upper_bound`] /
//! [`protocol::records::Records::estimate_size_in_bytes_upper_bound`] are Java
//! `DefaultRecord.recordSizeUpperBound` /
//! `DefaultRecordBatch.estimateBatchSizeUpperBound` /
//! `AbstractRecords.estimateSizeInBytesUpperBound` (magic-v2). `send` /
//! `try_send` use that upper bound for [`Error::RecordTooLarge`]
//! (`max.request.size` first, then `buffer.memory`; Java
//! `KafkaProducer.ensureValidRecordSize`).
//! [`protocol::records::Records::estimate_size_in_bytes`] /
//! [`protocol::records::Records::estimate_size_in_bytes_from`] /
//! [`protocol::records::Records::record_batch_header_size_in_bytes`] are Java
//! `AbstractRecords.estimateSizeInBytes` / `recordBatchHeaderSizeInBytes`
//! (magic-v2; compressed estimate is `max(size / 2, 1024)` capped at 65536).
//! [`protocol::records::Records::has_matching_magic`] /
//! [`protocol::records::Records::first_batch`] /
//! [`protocol::records::Records::last_batch`] are Java
//! `AbstractRecords.hasMatchingMagic` / `firstBatch` / `lastBatch` (empty
//! matching-magic is true; empty first/last is `None`).
//! [`protocol::records::Records::first_batch_size`] /
//! [`protocol::records::Records::valid_bytes`] are Java
//! `MemoryRecords.firstBatchSize` / `validBytes` (short header is `None`;
//! undersized or invalid magic is [`Error::protocol`]; `validBytes` sums
//! complete batches and ignores a truncated tail).
//! Magic-v2 record decode matches Java `DefaultRecord.readFrom`
//! `InvalidRecordException` messages (negative header count, header count
//! larger than remaining bytes, negative header key size, declared body
//! larger than remaining, leftover payload bytes after headers).
//! Batch decode matches Java `DefaultRecordBatch.RecordIterator`
//! (`Found invalid record count` / leftover records after the declared
//! count / premature EOF). A declared count of zero does not scan leftover
//! record bytes (Java `iterator()` returns empty).
//! [`protocol::records::decode_record_batch`] matches Java
//! `DefaultRecordBatch.ensureValid` (`Record batch is corrupt` size
//! overhead / `Record is corrupt` stored vs computed CRC).
//! [`Record::EMPTY_HEADERS`] is Java `Record.EMPTY_HEADERS`.
//! [`Record::has_magic`] / [`Record::is_compressed`] /
//! [`Record::has_timestamp_type`] match Java `Record.hasMagic` /
//! `isCompressed` / `hasTimestampType` (magic-v2: `hasMagic` is true when
//! magic is 2 or greater; the other two are always false).
//! [`RecordBatch::count_or_null`] is Java `RecordBatch.countOrNull`.
//! [`RecordBatch::encoded_count_or_null`] is the buffer form.
//! [`RecordBatch::has_producer_id`] is Java `AbstractRecordBatch.hasProducerId`
//! (`NO_PRODUCER_ID < producerId`).
//! [`RecordBatch::encoded_has_producer_id`] is the buffer form.
//! Fetch LastFetchedEpoch resets,
//! [`Consumer::seek`], and omitted last-fetched epoch use
//! [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
//! [`RecordBatch::is_transactional`] / [`RecordBatch::is_control_batch`] are
//! Java `DefaultRecordBatch.isTransactional` / `isControlBatch`.
//! [`RecordBatch::encoded_is_transactional`] /
//! [`RecordBatch::encoded_is_control_batch`] /
//! [`RecordBatch::encoded_timestamp_type`] are the buffer forms.
//! [`ControlRecordType`] / [`EndTransactionMarker`] are Java
//! `ControlRecordType` / `EndTransactionMarker` (`type` / `fromTypeId` /
//! `parse`; COMMIT/ABORT marker key and value).
//! [`RecordBatch::with_end_transaction_marker`] is Java
//! `MemoryRecords.withEndTransactionMarker`.
//! [`protocol::records::Records::LOG_OVERHEAD`] is Java `Records.LOG_OVERHEAD`
//! (offset + size prefix).
//! [`RecordBatch::last_offset`] / [`RecordBatch::next_offset`] /
//! [`RecordBatch::last_sequence`] use record count (`count - 1`).
//! [`RecordBatch::encoded_last_offset`] / [`RecordBatch::encoded_next_offset`]
//! / [`RecordBatch::encoded_last_sequence`] are Java
//! `DefaultRecordBatch.lastOffset` / `nextOffset` / `lastSequence` on a buffer.
//! [`RecordBatch::set_last_offset`] is Java `DefaultRecordBatch.setLastOffset`
//! on a buffer.
//! [`RecordBatch::is_compressed`] is Java `isCompressed`.
//! [`RecordBatch::offset_of_max_timestamp`] /
//! [`RecordBatch::delete_horizon_ms`] are Java `offsetOfMaxTimestamp` /
//! `deleteHorizonMs`. [`RecordBatch::encoded_delete_horizon_ms`] is the
//! buffer form of `deleteHorizonMs`.
//! [`FetchedRecord::serialized_key_size`] / [`FetchedRecord::serialized_value_size`]
//! match Java `serializedKeySize` / `serializedValueSize`.
//! [`Admin::create_partitions`] takes [`NewPartitions`].
//! [`NewPartitions::with_assignments`] is Java
//! `NewPartitions.increaseTo(int, List<List<Integer>>)` (null Assignments
//! means the broker assigns replicas).
//! [`NewTopic::with_assignments`] is Java
//! `NewTopic(String, Map<Integer, List<Integer>>)` (NumPartitions /
//! ReplicationFactor [`protocol::admin::CreateTopicsRequest::NO_NUM_PARTITIONS`] /
//! [`protocol::admin::CreateTopicsRequest::NO_REPLICATION_FACTOR`]; empty
//! Assignments is `NewTopic(String, int, short)`).
//! [`NewTopic::broker_defaults`] is Java
//! `NewTopic(String, Optional.empty(), Optional.empty())` (KIP-464;
//! [`protocol::admin::CreateTopicsRequest::NO_NUM_PARTITIONS`] /
//! [`protocol::admin::CreateTopicsRequest::NO_REPLICATION_FACTOR`]).
//! [`NewTopic::configs`] is Java `NewTopic.configs(Map)`.
//! [`Admin::create_topics_timeout`] is Java `CreateTopicsOptions.timeoutMs`.
//! [`Admin::create_topics_with_quota_retry`] is Java
//! `CreateTopicsOptions.retryOnQuotaViolation` (default `true`; KIP-599).
//! [`Admin::delete_topics_timeout`] is Java `DeleteTopicsOptions.timeoutMs`.
//! [`Admin::delete_topics_with_quota_retry`] is Java
//! `DeleteTopicsOptions.retryOnQuotaViolation` (default `true`; KIP-599).
//! [`Admin::delete_topics_by_id`] is Java `deleteTopics(TopicCollection.ofTopicIds)`
//! (DeleteTopics v6 null Name + TopicId).
//! [`Admin::delete_topics_for`] is Java `deleteTopics(TopicCollection)`
//! ([`TopicCollection::of_topic_names`] / [`TopicCollection::of_topic_ids`]).
//! [`Admin::delete_topics_by_id_with_quota_retry`] is Java
//! `DeleteTopicsOptions.retryOnQuotaViolation` on TopicId deletes.
//! [`Admin::create_partitions_timeout`] is Java `CreatePartitionsOptions.timeoutMs`.
//! [`Admin::create_partitions_with_quota_retry`] is Java
//! `CreatePartitionsOptions.retryOnQuotaViolation` (default `true`; KIP-599).
//! [`Admin::alter_partition_reassignments_timeout`] is Java
//! `AlterPartitionReassignmentsOptions.timeoutMs`.
//! [`Admin::alter_partition_reassignments_for`] is Java
//! `alterPartitionReassignments(Map)` ([`NewPartitionReassignment`];
//! `None` cancels).
//! [`protocol::admin::ReassignablePartition::error_result`] /
//! [`protocol::admin::ReassignableTopic::error_result`] /
//! [`protocol::admin::AlterPartitionReassignmentsResponse::error`] are Java
//! `AlterPartitionReassignmentsRequest.getErrorResponse` (one partition /
//! one topic / the Responses list). Nested bodies copy `PartitionIndex`
//! and `ErrorCode`; top-level and per-partition `ErrorMessage` stay the
//! JSON default (null); official Java also sets the English
//! `Errors.message` string.
//! [`protocol::admin::AlterPartitionReassignmentsResponse::error_counts`] is Java
//! `AlterPartitionReassignmentsResponse.errorCounts` (top-level `errorCode`
//! plus each partition-level code, including `NONE`).
//! [`protocol::admin::AlterPartitionReassignmentsResponse::should_client_throttle`]
//! is Java `AlterPartitionReassignmentsResponse.shouldClientThrottle`
//! (always).
//! [`Admin::list_partition_reassignments_timeout`] is Java
//! `ListPartitionReassignmentsOptions.timeoutMs`.
//! [`Admin::list_partition_reassignments_all`] is Java
//! `listPartitionReassignments()`.
//! [`Admin::list_partition_reassignments_for`] is Java
//! `listPartitionReassignments(Set)`.
//! [`protocol::admin::ListReassignmentTopic::error_result`] /
//! [`protocol::admin::ListPartitionReassignmentsResponse::error`] are Java
//! `ListPartitionReassignmentsRequest.getErrorResponse` (one topic /
//! the Topics list; null request Topics is empty). Nested partitions copy
//! `PartitionIndex`; replica lists stay JSON default empty. Top-level
//! `ErrorMessage` stays the JSON default (null); official Java also
//! sets the English `Errors.message` string.
//! [`protocol::admin::ListPartitionReassignmentsResponse::should_client_throttle`]
//! is Java `ListPartitionReassignmentsResponse.shouldClientThrottle`
//! (always).
//! [`Admin::incremental_alter_configs`] / [`Admin::alter_configs`] take
//! [`ConfigResource`] / [`ConfigResourceType`].
//! [`Admin::incremental_alter_configs_for`] is Java
//! `incrementalAlterConfigs(Map)` ([`ConfigResourceUpdate`]; Resources of N).
//! [`protocol::admin::AlterableResource::error_result`] /
//! [`AlterConfigsResourceResult::error`] /
//! [`AlterConfigsResourceResult::error_results`] are Java
//! `IncrementalAlterConfigsRequest.getErrorResponse` (one resource /
//! Responses). `ErrorMessage` stays the JSON default (null); official
//! Java also sets the English `Errors.message` string. Official Java
//! does not set `ThrottleTimeMs` (JSON default `0`).
//! [`protocol::admin::IncrementalAlterConfigsResponse::from_errors`] is Java
//! `IncrementalAlterConfigsResponse` constructed from a result map (type
//! id + name plus [`ApiError`] into Responses; `ErrorMessage` is copied;
//! throttle unused).
//! [`protocol::admin::IncrementalAlterConfigsRequest::from_configs`] is Java
//! `IncrementalAlterConfigsRequest.Builder` from a resource list and
//! configs map (missing `Map.get` is [`Error::protocol`]; mapKey first
//! stays; extra map entries omitted).
//! [`AlterConfig::append`] / [`AlterConfig::subtract`] are Java
//! `AlterConfigOp.OpType.APPEND` / `SUBTRACT` (LIST configs).
//! [`AlterConfig::from_entry`] is Java `AlterConfigOp(ConfigEntry, OpType)`
//! ([`AlterConfigOpType`]). [`AlterConfig::op_type`] is Java
//! `AlterConfigOp.opType()`.
//! [`Admin::alter_configs_for`] is Java `alterConfigs(Map)`
//! ([`ConfigReplacement`]; Resources of N).
//! [`protocol::admin::AlterConfigsResource::error_result`] /
//! [`protocol::admin::AlterConfigsResource::error_results`] are Java
//! `AlterConfigsRequest.getErrorResponse` (one resource / Responses).
//! `ErrorMessage` stays the JSON default (null); official Java also
//! sets the English `Errors.message` string.
//! [`protocol::admin::AlterConfigsRequest::configs`] is Java
//! `AlterConfigsRequest.configs` (ConfigResource to [`Config`]; unknown
//! resource types are UNKNOWN; each value is [`ConfigEntry::new`]).
//! [`protocol::admin::AlterConfigsRequest::from_configs`] is Java
//! `AlterConfigsRequest.Builder` from a configs map (null Value is
//! [`Error::protocol`]; mapKey first stays).
//! [`Admin::alter_configs_with`] is Java `alterConfigs(Map)` with a
//! [`Config`] value. [`DescribeConfigsResult::config`] is the Java
//! `describeConfigs` result `Config` (`entries` / `get`).
//! [`protocol::admin::DescribeConfigsResource::error_result`] /
//! [`DescribeConfigsResult::error`] /
//! [`DescribeConfigsResult::error_results`] are Java
//! `DescribeConfigsRequest.getErrorResponse` (one resource / Results).
//! Configs stay JSON default empty. `ErrorMessage` stays the JSON
//! default (null); official Java also sets the English `Errors.message`
//! string.
//! [`protocol::admin::DescribeConfigsRequest::error_response`] is Java
//! `DescribeConfigsRequest.getErrorResponse` (copies names / types;
//! `ErrorMessage` stays JSON-null; always writes the `throttleTimeMs`
//! argument).
//! [`ConfigEntry::source`] / [`ConfigEntry::config_type`] /
//! [`ConfigEntry::is_default`] / [`CreatedTopicConfig::is_default`] are Java
//! `ConfigEntry.source` / `type` / `isDefault` ([`ConfigSource`] /
//! [`ConfigType`]). [`Config`] / [`ConfigEntry`] / [`ConfigResource`] /
//! [`CreatedTopicConfig`] / [`ListedConfigResource`] `Display` match Java
//! `toString` ([`ConfigEntry`] redacts sensitive values).
//! [`ConfigResource::is_default`] / [`ListedConfigResource::is_default`]
//! are Java `ConfigResource.isDefault`.
//! [`Admin::incremental_alter_configs_timeout`] /
//! [`Admin::alter_configs_timeout`] are Java `AlterConfigsOptions.timeoutMs`
//! (RPC deadline; these RPCs have no TimeoutMs).
//! [`Consumer::current_lag`] is Java `currentLag` (an unassigned partition is
//! `No current assignment for partition`).
//! [`Consumer::list_topics`] is cluster Metadata. [`Consumer::assign_many`]
//! / [`Consumer::assign_partitions`] / [`Consumer::unassign`] replace or
//! drop a manual assignment ([`Consumer::assign_partitions`] is Java
//! `assign(Collection)` and uses [`ConsumerConfig::auto_offset_reset`];
//! topic names use [`protocol::group::Topic::validate`]).
//! [`Consumer::beginning_offsets`] / [`Consumer::end_offsets`] take
//! [`TopicPartition`]. [`Consumer::list_offset`] is ListOffsets for one
//! partition. [`Consumer::assignment`] is Java `assignment`
//! ([`Consumer::assigned_partitions`] is the same list; [`Consumer::positions`]
//! pairs each partition with its next fetch offset).
//! [`Consumer::fetch`] / [`ConsumerGroup::poll`] return [`ConsumerRecords`]
//! (Java `empty` / `isEmpty` / `count` / `partitions` / `records` /
//! `nextOffsets`; metadata is [`OffsetAndMetadata::NO_METADATA`]).
//! [`ShareGroup::poll`] returns [`ShareRecords`] (Java `empty` / `isEmpty` /
//! `count` / `partitions` / `records` / `nextOffsets`; metadata is
//! [`OffsetAndMetadata::NO_METADATA`]).
//! [`Consumer::fetch_timeout`] /
//! [`ConsumerGroup::poll_timeout`] / [`ShareGroup::poll_timeout`] are Java
//! `poll(Duration)`. [`ConsumerGroup::committed_timeout`] is Java
//! `committed(Duration)`. [`ConsumerGroup::commit_timeout`] is Java
//! `commitSync(Duration)`. [`Consumer::partitions_for_timeout`] /
//! [`Producer::partitions_for_timeout`] /
//! [`Consumer::list_topics_timeout`] / [`Consumer::beginning_offsets_timeout`] /
//! [`Consumer::end_offsets_timeout`] / [`Consumer::offsets_for_times_timeout`]
//! are Java `partitionsFor` / `listTopics` / `beginningOffsets` / `endOffsets` /
//! `offsetsForTimes` with a `Duration`.
//! [`ConsumerGroup::commit_offsets`] takes [`TopicPartition`] (or anything
//! that converts to one) plus the next fetch offset.
//! [`ConsumerGroup::commit_with_metadata`] takes
//! [`ConsumerRecords::next_offsets`] (Java `commitSync(records.nextOffsets())`).
//! [`Admin::delete_records`] / [`Admin::describe_producers`] /
//! [`Admin::describe_producers_for`] /
//! [`Admin::describe_producers_timeout`] /
//! [`Admin::list_offsets`] / [`Admin::delete_offsets`] /
//! [`Admin::delete_consumer_group_offsets`] /
//! [`Admin::list_consumer_group_offsets`] /
//! [`Admin::alter_consumer_group_offsets`] take [`TopicPartition`].
//! [`Admin::list_all_consumer_group_offsets`] is Java
//! `listConsumerGroupOffsets(groupId)` (OffsetFetch null Topics).
//! [`Admin::list_all_consumer_group_offsets_timeout`] is Java
//! `ListConsumerGroupOffsetsOptions.timeoutMs` (RPC deadline; OffsetFetch
//! has no TimeoutMs).
//! [`Admin::list_consumer_group_offsets_with`] /
//! [`Admin::list_all_consumer_group_offsets_with`] are Java
//! `ListConsumerGroupOffsetsOptions.requireStable` and `timeoutMs`.
//! [`Admin::list_consumer_group_offsets_for_groups`] /
//! [`Admin::list_consumer_group_offsets_for_groups_timeout`] /
//! [`Admin::list_consumer_group_offsets_for_groups_with`] are Java
//! `listConsumerGroupOffsets(Map)` ([`ListConsumerGroupOffsetsSpec`];
//! OffsetFetch v8+ Groups array of N, KIP-709; FindCoordinator v4+
//! CoordinatorKeys array of N, KIP-699).
//! [`ListConsumerGroupOffsetsSpec`] `Display` is Java
//! `ListConsumerGroupOffsetsSpec.toString` (`topicPartitions=null` when
//! [`ListConsumerGroupOffsetsSpec::all`]).
//! [`Admin::delete_records_for`] is Java `deleteRecords(Map)`
//! ([`RecordsToDelete`] / [`DeletedRecords`]; one DeleteRecords RPC per leader;
//! [`protocol::admin::DeleteRecordsRequest::HIGH_WATERMARK`] truncates to the
//! high watermark; [`DeletedRecords::INVALID_LOW_WATERMARK`] is Java
//! `DeleteRecordsResponse.INVALID_LOW_WATERMARK`).
//! [`Admin::delete_records_timeout`] /
//! [`Admin::delete_records_for_timeout`] are Java
//! `DeleteRecordsOptions.timeoutMs` (RPC deadline and TimeoutMs).
//! [`Admin::describe_producers_for`] is Java `describeProducers(Collection)`
//! (one DescribeProducers RPC per leader; Topics of N).
//! [`Admin::describe_producers_for_on_broker`] is Java
//! `DescribeProducersOptions.brokerId`.
//! [`Admin::describe_producers_timeout`] /
//! [`Admin::describe_producers_for_timeout`] are Java
//! `DescribeProducersOptions.timeoutMs` (RPC deadline; DescribeProducers
//! has no TimeoutMs).
//! [`ActiveProducer`] getters match Java `ProducerState`
//! (`coordinatorEpoch` / `currentTransactionStartOffset` are `None` when
//! the wire value is negative).
//! [`Admin::list_offsets`] is Java `listOffsets` ([`OffsetAndTimestamp`] /
//! [`OffsetSpec`]; one RPC per leader; ListOffsets v1–v10;
//! decode below v4 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]).
//! [`protocol::offsets::ListOffsetsPartition`] getters / `Display` match
//! Java `ListOffsetsResult.ListOffsetsResultInfo` (`leaderEpoch` is
//! `Optional.empty` when the wire value is `-1`).
//! [`protocol::offsets::ListOffsetsPartition::UNKNOWN_OFFSET`] /
//! [`protocol::offsets::ListOffsetsPartition::UNKNOWN_TIMESTAMP`] /
//! [`protocol::offsets::ListOffsetsPartition::UNKNOWN_EPOCH`] are Java
//! `ListOffsetsResponse.UNKNOWN_OFFSET` / `UNKNOWN_TIMESTAMP` / `UNKNOWN_EPOCH`.
//! [`protocol::offsets::ListOffsetsResponse::should_client_throttle`] is Java
//! `ListOffsetsResponse.shouldClientThrottle` (v3+).
//! [`protocol::offsets::ListOffsetsResponse::error_counts`] is Java
//! `ListOffsetsResponse.errorCounts` (partition-level codes, including `NONE`).
//! [`protocol::offsets::ListOffsetsResponse::singleton_list_offsets_topic_response`]
//! is Java `ListOffsetsResponse.singletonListOffsetsTopicResponse`.
//! [`protocol::offsets::ListOffsetsResponsePartition::error`] /
//! [`protocol::offsets::ListOffsetsTopicRequest::error_result`] are Java
//! `ListOffsetsRequest.getErrorResponse` (partition body / one topic);
//! [`protocol::offsets::ListOffsetsRequest::error_response`] is Java
//! `ListOffsetsRequest.getErrorResponse` (copies names and partition
//! indexes with `UNKNOWN_OFFSET` / `UNKNOWN_TIMESTAMP`; v2+ writes the
//! `throttleTimeMs` argument; below v2 omits it);
//! v2+ round-trips ThrottleTimeMs; below v2 encode omits it even when
//! the body has a non-zero value and decode fills `0`;
//! [`protocol::offsets::encode_list_offsets_topics_response`] still writes `0`;
//! [`protocol::offsets::ListOffsetsRequest::duplicate_partitions`] is Java
//! `ListOffsetsRequest.duplicatePartitions` (`(topic, partition)` pairs
//! that appear more than once);
//! [`protocol::offsets::ListOffsetsRequest::to_list_offsets_topics`] is Java
//! `ListOffsetsRequest.toListOffsetsTopics` (group by name; a later
//! entry for the same topic appends; first-seen topic order);
//! [`protocol::offsets::ListOffsetsRequest::for_consumer`] is Java
//! `ListOffsetsRequest.Builder.forConsumer` (else-if first match:
//! tiered v9, earliest-local v8, max-timestamp v7, `READ_COMMITTED`
//! v2, timestamp v1; all false is `0`).
//! [`Admin::list_offsets_with_isolation`] is Java `listOffsets` plus
//! `ListOffsetsOptions.isolationLevel`.
//! [`Admin::list_offsets_timeout`] / [`Admin::list_offsets_with_isolation_timeout`]
//! are Java `ListOffsetsOptions.timeoutMs` (RPC deadline and ListOffsets v10 TimeoutMs).
//! [`Admin::list_transactions_with_duration`] is Java `listTransactions`
//! plus `ListTransactionsOptions.filterOnDuration` (ListTransactions v1;
//! v0 with a non-negative DurationFilter is Java
//! `UnsupportedVersionException`).
//! [`Admin::list_transactions_timeout`] /
//! [`Admin::list_transactions_with_duration_timeout`] are Java
//! `ListTransactionsOptions.timeoutMs` (RPC deadline; ListTransactions
//! has no TimeoutMs).
//! [`Admin::list_transactions_all`] is Java `listTransactions()`.
//! [`TransactionListing::state`] is Java `TransactionListing.state` as
//! the broker string. [`TransactionState::state`] is Java
//! `TransactionDescription.state`; [`TransactionState::transaction_start_time_ms`]
//! is Java `OptionalLong` (`None` when the wire value is negative).
//! [`TransactionState::error`] / [`TransactionState::error_results`] are Java
//! `DescribeTransactionsRequest.getErrorResponse` (one transactional.id /
//! the `TransactionStates` list).
//! [`protocol::admin::DescribeTransactionsResponse::error_counts`] is Java
//! `DescribeTransactionsResponse.errorCounts` (per-transactional-id codes, including `NONE`).
//! [`Admin::describe_transactions_timeout`] is Java
//! `DescribeTransactionsOptions.timeoutMs` (RPC deadline;
//! DescribeTransactions has no TimeoutMs).
//! [`Admin::describe_configs_with_documentation`] is Java `describeConfigs`
//! plus `DescribeConfigsOptions.includeDocumentation` (DescribeConfigs v3).
//! [`Admin::describe_configs_timeout`] /
//! [`Admin::describe_configs_with_documentation_timeout`] are Java
//! `DescribeConfigsOptions.timeoutMs` (RPC deadline; DescribeConfigs has
//! no TimeoutMs).
//! [`Admin::describe_cluster_with`] is Java `describeCluster` plus
//! `DescribeClusterOptions` (DescribeCluster v0–v2; v1 EndpointType, v2
//! IncludeFencedBrokers). [`EndpointType::id`] / [`EndpointType::from_id`]
//! are Java `EndpointType.id` / `fromId` (Java `UNKNOWN` is `None`).
//! [`EndpointType`] `Display` is Java `EndpointType.toString` (`BROKER`).
//! [`Admin::describe_cluster_timeout`] / [`Admin::describe_cluster_with_timeout`]
//! are Java `DescribeClusterOptions.timeoutMs` (RPC deadline).
//! [`ClusterDescription::nodes`] / [`ClusterDescription::controller`] are Java
//! `DescribeClusterResult.nodes` / `controller` ([`Node`] is Java
//! `org.apache.kafka.common.Node`, an alias of [`DescribeClusterBroker`]).
//! [`protocol::admin::DescribeClusterResponse::nodes`] is Java
//! `DescribeClusterResponse.nodes` (duplicate broker id is [`Error::protocol`]).
//! [`ClusterDescription::cluster_resource`] is Java `ClusterResource` from the
//! DescribeCluster cluster id ([`ClusterResource`] `Display` is
//! `ClusterResource.toString`; missing id prints `null`).
//! [`Node::id_string`] / [`Node::is_empty`] / [`Node::no_node`] are Java
//! `Node.idString` / `isEmpty` / `noNode`. Metadata `Broker` and
//! Produce/Fetch `NodeEndpoint` have the same getters and convert with `From`.
//! [`Admin::update_features_with`] is Java `updateFeatures` plus
//! `UpdateFeaturesOptions.validateOnly` (UpdateFeatures v0–v2; v1
//! UpgradeType / ValidateOnly; v2 omits Results; Java `FeatureUpdate`
//! constructor rejects maxVersionLevel 0 with `UpgradeType.UPGRADE` and a
//! negative maxVersionLevel; `Admin::update_features` rejects an empty
//! list and a blank feature name).
//! [`protocol::admin::UpdateFeaturesRequest::get_feature`] /
//! [`protocol::admin::UpdateFeaturesRequest::feature_updates`] are Java
//! `UpdateFeaturesRequest.getFeature` / `featureUpdates` (v0 is
//! `AllowDowngrade` → `SAFE_DOWNGRADE` / `UPGRADE`; v1+ is
//! `UpgradeType.fromCode`, unknown codes become `0`; missing name is
//! [`Error::protocol`]; duplicate names all become the first match;
//! encode still writes FeatureUpdates as-is).
//! [`protocol::admin::UpdateFeaturesResponse::create_with_errors`] /
//! [`protocol::admin::UpdateFeaturesResponse::error`] /
//! [`protocol::admin::UpdatableFeatureResult::error`] are Java
//! `UpdateFeaturesResponse.createWithErrors` /
//! `UpdateFeaturesRequest.getErrorResponse`. Results are filled only when
//! the top-level error is NONE; otherwise Results stay empty (Java
//! getErrorResponse passes `Collections.emptySet`). `ErrorMessage` stays
//! the JSON default (null); official Java also sets the English
//! `Errors.message` string. Throttle is the JSON default (`0`). v2 omits
//! Results on the wire.
//! [`protocol::admin::UpdateFeaturesResponse::error_counts`] is Java
//! `UpdateFeaturesResponse.errorCounts` (top-level `errorCode` plus each
//! per-feature code, including `NONE`).
//! [`protocol::admin::UpdateFeaturesResponse::top_level_error`] is Java
//! `UpdateFeaturesResponse.topLevelError`.
//! [`Admin::update_features_timeout`] / [`Admin::update_features_with_timeout`]
//! are Java `UpdateFeaturesOptions.timeoutMs` (RPC deadline and TimeoutMs).
//! [`Admin::fence_producers`] is Java `fenceProducers` ([`FencedProducer`]).
//! [`Admin::fence_producers_timeout`] is Java `fenceProducers` plus
//! `FenceProducersOptions.timeoutMs`.
//! [`Admin::force_terminate_transaction`] is Java `forceTerminateTransaction`.
//! [`Admin::force_terminate_transaction_timeout`] is the same plus timeout.
//! [`Admin::describe_classic_groups`] is Java `describeClassicGroups`.
//! [`Admin::describe_consumer_groups`] is Java `describeConsumerGroups`
//! ([`ConsumerGroupDescription`]; ConsumerGroupDescribe first, then
//! DescribeGroups). [`ConsumerGroupDescription`] getters match Java
//! (`partitionAssignor`, `type`, `groupEpoch` / `targetAssignmentEpoch`
//! empty for CLASSIC). [`ConsumerGroupMember`] / [`DescribedGroupMember`]
//! getters match Java `MemberDescription`. [`DescribeLogDirsResult`]
//! getters match Java `LogDirDescription` (`totalBytes` / `usableBytes`
//! are `None` when [`UNKNOWN_VOLUME_BYTES`]). [`INVALID_OFFSET_LAG`] is Java
//! `DescribeLogDirsResponse.INVALID_OFFSET_LAG`. [`DescribeLogDirsPartition`]
//! getters match Java `ReplicaInfo`.
//! [`Admin::describe_classic_groups_timeout`] /
//! [`Admin::describe_consumer_groups_timeout`] /
//! [`Admin::describe_groups_timeout`] are Java
//! `DescribeClassicGroupsOptions` / `DescribeConsumerGroupsOptions.timeoutMs`
//! (RPC deadline; neither RPC has TimeoutMs).
//! [`Admin::list_consumer_groups`] is Java `listConsumerGroups`.
//! [`Admin::list_groups_all`] / [`Admin::list_consumer_groups_all`] are Java
//! `listGroups()` / `listConsumerGroups()`.
//! [`Admin::list_groups_with`] / [`Admin::list_consumer_groups_with`] are Java
//! `listGroups` / `listConsumerGroups` plus `ListGroupsOptions.inGroupStates`
//! / `withTypes` ([`GroupState`] / [`GroupType`]).
//! [`Admin::list_groups_timeout`] / [`Admin::list_consumer_groups_timeout`]
//! are Java `ListGroupsOptions` / `ListConsumerGroupsOptions.timeoutMs`
//! (RPC deadline; ListGroups has no TimeoutMs).
//! [`Admin::delete_consumer_groups`] is Java `deleteConsumerGroups`.
//! [`Admin::delete_groups_timeout`] / [`Admin::delete_consumer_groups_timeout`] /
//! [`Admin::delete_share_groups_timeout`] are Java
//! `DeleteConsumerGroupsOptions` / `DeleteShareGroupsOptions.timeoutMs`
//! (RPC deadline; DeleteGroups has no TimeoutMs).
//! [`Admin::describe_share_groups`] is Java `describeShareGroups`
//! (ShareGroupDescribe v0–v1; FindCoordinator v4+ CoordinatorKeys of N).
//! [`ShareGroupMember`] getters match Java `ShareMemberDescription`.
//! [`DescribedShareGroup`] getters match Java `ShareGroupDescription`
//! (without `coordinator`). [`ConfigEntry`] `Debug` redacts sensitive
//! values (Java `ConfigEntry.toString`).
//! [`Admin::share_group_describe_timeout`] /
//! [`Admin::describe_share_groups_timeout`] are Java
//! `DescribeShareGroupsOptions.timeoutMs` (RPC deadline;
//! ShareGroupDescribe has no TimeoutMs).
//! [`Admin::consumer_group_describe_timeout`] is the crate-first
//! ConsumerGroupDescribe (api 69) RPC deadline. Java
//! `describeConsumerGroups` is [`Admin::describe_consumer_groups_timeout`]
//! (api 69 first, then DescribeGroups).
//! [`Admin::list_client_metrics_resources`] is Java `listClientMetricsResources`.
//! [`Admin::list_config_resources_all`] is Java `listConfigResources()`.
//! [`Admin::list_config_resources_timeout`] /
//! [`Admin::list_client_metrics_resources_timeout`] are Java
//! `ListConfigResourcesOptions` / `ListClientMetricsResourcesOptions.timeoutMs`
//! (RPC deadline; ListConfigResources has no TimeoutMs).
//! [`Admin::list_share_group_offsets`] is Java `listShareGroupOffsets`
//! (DescribeShareGroupOffsets; FindCoordinator v4+ CoordinatorKeys of N).
//! [`DescribeShareGroupOffsetsGroup::all`] is official nullable Topics
//! (`None` lists every topic-partition). Share-offset result getters
//! cover Describe/Alter/Delete ShareGroupOffsets v0.
//! [`Admin::describe_share_group_offsets_timeout`] /
//! [`Admin::list_share_group_offsets_timeout`] are Java
//! `ListShareGroupOffsetsOptions.timeoutMs` (RPC deadline;
//! DescribeShareGroupOffsets has no TimeoutMs).
//! [`Admin::alter_share_group_offsets_timeout`] /
//! [`Admin::delete_share_group_offsets_timeout`] are Java
//! `AlterShareGroupOffsetsOptions` / `DeleteShareGroupOffsetsOptions.timeoutMs`
//! (RPC deadline; these RPCs have no TimeoutMs).
//! [`Admin::delete_consumer_group_offsets`] is Java `deleteConsumerGroupOffsets`.
//! [`OffsetDeleteResult::new`] /
//! [`protocol::group::OffsetDeleteTopic::error_result`] are Java
//! `OffsetDeleteResponse.Builder.addPartition` / `addPartitions` (one
//! partition / one topic). Official Java
//! [`protocol::group::OffsetDeleteRequest::error_response`] is Java
//! `OffsetDeleteRequest.getErrorResponse` (top-level ErrorCode only;
//! empty Topics).
//! [`protocol::group::OffsetDeleteResponse::error_counts`] is Java
//! `OffsetDeleteResponse.errorCounts` (top-level `errorCode` plus each
//! partition-level code, including `NONE`);
//! [`protocol::group::OffsetDeleteResponse::merge`] is Java
//! `OffsetDeleteResponse.Builder.merge` (replace when the new top-level
//! ErrorCode is not `NONE` or current Topics are empty; otherwise append
//! topics / partitions; overlapping partitions are not checked).
//! [`Admin::delete_offsets_timeout`] / [`Admin::delete_consumer_group_offsets_timeout`]
//! are Java `DeleteConsumerGroupOffsetsOptions.timeoutMs` (RPC deadline;
//! OffsetDelete has no TimeoutMs).
//! [`Admin::alter_consumer_group_offsets_timeout`] is Java
//! `AlterConsumerGroupOffsetsOptions.timeoutMs` (RPC deadline;
//! OffsetCommit has no TimeoutMs).
//! [`Admin::delete_share_groups`] is Java `deleteShareGroups` (DeleteGroups).
//! [`Admin::abort_transaction`] is Java `abortTransaction`
//! ([`AbortTransactionSpec`]; WriteTxnMarkers v0–1;
//! [`TransactionResult::Abort`]). [`AbortTransactionSpec`]
//! `Display` is Java `AbortTransactionSpec.toString`.
//! [`protocol::txn::WritableTxnMarker`] `Display` is Java
//! `WriteTxnMarkersRequest.TxnMarkerEntry.toString`;
//! [`protocol::txn::WritableTxnMarker::partitions`] is Java
//! `WriteTxnMarkersRequest.TxnMarkerEntry.partitions` (flatten of nested
//! topics; duplicates kept);
//! [`protocol::txn::WritableTxnMarker::from_partitions`] is Java
//! `WriteTxnMarkersRequest.Builder` one marker (group by name; a later
//! entry for the same topic appends; first-seen topic order; duplicate
//! pairs kept).
//! [`protocol::txn::WriteTxnMarkersRequest::error_response`] is Java
//! `WriteTxnMarkersRequest.getErrorResponse` (one error on every request
//! partition; inner `HashMap.put` keeps the last pair per marker; a later
//! marker overwrites the same producer id; empty topics dropped).
//! [`protocol::txn::WriteTxnMarkersResponse::error_counts`] is Java
//! `WriteTxnMarkersResponse.errorCounts` (partition-level codes, including `NONE`);
//! [`protocol::txn::WriteTxnMarkersResponse::errors_by_producer_id`] is Java
//! `WriteTxnMarkersResponse.errorsByProducerId` (producer id to
//! `(topic, partition)` codes; a later marker overwrites);
//! [`protocol::txn::WriteTxnMarkersResponse::from_errors`] is Java
//! `WriteTxnMarkersResponse` constructor from an errors map (group by
//! topic name; a later entry for the same topic appends; first-seen
//! topic order).
//! [`Admin::abort_transaction_timeout`] is Java
//! `AbortTransactionOptions.timeoutMs` (RPC deadline; WriteTxnMarkers has
//! no TimeoutMs; caps `NOT_LEADER_OR_FOLLOWER`).
//! [`Admin::remove_members_from_consumer_group`] is Java
//! `removeMembersFromConsumerGroup` ([`MemberToRemove`]; LeaveGroup v3–v5,
//! [`DEFAULT_LEAVE_GROUP_REASON`] on v5).
//! [`Admin::remove_all_members_from_consumer_group`] is Java
//! `RemoveMembersFromConsumerGroupOptions.removeAll`.
//! [`Admin::remove_members_from_consumer_group_with_reason`] /
//! [`Admin::remove_all_members_from_consumer_group_with_reason`] are Java
//! `RemoveMembersFromConsumerGroupOptions.reason` (LeaveGroup v5; empty
//! uses [`DEFAULT_LEAVE_GROUP_REASON`]; truncated to 255 characters).
//! [`Admin::remove_members_from_consumer_group_timeout`] /
//! [`Admin::remove_all_members_from_consumer_group_timeout`] are Java
//! `RemoveMembersFromConsumerGroupOptions.timeoutMs` (RPC deadline;
//! LeaveGroup and DescribeGroups have no TimeoutMs).
//! [`Admin::describe_features`] is Java `describeFeatures`
//! ([`FeatureMetadata`]; ApiVersions v3–v4 tagged fields; KIP-511 retry).
//! [`Admin::describe_features_timeout`] is Java
//! `DescribeFeaturesOptions.timeoutMs` (RPC deadline; ApiVersions has no
//! TimeoutMs).
//! [`Admin::describe_client_quotas_timeout`] /
//! [`Admin::alter_client_quotas_timeout`] are Java
//! `DescribeClientQuotasOptions` / `AlterClientQuotasOptions.timeoutMs`
//! (RPC deadline; these RPCs have no TimeoutMs).
//! [`Admin::describe_client_quotas_all`] is Java
//! `describeClientQuotas(ClientQuotaFilter.all())`.
//! [`Admin::describe_client_quotas_with`] is Java
//! `describeClientQuotas(ClientQuotaFilter)`
//! ([`ClientQuotaFilter::contains`] / [`ClientQuotaFilter::contains_only`]).
//! [`ClientQuotaFilterComponent::of_entity`] /
//! [`ClientQuotaFilterComponent::of_default_entity`] /
//! [`ClientQuotaFilterComponent::of_entity_type`] are Java
//! `ClientQuotaFilterComponent` factories.
//! [`ClientQuotaEntity::USER`] / [`ClientQuotaEntity::CLIENT_ID`] /
//! [`ClientQuotaEntity::IP`] match Java `ClientQuotaEntity` constants.
//! [`protocol::admin::DescribeClientQuotasRequest::filter`] is Java
//! `DescribeClientQuotasRequest.filter` (unknown MatchType is
//! [`Error::protocol`]).
//! [`protocol::admin::DescribeClientQuotasRequest::from_filter`] is Java
//! `DescribeClientQuotasRequest.Builder` from a filter (MatchType from
//! [`ClientQuotaFilterComponent::matched`]; leftover Match on
//! default/specified is null).
//! [`protocol::admin::DescribeClientQuotasResponse::error`] is Java
//! `DescribeClientQuotasRequest.getErrorResponse` (`Entries` null, not
//! empty).
//! [`protocol::admin::DescribeClientQuotasResponse::from_quota_entities`] is Java
//! `DescribeClientQuotasResponse.fromQuotaEntities` (type/name pairs
//! plus values into `Entries`; `ErrorCode` `0`; `ErrorMessage` null;
//! empty input is empty `Entries`, not null; throttle unused).
//! [`ClientQuotaAlteration::error_result`] /
//! [`ClientQuotaAlterationResult::error`] /
//! [`ClientQuotaAlterationResult::error_results`] are Java
//! `AlterClientQuotasRequest.getErrorResponse` (one entry / Entries).
//! `ErrorMessage` stays the JSON default (null); official Java also
//! sets the English `Errors.message` string.
//! [`protocol::admin::AlterClientQuotasResponse::error_counts`] is Java
//! `AlterClientQuotasResponse.errorCounts` (per-entry codes, including `NONE`).
//! [`protocol::admin::AlterClientQuotasResponse::from_quota_entities`] is Java
//! `AlterClientQuotasResponse.fromQuotaEntities` (type/name pairs plus
//! [`ApiError`] into `Entries`; `ErrorMessage` is copied; throttle unused).
//! [`protocol::admin::AlterClientQuotasRequest::entries`] is Java
//! `AlterClientQuotasRequest.entries` (duplicate EntityType last-wins;
//! leftover Value on remove is ignored).
//! [`Admin::alter_user_scram_credentials_with`] is Java
//! `alterUserScramCredentials(List)` ([`UserScramCredentialAlteration`]).
//! [`AlterUserScramCredentialsResult::error`] /
//! [`AlterUserScramCredentialsResult::error_results`] are Java
//! `AlterUserScramCredentialsRequest.getErrorResponse` (one user /
//! unique sorted names from Deletions and Upsertions). `ErrorMessage`
//! stays the JSON default (null); official Java also sets the English
//! `Errors.message` string.
//! [`protocol::admin::AlterUserScramCredentialsResponse::error_counts`] /
//! [`protocol::admin::AlterUserScramCredentialsResponse::should_client_throttle`]
//! are Java `AlterUserScramCredentialsResponse.errorCounts` (per-user codes,
//! including `NONE`) / `shouldClientThrottle` (always).
//! [`Admin::alter_user_scram_credentials_timeout`] /
//! [`Admin::describe_user_scram_credentials_timeout`] are Java
//! `AlterUserScramCredentialsOptions` /
//! `DescribeUserScramCredentialsOptions.timeoutMs` (RPC deadline; these
//! RPCs have no TimeoutMs).
//! [`Admin::describe_user_scram_credentials_all`] is Java
//! `describeUserScramCredentials()`.
//! [`Admin::unregister_broker_timeout`] is Java
//! `UnregisterBrokerOptions.timeoutMs` (RPC deadline; UnregisterBroker has
//! no TimeoutMs; caps `NOT_CONTROLLER`).
//! [`UnregisterBrokerResponse::error_counts`] is Java
//! `UnregisterBrokerResponse.errorCounts` (top-level code only when it is
//! not `NONE`; success is an empty map).
//! [`UnregisterBrokerResponse::should_client_throttle`] is Java
//! `UnregisterBrokerResponse.shouldClientThrottle` (always).
//! [`Admin::allocate_producer_ids_timeout`] is the crate-first
//! AllocateProducerIds (api 67) RPC deadline; Java `Admin` has no
//! `allocateProducerIds`. [`Admin::new`] does not require that API,
//! UnregisterBroker, DescribeProducers, DescribeCluster, UpdateFeatures,
//! DescribeClientQuotas, AlterClientQuotas, AlterUserScramCredentials,
//! DescribeUserScramCredentials, AlterReplicaLogDirs, DescribeLogDirs,
//! the delegation-token APIs, DescribeTransactions, ListTransactions,
//! AlterPartitionReassignments, ListPartitionReassignments, OffsetDelete,
//! IncrementalAlterConfigs, ShareGroupDescribe, the share-offset RPCs, ListConfigResources,
//! GetTelemetrySubscriptions, PushTelemetry, or AssignReplicasToDirs.
//! [`Admin::assign_replicas_to_dirs_timeout`] is Java
//! `AssignReplicasToDirsOptions.timeoutMs` (RPC deadline;
//! AssignReplicasToDirs has no TimeoutMs; caps `NOT_CONTROLLER`).
//! [`AssignReplicasToDirsResponse::error_counts`] is Java
//! `AssignReplicasToDirsResponse.errorCounts` (top-level code only,
//! including `NONE`; nested partition codes are not counted).
//! [`Admin::alter_replica_log_dirs_timeout`] is Java
//! `AlterReplicaLogDirsOptions.timeoutMs` (RPC deadline;
//! AlterReplicaLogDirs has no TimeoutMs).
//! [`Admin::alter_replica_log_dirs_for`] is Java
//! `alterReplicaLogDirs(Map)` (one AlterReplicaLogDirs per replica broker).
//! [`Admin::create_delegation_token_timeout`] /
//! [`Admin::renew_delegation_token_timeout`] /
//! [`Admin::expire_delegation_token_timeout`] /
//! [`Admin::describe_delegation_token_timeout`] are Java
//! `CreateDelegationTokenOptions` / `RenewDelegationTokenOptions` /
//! `ExpireDelegationTokenOptions` / `DescribeDelegationTokenOptions.timeoutMs`
//! (RPC deadline; these RPCs have no TimeoutMs).
//! [`Admin::create_delegation_token_default`] is Java `createDelegationToken()`.
//! [`Admin::renew_delegation_token_hmac`] / [`Admin::expire_delegation_token_hmac`]
//! are Java `renewDelegationToken(byte[])` / `expireDelegationToken(byte[])`.
//! [`Admin::describe_delegation_tokens`] is Java `describeDelegationToken()`.
//! [`DescribedDelegationToken`] `Display` is Java `DelegationToken.toString`
//! (nested `TokenInformation.toString`; `hmac=[*******]`).
//! [`DescribedDelegationToken::renewers_as_string`] /
//! [`DescribedDelegationToken::owner_or_renewer`] are Java
//! `TokenInformation.renewersAsString` / `ownerOrRenewer`.
//! [`CreatableRenewer::USER_TYPE`] / [`CreatableRenewer::anonymous`]
//! (and the same names on [`DescribeDelegationTokenOwner`] /
//! [`DescribedDelegationTokenRenewer`]) are Java `KafkaPrincipal.USER_TYPE`
//! / `ANONYMOUS`.
//! [`Admin::describe_topic_partitions_timeout`] is the crate-first
//! DescribeTopicPartitions (api 75) RPC deadline; Java `describeTopics`
//! is [`Admin::describe_topics_timeout`].
//! [`protocol::admin::DescribeTopicPartitionsResponse::error_counts`] is Java
//! `DescribeTopicPartitionsResponse.errorCounts` (topic-level and
//! partition-level codes, including `NONE`);
//! [`protocol::admin::DescribeTopicPartitionsResponse::should_client_throttle`]
//! is Java `DescribeTopicPartitionsResponse.shouldClientThrottle` (always);
//! [`protocol::admin::DescribeTopicPartitionsResponse::partition_to_topic_partition_info`]
//! is Java `DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo`
//! (leader `HashMap.get`; replica lists `getOrDefault` `Node(id, "", -1)`);
//! [`TopicPartitionInfo`] is Java `TopicPartitionInfo`;
//! [`protocol::admin::DescribeTopicPartitionsRequest::error_response`] is Java
//! `DescribeTopicPartitionsRequest.getErrorResponse` (one topic per request
//! name; `isInternal` false; empty partitions; request Cursor / limit are
//! not copied);
//! [`Admin::list_topics`] / [`Admin::list_topics_with`] /
//! [`Admin::list_topics_timeout`] / [`Admin::describe_topics`] /
//! [`Admin::describe_topics_with`] / [`Admin::describe_topics_timeout`] /
//! [`Admin::describe_topics_with_partition_limit`] /
//! [`Admin::describe_topics_by_id`] are Java
//! `listTopics` / `ListTopicsOptions.listInternal` /
//! `ListTopicsOptions.timeoutMs` / `describeTopics` (DescribeTopicPartitions api 75, Metadata fallback) /
//! `DescribeTopicsOptions.includeAuthorizedOperations` /
//! `DescribeTopicsOptions.timeoutMs` /
//! `DescribeTopicsOptions.partitionSizeLimitPerResponse` /
//! `describeTopics(TopicCollection.ofTopicNames)` /
//! `describeTopics(TopicCollection.ofTopicIds)` (Metadata v12+)
//! ([`TopicCollection`] / [`TopicListing`] / [`TopicDescription`] / [`Uuid`];
//! [`Uuid::random_uuid`] is Java `Uuid.randomUuid`).
//! [`Admin::describe_replica_log_dirs`] is Java `describeReplicaLogDirs`
//! ([`TopicPartitionReplica`] / [`ReplicaLogDirInfo`]).
//! [`Admin::describe_broker_log_dirs`] is Java
//! `describeLogDirs(Collection<Integer>)`.
//! [`Admin::describe_log_dirs_timeout`] /
//! [`Admin::describe_replica_log_dirs_timeout`] /
//! [`Admin::describe_broker_log_dirs_timeout`] are Java
//! `DescribeLogDirsOptions.timeoutMs` (RPC deadline; DescribeLogDirs has
//! no TimeoutMs).
//! [`Admin::metrics`] is Java `Admin.metrics()` ([`AdminMetrics`]).
//! [`AclResourceType::code`] / [`AclPatternType::code`] /
//! [`AclOperation::code`] / [`AclPermission::code`] are Java
//! `ResourceType.code` / `PatternType.code` / `AclOperation.code` /
//! `AclPermissionType.code`.
//! [`AclBinding::allow_topic`] / [`AclBinding::new`] / [`AclBindingFilter`] /
//! [`ResourcePattern`] / [`AccessControlEntry`] / [`AclResourceType`] /
//! [`AclOperation`] / [`AclPermission`] cover CreateAcls / DescribeAcls /
//! DeleteAcls. [`AclBinding`] `Display` is Java `AclBinding.toString`. [`Admin::describe_acls_with`] is Java `describeAcls(AclBindingFilter)`.
//! [`Admin::describe_acls_any`] is Java `describeAcls(AclBindingFilter.ANY)`.
//! [`Admin::delete_acls_with`] is Java `deleteAcls(Collection)` (DeleteAcls Filters of N).
//! [`Admin::create_acls_timeout`] / [`Admin::describe_acls_timeout`] /
//! [`Admin::delete_acls_timeout`] are Java `CreateAclsOptions` /
//! `DescribeAclsOptions` / `DeleteAclsOptions.timeoutMs` (RPC deadline).
//! [`Producer::init_transactions`] / [`Producer::flush_timeout`] /
//! [`Producer::close_timeout`] match Java (`initTransactions` without
//! `transactional.id` is `Cannot use transactional methods without enabling transactions`). [`Consumer::close_timeout`]
//! drops fetch connections (Java `close(Duration)`; no LeaveGroup).
//! [`ConsumerGroup::close_timeout`] / [`ShareGroup::close_timeout`] cap
//! `leave`.
//! [`ProducerConfig::interceptor`] / [`ConsumerConfig::interceptor`] observe
//! or rewrite records (`close` / [`ConsumerInterceptor::on_commit`]).
//!
//! # Groups
//!
//! [`ConsumerGroup::join`] is classic range, [`ConsumerGroup::join_sticky`]
//! is sticky, [`ConsumerGroup::join_cooperative_sticky`] is KIP-429,
//! [`ConsumerGroup::join_with_assignors`] is Java `partition.assignment.strategy`
//! (JoinGroup Protocols of N; empty assignors is `Must configure at least one
//! partition assigner class name`), and
//! [`ConsumerGroup::join_consumer`] is KIP-848
//! ([`protocol::cgheartbeat::ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH`]
//! on join; leave uses
//! [`protocol::cgheartbeat::ConsumerGroupHeartbeatRequest::leave_group_epoch`]).
//! An empty `group.id` on [`ConsumerGroup::join`] is
//! `The configured group.id should not be an empty string or whitespace.`;
//! [`ShareGroup::join`] is
//! `You must provide a valid group.id in the consumer configuration.`. Each has a
//! `_topics` variant for several topics. [`ConsumerGroup::group_protocol`] is
//! Java `GroupProtocol` (`CLASSIC` / `CONSUMER`; [`GroupProtocol::of`] is Java
//! `GroupProtocol.of`). [`ConsumerGroup::join_matching`] /
//! [`ConsumerGroup::join_sticky_matching`] /
//! [`ConsumerGroup::join_cooperative_sticky_matching`] /
//! [`ConsumerGroup::join_consumer_matching`] are Java `subscribe(Pattern)`
//! at join (range, sticky, cooperative-sticky, KIP-848).
//! [`ConsumerConfig::group_instance_id`] is static membership.
//! [`ConsumerConfig::auto_offset_reset`] is used when OffsetFetch has no
//! committed offset. [`ShareGroup`] is KIP-932 (`join` / [`ShareGroup::join_topics`] /
//! [`ShareGroup::join_matching`] / [`ShareGroup::subscribe`] /
//! [`ShareGroup::subscribe_matching`] / [`ShareGroup::unsubscribe`] /
//! [`ShareGroup::accept`] / [`ShareGroup::release`] / [`ShareGroup::reject`] /
//! [`ShareGroup::acknowledge`] (acknowledge before [`ShareGroup::poll`] is
//! `Acknowledge called before poll.`);
//! [`protocol::share::ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH`] /
//! [`protocol::share::ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH`]
//! are Java `ShareGroupHeartbeatRequest` join/leave epochs;
//! [`ShareRequestMetadata`] is Java `ShareRequestMetadata` share-session
//! member id and epoch).
//! [`Consumer::seek_with_metadata`] / [`ConsumerGroup::seek_with_metadata`]
//! are Java `seek(TopicPartition, OffsetAndMetadata)` (Fetch
//! `LastFetchedEpoch` from the leader epoch; metadata string ignored;
//! negative offset / unassigned partition match Java `seek`).
//! [`ConsumerGroup::commit_with_metadata`] sends [`OffsetAndMetadata`]
//! (leader epoch and a metadata string). [`ConsumerGroup::commit_timeout`] /
//! [`ConsumerGroup::commit_with_metadata_timeout`] are Java
//! `commitSync(Duration)`. [`ConsumerGroup::commit_async`] /
//! [`ConsumerGroup::commit_async_with`] are Java `commitAsync` (OffsetCommit
//! on the next poll / leave; no spawned task). [`ConsumerGroup::enforce_rebalance`]
//! / [`ConsumerGroup::enforce_rebalance_with`] rejoin on the next poll (Java
//! `enforceRebalance` / `enforceRebalance(String)`; JoinGroup v8+ Reason,
//! default [`DEFAULT_ENFORCE_REBALANCE_REASON`]). [`ConsumerConfig::on_rebalance`] receives
//! [`TopicPartition`] slices. [`ConsumerGroup::subscribe`] /
//! [`ConsumerGroup::subscribe_matching`] / [`ConsumerGroup::unsubscribe`]
//! change the topic list without dropping the handle.
//! [`ConsumerGroup::group_metadata`] is Java `ConsumerGroupMetadata`.
//! [`ConsumerGroupMetadata`] `Display` is Java `toString`
//! (`GroupMetadata(...)`; empty `groupInstanceId` is `orElse("")`).
//! [`ConsumerGroupMetadata::new`] is Java `ConsumerGroupMetadata(String)`
//! ([`ConsumerGroupMetadata::UNKNOWN_GENERATION_ID`] /
//! [`ConsumerGroupMetadata::UNKNOWN_MEMBER_ID`]; Java
//! `JoinGroupRequest.UNKNOWN_GENERATION_ID` / `UNKNOWN_MEMBER_ID`).
//! [`protocol::group::JoinGroupRequest::UNKNOWN_MEMBER_ID`] /
//! [`protocol::group::JoinGroupRequest::UNKNOWN_GENERATION_ID`] /
//! [`protocol::group::JoinGroupRequest::UNKNOWN_PROTOCOL_NAME`] /
//! [`protocol::group::JoinGroupRequest::error_response`] /
//! [`protocol::group::JoinGroupRequest::maybe_truncate_reason`] /
//! [`protocol::group::JoinGroupRequest::join_reason`] /
//! [`protocol::group::JoinGroupRequest::validate_group_instance_id`] /
//! [`protocol::group::Topic::validate`] /
//! [`protocol::group::Topic::is_valid`] /
//! [`protocol::group::Topic::is_internal`] /
//! [`protocol::group::Topic::has_collision_chars`] /
//! [`protocol::group::Topic::unify_collision_chars`] /
//! [`protocol::group::Topic::has_collision`] /
//! [`protocol::group::JoinGroupRequest::requires_known_member_id`] /
//! [`protocol::group::JoinGroupRequest::requires_known_member_id_for`] /
//! [`protocol::group::JoinGroupRequest::supports_skipping_assignment`] /
//! [`protocol::group::JoinGroupRequest::build`] are Java
//! `JoinGroupRequest.UNKNOWN_MEMBER_ID` / `UNKNOWN_GENERATION_ID` /
//! `UNKNOWN_PROTOCOL_NAME` / `getErrorResponse` / `maybeTruncateReason` / `joinReason` /
//! `validateGroupInstanceId` / `Topic.validate` /
//! `Topic.isValid` / `Topic.isInternal` / `Topic.hasCollisionChars` /
//! `Topic.unifyCollisionChars` / `Topic.hasCollision` /
//! `requiresKnownMemberId` / `requiresKnownMemberId(JoinGroupRequestData, short)` /
//! `supportsSkippingAssignment` / `Builder.build`. Classic JoinGroup two-steps on
//! `MEMBER_ID_REQUIRED` when that request-aware check is true (KIP-394;
//! JoinGroup v4+ without `group.instance.id`). JoinGroup v2–v3 and static
//! members join in one RPC. JoinGroup v7+ encodes empty ProtocolName as null
//! (Java `JoinGroupResponse`).
//! [`Producer::send_offsets_with_metadata`] / [`Producer::send_offsets_for_group`]
//! commit transactional offsets with epoch and metadata.
//! `send_offsets_for_group` also sends generation / member / instance on
//! TxnOffsetCommit v3+ ([`protocol::txn::TxnOffsetCommitMember::unknown`] is
//! Java `TxnOffsetCommitRequest.Builder` without group metadata;
//! [`protocol::txn::TxnOffsetCommitMember::group_metadata_set`] is Java
//! `groupMetadataSet`, rejected below v3; Java
//! `throwIfInvalidGroupMetadata` rejects `generationId` greater than 0
//! with unknown `member.id`).
//! [`Producer::send_offsets_to_transaction`] takes [`TopicPartition`].
//! [`Admin::close`] / [`Admin::close_timeout`] drop the admin connection
//! (Java `close(Duration)`; the duration is unused).
//!
//! # Configure
//!
//! ```no_run
//! use std::time::Duration;
//! use partitionline::{Acks, Compression, IsolationLevel, ProducerConfig, Sasl, SecurityProtocol};
//!
//! let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
//!     .acks(Acks::All)
//!     .linger(Duration::from_millis(5))
//!     .compression(Compression::Lz4)
//!     .sasl(Sasl::scram_sha256("alice", "secret"));
//!
//! let _iso = IsolationLevel::ReadCommitted;
//! let _proto = SecurityProtocol::Plaintext;
//! ```
//!
//! TLS is [`TlsConfig`] on the same builders (rustls, no OpenSSL).
//! [`net::get_host`] / [`net::get_port`] / [`net::format_address`] /
//! [`net::valid_host_pattern`] are Java `Utils.getHost` / `getPort` /
//! `formatAddress` / `validHostPattern` (IPv6 brackets, optional
//! `PLAINTEXT://` scheme). TLS SNI uses `getHost` when the bootstrap
//! address parses. [`net::parse_and_validate_addresses`] is Java
//! `ClientUtils.parseAndValidateAddresses` without DNS (`Invalid url in
//! bootstrap.servers`, `Invalid port in bootstrap.servers`; an empty list
//! is still `no bootstrap servers`; all-blank entries are `No resolvable
//! bootstrap urls given in bootstrap.servers`).
//! [`net::MIN_RESERVED_CORRELATION_ID`] /
//! [`net::MAX_RESERVED_CORRELATION_ID`] /
//! [`net::is_reserved_correlation_id`] / [`net::next_correlation_id`] /
//! [`net::next_sasl_correlation_id`] /
//! [`net::check_parse_response_correlation`] are Java
//! `SaslClientAuthenticator` reserved correlation ids,
//! `NetworkClient.nextCorrelationId`,
//! `SaslClientAuthenticator.nextCorrelationId`, and
//! `NetworkClient.parseResponse` (`SchemaException` when a SASL reserved
//! request id is paired with a non-reserved response id).
//! [`ProducerConfig::delivery_timeout`] is Kafka `delivery.timeout.ms`
//! (default 30s; Java defaults to 120s). [`ProducerConfig::max_block`] is
//! Kafka `max.block.ms` (how long `send` waits for metadata and
//! [`ProducerConfig::buffer_memory`]; default 30s, Java 60s).
//! [`ProducerConfig::buffer_memory`] is Kafka `buffer.memory` (queued
//! key-plus-value bytes not yet acked; default 32 MiB, Java; zero is no
//! client-side cap; a record whose
//! [`protocol::records::Records::estimate_size_in_bytes_upper_bound`] exceeds
//! this is [`Error::RecordTooLarge`], Java `ensureValidRecordSize`
//! `buffer.memory`). [`ProducerConfig::max_request_size`] is Kafka
//! `max.request.size` ([`protocol::records::Records::estimate_size_in_bytes_upper_bound`]
//! of one record; default 1 MiB, Java; zero is no extra cap; oversized
//! records return [`Error::RecordTooLarge`], Java `RecordTooLargeException`
//! `The message is {size} bytes when serialized which is larger than {max}, which is the value of the max.request.size configuration.`). [`ProducerConfig::retry_backoff`] /
//! [`ProducerConfig::retry_backoff_max`] are Kafka `retry.backoff.ms` /
//! `retry.backoff.max.ms` (exponential wait after a retriable Produce;
//! default 100ms / 1s). [`ConsumerConfig::retry_backoff`] is the same pair
//! for retriable Fetch (preferred-replica redirects do not wait).
//! [`ProducerConfig::reconnect_backoff`] /
//! [`ProducerConfig::reconnect_backoff_max`] are Kafka `reconnect.backoff.ms` /
//! `reconnect.backoff.max.ms` (exponential wait after a failed broker TCP
//! connect; default 50ms / 1s, same as Java). The same pair is on
//! [`ConsumerConfig`] and [`AdminConfig`].
//! [`ProducerConfig::connections_max_idle`] / [`ConsumerConfig::connections_max_idle`] /
//! [`AdminConfig::connections_max_idle`] are Kafka `connections.max.idle.ms`
//! (close unused broker TCP connections; default 9 minutes, Java; zero never
//! closes for idle). Admin bootstrap RPCs and group/share coordinator sockets
//! reconnect after the same idle.
//! [`AdminConfig::retry_backoff`] / [`AdminConfig::retry_backoff_max`] are
//! Kafka `retry.backoff.ms` / `retry.backoff.max.ms` on admin RPCs
//! (`NOT_CONTROLLER`, coordinator moves, retriable IO; default 100ms / 1s).
//! [`ProducerConfig::transaction_timeout`] is Kafka `transaction.timeout.ms`
//! on InitProducerId v0–v5 (default 60s, same as Java).
//! [`ProducerConfig::metadata_max_age`] / [`ConsumerConfig::metadata_max_age`]
//! are Kafka `metadata.max.age.ms` (default 5 minutes; zero refreshes every
//! lookup).
//! [`ProducerConfig::allow_auto_create_topics`] /
//! [`ConsumerConfig::allow_auto_create_topics`] are Kafka
//! `allow.auto.create.topics` (this crate defaults to `false`; Java consumer
//! defaults to `true`).
//! [`ConsumerConfig::isolation`] is [`IsolationLevel`].
//! [`ConfigResourceType`] / [`ScramMechanism`] type admin config
//! resources and user SCRAM.
//!
//! # Admin
//!
//! [`Admin`] covers topics, partitions, configs, ACLs, groups, transactions,
//! quotas, telemetry, log dirs, and delegation tokens. See the [`admin`]
//! module. Still missing versus librdkafka: zstd and Kerberos (C libraries)
//! and Schema Registry. Tracker: `docs/gaps.md`.

#![forbid(unsafe_code)]

/// Admin client: topics, partitions, configs, ACLs, and the rest of Kafka admin.
pub mod admin;
pub(crate) mod cluster;
/// Shared config: [`Acks`], [`IsolationLevel`], [`SecurityProtocol`], [`ListenerName`], [`Endpoint`], [`Sasl`].
pub mod config;
/// Fetch client with manual partition assignment.
pub mod consumer;
/// Kafka and client error types.
pub mod error;
/// Consumer-group join / sync / heartbeat / commit.
pub mod group;
/// Produce and fetch interceptors.
pub mod interceptor;
/// Client counters, latency min/mean/max plus p50/p99, per-topic rows, and [`Quota`]: [`ProducerMetrics`], [`ConsumerMetrics`], [`ShareMetrics`], [`AdminMetrics`].
pub mod metrics;
/// TCP and TLS broker connections.
pub mod net;
/// Kafka murmur2 partitioner.
pub mod partitioner;
/// Produce client.
pub mod producer;
/// Kafka protocol codecs. Public so integration tests can speak the wire.
pub mod protocol;
/// Share groups (KIP-932).
pub mod share;

pub use admin::{
    AbortTransactionSpec, AccessControlEntry, AccessControlEntryFilter, AclBinding,
    AclBindingFilter, AclCreationResult, AclOperation, AclPatternType, AclPermission,
    AclResourceType, ActiveProducer, Admin, AdminConfig, AlterConfig, AlterConfigOp,
    AlterConfigOpType, AlterConfigsResourceResult, AlterReplicaLogDirsDirectory,
    AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse, AlterReplicaLogDirsResponsePartition,
    AlterReplicaLogDirsResponseTopic, AlterReplicaLogDirsTopic, AlterShareGroupOffsetsPartition,
    AlterShareGroupOffsetsTopic, AlteredShareGroupOffsets, AlteredShareGroupOffsetsPartition,
    AlteredShareGroupOffsetsTopic, AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition,
    AssignReplicasToDirsRequest, AssignReplicasToDirsResponse,
    AssignReplicasToDirsResponseDirectory, AssignReplicasToDirsResponsePartition,
    AssignReplicasToDirsResponseTopic, AssignReplicasToDirsTopic, ClientQuotaAlteration,
    ClientQuotaAlterationResult, ClientQuotaEntity, ClientQuotaEntry, ClientQuotaFilter,
    ClientQuotaFilterComponent, ClientQuotaOp, ClientQuotaValue, ClusterDescription,
    ClusterResource, Config, ConfigEntry, ConfigReplacement, ConfigResource, ConfigResourceType,
    ConfigResourceUpdate, ConfigSource, ConfigType, ConsumerGroupAssignment,
    ConsumerGroupDescription, ConsumerGroupMember, ConsumerGroupTopicPartitions, CreatableRenewer,
    CreateDelegationTokenRequest, CreateDelegationTokenResponse, DeletableGroupResult,
    DeleteShareGroupOffsetsTopic, DeletedAclsFilterResult, DeletedRecords,
    DeletedShareGroupOffsets, DeletedShareGroupOffsetsTopic, DescribableLogDirTopic,
    DescribeClusterBroker, DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
    DescribeDelegationTokenResponse, DescribeLogDirsPartition, DescribeLogDirsRequest,
    DescribeLogDirsResponse, DescribeLogDirsResult, DescribeLogDirsTopic,
    DescribeProducersPartition, DescribeProducersTopic, DescribeShareGroupOffsetsGroup,
    DescribeShareGroupOffsetsTopic, DescribeTopicPartitionsResponse,
    DescribeUserScramCredentialsResult, DescribedConsumerGroup, DescribedDelegationToken,
    DescribedDelegationTokenRenewer, DescribedGroup, DescribedGroupMember, DescribedShareGroup,
    DescribedShareGroupOffsets, DescribedShareGroupOffsetsPartition,
    DescribedShareGroupOffsetsTopic, DescribedTopicPartition, DescribedTopicPartitions,
    EndpointType, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse, FeatureMetadata,
    FeatureUpdate, FeatureUpdateResult, FencedProducer, FinalizedVersionRange,
    GetTelemetrySubscriptionsResponse, GroupState, GroupType, ListConsumerGroupOffsetsSpec,
    ListedConfigResource, ListedGroup, MemberToRemove, NewPartitionReassignment, NewPartitions,
    NewTopic, Node, OffsetDeleteResult, OngoingReassignment, PartitionReassignment,
    ProducerIdBlock, PushTelemetryResponse, ReassignmentResult, RecordsToDelete, RemovedMember,
    RenewDelegationTokenRequest, RenewDelegationTokenResponse, ReplicaLogDirInfo, ResourcePattern,
    ResourcePatternFilter, ScramCredentialInfo, ScramMechanism, ShareGroupAssignment,
    ShareGroupMember, ShareGroupTopicPartitions, SupportedVersionRange, TopicCollection,
    TopicDescription, TopicListing, TopicPartitionCursor, TopicPartitionInfo,
    TopicPartitionReplica, TransactionListing, TransactionState, TransactionTopic,
    UnregisterBrokerResponse, UpgradeType, UserScramCredentialAlteration,
    UserScramCredentialDeletion, UserScramCredentialResult, UserScramCredentialUpsertion, Uuid,
    ALTER_CONFIG_APPEND, ALTER_CONFIG_DELETE, ALTER_CONFIG_SET, ALTER_CONFIG_SUBTRACT,
    AUTHORIZED_OPERATIONS_OMITTED, CONFIG_RESOURCE_BROKER, CONFIG_RESOURCE_BROKER_LOGGER,
    CONFIG_RESOURCE_CLIENT_METRICS, CONFIG_RESOURCE_GROUP, CONFIG_RESOURCE_TOPIC,
    CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_DYNAMIC_BROKER, CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER,
    CONFIG_SOURCE_DYNAMIC_CLIENT_METRICS, CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
    CONFIG_SOURCE_DYNAMIC_GROUP, CONFIG_SOURCE_DYNAMIC_TOPIC, CONFIG_SOURCE_STATIC_BROKER,
    CONFIG_SOURCE_UNKNOWN, CONFIG_TYPE_BOOLEAN, CONFIG_TYPE_CLASS, CONFIG_TYPE_DOUBLE,
    CONFIG_TYPE_INT, CONFIG_TYPE_LIST, CONFIG_TYPE_LONG, CONFIG_TYPE_PASSWORD, CONFIG_TYPE_SHORT,
    CONFIG_TYPE_STRING, CONFIG_TYPE_UNKNOWN, DEFAULT_LEAVE_GROUP_REASON, ENDPOINT_TYPE_BROKERS,
    ENDPOINT_TYPE_CONTROLLERS, INVALID_OFFSET_LAG, QUOTA_MATCH_ANY, QUOTA_MATCH_DEFAULT,
    QUOTA_MATCH_EXACT, SCRAM_SHA_256, SCRAM_SHA_512, SCRAM_UNKNOWN, UNKNOWN_VOLUME_BYTES,
    UPGRADE_TYPE_SAFE_DOWNGRADE, UPGRADE_TYPE_UNSAFE_DOWNGRADE, UPGRADE_TYPE_UPGRADE,
};
pub use config::{
    Acks, AutoOffsetReset, Endpoint, IsolationLevel, ListenerName, Sasl, SecurityProtocol,
};
pub use consumer::{
    Consumer, ConsumerConfig, ConsumerRecords, FetchedRecord, OffsetAndMetadata,
    OffsetAndTimestamp, PartitionInfo, RebalanceListener, TopicIdPartition, TopicPartition,
    WakeupHandle,
};
pub use error::{ApiError, Error, Result};
pub use group::{
    ConsumerGroup, ConsumerGroupMetadata, CoordinatorType, GroupProtocol,
    DEFAULT_ENFORCE_REBALANCE_REASON, LEAVE_GROUP_REASON_CLOSED, LEAVE_GROUP_REASON_POLL_TIMEOUT,
    LEAVE_GROUP_REASON_UNSUBSCRIBED,
};
pub use interceptor::{ConsumerInterceptor, ProducerInterceptor};
pub use metrics::{
    AdminMetrics, ConsumerMetrics, LatencyStats, ProducerMetrics, Quota, ShareMetrics,
    TopicFetchMetrics, TopicProduceMetrics,
};
pub use net::TlsConfig;
pub use partitioner::{
    murmur2, partition_for_key, DefaultPartitioner, Partitioner, PartitionerBox,
};
pub use producer::{ProduceRecord, Producer, ProducerConfig, RecordMetadata};
pub use protocol::acl::{
    ACL_OPERATION_ALL, ACL_OPERATION_ANY, ACL_OPERATION_CREATE_TOKENS,
    ACL_OPERATION_DESCRIBE_TOKENS, ACL_PATTERN_ANY, ACL_PATTERN_LITERAL, ACL_PATTERN_PREFIXED,
    ACL_PERMISSION_ALLOW, ACL_PERMISSION_ANY, ACL_RESOURCE_ANY, ACL_RESOURCE_TOPIC,
    WILDCARD_RESOURCE,
};
pub use protocol::admin::{CreatedTopicConfig, DescribeConfigsResult, TopicResult};
pub use protocol::offsets::{
    OffsetSpec, EARLIEST_LOCAL_TIMESTAMP, EARLIEST_TIMESTAMP, LATEST_TIERED_TIMESTAMP,
    LATEST_TIMESTAMP, MAX_TIMESTAMP,
};
pub use protocol::oidc::OidcConfig;
pub use protocol::records::{
    Compression, ControlRecordType, EndTransactionMarker, Header, Record, RecordBatch,
    TimestampType,
};
pub use protocol::txn::TransactionResult;
pub use share::{
    AcknowledgeType, ShareGroup, ShareRecord, ShareRecords, ShareRequestMetadata, SHARE_ACK_ACCEPT,
    SHARE_ACK_REJECT, SHARE_ACK_RELEASE,
};

/// Software name sent in ApiVersions v3–v4.
pub const CLIENT_NAME: &str = "partitionline";
/// Crate version sent in ApiVersions v3–v4.
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
