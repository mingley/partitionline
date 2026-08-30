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
//! InitProducerId is v0–v5 (v2+ flexible; v3+ KIP-360 ProducerId;
//! first init [`RecordBatch::NO_PRODUCER_ID`] /
//! [`RecordBatch::NO_PRODUCER_EPOCH`], epoch-bump resume sends the last
//! id/epoch). Java `InitProducerIdRequest.getErrorResponse` writes those
//! sentinels. Java `InitProducerIdRequest.Builder.build` rejects a
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
//! [`protocol::api::MetadataResponse::topics_by_error`] are Java
//! `MetadataResponse.errors` / `errorsByTopicId` / `topicsByError`
//! (map values are Kafka error codes; `errors` throws when any topic name is
//! `None`; `errors_by_topic_id` throws when any topic id is zeros).
//! [`PartitionInfo::from_partition_metadata`] is Java
//! `MetadataResponse.toPartitionInfo` (broker ids, not `Node`).
//! [`protocol::api::MetadataRequestTopic::convert_from_names`] /
//! [`protocol::api::MetadataRequestTopic::convert_from_ids`] are Java
//! `MetadataRequest.convertToMetadataRequestTopic` /
//! `convertTopicIdsToMetadataRequestTopic`.
//! [`protocol::api::TopicMetadata::error`] /
//! [`protocol::api::MetadataRequestTopic::error_result`] are Java
//! `MetadataRequest.getErrorResponse` (one topic).
//! Name-based [`Admin::describe_topics`] uses DescribeTopicPartitions (api 75).
//! Groups and transactions negotiate FindCoordinator v1–v6 (v3+ flexible;
//! v4+ KIP-699 CoordinatorKeys; v5 TRANSACTION_ABORTABLE; v6 share groups).
//! [`CoordinatorType`] is Java `FindCoordinatorRequest.CoordinatorType`
//! (`id` / `forId`; unknown is `None`). [`protocol::group::MIN_BATCHED_VERSION`]
//! is Java `FindCoordinatorRequest.MIN_BATCHED_VERSION`.
//! [`protocol::group::FindCoordinatorResponse::should_client_throttle`] is Java
//! `FindCoordinatorResponse.shouldClientThrottle` (v2+);
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
//! OffsetCommit v2–v9 (v2–v4 [`protocol::group::DEFAULT_RETENTION_TIME`]; v6+ epoch;
//! decode below v6 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]; v7 GroupInstanceId; v8+ flexible; v9 KIP-848 errors;
//! [`protocol::group::OffsetCommitResponse::should_client_throttle`] is Java
//! `OffsetCommitResponse.shouldClientThrottle` (v4+);
//! [`protocol::group::OffsetTopic::error_result`] /
//! [`protocol::group::OffsetTopic::error_results`] /
//! [`protocol::group::OffsetCommitResponsePartition::error`] are Java
//! `OffsetCommitRequest.getErrorResponse` (one topic / Topics / partition
//! body). Nested body is PartitionIndex + ErrorCode. Throttle is the JSON
//! default (`0`)),
//! OffsetFetch v1–v9 (v2 top-level error; v3 throttle; v5 epoch; v6+ flexible; v7 RequireStable; v8 Groups; v9 MemberId;
//! [`protocol::group::OffsetFetchGroup::is_all_partitions`] is Java
//! `OffsetFetchRequest.isAllPartitions` / `isAllPartitionsForGroup`
//! (`None` Topics is every committed partition; `Some` empty is not);
//! [`protocol::group::OffsetFetchGroup::error_result`] /
//! [`protocol::group::OffsetFetchGroup::error_results`] /
//! [`protocol::group::OffsetFetchGroupResult::error`] are Java
//! `OffsetFetchRequest.getErrorResponse` one group / Groups on v8+
//! (empty Topics; request partitions are not copied). Throttle is the
//! JSON default (`0`)),
//! Heartbeat v0–v4 (v1+ throttle; v3 GroupInstanceId; v4 flexible;
//! [`protocol::group::HeartbeatResponse::should_client_throttle`] is Java
//! `HeartbeatResponse.shouldClientThrottle` (v2+)),
//! SyncGroup v0–v5 (v1+ throttle; v3 GroupInstanceId; v4+ flexible; v5 ProtocolType / ProtocolName;
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
//! [`protocol::group::JoinGroupResponse::should_client_throttle`] are Java
//! `JoinGroupResponse.isLeader` / `shouldClientThrottle`).
//! LeaveGroup v0–v5 (v3 Members / GroupInstanceId; v4 flexible; v5 Reason;
//! [`protocol::group::LeaveGroupResponse::should_client_throttle`] is Java
//! `LeaveGroupResponse.shouldClientThrottle` (v2+);
//! [`LEAVE_GROUP_REASON_CLOSED`] on leave / close, [`LEAVE_GROUP_REASON_UNSUBSCRIBED`]
//! on unsubscribe, [`LEAVE_GROUP_REASON_POLL_TIMEOUT`] on `max.poll.interval.ms`),
//! SaslHandshake v0–v1 (never flexible; v1 enables SaslAuthenticate),
//! SaslAuthenticate v0–v2 (v1 SessionLifetimeMs; v2 flexible),
//! ApiVersions v0–v4 (v3+ ClientSoftwareName; v4 SupportedFeatures.MinVersion 0; KIP-511 retry;
//! [`protocol::api::ApiVersionsRequest::is_valid`] is Java `ApiVersionsRequest.isValid`;
//! [`protocol::api::ApiVersionsResponse::api_version`] /
//! [`protocol::api::ApiVersionsResponse::UNKNOWN_FINALIZED_FEATURES_EPOCH`] /
//! [`protocol::api::ApiVersionsResponse::should_client_throttle`] /
//! [`protocol::api::ApiVersionsResponse::intersect`] are Java
//! `ApiVersionsResponse.apiVersion` / `UNKNOWN_FINALIZED_FEATURES_EPOCH` /
//! `shouldClientThrottle` / `intersect` (`null` is `None`; mismatched api
//! keys are `IllegalArgumentException`);
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
//! ShareGroupDescribe v0–v1 (v0 Kafka 4.0 early access; v1 Kafka 4.1 stable; same fields; FindCoordinator v4+ CoordinatorKeys of N),
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
//! (crate encode). Throttle is the JSON default (`0`)),
//! ShareAcknowledge v0–v1 (v0 Kafka 4.0 early access; v1 Kafka 4.1 stable; same fields;
//! [`protocol::share::ShareAcknowledgeResponsePartition::partition_response`] is Java
//! `ShareAcknowledgeResponse.partitionResponse` (`PartitionIndex` and `ErrorCode`).
//! Official Java leaves ErrorMessage and CurrentLeader at JSON defaults
//! (null / 0/0). Crate encode writes ErrorMessage null, CurrentLeader id 0
//! epoch 0, empty NodeEndpoints. Top-level ErrorCode stays 0 (crate encode
//! of this factory). Throttle is the JSON default (`0`). Official Java
//! `ShareAcknowledgeRequest.getErrorResponse` writes only the top-level
//! ErrorCode (empty Responses)),
//! ConsumerGroupDescribe v0–v1 (v1 MemberType; FindCoordinator v4+ CoordinatorKeys of N),
//! ListTransactions v0–v1 (v1 DurationFilter, KIP-994;
//! Java `ListTransactionsRequest.Builder.build` rejects a non-negative
//! DurationFilter on v0),
//! CreateTopics v0–v7 (v5+ flexible; v5 KIP-525 configs; v7 TopicId;
//! [`protocol::admin::CreateTopicsResponse::should_client_throttle`] is Java
//! `CreateTopicsResponse.shouldClientThrottle` (v3+);
//! [`protocol::admin::CreatableTopic::error_result`] /
//! [`protocol::admin::CreateTopicsRequest::error_results`] are Java
//! `CreateTopicsRequest.getErrorResponse` (one topic / Topics). v5+
//! NumPartitions / ReplicationFactor stay `-1`, Configs empty, TopicId
//! zero. `ErrorMessage` stays the JSON default (null); official Java
//! also sets the English `Errors.message` string),
//! DeleteTopics v0–v6 (v4+ flexible; v5 ErrorMessage; v6 TopicId, `delete_topics_by_id`;
//! [`protocol::admin::DeleteTopicsResponse::should_client_throttle`] is Java
//! `DeleteTopicsResponse.shouldClientThrottle` (v2+);
//! [`protocol::admin::TopicResult::error`] /
//! [`protocol::admin::DeleteTopicState::error_result`] are Java
//! `DeleteTopicsRequest.getErrorResponse` (one topic)),
//! DescribeGroups v0–v6 (v3 IncludeAuthorizedOperations; v4 GroupInstanceId; v5 flexible; v6 ErrorMessage; FindCoordinator v4+ CoordinatorKeys of N;
//! [`protocol::admin::DescribeGroupsResponse::UNKNOWN_STATE`] /
//! [`protocol::admin::DescribeGroupsResponse::UNKNOWN_PROTOCOL_TYPE`] /
//! [`protocol::admin::DescribeGroupsResponse::UNKNOWN_PROTOCOL`] /
//! [`protocol::admin::DescribeGroupsResponse::AUTHORIZED_OPERATIONS_OMITTED`] /
//! [`protocol::admin::DescribeGroupsResponse::should_client_throttle`] are Java
//! `DescribeGroupsResponse` error sentinels / `shouldClientThrottle` (v2+);
//! [`DescribedGroup::new`] is Java `groupError`),
//! ListGroups v0–v5 (v3 flexible; v4 StatesFilter / GroupState; v5 TypesFilter / GroupType;
//! [`protocol::admin::ListGroupsResponse::should_client_throttle`] is Java
//! `ListGroupsResponse.shouldClientThrottle` (v2+)),
//! DeleteGroups v0–v2 (v0–v1 classic; v2 flexible; FindCoordinator v4+ CoordinatorKeys of N;
//! [`protocol::admin::DeleteGroupsResponse::should_client_throttle`] is Java
//! `DeleteGroupsResponse.shouldClientThrottle` (v1+)),
//! DescribeClientQuotas / AlterClientQuotas v0–v1 (v1 flexible;
//! [`protocol::admin::DescribeClientQuotasResponse::error`] is Java
//! `DescribeClientQuotasRequest.getErrorResponse` (`Entries` null, not
//! empty). `ErrorMessage` stays the JSON default (null); official Java
//! also sets the English `Errors.message` string. Throttle is the JSON
//! default (`0`)),
//! ListConfigResources v0–v1 (v0 ListClientMetricsResources; v1 ResourceTypes),
//! AlterReplicaLogDirs v1–v2 (v1 classic; v2 flexible;
//! [`protocol::admin::AlterReplicaLogDirsResponse::should_client_throttle`] is Java
//! `AlterReplicaLogDirsResponse.shouldClientThrottle` (v1+);
//! [`AlterReplicaLogDirsTopic::error_result`] /
//! [`AlterReplicaLogDirsRequest::error_result`] are Java
//! `AlterReplicaLogDirsRequest.getErrorResponse` (one topic / flatten dirs)),
//! DescribeLogDirs v1–v4 (v1 classic; v2+ flexible; v3 ErrorCode; v4 TotalBytes;
//! [`protocol::admin::DescribeLogDirsResponse::UNKNOWN_VOLUME_BYTES`] /
//! [`protocol::admin::DescribeLogDirsResponse::INVALID_OFFSET_LAG`] /
//! [`protocol::admin::DescribeLogDirsResponse::should_client_throttle`] are Java
//! `DescribeLogDirsResponse` sentinels / `shouldClientThrottle` (v1+);
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
//! `DescribeConfigsResponse.shouldClientThrottle` (v2+)),
//! CreatePartitions v0–v3 (v2+ flexible; v3 KIP-599;
//! [`protocol::admin::CreatePartitionsResponse::should_client_throttle`] is Java
//! `CreatePartitionsResponse.shouldClientThrottle` (v1+);
//! [`protocol::admin::CreatePartitionsTopic::error_result`] /
//! [`protocol::admin::CreatePartitionsTopic::error_results`] are Java
//! `CreatePartitionsRequest.getErrorResponse` (one topic / Results).
//! `ErrorMessage` stays the JSON default (null); official Java also
//! sets the English `Errors.message` string),
//! IncrementalAlterConfigs v0–v1 (v1 flexible; Resources of N;
//! [`protocol::admin::IncrementalAlterConfigsResponse::should_client_throttle`] is Java
//! `IncrementalAlterConfigsResponse.shouldClientThrottle` (v0+)),
//! AlterConfigs v0–v2 (v2 flexible; Resources of N;
//! [`protocol::admin::AlterConfigsResponse::should_client_throttle`] is Java
//! `AlterConfigsResponse.shouldClientThrottle` (v1+)),
//! DeleteRecords v0–v2 (v2 flexible;
//! [`protocol::admin::DeleteRecordsRequest::HIGH_WATERMARK`];
//! [`DeletedRecords::INVALID_LOW_WATERMARK`];
//! [`protocol::admin::DeleteRecordsResponse::should_client_throttle`] is Java
//! `DeleteRecordsResponse.shouldClientThrottle` (v1+);
//! [`protocol::admin::DeletedRecordsPartition::error`] /
//! [`protocol::admin::DeleteRecordsTopic::error_result`] are Java
//! `DeleteRecordsRequest.getErrorResponse` (partition body / one topic)),
//! CreateAcls / DescribeAcls / DeleteAcls v0–v3 (v1 ResourcePatternType; v2+ flexible;
//! [`protocol::acl::CreateAclsResponse::should_client_throttle`] /
//! [`protocol::acl::DescribeAclsResponse::should_client_throttle`] /
//! [`protocol::acl::DeleteAclsResponse::should_client_throttle`] are Java
//! `shouldClientThrottle` (v1+);
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
//! [`DeletedAclsFilterResult::error`] / [`DeletedAclsFilterResult::error_results`]
//! are Java `DeleteAclsRequest.getErrorResponse` (one FilterResult /
//! `nCopies`). MatchingAcls stay the JSON default (empty); `ErrorMessage`
//! stays the JSON default (null); official Java also sets the English
//! `Errors.message` string. Throttle is the JSON default (`0`)),
//! AddPartitionsToTxn v0–v3 (v3 flexible;
//! [`protocol::txn::AddPartitionsToTxnResponse::should_client_throttle`] is Java
//! `AddPartitionsToTxnResponse.shouldClientThrottle` (v1+);
//! [`protocol::txn::TxnPartitionsTopic::error_result`] /
//! [`protocol::txn::TxnPartitionsTopic::error_results`] /
//! [`protocol::txn::AddPartitionsToTxnPartitionResult::error`] are Java
//! `AddPartitionsToTxnRequest.getErrorResponse` / `errorResponseForTopics`
//! (one topic / Topics / partition body). Nested body is PartitionIndex
//! and PartitionErrorCode (`ResultsByTopicV3AndBelow`). Throttle is the
//! JSON default (`0`)), AddOffsetsToTxn v0–v4
//! (v3+ flexible; v4 TRANSACTION_ABORTABLE;
//! [`protocol::txn::AddOffsetsToTxnResponse::should_client_throttle`] is Java
//! `AddOffsetsToTxnResponse.shouldClientThrottle` (v1+)), EndTxn v0–v5
//! (v3+ flexible; v4 TRANSACTION_ABORTABLE; v5 ProducerId / ProducerEpoch;
//! [`protocol::txn::EndTxnRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`];
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
//! `TxnOffsetCommitRequest.CommittedOffset`).
//! [`Producer::metrics`] is a snapshot of queued / acked / error counts
//! plus produce-ack latency min/mean/max and p50/p99 (last 1024 samples),
//! with per-topic rows on [`ProducerMetrics::topics`].
//! [`metrics::format_bytes`] is Java `Utils.formatBytes` (English `0.##`
//! scale; `-1` is `-1`; `1024` is `1 KB`). [`partitioner::abs`] is Java
//! `Utils.abs` ([`i32::MIN`] is `0`). [`partitioner::to_positive`] is Java
//! `Utils.toPositive`.
//! [`Admin::metrics`] is the same snapshot pattern for Admin RPCs
//! ([`AdminMetrics`]; Java `Admin.metrics()`).
//! [`Producer::client_instance_id`] is Java `clientInstanceId` (KIP-714;
//! returns [`Uuid`]).
//! [`Producer::client_instance_id_timeout`] is Java `clientInstanceId(Duration)`.
//! [`RecordMetadata::timestamp`] / [`RecordMetadata::has_timestamp`] /
//! [`RecordMetadata::serialized_key_size`] / [`RecordMetadata::serialized_value_size`]
//! match Java `RecordMetadata`. [`RecordMetadata::UNKNOWN_PARTITION`] is Java
//! `RecordMetadata.UNKNOWN_PARTITION`. [`protocol::api::ProducePartitionResponse::INVALID_OFFSET`]
//! is Java `ProduceResponse.INVALID_OFFSET`.
//! [`protocol::api::ProducePartitionResponse::partition_response`] is Java
//! `ProduceResponse.PartitionResponse(Errors)`.
//! [`protocol::api::ProduceTopicData::error_result`] is Java
//! `ProduceRequest.getErrorResponse` (one topic).
//! [`protocol::api::ProduceResponse::should_client_throttle`] is Java
//! `ProduceResponse.shouldClientThrottle` (v6+). Produce decode below v5 fills
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
//! decode below v12 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]). v18+
//! is not spoken. [`protocol::fetch::FetchedPartition::INVALID_HIGH_WATERMARK`] /
//! [`protocol::fetch::FetchedPartition::INVALID_LAST_STABLE_OFFSET`] /
//! [`protocol::fetch::FetchedPartition::INVALID_LOG_START_OFFSET`] /
//! [`protocol::fetch::FetchedPartition::INVALID_PREFERRED_REPLICA_ID`] are Java
//! `FetchResponse` sentinels (`-1`).
//! [`protocol::fetch::FetchedPartition::partition_response`] is Java
//! `FetchResponse.partitionResponse`.
//! [`protocol::fetch::FetchTopic::error_result`] is Java
//! `FetchRequest.getErrorResponse` (one topic; v13 and later omit partitions).
//! [`protocol::fetch::FetchedPartition::preferred_read_replica()`] /
//! [`protocol::fetch::FetchedPartition::is_preferred_replica`] /
//! [`protocol::fetch::FetchedPartition::is_diverging_epoch`] are Java
//! `FetchResponse.preferredReadReplica` / `isPreferredReplica` /
//! `isDivergingEpoch`.
//! [`protocol::fetch::FetchResponse::should_client_throttle`] is Java
//! `FetchResponse.shouldClientThrottle` (v8+). Omitted Fetch
//! v12+ CurrentLeader fills [`protocol::api::MetadataResponse::NO_LEADER_ID`] /
//! [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]; omitted DivergingEpoch fills
//! [`protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH`] /
//! [`protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH_OFFSET`].
//! [`protocol::fetch::CONSUMER_REPLICA_ID`] is Java
//! `FetchRequest.CONSUMER_REPLICA_ID` (written through v14).
//! [`protocol::offsets::CONSUMER_REPLICA_ID`] /
//! [`protocol::epoch::CONSUMER_REPLICA_ID`] are Java `ListOffsetsRequest` /
//! `OffsetsForLeaderEpochRequest` consumer replica ids.
//! [`protocol::fetch::is_consumer`] / [`protocol::fetch::is_valid_broker_id`] /
//! [`protocol::fetch::describe_replica_id`] are Java `FetchRequest.isConsumer` /
//! `isValidBrokerId` / `describeReplicaId`.
//! [`protocol::fetch::FetchMetadata`] is Java `FetchMetadata`
//! ([`protocol::fetch::FetchMetadata::LEGACY`] on Fetch requests).
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
//! [`protocol::group::OffsetFetchResponse::should_client_throttle`]
//! is Java `OffsetFetchResponse.shouldClientThrottle` (v4+). [`OffsetAndMetadata::NO_METADATA`] /
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
//! `aclsResources` / `aclBindings` (group by [`ResourcePattern`]). [`NewTopic`] / [`NewPartitions`] / [`ListedGroup`] `Display`
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
//! `UpdateFeaturesRequest.FeatureUpdateItem.isDeleteRequest`. Java `SupportedVersionRange` /
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
//! [`ScramMechanism::id`] is Java
//! `ScramMechanism.type`. [`ActiveProducer`] `Display`
//! is Java `ProducerState.toString`. [`DescribeProducersPartition`]
//! `Display` is Java `PartitionProducerState.toString`.
//! [`DescribeProducersPartition::error`] /
//! [`protocol::admin::DescribeProducersTopicRequest::error_result`] are Java
//! `DescribeProducersRequest.getErrorResponse` (partition body / one topic).
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
//! `IsolationLevel.id` / `forId`. [`Compression::id`] /
//! [`Compression::from_id`] / [`Compression::from_name`] are Java
//! `CompressionType.id` / `forId` / `forName`
//! (zstd `4` is `None`; this crate does not speak zstd).
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
//! [`RecordBatch::size_in_bytes`] is Java `DefaultRecordBatch.sizeInBytes()`
//! (encoded size, including compression). [`RecordBatch::size_in_bytes_of`]
//! and [`RecordBatch::size_in_bytes_from`] are the static helpers (empty is
//! `0`). [`RecordBatch::checksum`] is Java `DefaultRecordBatch.checksum`.
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
//! [`RecordBatch::has_producer_id`] is Java `AbstractRecordBatch.hasProducerId`
//! (`NO_PRODUCER_ID < producerId`). Fetch LastFetchedEpoch resets,
//! [`Consumer::seek`], and omitted last-fetched epoch use
//! [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
//! [`RecordBatch::is_transactional`] / [`RecordBatch::is_control_batch`] are
//! Java `DefaultRecordBatch.isTransactional` / `isControlBatch`.
//! [`ControlRecordType`] / [`EndTransactionMarker`] are Java
//! `ControlRecordType` / `EndTransactionMarker` (`type` / `fromTypeId` /
//! `parse`; COMMIT/ABORT marker key and value).
//! [`RecordBatch::with_end_transaction_marker`] is Java
//! `MemoryRecords.withEndTransactionMarker`.
//! [`protocol::records::Records::LOG_OVERHEAD`] is Java `Records.LOG_OVERHEAD`
//! (offset + size prefix).
//! [`RecordBatch::last_offset`] / [`RecordBatch::next_offset`] /
//! [`RecordBatch::last_sequence`] are Java `lastOffset` / `nextOffset` /
//! `lastSequence`. [`RecordBatch::is_compressed`] is Java `isCompressed`.
//! [`RecordBatch::offset_of_max_timestamp`] /
//! [`RecordBatch::delete_horizon_ms`] are Java `offsetOfMaxTimestamp` /
//! `deleteHorizonMs`.
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
//! [`protocol::offsets::ListOffsetsResponse::singleton_list_offsets_topic_response`]
//! is Java `ListOffsetsResponse.singletonListOffsetsTopicResponse`.
//! [`protocol::offsets::ListOffsetsResponsePartition::error`] /
//! [`protocol::offsets::ListOffsetsTopicRequest::error_result`] are Java
//! `ListOffsetsRequest.getErrorResponse` (partition body / one topic).
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
//! `OffsetDeleteRequest.getErrorResponse` writes only the top-level
//! ErrorCode.
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
//! `WriteTxnMarkersRequest.TxnMarkerEntry.toString`.
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
//! [`protocol::admin::DescribeClientQuotasResponse::error`] is Java
//! `DescribeClientQuotasRequest.getErrorResponse` (`Entries` null, not
//! empty). [`ClientQuotaAlteration::error_result`] /
//! [`ClientQuotaAlterationResult::error`] /
//! [`ClientQuotaAlterationResult::error_results`] are Java
//! `AlterClientQuotasRequest.getErrorResponse` (one entry / Entries).
//! `ErrorMessage` stays the JSON default (null); official Java also
//! sets the English `Errors.message` string.
//! [`Admin::alter_user_scram_credentials_with`] is Java
//! `alterUserScramCredentials(List)` ([`UserScramCredentialAlteration`]).
//! [`AlterUserScramCredentialsResult::error`] /
//! [`AlterUserScramCredentialsResult::error_results`] are Java
//! `AlterUserScramCredentialsRequest.getErrorResponse` (one user /
//! unique sorted names from Deletions and Upsertions). `ErrorMessage`
//! stays the JSON default (null); official Java also sets the English
//! `Errors.message` string.
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
//! [`protocol::group::JoinGroupRequest::supports_skipping_assignment`] are Java
//! `JoinGroupRequest.UNKNOWN_MEMBER_ID` / `UNKNOWN_GENERATION_ID` /
//! `UNKNOWN_PROTOCOL_NAME` / `maybeTruncateReason` / `joinReason` /
//! `validateGroupInstanceId` / `Topic.validate` /
//! `Topic.isValid` / `Topic.isInternal` / `Topic.hasCollisionChars` /
//! `Topic.unifyCollisionChars` / `Topic.hasCollision` /
//! `requiresKnownMemberId` / `requiresKnownMemberId(JoinGroupRequestData, short)` /
//! `supportsSkippingAssignment`. Classic JoinGroup two-steps on
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
//! use partitionline::{Acks, Compression, IsolationLevel, ProducerConfig, Sasl};
//!
//! let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
//!     .acks(Acks::All)
//!     .linger(Duration::from_millis(5))
//!     .compression(Compression::Lz4)
//!     .sasl(Sasl::scram_sha256("alice", "secret"));
//!
//! let _iso = IsolationLevel::ReadCommitted;
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
/// Shared config: [`Acks`], [`IsolationLevel`], [`Sasl`].
pub mod config;
/// Fetch client with manual partition assignment.
pub mod consumer;
/// Kafka and client error types.
pub mod error;
/// Consumer-group join / sync / heartbeat / commit.
pub mod group;
/// Produce and fetch interceptors.
pub mod interceptor;
/// Client counters, latency min/mean/max plus p50/p99, and per-topic rows: [`ProducerMetrics`], [`ConsumerMetrics`], [`ShareMetrics`], [`AdminMetrics`].
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
    TopicDescription, TopicListing, TopicPartitionCursor, TopicPartitionReplica,
    TransactionListing, TransactionState, TransactionTopic, UnregisterBrokerResponse, UpgradeType,
    UserScramCredentialAlteration, UserScramCredentialDeletion, UserScramCredentialResult,
    UserScramCredentialUpsertion, Uuid, ALTER_CONFIG_APPEND, ALTER_CONFIG_DELETE, ALTER_CONFIG_SET,
    ALTER_CONFIG_SUBTRACT, AUTHORIZED_OPERATIONS_OMITTED, CONFIG_RESOURCE_BROKER,
    CONFIG_RESOURCE_BROKER_LOGGER, CONFIG_RESOURCE_CLIENT_METRICS, CONFIG_RESOURCE_GROUP,
    CONFIG_RESOURCE_TOPIC, CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_DYNAMIC_BROKER,
    CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER, CONFIG_SOURCE_DYNAMIC_CLIENT_METRICS,
    CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER, CONFIG_SOURCE_DYNAMIC_GROUP, CONFIG_SOURCE_DYNAMIC_TOPIC,
    CONFIG_SOURCE_STATIC_BROKER, CONFIG_SOURCE_UNKNOWN, CONFIG_TYPE_BOOLEAN, CONFIG_TYPE_CLASS,
    CONFIG_TYPE_DOUBLE, CONFIG_TYPE_INT, CONFIG_TYPE_LIST, CONFIG_TYPE_LONG, CONFIG_TYPE_PASSWORD,
    CONFIG_TYPE_SHORT, CONFIG_TYPE_STRING, CONFIG_TYPE_UNKNOWN, DEFAULT_LEAVE_GROUP_REASON,
    ENDPOINT_TYPE_BROKERS, ENDPOINT_TYPE_CONTROLLERS, INVALID_OFFSET_LAG, QUOTA_MATCH_ANY,
    QUOTA_MATCH_DEFAULT, QUOTA_MATCH_EXACT, SCRAM_SHA_256, SCRAM_SHA_512, SCRAM_UNKNOWN,
    UNKNOWN_VOLUME_BYTES, UPGRADE_TYPE_SAFE_DOWNGRADE, UPGRADE_TYPE_UNSAFE_DOWNGRADE,
    UPGRADE_TYPE_UPGRADE,
};
pub use config::{Acks, AutoOffsetReset, IsolationLevel, Sasl};
pub use consumer::{
    Consumer, ConsumerConfig, ConsumerRecords, FetchedRecord, OffsetAndMetadata,
    OffsetAndTimestamp, PartitionInfo, RebalanceListener, TopicIdPartition, TopicPartition,
    WakeupHandle,
};
pub use error::{Error, Result};
pub use group::{
    ConsumerGroup, ConsumerGroupMetadata, CoordinatorType, GroupProtocol,
    DEFAULT_ENFORCE_REBALANCE_REASON, LEAVE_GROUP_REASON_CLOSED, LEAVE_GROUP_REASON_POLL_TIMEOUT,
    LEAVE_GROUP_REASON_UNSUBSCRIBED,
};
pub use interceptor::{ConsumerInterceptor, ProducerInterceptor};
pub use metrics::{
    AdminMetrics, ConsumerMetrics, LatencyStats, ProducerMetrics, ShareMetrics, TopicFetchMetrics,
    TopicProduceMetrics,
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
