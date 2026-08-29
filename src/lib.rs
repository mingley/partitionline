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
//! println!("{}-{}@{}", md.topic, md.partition, md.offset);
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
//! is not spoken.
//! InitProducerId is v0–v5 (v2+ flexible; v3+ KIP-360 ProducerId;
//! first init `-1`/`-1`, epoch-bump resume sends the last id/epoch).
//! Metadata negotiates v1–v13 (v9+ flexible; v13 top-level ErrorCode;
//! v8+ IncludeTopicAuthorizedOperations on [`Admin::describe_topics_by_id_with`];
//! v10+ TopicId on [`Admin::describe_topics_by_id`]).
//! Name-based [`Admin::describe_topics`] uses DescribeTopicPartitions (api 75).
//! Groups and transactions negotiate FindCoordinator v1–v6 (v3+ flexible;
//! v4+ KIP-699 CoordinatorKeys; v5 TRANSACTION_ABORTABLE; v6 share groups),
//! OffsetCommit v2–v9 (v2–v4 retention `-1`; v6+ epoch; v7 GroupInstanceId; v8+ flexible; v9 KIP-848 errors),
//! OffsetFetch v1–v9 (v2 top-level error; v3 throttle; v5 epoch; v6+ flexible; v7 RequireStable; v8 Groups; v9 MemberId),
//! Heartbeat v0–v4 (v1+ throttle; v3 GroupInstanceId; v4 flexible),
//! SyncGroup v0–v5 (v1+ throttle; v3 GroupInstanceId; v4+ flexible; v5 ProtocolType / ProtocolName),
//! JoinGroup v2–v9 (v5 GroupInstanceId; v6+ flexible; v8 Reason; v9 SkipAssignment;
//! Protocols of N via [`ConsumerGroup::join_with_assignors`]),
//! LeaveGroup v0–v5 (v3 Members / GroupInstanceId; v4 flexible; v5 Reason;
//! [`LEAVE_GROUP_REASON_CLOSED`] on leave / close, [`LEAVE_GROUP_REASON_UNSUBSCRIBED`]
//! on unsubscribe, [`LEAVE_GROUP_REASON_POLL_TIMEOUT`] on `max.poll.interval.ms`),
//! SaslHandshake v0–v1 (never flexible; v1 enables SaslAuthenticate),
//! SaslAuthenticate v0–v2 (v1 SessionLifetimeMs; v2 flexible),
//! ApiVersions v0–v4 (v3+ ClientSoftwareName; v4 SupportedFeatures.MinVersion 0; KIP-511 retry),
//! ConsumerGroupHeartbeat v0–v1 (v1 SubscribedTopicRegex / KIP-1082 member id),
//! ShareGroupHeartbeat v0–v1 (v0 Kafka 4.0 early access; v1 Kafka 4.1 stable; same fields),
//! ShareGroupDescribe v0–v1 (v0 Kafka 4.0 early access; v1 Kafka 4.1 stable; same fields; FindCoordinator v4+ CoordinatorKeys of N),
//! ShareFetch v0–v1 (v0 PartitionMaxBytes; v1 MaxRecords / BatchSize / AcquisitionLockTimeoutMs),
//! ShareAcknowledge v0–v1 (v0 Kafka 4.0 early access; v1 Kafka 4.1 stable; same fields),
//! ConsumerGroupDescribe v0–v1 (v1 MemberType; FindCoordinator v4+ CoordinatorKeys of N),
//! ListTransactions v0–v1 (v1 DurationFilter, KIP-994),
//! CreateTopics v0–v7 (v5+ flexible; v5 KIP-525 configs; v7 TopicId),
//! DeleteTopics v0–v6 (v4+ flexible; v5 ErrorMessage; v6 TopicId, `delete_topics_by_id`),
//! DescribeGroups v0–v6 (v3 IncludeAuthorizedOperations; v4 GroupInstanceId; v5 flexible; v6 ErrorMessage; FindCoordinator v4+ CoordinatorKeys of N),
//! ListGroups v0–v5 (v3 flexible; v4 StatesFilter / GroupState; v5 TypesFilter / GroupType),
//! DeleteGroups v0–v2 (v0–v1 classic; v2 flexible; FindCoordinator v4+ CoordinatorKeys of N),
//! DescribeClientQuotas / AlterClientQuotas v0–v1 (v1 flexible),
//! ListConfigResources v0–v1 (v0 ListClientMetricsResources; v1 ResourceTypes),
//! AlterReplicaLogDirs v1–v2 (v1 classic; v2 flexible),
//! DescribeLogDirs v1–v4 (v1 classic; v2+ flexible; v3 ErrorCode; v4 TotalBytes),
//! CreateDelegationToken v1–v3 (v1 classic; v2+ flexible; v3 owner/requester),
//! RenewDelegationToken v1–v2 (v1 classic; v2 flexible),
//! ExpireDelegationToken v1–v2 (v1 classic; v2 flexible),
//! DescribeDelegationToken v1–v3 (v1 classic; v2+ flexible; v3 TokenRequester),
//! DescribeConfigs v0–v4 (v1 synonyms; v3 IncludeDocumentation / ConfigType; v4 flexible),
//! CreatePartitions v0–v3 (v2+ flexible; v3 KIP-599),
//! IncrementalAlterConfigs v0–v1 (v1 flexible; Resources of N),
//! AlterConfigs v0–v2 (v2 flexible; Resources of N),
//! DeleteRecords v0–v2 (v2 flexible),
//! CreateAcls / DescribeAcls / DeleteAcls v0–v3 (v1 ResourcePatternType; v2+ flexible),
//! AddPartitionsToTxn v0–v3 (v3 flexible), AddOffsetsToTxn v0–v4
//! (v3+ flexible; v4 TRANSACTION_ABORTABLE), EndTxn v0–v5
//! (v3+ flexible; v4 TRANSACTION_ABORTABLE; v5 ProducerId / ProducerEpoch),
//! and TxnOffsetCommit v0–v5
//! (v3+ flexible; GenerationId / MemberId / GroupInstanceId;
//! v5 skips AddOffsetsToTxn, KIP-890 Part 2).
//! [`Producer::metrics`] is a snapshot of queued / acked / error counts
//! plus produce-ack latency min/mean/max and p50/p99 (last 1024 samples),
//! with per-topic rows on [`ProducerMetrics::topics`].
//! [`Admin::metrics`] is the same snapshot pattern for Admin RPCs
//! ([`AdminMetrics`]; Java `Admin.metrics()`).
//! [`Producer::client_instance_id`] is Java `clientInstanceId` (KIP-714).
//! [`Producer::client_instance_id_timeout`] is Java `clientInstanceId(Duration)`.
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
//! Fetch `LastFetchedEpoch`). [`Consumer::pause`] / [`Consumer::resume`] skip
//! partitions without dropping the assignment. [`Consumer::fetch`] talks to
//! every partition leader in parallel. Fetch negotiates v4–v17 (v12+ is
//! flexible; v13+ topic IDs, KIP-516; v15 omits untagged ReplicaId, KIP-903;
//! v16 CurrentLeader / NodeEndpoints, KIP-951; v17 omits ReplicaDirectoryId, KIP-853;
//! v12+ LastFetchedEpoch from the last consumed batch, KIP-320). v18+
//! is not spoken. OffsetForLeaderEpoch negotiates v0–v4 (v2 CurrentLeaderEpoch;
//! v3 ReplicaId; v4 flexible; Topics/Partitions of N). v5+ is not spoken. [`ConsumerConfig::max_bytes`] sets
//! both `fetch.max.bytes` and `max.partition.fetch.bytes`;
//! [`ConsumerConfig::fetch_max_bytes`] /
//! [`ConsumerConfig::max_partition_fetch_bytes`] set them independently.
//! [`Consumer::partitions_for`] /
//! [`Producer::partitions_for`] return Metadata (leader, replicas, ISR,
//! [`PartitionInfo::offline_replicas`], [`PartitionInfo::leader_epoch`]).
//! [`Consumer::wakeup`] interrupts fetch
//! (clone [`WakeupHandle`] for another task).
//! [`Consumer::client_instance_id`] is Java `clientInstanceId` (KIP-714).
//! [`Consumer::client_instance_id_timeout`] /
//! [`ConsumerGroup::client_instance_id_timeout`] /
//! [`ShareGroup::client_instance_id_timeout`] /
//! [`Admin::client_instance_id_timeout`] are Java `clientInstanceId(Duration)`.
//! [`Consumer::offsets_for_times`] is Java `offsetsForTimes`
//! ([`OffsetAndTimestamp::leader_epoch`] is Java `getLeaderEpoch`).
//! [`FetchedRecord::leader_epoch`] is the record-batch partition leader epoch.
//! [`FetchedRecord::serialized_key_size`] / [`FetchedRecord::serialized_value_size`]
//! match Java `serializedKeySize` / `serializedValueSize`.
//! [`Admin::create_partitions`] takes [`NewPartitions`].
//! [`NewPartitions::with_assignments`] is Java
//! `NewPartitions.increaseTo(int, List<List<Integer>>)` (null Assignments
//! means the broker assigns replicas).
//! [`NewTopic::with_assignments`] is Java
//! `NewTopic(String, Map<Integer, List<Integer>>)` (NumPartitions /
//! ReplicationFactor `-1`; empty Assignments is `NewTopic(String, int, short)`).
//! [`NewTopic::broker_defaults`] is Java
//! `NewTopic(String, Optional.empty(), Optional.empty())` (KIP-464; `-1` / `-1`).
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
//! [`Admin::list_partition_reassignments_timeout`] is Java
//! `ListPartitionReassignmentsOptions.timeoutMs`.
//! [`Admin::list_partition_reassignments_all`] is Java
//! `listPartitionReassignments()`.
//! [`Admin::list_partition_reassignments_for`] is Java
//! `listPartitionReassignments(Set)`.
//! [`Admin::incremental_alter_configs`] / [`Admin::alter_configs`] take
//! [`ConfigResource`] / [`ConfigResourceType`].
//! [`Admin::incremental_alter_configs_for`] is Java
//! `incrementalAlterConfigs(Map)` ([`ConfigResourceUpdate`]; Resources of N).
//! [`AlterConfig::append`] / [`AlterConfig::subtract`] are Java
//! `AlterConfigOp.OpType.APPEND` / `SUBTRACT` (LIST configs).
//! [`AlterConfig::from_entry`] is Java `AlterConfigOp(ConfigEntry, OpType)`
//! ([`AlterConfigOpType`]). [`AlterConfig::op_type`] is Java
//! `AlterConfigOp.opType()`.
//! [`Admin::alter_configs_for`] is Java `alterConfigs(Map)`
//! ([`ConfigReplacement`]; Resources of N).
//! [`Admin::alter_configs_with`] is Java `alterConfigs(Map)` with a
//! [`Config`] value. [`DescribeConfigsResult::config`] is the Java
//! `describeConfigs` result `Config` (`entries` / `get`).
//! [`ConfigEntry::source`] / [`ConfigEntry::config_type`] /
//! [`ConfigEntry::is_default`] are Java `ConfigEntry.source` / `type` /
//! `isDefault` ([`ConfigSource`] / [`ConfigType`]).
//! [`Admin::incremental_alter_configs_timeout`] /
//! [`Admin::alter_configs_timeout`] are Java `AlterConfigsOptions.timeoutMs`
//! (RPC deadline; these RPCs have no TimeoutMs).
//! [`Consumer::current_lag`] is Java `currentLag`.
//! [`Consumer::list_topics`] is cluster Metadata. [`Consumer::assign_many`]
//! / [`Consumer::assign_partitions`] / [`Consumer::unassign`] replace or
//! drop a manual assignment ([`Consumer::assign_partitions`] is Java
//! `assign(Collection)` and uses [`ConsumerConfig::auto_offset_reset`]).
//! [`Consumer::beginning_offsets`] / [`Consumer::end_offsets`] take
//! [`TopicPartition`]. [`Consumer::list_offset`] is ListOffsets for one
//! partition. [`Consumer::assignment`] is Java `assignment`
//! ([`Consumer::assigned_partitions`] is the same list; [`Consumer::positions`]
//! pairs each partition with its next fetch offset).
//! [`Consumer::fetch`] / [`ConsumerGroup::poll`] return [`ConsumerRecords`]
//! (Java `count` / `partitions` / `records` / `nextOffsets`).
//! [`ShareGroup::poll`] returns [`ShareRecords`]. [`Consumer::fetch_timeout`] /
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
//! [`Admin::delete_records_for`] is Java `deleteRecords(Map)`
//! ([`RecordsToDelete`] / [`DeletedRecords`]; one DeleteRecords RPC per leader).
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
//! [`OffsetSpec`]; one RPC per leader; ListOffsets v1–v10).
//! [`Admin::list_offsets_with_isolation`] is Java `listOffsets` plus
//! `ListOffsetsOptions.isolationLevel`.
//! [`Admin::list_offsets_timeout`] / [`Admin::list_offsets_with_isolation_timeout`]
//! are Java `ListOffsetsOptions.timeoutMs` (RPC deadline and ListOffsets v10 TimeoutMs).
//! [`Admin::list_transactions_with_duration`] is Java `listTransactions`
//! plus `ListTransactionsOptions.filterOnDuration` (ListTransactions v1).
//! [`Admin::list_transactions_timeout`] /
//! [`Admin::list_transactions_with_duration_timeout`] are Java
//! `ListTransactionsOptions.timeoutMs` (RPC deadline; ListTransactions
//! has no TimeoutMs).
//! [`Admin::list_transactions_all`] is Java `listTransactions()`.
//! [`TransactionListing::state`] is Java `TransactionListing.state` as
//! the broker string. [`TransactionState::state`] is Java
//! `TransactionDescription.state`; [`TransactionState::transaction_start_time_ms`]
//! is Java `OptionalLong` (`None` when the wire value is negative).
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
//! IncludeFencedBrokers).
//! [`Admin::describe_cluster_timeout`] / [`Admin::describe_cluster_with_timeout`]
//! are Java `DescribeClusterOptions.timeoutMs` (RPC deadline).
//! [`Admin::update_features_with`] is Java `updateFeatures` plus
//! `UpdateFeaturesOptions.validateOnly` (UpdateFeatures v0–v2; v1
//! UpgradeType / ValidateOnly; v2 omits Results).
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
//! DescribeGroups).
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
//! [`Admin::describe_share_group_offsets_timeout`] /
//! [`Admin::list_share_group_offsets_timeout`] are Java
//! `ListShareGroupOffsetsOptions.timeoutMs` (RPC deadline;
//! DescribeShareGroupOffsets has no TimeoutMs).
//! [`Admin::alter_share_group_offsets_timeout`] /
//! [`Admin::delete_share_group_offsets_timeout`] are Java
//! `AlterShareGroupOffsetsOptions` / `DeleteShareGroupOffsetsOptions.timeoutMs`
//! (RPC deadline; these RPCs have no TimeoutMs).
//! [`Admin::delete_consumer_group_offsets`] is Java `deleteConsumerGroupOffsets`.
//! [`Admin::delete_offsets_timeout`] / [`Admin::delete_consumer_group_offsets_timeout`]
//! are Java `DeleteConsumerGroupOffsetsOptions.timeoutMs` (RPC deadline;
//! OffsetDelete has no TimeoutMs).
//! [`Admin::alter_consumer_group_offsets_timeout`] is Java
//! `AlterConsumerGroupOffsetsOptions.timeoutMs` (RPC deadline;
//! OffsetCommit has no TimeoutMs).
//! [`Admin::delete_share_groups`] is Java `deleteShareGroups` (DeleteGroups).
//! [`Admin::abort_transaction`] is Java `abortTransaction`
//! ([`AbortTransactionSpec`]; WriteTxnMarkers v0–1).
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
//! [`Admin::alter_user_scram_credentials_with`] is Java
//! `alterUserScramCredentials(List)` ([`UserScramCredentialAlteration`]).
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
//! `describeTopics(TopicCollection.ofTopicIds)` (Metadata v10+)
//! ([`TopicCollection`] / [`TopicListing`] / [`TopicDescription`] / [`Uuid`]).
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
//! [`AclBinding::allow_topic`] / [`AclBinding::new`] / [`AclBindingFilter`] /
//! [`ResourcePattern`] / [`AccessControlEntry`] / [`AclResourceType`] /
//! [`AclOperation`] / [`AclPermission`] cover CreateAcls / DescribeAcls /
//! DeleteAcls. [`Admin::describe_acls_with`] is Java `describeAcls(AclBindingFilter)`.
//! [`Admin::describe_acls_any`] is Java `describeAcls(AclBindingFilter.ANY)`.
//! [`Admin::delete_acls_with`] is Java `deleteAcls(Collection)` (DeleteAcls Filters of N).
//! [`Admin::create_acls_timeout`] / [`Admin::describe_acls_timeout`] /
//! [`Admin::delete_acls_timeout`] are Java `CreateAclsOptions` /
//! `DescribeAclsOptions` / `DeleteAclsOptions.timeoutMs` (RPC deadline).
//! [`Producer::init_transactions`] / [`Producer::flush_timeout`] /
//! [`Producer::close_timeout`] match Java. [`Consumer::close_timeout`]
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
//! (JoinGroup Protocols of N), and
//! [`ConsumerGroup::join_consumer`] is KIP-848. Each has a
//! `_topics` variant for several topics. [`ConsumerGroup::join_matching`] /
//! [`ConsumerGroup::join_sticky_matching`] /
//! [`ConsumerGroup::join_cooperative_sticky_matching`] /
//! [`ConsumerGroup::join_consumer_matching`] are Java `subscribe(Pattern)`
//! at join (range, sticky, cooperative-sticky, KIP-848).
//! [`ConsumerConfig::group_instance_id`] is static membership.
//! [`ConsumerConfig::auto_offset_reset`] is used when OffsetFetch has no
//! committed offset. [`ShareGroup`] is KIP-932 (`join` / [`ShareGroup::join_topics`] /
//! [`ShareGroup::join_matching`] / [`ShareGroup::subscribe`] /
//! [`ShareGroup::subscribe_matching`] / [`ShareGroup::unsubscribe`] /
//! [`ShareGroup::accept`] / [`ShareGroup::release`] / [`ShareGroup::reject`]).
//! [`Consumer::seek_with_metadata`] / [`ConsumerGroup::seek_with_metadata`]
//! are Java `seek(TopicPartition, OffsetAndMetadata)` (Fetch
//! `LastFetchedEpoch` from the leader epoch; metadata string ignored).
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
//! [`Producer::send_offsets_with_metadata`] / [`Producer::send_offsets_for_group`]
//! commit transactional offsets with epoch and metadata.
//! `send_offsets_for_group` also sends generation / member / instance on
//! TxnOffsetCommit v3+.
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
//! [`ProducerConfig::delivery_timeout`] is Kafka `delivery.timeout.ms`
//! (default 30s; Java defaults to 120s). [`ProducerConfig::max_block`] is
//! Kafka `max.block.ms` (how long `send` waits for metadata and
//! [`ProducerConfig::buffer_memory`]; default 30s, Java 60s).
//! [`ProducerConfig::buffer_memory`] is Kafka `buffer.memory` (queued
//! key-plus-value bytes not yet acked; default 32 MiB, Java; zero is no
//! client-side cap). [`ProducerConfig::max_request_size`] is Kafka
//! `max.request.size` (key-plus-value bytes of one record; default 1 MiB,
//! Java; zero is no extra cap; oversized records return
//! [`Error::RecordTooLarge`]). [`ProducerConfig::retry_backoff`] /
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
    AclBindingFilter, AclOperation, AclPatternType, AclPermission, AclResourceType, ActiveProducer,
    Admin, AdminConfig, AlterConfig, AlterConfigOp, AlterConfigOpType, AlterConfigsResourceResult,
    AlterReplicaLogDirsDirectory, AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse,
    AlterReplicaLogDirsResponsePartition, AlterReplicaLogDirsResponseTopic,
    AlterReplicaLogDirsTopic, AlterShareGroupOffsetsPartition, AlterShareGroupOffsetsTopic,
    AlteredShareGroupOffsets, AlteredShareGroupOffsetsPartition, AlteredShareGroupOffsetsTopic,
    AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition, AssignReplicasToDirsRequest,
    AssignReplicasToDirsResponse, AssignReplicasToDirsResponseDirectory,
    AssignReplicasToDirsResponsePartition, AssignReplicasToDirsResponseTopic,
    AssignReplicasToDirsTopic, ClientQuotaAlteration, ClientQuotaAlterationResult,
    ClientQuotaEntity, ClientQuotaEntry, ClientQuotaFilter, ClientQuotaFilterComponent,
    ClientQuotaOp, ClientQuotaValue, ClusterDescription, Config, ConfigEntry, ConfigReplacement,
    ConfigResource, ConfigResourceType, ConfigResourceUpdate, ConfigSource, ConfigType,
    ConsumerGroupAssignment, ConsumerGroupDescription, ConsumerGroupMember,
    ConsumerGroupTopicPartitions, CreatableRenewer, CreateDelegationTokenRequest,
    CreateDelegationTokenResponse, DeletableGroupResult, DeleteShareGroupOffsetsTopic,
    DeletedAclsFilterResult, DeletedRecords, DeletedShareGroupOffsets,
    DeletedShareGroupOffsetsTopic, DescribableLogDirTopic, DescribeClusterBroker,
    DescribeDelegationTokenOwner, DescribeDelegationTokenRequest, DescribeDelegationTokenResponse,
    DescribeLogDirsPartition, DescribeLogDirsRequest, DescribeLogDirsResponse,
    DescribeLogDirsResult, DescribeLogDirsTopic, DescribeProducersPartition,
    DescribeProducersTopic, DescribeShareGroupOffsetsGroup, DescribeShareGroupOffsetsTopic,
    DescribeTopicPartitionsResponse, DescribeUserScramCredentialsResult, DescribedConsumerGroup,
    DescribedDelegationToken, DescribedDelegationTokenRenewer, DescribedGroup,
    DescribedGroupMember, DescribedShareGroup, DescribedShareGroupOffsets,
    DescribedShareGroupOffsetsPartition, DescribedShareGroupOffsetsTopic, DescribedTopicPartition,
    DescribedTopicPartitions, EndpointType, ExpireDelegationTokenRequest,
    ExpireDelegationTokenResponse, FeatureMetadata, FeatureUpdate, FeatureUpdateResult,
    FencedProducer, FinalizedVersionRange, GetTelemetrySubscriptionsResponse, GroupState,
    GroupType, ListConsumerGroupOffsetsSpec, ListedConfigResource, ListedGroup, MemberToRemove,
    NewPartitionReassignment, NewPartitions, NewTopic, OffsetDeleteResult, OngoingReassignment,
    PartitionReassignment, ProducerIdBlock, PushTelemetryResponse, ReassignmentResult,
    RecordsToDelete, RemovedMember, RenewDelegationTokenRequest, RenewDelegationTokenResponse,
    ReplicaLogDirInfo, ResourcePattern, ResourcePatternFilter, ScramCredentialInfo, ScramMechanism,
    ShareGroupAssignment, ShareGroupMember, ShareGroupTopicPartitions, SupportedVersionRange,
    TopicCollection, TopicDescription, TopicListing, TopicPartitionCursor, TopicPartitionReplica,
    TransactionListing, TransactionState, TransactionTopic, UpgradeType,
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
    ENDPOINT_TYPE_BROKERS, ENDPOINT_TYPE_CONTROLLERS, QUOTA_MATCH_ANY, QUOTA_MATCH_DEFAULT,
    QUOTA_MATCH_EXACT, SCRAM_SHA_256, SCRAM_SHA_512, SCRAM_UNKNOWN, UPGRADE_TYPE_SAFE_DOWNGRADE,
    UPGRADE_TYPE_UNSAFE_DOWNGRADE, UPGRADE_TYPE_UPGRADE,
};
pub use config::{Acks, AutoOffsetReset, IsolationLevel, Sasl};
pub use consumer::{
    Consumer, ConsumerConfig, ConsumerRecords, FetchedRecord, OffsetAndMetadata,
    OffsetAndTimestamp, PartitionInfo, RebalanceListener, TopicPartition, WakeupHandle,
};
pub use error::{Error, Result};
pub use group::{
    ConsumerGroup, ConsumerGroupMetadata, DEFAULT_ENFORCE_REBALANCE_REASON,
    LEAVE_GROUP_REASON_CLOSED, LEAVE_GROUP_REASON_POLL_TIMEOUT, LEAVE_GROUP_REASON_UNSUBSCRIBED,
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
pub use protocol::records::{Compression, Header, Record, RecordBatch};
pub use share::{
    ShareGroup, ShareRecord, ShareRecords, SHARE_ACK_ACCEPT, SHARE_ACK_REJECT, SHARE_ACK_RELEASE,
};

/// Software name sent in ApiVersions v3–v4.
pub const CLIENT_NAME: &str = "partitionline";
/// Crate version sent in ApiVersions v3–v4.
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
