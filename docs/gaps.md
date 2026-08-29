# Gaps vs librdkafka

This is the tracker for **full client parity with librdkafka**. It is not a
promise that every row ships in the next commit. Status is the contract:

| Status | Meaning |
|---|---|
| **done** | Callers can use it in this crate today |
| **in progress** | Work has started in-tree |
| **not started** | Tracked, not implemented |
| **blocked on C** | Typical Kafka ecosystem implementation is a C library; default features will not take that dependency |

librdkafka is the C client. Matching it means matching **behavior a Kafka
application needs**, not cloning `rd_kafka_*` symbols.

## Inventory

| Capability | partitionline | librdkafka | Status |
|---|---|---|---|
| Produce (acks, linger, batches, offsets, `delivery.timeout.ms`, `max.block.ms`, `buffer.memory`, `max.request.size`, `retry.backoff.ms`, `reconnect.backoff.ms`, `connections.max.idle.ms`, `metadata.max.age.ms`) | yes (Produce v3–v12; v9+ flexible; v10+ KIP-951 CurrentLeader tagged fields applied when the leader is a known broker, NodeEndpoints inserted first when it is not; v12 skips AddPartitionsToTxn, KIP-890 Part 2; Metadata v1–v13, v13 top-level ErrorCode; to Metadata leader; retriable errors refresh and retry with exponential `retry_backoff` / `retry_backoff_max`; failed broker TCP/handshake retries with exponential `reconnect_backoff` / `reconnect_backoff_max`; idle TCP connections close after `connections_max_idle`; `delivery_timeout` caps queue-to-ack; `max_block` caps `send` metadata and `buffer.memory` wait; `buffer_memory` caps queued key+value bytes; `max_request_size` rejects oversized records with `Error::RecordTooLarge` and caps Produce batches; `metadata_max_age` refreshes cached Metadata) | yes | **done** |
| Fetch with manual assignment | yes (Fetch v4–v17; v12+ flexible; v13+ topic IDs, KIP-516; v15 omits untagged ReplicaId, KIP-903; v16 same request as v15; v12+ CurrentLeader tagged field 1 applied when the leader is a known broker; v16+ NodeEndpoints tagged field 0 inserted first when it is not; v17 omits ReplicaDirectoryId, KIP-853; to Metadata leader; retriable errors wait `retry.backoff.ms` then refresh and retry; preferred-replica redirects are immediate; failed broker TCP/handshake retries with `reconnect.backoff.ms`; idle TCP connections close after `connections.max.idle.ms`; `metadata.max.age.ms` refreshes cached Metadata; Fetch sends Metadata `leader_epoch`; `ConsumerRecords` including `nextOffsets`; `allow.auto.create.topics` on Metadata; `fetch.max.bytes` / `max.partition.fetch.bytes` are independent, `max_bytes()` sets both) | yes | **done** |
| OffsetForLeaderEpoch / fetch fencing | yes (api 23; `FENCED_LEADER_EPOCH` / `UNKNOWN_LEADER_EPOCH` recover then fetch) | yes | **done** |
| ListOffsets, seek, `isolation.level` | yes (earliest/latest/max-timestamp/earliest-local/latest-tiered; ListOffsets v1–v10; v10 TimeoutMs is `request_timeout`; `offsets_for_times` with `OffsetAndTimestamp.leader_epoch`; Fetch isolation 0 or 1; `FetchedRecord.leader_epoch`) | yes | **done** |
| Classic consumer groups (join / sync / heartbeat / commit) | yes (range then sticky over all partitions; several topics via `join_topics`; cooperative-sticky / KIP-429; `group.instance.id`; heartbeat loop; rebalance; LeaveGroup v0–v5, v3 Members / GroupInstanceId, v4 flexible, v5 Reason; FindCoordinator v1–v6, v3+ flexible, v4+ CoordinatorKeys; OffsetCommit v7–v9, v8+ flexible; OffsetFetch v5–v9, v6+ flexible, v7 RequireStable, v8 Groups, v9 MemberId; Heartbeat v0–v4, v1+ throttle, v3 GroupInstanceId, v4 flexible; SyncGroup v0–v5, v1+ throttle, v3 GroupInstanceId, v4+ flexible, v5 ProtocolType / ProtocolName; JoinGroup v2–v9, v5 GroupInstanceId, v6+ flexible, v8 Reason, v9 SkipAssignment; coordinator sockets close after `connections.max.idle.ms`) | yes | **done** |
| gzip | yes (`flate2` rust backend) | yes | **done** |
| snappy | yes (`snap`, snappy-java framing on produce; raw snappy on fetch) | yes | **done** |
| SASL PLAIN | yes (SaslHandshake v0–v1; SaslAuthenticate v0–v2, v2 flexible) | yes | **done** |
| SASL SCRAM-SHA-256 | yes (RFC 5802/7677, PBKDF2-HMAC-SHA-256, no C SASL library) | yes | **done** |
| Java murmur2 partitioner | yes | optional (`murmur2`) | **done** |
| TLS / SSL | yes (`rustls` + `ring`, no OpenSSL; custom CA PEM or webpki-roots; optional mTLS) | yes (OpenSSL) | **done** |
| lz4 | yes (`lz4_flex` frame, independent 64KiB blocks, magic ≥ 1) | yes | **done** |
| zstd | no | yes (libzstd C) | **blocked on C** |
| SASL SCRAM-SHA-512 | yes (RFC 5802, PBKDF2-HMAC-SHA-512, no C SASL library) | yes | **done** |
| SASL GSSAPI / Kerberos | no | yes (cyrus-sasl C) | **blocked on C** |
| SASL OAUTHBEARER | yes (RFC 7628, unsecured JWT `alg=none`, matches librdkafka `enable.sasl.oauthbearer.unsecure.jwt`) | yes | **done** |
| SASL OIDC (token endpoint) | yes (RFC 6749 client_credentials `http://` or `https://` rustls POST, then OAUTHBEARER) | yes | **done** |
| Idempotent produce (`enable.idempotence`, PID/epoch/seq) | yes (`InitProducerId` v0–v5, v2+ flexible, v3+ KIP-360 ProducerId on the request with first-init `-1`; UNKNOWN_PRODUCER_ID bumps the epoch locally and retries; per-partition sequences, one TCP conn per partition, acks=all, max in-flight 5; `flush` fails on broker error) | yes | **done** |
| Transactions / EOS | yes (`transactional.id`, `transaction.timeout.ms` on InitProducerId, `init_transactions`, begin/commit/abort, FindCoordinator v1–v6 for the txn coordinator, AddPartitionsToTxn v0–v3 with v3 flexible, AddOffsetsToTxn v0–v4 with v3+ flexible, EndTxn v0–v5 with v3+ flexible and v5 ProducerId / ProducerEpoch apply, TxnOffsetCommit v0–v5 with v3+ flexible, GenerationId / MemberId / GroupInstanceId, v5 skipping AddOffsetsToTxn, and Produce v12 skipping AddPartitionsToTxn; after UNKNOWN_PRODUCER_ID / INVALID_PRODUCER_EPOCH / INVALID_PRODUCER_ID_MAPPING, abort still EndTxn-aborts then re-inits with the last producer id and epoch when EndTxn is below v5) | yes | **done** |
| Admin: CreateTopics, DeleteTopics, DescribeConfigs | yes (CreateTopics v0–7, v5+ flexible, v5 KIP-525 configs, v7 TopicId; DeleteTopics v0–6, v4+ flexible, v5 ErrorMessage, v6 TopicId; DescribeConfigs v0–4, v4 flexible, v3 IncludeDocumentation / ConfigType; CreatePartitions v0–3, v2+ flexible); `NewPartitions` for CreatePartitions; admin RPCs wait exponential `retry.backoff.ms` / `retry.backoff.max.ms` on `NOT_CONTROLLER`, coordinator moves, and retriable IO; idle bootstrap sockets close after `connections.max.idle.ms` | yes | **done** |
| Admin: IncrementalAlterConfigs, CreatePartitions, ACLs, OffsetDelete, OffsetFetch (`listConsumerGroupOffsets`), OffsetCommit (`alterConsumerGroupOffsets`), ListOffsets (`listOffsets` v1–v10, one RPC per leader, `list_offsets_with_isolation`), FenceProducers (`fenceProducers`), AbortTransaction (`abortTransaction`, WriteTxnMarkers v0–1), LeaveGroup v3–v5 (`removeMembersFromConsumerGroup`, `removeAll`), DescribeFeatures (`describeFeatures`, ApiVersions v3–v4, KAFKA-17011 MinVersion 0), AlterPartitionReassignments, ListPartitionReassignments, UpdateFeatures (v0–2, v1 UpgradeType / ValidateOnly, v2 omits Results, `update_features_with`), AlterUserScramCredentials, DescribeUserScramCredentials, AlterClientQuotas (v0–v1), DescribeClientQuotas (v0–v1), DescribeProducers, AllocateProducerIds, DescribeTransactions, ListTransactions (`list_transactions` v0–v1, `list_transactions_with_duration`), UnregisterBroker, ConsumerGroupDescribe (v0–v1), DescribeGroups (`describe_classic_groups`, `describe_consumer_groups`, v0–v6), ListGroups (`list_consumer_groups`, v0–v5), DeleteGroups (`delete_share_groups`, `delete_consumer_groups`, v0–v2), ShareGroupDescribe (`describe_share_groups`, v0–v1), DescribeShareGroupOffsets (`list_share_group_offsets`), AlterShareGroupOffsets, DeleteShareGroupOffsets, DescribeTopicPartitions, ListConfigResources (`list_client_metrics_resources`, v0–v1), GetTelemetrySubscriptions, PushTelemetry, AssignReplicasToDirs, AlterReplicaLogDirs (v1–v2), DescribeLogDirs (v1–v4), CreateDelegationToken (v1–v3), RenewDelegationToken (v1–v2), ExpireDelegationToken (v1–v2), DescribeDelegationToken (v1–v3) | yes (`incremental_alter_configs` v0–1, v1 flexible; CreateAcls / DescribeAcls / DeleteAcls v0–3, v1 ResourcePatternType, v2+ flexible; DescribeClientQuotas / AlterClientQuotas v0–1, v1 flexible; ListConfigResources v0–1, v0 names only, v1 ResourceTypes; AlterReplicaLogDirs v1–2, v2 flexible; DescribeLogDirs v1–4, v3 ErrorCode, v4 TotalBytes; CreateDelegationToken v1–3, v1 classic, v2 flexible, v3 owner/requester; RenewDelegationToken v1–2, v1 classic, v2 flexible; ExpireDelegationToken v1–2, v1 classic, v2 flexible; DescribeDelegationToken v1–3, v1 classic, v2 flexible, v3 TokenRequester; ConsumerGroupDescribe v0–1, v1 MemberType; DescribeGroups v0–6, v5+ flexible, v6 ErrorMessage; ListGroups v0–5, v3+ flexible, v4 StatesFilter, v5 TypesFilter; DeleteGroups v0–2, v2 flexible, throttle v0+; `alter_configs` take `ConfigResource`; `ConfigResourceType`; `ScramMechanism`; `AlterConfig::set` / `delete`; `list_consumer_group_offsets` / `alter_consumer_group_offsets`; `list_offsets` / `list_offsets_with_isolation`; `list_transactions` / `list_transactions_with_duration`; `fence_producers` / `force_terminate_transaction`; `abort_transaction` (WriteTxnMarkers v0–1); `remove_members_from_consumer_group` / `remove_all_members_from_consumer_group`; `describe_features`; `update_features` / `update_features_with`) | yes | **done** |
| Admin: AlterConfigs, DeleteRecords, DescribeCluster | yes (legacy AlterConfigs 33 v0–2, v2 flexible; DeleteRecords 21 v0–2, v2 flexible; DescribeCluster 60 v0–2, v1 EndpointType, v2 IncludeFencedBrokers / IsFenced; `describe_cluster_with`); `Admin::close` / `Admin::close_timeout`; `delete_records` / `describe_producers` / `list_offsets` / `delete_offsets` / `delete_consumer_group_offsets` take `TopicPartition`; `PartitionReassignment::assign` takes `TopicPartition`; `AclBinding::allow_topic` / `AclResourceType` / `AclOperation` / `AclPermission`; `list_topics` / `describe_topics` (`TopicListing` / `TopicDescription`); `describe_replica_log_dirs` (`TopicPartitionReplica` / `ReplicaLogDirInfo`); `describe_broker_log_dirs` (Java `describeLogDirs(Collection<Integer>)`) | yes | **done** |
| KIP-848 next-gen consumer groups | yes (`ConsumerGroup::join_consumer` / `join_consumer_topics`, ConsumerGroupHeartbeat api 68; `group.instance.id` and `client.rack`; classic Join/Sync still work) | yes (newer releases) | **done** |
| Fetch from follower / rack awareness | yes (`ConsumerConfig.rack`; follow Fetch `preferred_read_replica`) | yes | **done** |
| Pause / resume, position, `max.poll.records` | yes (`TopicPartition` on `assignment` / pause/resume/paused/`seek_to`/`position_of`/`seek_to_beginning_of`/`seek_to_end_of`; `assign_partitions` is Java `assign(Collection)` using `auto.offset.reset`; rebalance listener too) | yes | **done** |
| `auto.offset.reset`, `committed` | yes (`Earliest` default; Java is `latest`; `committed_timeout` is Java `committed(Duration)`; group/share RPCs use `request_timeout`) | yes | **done** |
| Custom partitioner | yes (`Partitioner` trait; default murmur2 / round-robin) | yes | **done** |
| `partitionsFor` | yes (`Producer::partitions_for` / `Producer::partitions_for_timeout` / `Consumer::partitions_for` / `Consumer::partitions_for_timeout`; `PartitionInfo.leader_epoch` / `offline_replicas`) | yes | **done** |
| Client metrics | yes (`Producer::metrics` / `Consumer::metrics` / `ShareGroup::metrics` / `Admin::metrics` counters plus produce-ack / fetch-round / Admin-RPC latency min/mean/max and p50/p99 over the last 1024 samples; per-topic rows on produce/fetch `topics`; share includes bytes/errors; `AdminMetrics.errors` is I/O not broker `error_code`) | yes | **done** |
| `clientInstanceId` | yes (`Producer` / `Consumer` / `ConsumerGroup` / `ShareGroup` / `Admin`; KIP-714 GetTelemetrySubscriptions, cached after first call) | yes | **done** |
| `max.poll.interval.ms` | yes (poll error and heartbeat LeaveGroup) | yes | **done** |
| `wakeup()` | yes (`Consumer::wakeup` / `WakeupHandle`; interrupts in-flight Fetch) | yes | **done** |
| Interceptors | yes (`ProducerInterceptor` / `ConsumerInterceptor`; `close`; consumer `on_commit`) | yes | **done** |
| OffsetAndMetadata / commit metadata | yes (`commit_with_metadata`; `commit_timeout` / `commit_with_metadata_timeout` are Java `commitSync(Duration)`; `ConsumerRecords::next_offsets`; OffsetCommit v7–v9 epoch + metadata, v8+ flexible) | yes | **done** |
| `currentLag` | yes (`Consumer::current_lag` / `ConsumerGroup::current_lag`) | yes | **done** |
| `enforceRebalance` | yes (`ConsumerGroup::enforce_rebalance` on next poll) | yes | **done** |
| `subscribe` / `unsubscribe` | yes (`ConsumerGroup` and `ShareGroup`; `subscribe_matching` / `join_matching` / `join_sticky_matching` / `join_cooperative_sticky_matching` / `join_consumer_matching` are Java `subscribe(Pattern)`, re-list on poll; `Consumer::assign_many` / `assign_partitions` / `unassign`) | yes | **done** |
| `listTopics` / `ConsumerGroupMetadata` | yes (`list_topics_timeout` is Java `listTopics(Duration)`; `partitions_for_timeout` / `beginning_offsets_timeout` / `end_offsets_timeout` / `offsets_for_times_timeout` match the Java `Duration` overloads) | yes | **done** |
| `poll(Duration)` | yes (`fetch_timeout` / `poll_timeout` on consumer, group, and share; `ConsumerRecords` / `ShareRecords`) | yes | **done** |
| `close(Duration)` | yes (`Producer::close_timeout`; `Consumer::close_timeout` drops fetch connections; `ConsumerGroup::close_timeout` / `ShareGroup::close_timeout` cap `leave`; `Admin::close_timeout` unused duration) | yes | **done** |
| TxnOffsetCommit metadata | yes (`send_offsets_to_transaction` / `send_offsets_with_metadata` / `send_offsets_for_group` take `TopicPartition`; v3+ sends generation / member / instance from `ConsumerGroupMetadata`) | yes | **done** |
| Share groups | yes (`ShareGroup::join` / `join_topics` / `join_matching` / `subscribe` / `subscribe_matching` / `poll` / `accept` / `release` / `leave`; `ShareRecords`; ShareGroupHeartbeat 76 v0–v1, ShareFetch 78 v0–v1, ShareAcknowledge 79 v0–v1; ACCEPT/RELEASE; queue sharing; coordinator sockets close after `connections.max.idle.ms`) | yes | **done** |
| Schema Registry | no | via extras | **not started** (out of scope) |

TLS produce vs C **was measured** on a dedicated `apache/kafka:3.9.1` SSL
listener (`localhost:9093`). SCRAM-SHA-256 and SCRAM-SHA-512 produce vs C
**were measured** on SASL_PLAINTEXT `localhost:9095` (admin `localhost:9096`).
OAUTHBEARER produce vs C **was measured** on SASL_PLAINTEXT `localhost:9097`
(admin `localhost:9098`). Fetch writeup vs rust-rdkafka **0.39.0** **was
recorded** on this-VM 2026-08-28 against Apache Kafka 3.9.1 KRaft
(`examples/bench_fetch.rs` vs a standalone `rdkafka` 0.39.0 `BaseConsumer`,
not in this crate). Produce-ack latency vs rust-rdkafka **0.39.0**
`FutureProducer` **was recorded** on this-VM 2026-08-28
(`examples/bench_latency.rs` vs a standalone crate, not in this package).
Both writeups are **unsigned** until Kernel Integrity signs. See
`docs/benchmark.md` and `docs/STATUS.md`. Mock produce over
OAUTH is `sasl_oauthbearer_then_produce` in `tests/full_surface.rs`. Mock
admin is `admin_create_then_produce_fetch`.

## Notes on the C-blocked rows

- **zstd**: Kafka-world zstd is almost always `libzstd` (`zstd-sys`). A default
  feature that links it would violate the no-C-codec rule. Pure-Rust zstd
  exists as research; it is not the Kafka ecosystem default.
- **GSSAPI**: librdkafka talks to Cyrus SASL. No plan to vendor that as a
  default feature.

## Next implementation order

1. Schema Registry stays out of crate scope. Latency vs C 2.15.0 / Java is
   not claimed. This-VM produce-ack vs rust-rdkafka 0.39.0 is recorded and
   **unsigned** (see `docs/benchmark.md`).

## What “done” on this list does *not* mean

It does not mean a drop-in `rd_kafka_*` C API or rust-rdkafka types. Fetch
throughput vs rust-rdkafka 0.39.0 is recorded in `docs/benchmark.md` and is
**unsigned**. Produce-ack latency vs the same rust-rdkafka 0.39.0 on this-VM
is also recorded there and is **unsigned**. Neither is a Suite HOLD lift.
