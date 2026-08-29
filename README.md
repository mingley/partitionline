# partitionline

A Kafka client written in Rust. It does not call into C or librdkafka.

[![ci](https://github.com/mingley/partitionline/actions/workflows/ci.yml/badge.svg)](https://github.com/mingley/partitionline/actions/workflows/ci.yml)

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline" }
```

## Produce

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{ProduceRecord, Producer};

let producer = Producer::connect("127.0.0.1:9092").await?;
let md = producer
    .send(ProduceRecord::to("events").value(&b"hello"[..]))
    .await?;
println!("{}-{}@{}", md.topic, md.partition, md.offset);
producer.close().await?;
# Ok(())
# }
```

`send` waits for that record's offset. For many records, `send_all` queues
then waits; `try_send` plus `flush` is the throughput path.

## Fetch

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::Consumer;

let mut consumer = Consumer::connect("127.0.0.1:9092").await?;
consumer.assign("events", 0, 0).await?;
let recs = consumer.fetch().await?;
# let _ = recs;
# Ok(())
# }
```

`fetch` / group `poll` return `ConsumerRecords` (Java `count` /
`partitions` / `records` / `nextOffsets`). Share `poll` returns `ShareRecords`.
`assign_topic` assigns every partition. `seek` / `seek_to` /
`seek_with_metadata` / `seek_to_beginning` / `seek_to_end` /
`seek_to_beginning_of` / `seek_to_end_of` move the next fetch offset
(`seek_with_metadata` is Java `seek(TopicPartition, OffsetAndMetadata)`
and sends the leader epoch as Fetch `LastFetchedEpoch`). `pause` /
`resume` skip partitions without dropping the assignment. `fetch` talks to every
partition leader at once when there is more than one. Fetch v12+ sends
`LastFetchedEpoch` from the last consumed batch (or from
`seek_with_metadata`) and seeks on `DivergingEpoch`. Fenced Fetch
partitions recover with OffsetForLeaderEpoch Topics/Partitions of N (one
RPC per leader).
`ConsumerConfig::max_bytes` sets both `fetch.max.bytes` and
`max.partition.fetch.bytes`; `fetch_max_bytes` / `max_partition_fetch_bytes`
set them independently. `partitions_for`
returns Metadata (leader, replicas, ISR, offline replicas, leader epoch) on both `Consumer` and
`Producer`. `beginning_offsets` / `end_offsets` take `TopicPartition`.
`list_offset` is ListOffsets for one partition.
`commit_offsets` takes `TopicPartition` (or anything that converts to one)
plus the next fetch offset. `assignment` is Java `assignment` (`positions`
is next fetch offset). `Admin::delete_records` / `describe_producers` /
`describe_producers_for` / `list_offsets` / `delete_offsets` / `list_consumer_group_offsets` /
`alter_consumer_group_offsets` take `TopicPartition`.
`Admin::describe_producers_for` is Java `describeProducers(Collection)` (Topics of N per leader).
`Admin::describe_producers_for_on_broker` is Java `DescribeProducersOptions.brokerId`.
`Admin::describe_producers_timeout` / `describe_producers_for_timeout` are Java `DescribeProducersOptions.timeoutMs`.
`Admin::list_all_consumer_group_offsets` is Java `listConsumerGroupOffsets(groupId)` (OffsetFetch null Topics).
`Admin::list_consumer_group_offsets_with` is Java `ListConsumerGroupOffsetsOptions.requireStable` / `timeoutMs`.
`Admin::list_consumer_group_offsets_for_groups` is Java `listConsumerGroupOffsets(Map)` (`ListConsumerGroupOffsetsSpec`; OffsetFetch v8+ Groups array of N; FindCoordinator v4+ CoordinatorKeys array of N).
`Admin::delete_records_for` is Java `deleteRecords(Map)` (one DeleteRecords RPC per leader).
`Admin::delete_records_timeout` / `delete_records_for_timeout` are Java `DeleteRecordsOptions.timeoutMs`.
`NewTopic::with_assignments` is Java `NewTopic(String, Map<Integer, List<Integer>>)`.
`NewTopic::broker_defaults` is Java `NewTopic(String, Optional.empty(), Optional.empty())` (KIP-464).
`NewTopic::configs` is Java `NewTopic.configs(Map)`.
`Admin::create_topics_timeout` is Java `CreateTopicsOptions.timeoutMs`.
`Admin::create_topics_with_quota_retry` is Java `CreateTopicsOptions.retryOnQuotaViolation` (default `true`; KIP-599).
`Admin::delete_topics_timeout` is Java `DeleteTopicsOptions.timeoutMs`.
`Admin::delete_topics_with_quota_retry` is Java `DeleteTopicsOptions.retryOnQuotaViolation` (default `true`; KIP-599).
`Admin::delete_topics_by_id` is Java `deleteTopics(TopicCollection.ofTopicIds)` (DeleteTopics v6 null Name + TopicId).
`Admin::delete_topics_by_id_with_quota_retry` is Java `DeleteTopicsOptions.retryOnQuotaViolation` on TopicId deletes.
`Admin::describe_topics_by_id` is Java `describeTopics(TopicCollection.ofTopicIds)` (Metadata v10+ null Name + TopicId).
`Admin::describe_topics` is Java `describeTopics(TopicCollection.ofTopicNames)` (DescribeTopicPartitions api 75; Metadata fallback when the broker omits api 75).
`Admin::describe_topics_timeout` is Java `DescribeTopicsOptions.timeoutMs`.
`Admin::create_partitions_timeout` is Java `CreatePartitionsOptions.timeoutMs`.
`Admin::create_partitions_with_quota_retry` is Java `CreatePartitionsOptions.retryOnQuotaViolation` (default `true`; KIP-599).
`Admin::alter_partition_reassignments_timeout` is Java `AlterPartitionReassignmentsOptions.timeoutMs`.
`Admin::list_partition_reassignments_timeout` is Java `ListPartitionReassignmentsOptions.timeoutMs`.
`Admin::list_partition_reassignments_all` is Java `listPartitionReassignments()`. `Admin::list_partition_reassignments_for` is Java `listPartitionReassignments(Set)`.
`Admin::list_offsets` is Java `listOffsets` (`OffsetAndTimestamp`).
`Admin::list_offsets_with_isolation` is Java `ListOffsetsOptions.isolationLevel`.
`Admin::list_offsets_timeout` / `list_offsets_with_isolation_timeout` are Java `ListOffsetsOptions.timeoutMs`.
`Admin::list_transactions_with_duration` is Java `ListTransactionsOptions.filterOnDuration`.
`Admin::list_transactions_all` is Java `listTransactions()`.
`Admin::describe_configs_with_documentation` is Java `DescribeConfigsOptions.includeDocumentation`.
`Admin::describe_configs_timeout` is Java `DescribeConfigsOptions.timeoutMs`.
`Admin::describe_cluster_with` is Java `DescribeClusterOptions` (EndpointType / fenced brokers).
`Admin::describe_cluster_timeout` is Java `DescribeClusterOptions.timeoutMs`.
`Admin::update_features_with` is Java `UpdateFeaturesOptions.validateOnly` (UpgradeType).
`Admin::update_features_timeout` / `update_features_with_timeout` are Java `UpdateFeaturesOptions.timeoutMs`.
`Admin::describe_features_timeout` is Java `DescribeFeaturesOptions.timeoutMs`.
`Admin::describe_client_quotas_timeout` / `alter_client_quotas_timeout` are Java `DescribeClientQuotasOptions` / `AlterClientQuotasOptions.timeoutMs`.
`Admin::alter_user_scram_credentials_timeout` / `describe_user_scram_credentials_timeout` are Java `AlterUserScramCredentialsOptions` / `DescribeUserScramCredentialsOptions.timeoutMs`.
`Admin::describe_user_scram_credentials_all` is Java `describeUserScramCredentials()`.
`Admin::unregister_broker_timeout` is Java `UnregisterBrokerOptions.timeoutMs`.
`Admin::assign_replicas_to_dirs_timeout` is Java `AssignReplicasToDirsOptions.timeoutMs`.
`Admin::alter_replica_log_dirs_timeout` is Java `AlterReplicaLogDirsOptions.timeoutMs`.
`Admin::create_delegation_token_timeout` / `renew_delegation_token_timeout` / `expire_delegation_token_timeout` / `describe_delegation_token_timeout` are Java `CreateDelegationTokenOptions` / `RenewDelegationTokenOptions` / `ExpireDelegationTokenOptions` / `DescribeDelegationTokenOptions.timeoutMs`.
`Admin::create_delegation_token_default` is Java `createDelegationToken()`. `Admin::renew_delegation_token_hmac` / `expire_delegation_token_hmac` are Java `renewDelegationToken(byte[])` / `expireDelegationToken(byte[])`. `Admin::describe_delegation_tokens` is Java `describeDelegationToken()`.
`Admin::fence_producers` is Java `fenceProducers` (`FencedProducer`).
`Admin::abort_transaction_timeout` is Java `AbortTransactionOptions.timeoutMs`.
`Admin::force_terminate_transaction` is Java `forceTerminateTransaction`.
`Admin::delete_share_groups` is Java `deleteShareGroups`.
`Admin::delete_groups_timeout` / `delete_consumer_groups_timeout` / `delete_share_groups_timeout` are Java `DeleteConsumerGroupsOptions` / `DeleteShareGroupsOptions.timeoutMs`.
`Admin::describe_classic_groups` is Java `describeClassicGroups` (FindCoordinator v4+ CoordinatorKeys of N).
`Admin::describe_consumer_groups` is Java `describeConsumerGroups` (ConsumerGroupDescribe then DescribeGroups; FindCoordinator v4+ CoordinatorKeys of N).
`Admin::describe_classic_groups_timeout` / `describe_consumer_groups_timeout` / `describe_groups_timeout` are Java `DescribeClassicGroupsOptions` / `DescribeConsumerGroupsOptions.timeoutMs`.
`Admin::list_consumer_groups` is Java `listConsumerGroups`.
`Admin::list_groups_all` / `list_consumer_groups_all` are Java `listGroups()` / `listConsumerGroups()`.
`Admin::list_groups_timeout` / `list_consumer_groups_timeout` are Java `ListGroupsOptions` / `ListConsumerGroupsOptions.timeoutMs`.
`Admin::delete_consumer_groups` is Java `deleteConsumerGroups` (FindCoordinator v4+ CoordinatorKeys of N).
`Admin::describe_share_groups` is Java `describeShareGroups` (FindCoordinator v4+ CoordinatorKeys of N).
`Admin::list_client_metrics_resources` is Java `listClientMetricsResources`.
`Admin::list_config_resources_timeout` / `list_client_metrics_resources_timeout` are Java `ListConfigResourcesOptions` / `ListClientMetricsResourcesOptions.timeoutMs`.
`Admin::new` does not require ListConfigResources, GetTelemetrySubscriptions, PushTelemetry, or AssignReplicasToDirs.
`Admin::list_share_group_offsets` is Java `listShareGroupOffsets` (FindCoordinator v4+ CoordinatorKeys of N).
`Admin::delete_consumer_group_offsets` is Java `deleteConsumerGroupOffsets`.
`Admin::remove_members_from_consumer_group` is Java `removeMembersFromConsumerGroup`
(`MemberToRemove`). `Admin::remove_all_members_from_consumer_group` is Java
`RemoveMembersFromConsumerGroupOptions.removeAll`. `Admin::remove_members_from_consumer_group_with_reason` /
`remove_all_members_from_consumer_group_with_reason` are Java `RemoveMembersFromConsumerGroupOptions.reason`. `Admin::remove_members_from_consumer_group_timeout` /
`remove_all_members_from_consumer_group_timeout` are Java `RemoveMembersFromConsumerGroupOptions.timeoutMs`. `Admin::describe_broker_log_dirs` is Java
`describeLogDirs(Collection<Integer>)`. `Admin::describe_log_dirs_timeout` /
`describe_replica_log_dirs_timeout` / `describe_broker_log_dirs_timeout` are Java
`DescribeLogDirsOptions.timeoutMs`. `PartitionReassignment::assign`
takes `TopicPartition`. `send_offsets_to_transaction` takes
`TopicPartition`. `AclBinding::allow_topic`, `AclBindingFilter`, `AclResourceType`,
`AclOperation`, `AclPermission`, and `AclPatternType` cover CreateAcls /
DescribeAcls / DeleteAcls (v0–v3; v1 ResourcePatternType; v2+ flexible).
`Admin::describe_acls_with` is Java `describeAcls(AclBindingFilter)`.
`Admin::delete_acls_with` is Java `deleteAcls(Collection)` (DeleteAcls Filters of N).
`Admin::create_acls_timeout` / `Admin::describe_acls_timeout` / `Admin::delete_acls_timeout` are Java `CreateAclsOptions` / `DescribeAclsOptions` / `DeleteAclsOptions.timeoutMs`.
`ConsumerConfig.isolation_level` is `IsolationLevel`.
`ConfigResourceType` / `ScramMechanism` type ListConfigResources and
user SCRAM.

## Groups

Classic range, sticky, cooperative-sticky (KIP-429), KIP-848 (`join_consumer`), and KIP-932 share groups
(`ShareGroup::join` / `join_topics` / `join_matching` / `subscribe` / `subscribe_matching` / `unsubscribe` / `poll` / `accept` / `release` / `reject`).
`join_topics` / `join_sticky_topics` / `join_cooperative_sticky_topics` /
`join_with_assignors` / `join_with_assignors_topics` /
`join_consumer_topics` subscribe to
several topics. Set `group.instance.id` with
`ConsumerConfig::group_instance_id` for static membership.
`ConsumerConfig::auto_offset_reset` is used when the group has no
committed offset (`Earliest` by default, unlike Java's `latest`).
`auto_commit(true)` commits after poll (off by default).
`Producer::metrics` / `Consumer::metrics` / `ShareGroup::metrics` / `Admin::metrics` are counter snapshots
plus latency min/mean/max and p50/p99 over the last 1024 samples (produce-ack / fetch round / Admin RPC),
and per-topic rows on `ProducerMetrics::topics` / `ConsumerMetrics::topics` /
`ShareMetrics::topics`. `AdminMetrics` is Java `Admin.metrics()`.
`client_instance_id` is Java `clientInstanceId` (KIP-714) on producer, consumer, group, share, and admin.
`client_instance_id_timeout` is Java `clientInstanceId(Duration)`.
`max_poll_interval` is Kafka `max.poll.interval.ms` (default 5 minutes);
the heartbeat thread leaves the group if it is exceeded.
`Consumer::wakeup` / `WakeupHandle` interrupt fetch. Produce and fetch
interceptors are `ProducerConfig::interceptor` / `ConsumerConfig::interceptor`.
`TopicPartition` / `offsets_for_times` match Java `offsetsForTimes`
(`OffsetAndTimestamp::leader_epoch` is Java `getLeaderEpoch`).
`FetchedRecord::leader_epoch` is the record-batch partition leader epoch.
`Admin::create_partitions` takes `NewPartitions` (Java `increaseTo`).
`NewPartitions::with_assignments` is Java `increaseTo(int, List<List<Integer>>)`.
`incremental_alter_configs` / `incremental_alter_configs_for` / `alter_configs` /
`alter_configs_for` take `ConfigResource` / `ConfigResourceUpdate` /
`ConfigReplacement` (Java `incrementalAlterConfigs(Map)` / `alterConfigs(Map)`).
`AlterConfig::append` / `AlterConfig::subtract` are Java `AlterConfigOp.OpType.APPEND` / `SUBTRACT`.
`incremental_alter_configs_timeout` / `alter_configs_timeout` are Java `AlterConfigsOptions.timeoutMs`.
`OffsetAndMetadata` / `commit_with_metadata` send leader epoch and a
metadata string. `seek_with_metadata` is Java
`seek(TopicPartition, OffsetAndMetadata)` (Fetch `LastFetchedEpoch`;
metadata string ignored). `commit_with_metadata(recs.next_offsets())` is Java
`commitSync(records.nextOffsets())`. `commit_timeout` /
`commit_with_metadata_timeout` are Java `commitSync(Duration)`. `ProduceRecord::null_header` is a
null header value. `current_lag` is Java `currentLag`. `enforce_rebalance` /
`enforce_rebalance_with` rejoin on the next poll (Java `enforceRebalance`
/ `enforceRebalance(String)`; JoinGroup v8+ Reason). `subscription` is the topic list.
`subscribe` / `unsubscribe` change topics without dropping the handle.
`subscribe_matching` / `join_matching` / `join_sticky_matching` /
`join_cooperative_sticky_matching` / `join_consumer_matching` are Java
`subscribe(Pattern)` (re-list on poll when `metadata.max.age.ms` elapses;
names starting with `__` are skipped). Share groups have the same
`subscribe_matching` / `join_matching`.
`group_metadata` is Java `ConsumerGroupMetadata`. `list_topics` is cluster
Metadata. `Admin::list_topics_with` is Java `ListTopicsOptions.listInternal`. `Admin::list_topics_timeout` is Java `ListTopicsOptions.timeoutMs`. `describe_topics` is Java `describeTopics(TopicCollection.ofTopicNames)` (DescribeTopicPartitions; Metadata fallback when api 75 is missing). `describe_topics_timeout` is Java `DescribeTopicsOptions.timeoutMs`. `describe_topics_with` surfaces TopicAuthorizedOperations from that response. `describe_topics_with_partition_limit` is Java `DescribeTopicsOptions.partitionSizeLimitPerResponse`. `describe_topics_by_id` is Java `describeTopics(TopicCollection.ofTopicIds)`. `assign_many` / `assign_partitions` / `unassign` replace or drop a manual
assignment (`assign_partitions` is Java `assign(Collection)` and uses
`auto.offset.reset`).
`fetch_timeout` / `poll_timeout` are Java `poll(Duration)`.
`committed_timeout` is Java `committed(Duration)`. Group and share
coordinator RPCs use `ConsumerConfig::request_timeout`.
`commit_timeout` / `commit_offsets_timeout` /
`commit_with_metadata_timeout` are Java `commitSync(Duration)` (they do
not change `request_timeout` for heartbeats).
`Consumer::close` / `Consumer::close_timeout` drop fetch connections;
group `close` is `leave`.
`ConsumerGroup::close_timeout` / `ShareGroup::close_timeout` cap `leave`
(Java `close(Duration)`).
`Admin::close` / `Admin::close_timeout` drop the admin connection. Interceptors have `close`;
consumer interceptors also see `on_commit`.
`Producer::init_transactions` is a no-op after connect when
`transactional.id` is set. `flush_timeout` caps `flush`.
`close_timeout` is Java `close(Duration)` (producer flush, consumer drop
connections, group/share leave).

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{ConsumerConfig, ConsumerGroup};

let mut group = ConsumerGroup::join_topics(
    ConsumerConfig::bootstrap(["127.0.0.1:9092"]),
    "workers",
    ["orders", "payments"],
)
.await?;
let recs = group.poll().await?;
group.commit_with_metadata(recs.next_offsets()).await?;
group.leave().await?;
# Ok(())
# }
```

## Configure

Builders, not a property bag of strings:

```rust,no_run
use std::time::Duration;
use partitionline::{Acks, Compression, IsolationLevel, ProducerConfig, Sasl};

let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
    .acks(Acks::All)
    .linger(Duration::from_millis(5))
    .compression(Compression::Lz4)
    .sasl(Sasl::scram_sha256("alice", "secret"));
let _iso = IsolationLevel::ReadCommitted;
```

`ProducerConfig::allow_auto_create_topics` /
`ConsumerConfig::allow_auto_create_topics` are Kafka `allow.auto.create.topics`
(this crate defaults to `false`; Java consumer defaults to `true`).
`ProducerConfig::delivery_timeout` is Kafka `delivery.timeout.ms` (default 30s;
Java defaults to 120s). `ProducerConfig::max_block` is Kafka `max.block.ms`
(how long `send` waits for metadata and `buffer.memory`; default 30s, Java 60s).
`ProducerConfig::buffer_memory` is Kafka `buffer.memory` (queued key-plus-value
bytes not yet acked; default 32 MiB, same as Java; zero is no client-side cap).
`ProducerConfig::max_request_size` is Kafka `max.request.size` (key-plus-value
bytes of one record; default 1 MiB, same as Java; zero is no extra cap;
oversized records return `Error::RecordTooLarge`).
`ProducerConfig::retry_backoff` / `retry_backoff_max` are Kafka
`retry.backoff.ms` / `retry.backoff.max.ms` (exponential wait after a retriable
Produce; default 100ms / 1s). The same pair on `ConsumerConfig` covers retriable
Fetch (preferred-replica redirects do not wait).
`ProducerConfig::reconnect_backoff` / `reconnect_backoff_max` are Kafka
`reconnect.backoff.ms` / `reconnect.backoff.max.ms` (exponential wait after a
failed broker TCP connect; default 50ms / 1s, same as Java). The same pair is
on `ConsumerConfig` and `AdminConfig`.
`ProducerConfig::connections_max_idle` / `ConsumerConfig::connections_max_idle` /
`AdminConfig::connections_max_idle` are Kafka `connections.max.idle.ms`
(close unused broker TCP connections; default 9 minutes, same as Java; zero
never closes for idle). Admin bootstrap RPCs and group/share coordinator
sockets reconnect after the same idle.
`AdminConfig::retry_backoff` / `retry_backoff_max` are Kafka
`retry.backoff.ms` / `retry.backoff.max.ms` on admin RPCs (`NOT_CONTROLLER`,
coordinator moves, retriable IO; default 100ms / 1s).
`ProducerConfig::transaction_timeout` is Kafka `transaction.timeout.ms` on
InitProducerId (default 60s, same as Java).
`ProducerConfig::metadata_max_age` / `ConsumerConfig::metadata_max_age` are
Kafka `metadata.max.age.ms` (default 5 minutes; zero refreshes every lookup).
`connect_timeout` is on the producer, consumer, and admin builders.
TLS is `TlsConfig` on the same builders (`rustls`, no OpenSSL). Admin, gzip /
snappy / lz4, idempotent and transactional produce, fetch-from-follower, and
the Kafka 3.x / 4.x admin APIs are in the crate rustdoc. Produce partitioning
is murmur2 / round-robin; override it with `ProducerConfig::partitioner`.

**Not a drop-in for `rd_kafka_*`.** Still missing vs librdkafka: zstd and
Kerberos (C libraries), Schema Registry. Full list:
[docs/gaps.md](docs/gaps.md).

## Demo

Broker on `127.0.0.1:9092` (Docker `apache/kafka:3.9.1` is enough):

```
cargo run --release --example roundtrip
```

Also: `examples/produce.rs`, `examples/consume.rs`, `examples/group.rs`,
`examples/offsets.rs`, `examples/share.rs`, `examples/wakeup.rs`,
`examples/pause.rs`, `examples/metrics.rs`, `examples/cooperative.rs`.

Locked produce vs librdkafka 2.15.0 C (linger 5ms, 8e6×100B). Do not publish
rec/s unless broker high watermark equals records sent:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 KAFKA_TOPIC=plbench \
  cargo run --release --example bench_produce
```

Produce C bar (Lab A), fetch writeup (this-VM, unsigned), and latency
writeup (this-VM, unsigned): [docs/benchmark.md](docs/benchmark.md).
Suite HOLD: [docs/STATUS.md](docs/STATUS.md).

## Numbers

Locked produce vs librdkafka 2.15.0 C is Lab A (broker high watermark
equals records sent). Latency writeup is this-VM 2026-08-28, **unsigned**.

| Locked 8e6 × 100B produce (Lab A) | partitionline median | C 2.15.0 median |
|---|---|---|
| uncompressed, `acks=1` (2026-08-25) | 6.17M rec/s | 4.94M rec/s |
| uncompressed, `acks=1` (2026-08-24) | 7.28M rec/s | 3.88M rec/s |
| lz4 | 6.81M rec/s | 6.05M rec/s |
| idempotent (`acks=all`) | 7.16M rec/s | 3.13M rec/s |
| TLS / SSL | 7.42M rec/s | 1.52M rec/s |
| SASL SCRAM-SHA-256 | 6.81M rec/s | 3.98M rec/s |
| SASL SCRAM-SHA-512 | 6.89M rec/s | 3.43M rec/s |
| SASL OAUTHBEARER | 6.82M rec/s | 3.64M rec/s |

Fetch writeup **2026-08-28 this-VM** (Apache Kafka 3.9.1 KRaft; consumed
equals sent, HW 8e6). Comparison is rust-rdkafka **0.39.0**
`BaseConsumer::poll` (bundled librdkafka 2.12.1), not Lab A and not C
2.15.0. **Unsigned** until Kernel Integrity signs. Not a Suite HOLD lift.

| Fetch 8e6 × 100B (this-VM, unsigned) | partitionline median | rdkafka 0.39.0 median |
|---|---|---|
| consume, same log | 5.28M rec/s | 0.90M rec/s |

Latency writeup **2026-08-28 this-VM** (Apache Kafka 3.9.1 KRaft).
Sequential produce-ack, linger 0, 10k × 100 B, 1 partition, HW 11,000
(1k warmup + 10k timed). Comparison is rust-rdkafka **0.39.0**
`FutureProducer` (bundled librdkafka 2.12.1), not Lab A and not C 2.15.0.
**Unsigned** until Kernel Integrity signs. Not a Suite HOLD lift. Not a
"faster than librdkafka" claim: this-VM p50/p99 medians are not a win.

| Produce-ack 10k × 100B linger=0 (this-VM, unsigned) | partitionline median | rdkafka 0.39.0 median |
|---|---|---|
| p50 | 62 µs | 58 µs |
| p99 | 95 µs | 90 µs |

```
COUNT=10000 WARMUP=1000 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=0 \
  MODE=both KAFKA_TOPIC=pllat \
  cargo run --release --example bench_latency
```

## License

MIT OR Apache-2.0
