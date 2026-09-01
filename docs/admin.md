# Admin

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{Admin, NewTopic};

let mut admin = Admin::connect("127.0.0.1:9092").await?;
admin
    .create_topics(&[NewTopic::new("events", 3, 1)], 10_000, false)
    .await?;
admin.close().await?;
# Ok(())
# }
```

Most methods have a `_timeout` overload (Java `*Options.timeoutMs`). Quota
retries (`*_with_quota_retry`) default to **on** (KIP-599). If the broker
omits an API, the method returns `Error::Unsupported`. `Admin::new` does
not require the newer admin APIs to be present.

## Topics

`list_topics` / `list_topics_with` (include internal) /
`describe_topics` (names; DescribeTopicPartitions, Metadata fallback) /
`describe_topics_by_id` / `describe_topics_with` (authorized operations) /
`create_topics` / `delete_topics` / `delete_topics_by_id` /
`create_partitions`.

`NewTopic::with_assignments` is replica assignments.
`NewTopic::broker_defaults` is KIP-464 (`-1` partitions and RF).
`NewTopic::configs` sets topic configs. `NewPartitions::with_assignments`
is Java `increaseTo(int, List<List<Integer>>)`.

## Configs

`describe_configs` / `describe_configs_with_documentation` /
`incremental_alter_configs` / `alter_configs` (legacy API 33) /
`list_config_resources` / `list_client_metrics_resources`.

`AlterConfig::set` / `delete` / `append` / `subtract` map to
`AlterConfigOp.OpType`.

## ACLs

`create_acls` / `describe_acls_with` / `describe_acls_any` /
`delete_acls_with`. Types: `AclBinding`, `AclBindingFilter`,
`AclResourceType`, `AclOperation`, `AclPermission`, `AclPatternType`.
v0–v3 (v1 ResourcePatternType; v2+ flexible).

## Groups

`list_groups` / `list_consumer_groups` /
`describe_classic_groups` / `describe_consumer_groups` (ConsumerGroupDescribe
then DescribeGroups) / `delete_consumer_groups` /
`list_consumer_group_offsets` / `list_all_consumer_group_offsets` /
`list_consumer_group_offsets_for_groups` / `alter_consumer_group_offsets` /
`delete_consumer_group_offsets` / `remove_members_from_consumer_group` /
`remove_all_members_from_consumer_group`.

`describe_consumer_groups` tries api 69 first and falls back to
DescribeGroups.

## Share groups

`describe_share_groups` / `delete_share_groups` /
`list_share_group_offsets` / `alter_share_group_offsets` /
`delete_share_group_offsets`.

## Transactions

`describe_transactions` / `list_transactions` /
`list_transactions_with_duration` / `fence_producers` /
`force_terminate_transaction` / `abort_transaction` /
`allocate_producer_ids` / `describe_producers` /
`describe_producers_for` / `describe_producers_for_on_broker`.

## Cluster, logs, tokens, quotas

`describe_cluster` / `describe_features` / `update_features` /
`unregister_broker` / `assign_replicas_to_dirs` /
`alter_replica_log_dirs` / `describe_log_dirs` /
`describe_replica_log_dirs` / `describe_broker_log_dirs` /
`alter_partition_reassignments` / `list_partition_reassignments` /
`list_offsets` (with isolation) / `delete_records` /
`describe_client_quotas` / `alter_client_quotas` /
`alter_user_scram_credentials` / `describe_user_scram_credentials` /
delegation-token create / renew / expire / describe.

`Admin::metrics` is Java `Admin.metrics()`. `client_instance_id` is
KIP-714 (cached after the first successful call).

Not implemented: [gaps.md](gaps.md).
