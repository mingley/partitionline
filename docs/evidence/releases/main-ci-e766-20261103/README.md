# Main CI results at e7660f6e5

The client jobs in [run 41576616173](https://github.com/mingley/partitionline/actions/runs/41576616173)
failed in the library phase. Later integration targets in each command did not
run. The source is unchanged from the preceding Rust source commit; the number
and identity of timing-sensitive failures varied between runs.

- Default: 2,168 passed and 28 failed.
- All features: 2,170 passed and 26 failed.
- The current Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility jobs failed.
- The broker workflow and both client capability profiles passed separately.

These counts do not prove a new fix or qualify a release. The earlier
[b1 results](../main-ci-b1-20261102/README.md) remain recorded.

## Default

Job 125721483160: `test result: FAILED. 2168 passed; 28 failed; 0 ignored; 0 measured; 0 filtered out; finished in 142.26s`

```text
admin::tests::decoding_many_config_entries_does_not_hold_the_caller
admin::tests::decoding_many_deleted_acls_does_not_hold_the_caller
admin::tests::decoding_many_share_group_members_does_not_hold_the_caller
admin::tests::extending_many_described_topics_does_not_hold_the_caller
admin::tests::grouping_many_named_results_does_not_hold_the_caller
admin::tests::grouping_many_replica_dirs_does_not_hold_the_caller
admin::tests::indexing_many_coordinators_does_not_hold_the_caller
admin::tests::indexing_many_described_producers_does_not_hold_the_caller
consumer::tests::applying_many_one_record_fetch_batches_does_not_hold_the_caller
consumer::tests::indexing_many_metadata_names_does_not_hold_the_caller
consumer::tests::wakeup_keeps_a_leader_dial_that_already_finished
group::rediscover_stop_tests::replacing_an_idle_coordinator_lets_the_broker_leave
group::rediscover_stop_tests::replacing_an_idle_legacy_offset_coordinator_lets_the_broker_leave
group::rediscover_stop_tests::replacing_an_idle_offset_coordinator_lets_the_broker_leave
group::tests::building_many_offset_topics_does_not_hold_the_caller
group::tests::collecting_many_revoked_commits_does_not_hold_the_caller
group::tests::copying_many_assigned_members_does_not_hold_the_caller
group::tests::indexing_many_previous_members_does_not_hold_the_caller
group::tests::pairing_many_committed_offsets_does_not_hold_the_caller
group::tests::rebalancing_many_sticky_members_does_not_hold_the_caller
group::tests::retaining_many_interval_commits_does_not_hold_the_caller
net::tests::write_timeout_shuts_the_broker_down
producer::tests::assembling_many_produce_topics_does_not_hold_the_caller
producer::tests::dropping_many_ack_topics_does_not_hold_the_caller
producer::tests::encoding_many_produce_topics_does_not_hold_the_caller
producer::tests::replacing_an_idle_metadata_socket_lets_the_broker_leave
protocol::acl::tests::grouping_many_acl_resources_does_not_hold_the_caller
share::dial_tests::wakeup_keeps_a_share_dial_that_already_finished
```

## All features

Job 125721483031: `test result: FAILED. 2170 passed; 26 failed; 0 ignored; 0 measured; 0 filtered out; finished in 143.58s`

```text
admin::tests::decoding_many_left_members_does_not_hold_the_caller
admin::tests::extending_many_described_topics_does_not_hold_the_caller
admin::tests::indexing_many_coordinators_does_not_hold_the_caller
admin::tests::indexing_many_described_producers_does_not_hold_the_caller
cluster::tests::applying_many_topics_does_not_hold_the_caller
consumer::tests::applying_many_one_record_fetch_batches_does_not_hold_the_caller
consumer::tests::indexing_many_metadata_names_does_not_hold_the_caller
consumer::tests::wakeup_keeps_a_leader_dial_that_already_finished
group::rediscover_stop_tests::replacing_an_idle_coordinator_lets_the_broker_leave
group::rediscover_stop_tests::replacing_an_idle_legacy_offset_coordinator_lets_the_broker_leave
group::rediscover_stop_tests::replacing_an_idle_offset_coordinator_lets_the_broker_leave
group::tests::cloning_many_topic_ids_does_not_hold_the_caller
group::tests::collecting_many_revoked_commits_does_not_hold_the_caller
group::tests::copying_many_assigned_members_does_not_hold_the_caller
group::tests::indexing_many_previous_members_does_not_hold_the_caller
group::tests::pairing_many_committed_offsets_does_not_hold_the_caller
group::tests::rebalancing_many_sticky_members_does_not_hold_the_caller
group::tests::resolving_many_owned_partitions_does_not_hold_the_caller
net::tests::write_timeout_shuts_the_broker_down
producer::tests::assembling_many_produce_topics_does_not_hold_the_caller
producer::tests::dropping_many_ack_topics_does_not_hold_the_caller
producer::tests::encoding_many_produce_partitions_does_not_hold_the_caller
producer::tests::replacing_an_idle_metadata_socket_lets_the_broker_leave
protocol::acl::tests::grouping_many_acl_resources_does_not_hold_the_caller
share::dial_tests::wakeup_keeps_a_share_dial_that_already_finished
share::tests::naming_many_share_topics_does_not_hold_the_caller
```
