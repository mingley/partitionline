# Main client CI results

The [main CI run](https://github.com/mingley/partitionline/actions/runs/41572635665)
executed source `350b5e534215723be65680e6116b1ec3d495506c` with stable Rust.
Both all-target commands stopped at their library failures; later integration
targets were not reached. The number of failures differs from the earlier
`c025be19b` run. The Rust client source did not change between those commits;
the difference is not evidence of a performance or correctness improvement.

## default

test result: FAILED. 2167 passed; 29 failed; 0 ignored; 0 measured; 0 filtered out; finished in 144.99s

- `admin::tests::decoding_many_described_group_members_does_not_hold_the_caller`
- `admin::tests::decoding_many_listed_groups_does_not_hold_the_caller`
- `admin::tests::decoding_many_share_group_members_does_not_hold_the_caller`
- `admin::tests::extending_many_described_topics_does_not_hold_the_caller`
- `admin::tests::grouping_many_named_results_does_not_hold_the_caller`
- `admin::tests::grouping_many_replica_dirs_does_not_hold_the_caller`
- `admin::tests::indexing_many_coordinators_does_not_hold_the_caller`
- `admin::tests::indexing_many_described_producers_does_not_hold_the_caller`
- `cluster::tests::applying_many_topics_does_not_hold_the_caller`
- `consumer::tests::indexing_many_metadata_names_does_not_hold_the_caller`
- `consumer::tests::wakeup_keeps_a_leader_dial_that_already_finished`
- `group::rediscover_stop_tests::replacing_an_idle_coordinator_lets_the_broker_leave`
- `group::rediscover_stop_tests::replacing_an_idle_legacy_offset_coordinator_lets_the_broker_leave`
- `group::rediscover_stop_tests::replacing_an_idle_offset_coordinator_lets_the_broker_leave`
- `group::tests::building_many_offset_topics_does_not_hold_the_caller`
- `group::tests::collecting_many_revoked_commits_does_not_hold_the_caller`
- `group::tests::copying_many_assigned_members_does_not_hold_the_caller`
- `group::tests::indexing_many_previous_members_does_not_hold_the_caller`
- `group::tests::pairing_many_committed_offsets_does_not_hold_the_caller`
- `group::tests::rebalancing_many_sticky_members_does_not_hold_the_caller`
- `net::tests::write_timeout_shuts_the_broker_down`
- `producer::tests::assembling_many_produce_topics_does_not_hold_the_caller`
- `producer::tests::dropping_many_ack_topics_does_not_hold_the_caller`
- `producer::tests::encoding_many_produce_partitions_does_not_hold_the_caller`
- `producer::tests::replacing_an_idle_metadata_socket_lets_the_broker_leave`
- `protocol::acl::tests::grouping_many_acl_resources_does_not_hold_the_caller`
- `protocol::fetch::tests::decoding_many_fetch_topics_does_not_hold_the_caller`
- `share::dial_tests::wakeup_keeps_a_share_dial_that_already_finished`
- `share::tests::grouping_many_share_ack_topics_does_not_hold_the_caller`

## all-features

test result: FAILED. 2177 passed; 19 failed; 0 ignored; 0 measured; 0 filtered out; finished in 92.09s

- `admin::tests::decoding_many_delegation_tokens_does_not_hold_the_caller`
- `admin::tests::extending_many_described_topics_does_not_hold_the_caller`
- `admin::tests::flattening_many_reassignment_results_does_not_hold_the_caller`
- `admin::tests::grouping_many_named_results_does_not_hold_the_caller`
- `admin::tests::indexing_many_described_producers_does_not_hold_the_caller`
- `consumer::tests::prepending_many_pending_runs_does_not_hold_the_caller`
- `consumer::tests::wakeup_keeps_a_leader_dial_that_already_finished`
- `group::rediscover_stop_tests::replacing_an_idle_coordinator_lets_the_broker_leave`
- `group::rediscover_stop_tests::replacing_an_idle_legacy_offset_coordinator_lets_the_broker_leave`
- `group::rediscover_stop_tests::replacing_an_idle_offset_coordinator_lets_the_broker_leave`
- `group::tests::building_many_offset_topics_does_not_hold_the_caller`
- `group::tests::collecting_many_revoked_commits_does_not_hold_the_caller`
- `group::tests::copying_many_assigned_members_does_not_hold_the_caller`
- `group::tests::pairing_many_committed_offsets_does_not_hold_the_caller`
- `net::tests::write_timeout_shuts_the_broker_down`
- `producer::tests::indexing_many_metadata_topics_does_not_hold_the_caller`
- `producer::tests::indexing_many_same_topic_groups_does_not_hold_the_caller`
- `producer::tests::replacing_an_idle_metadata_socket_lets_the_broker_leave`
- `share::dial_tests::wakeup_keeps_a_share_dial_that_already_finished`

All three current live Kafka compatibility jobs also failed; the 4.3.1 job
reported a stack overflow. Broker-specific CI passed in its separate run.
The listed job logs and their artifacts remain on GitHub under its retention
policy.
