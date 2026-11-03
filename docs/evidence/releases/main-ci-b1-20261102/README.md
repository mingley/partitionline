# Main client CI results

The [main CI run](https://github.com/mingley/partitionline/actions/runs/41574318223)
executed source `b1b0488096f886c2fd60a7368bbf2ed57b5da2e3` with stable Rust.
Both all-target commands stopped at library failures; later integration
targets were not reached. The Rust client source is unchanged from the
[previous run](../main-ci-350-20261102/README.md). Differences in failure
counts are not evidence of a code or performance improvement.

## default

test result: FAILED. 2180 passed; 16 failed; 0 ignored; 0 measured; 0 filtered out; finished in 87.88s

- `admin::tests::extending_many_described_topics_does_not_hold_the_caller`
- `admin::tests::indexing_many_described_producers_does_not_hold_the_caller`
- `consumer::tests::indexing_many_metadata_names_does_not_hold_the_caller`
- `consumer::tests::wakeup_keeps_a_leader_dial_that_already_finished`
- `group::rediscover_stop_tests::replacing_an_idle_coordinator_lets_the_broker_leave`
- `group::rediscover_stop_tests::replacing_an_idle_legacy_offset_coordinator_lets_the_broker_leave`
- `group::rediscover_stop_tests::replacing_an_idle_offset_coordinator_lets_the_broker_leave`
- `group::tests::collecting_many_revoked_commits_does_not_hold_the_caller`
- `group::tests::copying_many_assigned_members_does_not_hold_the_caller`
- `group::tests::pairing_many_committed_offsets_does_not_hold_the_caller`
- `net::tests::write_timeout_shuts_the_broker_down`
- `producer::tests::assembling_many_produce_topics_does_not_hold_the_caller`
- `producer::tests::indexing_many_same_topic_groups_does_not_hold_the_caller`
- `producer::tests::releasing_many_failed_records_does_not_hold_the_caller`
- `producer::tests::replacing_an_idle_metadata_socket_lets_the_broker_leave`
- `share::dial_tests::wakeup_keeps_a_share_dial_that_already_finished`

## all-features

test result: FAILED. 2176 passed; 20 failed; 0 ignored; 0 measured; 0 filtered out; finished in 100.98s

- `admin::tests::extending_many_described_topics_does_not_hold_the_caller`
- `admin::tests::flattening_many_list_reassignments_does_not_hold_the_caller`
- `admin::tests::grouping_many_named_results_does_not_hold_the_caller`
- `admin::tests::indexing_many_coordinators_does_not_hold_the_caller`
- `consumer::tests::indexing_many_metadata_names_does_not_hold_the_caller`
- `consumer::tests::wakeup_keeps_a_leader_dial_that_already_finished`
- `group::rediscover_stop_tests::replacing_an_idle_coordinator_lets_the_broker_leave`
- `group::rediscover_stop_tests::replacing_an_idle_legacy_offset_coordinator_lets_the_broker_leave`
- `group::rediscover_stop_tests::replacing_an_idle_offset_coordinator_lets_the_broker_leave`
- `group::tests::collecting_many_revoked_commits_does_not_hold_the_caller`
- `group::tests::copying_many_assigned_members_does_not_hold_the_caller`
- `group::tests::pairing_many_committed_offsets_does_not_hold_the_caller`
- `net::tests::write_timeout_shuts_the_broker_down`
- `producer::tests::assembling_many_produce_topics_does_not_hold_the_caller`
- `producer::tests::indexing_many_same_topic_groups_does_not_hold_the_caller`
- `producer::tests::listing_many_unsent_txn_partitions_does_not_hold_the_caller`
- `producer::tests::releasing_many_failed_records_does_not_hold_the_caller`
- `producer::tests::replacing_an_idle_metadata_socket_lets_the_broker_leave`
- `producer::tests::shelling_many_produce_partitions_does_not_hold_the_caller`
- `share::dial_tests::wakeup_keeps_a_share_dial_that_already_finished`

All three current live Kafka compatibility jobs failed; the 4.3.1 job
reported a stack overflow. Broker-specific and focused capability CI passed
in separate runs. Job logs and artifacts remain on GitHub under its retention
policy.
