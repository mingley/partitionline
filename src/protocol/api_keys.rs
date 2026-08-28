//! Kafka request api keys (`ApiKeys.java`) and version negotiation.

/// Produce (0).
pub const PRODUCE: i16 = 0;
/// Fetch (1).
pub const FETCH: i16 = 1;
/// ListOffsets (2).
pub const LIST_OFFSETS: i16 = 2;
/// Metadata (3).
pub const METADATA: i16 = 3;
/// OffsetCommit (8).
pub const OFFSET_COMMIT: i16 = 8;
/// OffsetFetch (9).
pub const OFFSET_FETCH: i16 = 9;
/// FindCoordinator (10).
pub const FIND_COORDINATOR: i16 = 10;
/// JoinGroup (11).
pub const JOIN_GROUP: i16 = 11;
/// Heartbeat (12).
pub const HEARTBEAT: i16 = 12;
/// LeaveGroup (13).
pub const LEAVE_GROUP: i16 = 13;
/// SyncGroup (14).
pub const SYNC_GROUP: i16 = 14;
/// DescribeGroups (15).
pub const DESCRIBE_GROUPS: i16 = 15;
/// ListGroups (16).
pub const LIST_GROUPS: i16 = 16;
/// SaslHandshake (17).
pub const SASL_HANDSHAKE: i16 = 17;
/// ApiVersions (18).
pub const API_VERSIONS: i16 = 18;
/// CreateTopics (19).
pub const CREATE_TOPICS: i16 = 19;
/// DeleteTopics (20).
pub const DELETE_TOPICS: i16 = 20;
/// DeleteRecords (21).
pub const DELETE_RECORDS: i16 = 21;
/// InitProducerId (22).
pub const INIT_PRODUCER_ID: i16 = 22;
/// OffsetForLeaderEpoch (23).
pub const OFFSET_FOR_LEADER_EPOCH: i16 = 23;
/// AddPartitionsToTxn (24).
pub const ADD_PARTITIONS_TO_TXN: i16 = 24;
/// AddOffsetsToTxn (25).
pub const ADD_OFFSETS_TO_TXN: i16 = 25;
/// EndTxn (26).
pub const END_TXN: i16 = 26;
/// WriteTxnMarkers (27).
pub const WRITE_TXN_MARKERS: i16 = 27;
/// TxnOffsetCommit (28).
pub const TXN_OFFSET_COMMIT: i16 = 28;
/// DescribeAcls (29).
pub const DESCRIBE_ACLS: i16 = 29;
/// CreateAcls (30).
pub const CREATE_ACLS: i16 = 30;
/// DeleteAcls (31).
pub const DELETE_ACLS: i16 = 31;
/// DescribeConfigs (32).
pub const DESCRIBE_CONFIGS: i16 = 32;
/// AlterConfigs (33).
pub const ALTER_CONFIGS: i16 = 33;
/// AlterReplicaLogDirs (34).
pub const ALTER_REPLICA_LOG_DIRS: i16 = 34;
/// DescribeLogDirs (35).
pub const DESCRIBE_LOG_DIRS: i16 = 35;
/// SaslAuthenticate (36).
pub const SASL_AUTHENTICATE: i16 = 36;
/// CreatePartitions (37).
pub const CREATE_PARTITIONS: i16 = 37;
/// CreateDelegationToken (38).
pub const CREATE_DELEGATION_TOKEN: i16 = 38;
/// RenewDelegationToken (39).
pub const RENEW_DELEGATION_TOKEN: i16 = 39;
/// ExpireDelegationToken (40).
pub const EXPIRE_DELEGATION_TOKEN: i16 = 40;
/// DescribeDelegationToken (41).
pub const DESCRIBE_DELEGATION_TOKEN: i16 = 41;
/// DeleteGroups (42).
pub const DELETE_GROUPS: i16 = 42;
/// IncrementalAlterConfigs (44).
pub const INCREMENTAL_ALTER_CONFIGS: i16 = 44;
/// AlterPartitionReassignments (45).
pub const ALTER_PARTITION_REASSIGNMENTS: i16 = 45;
/// ListPartitionReassignments (46).
pub const LIST_PARTITION_REASSIGNMENTS: i16 = 46;
/// OffsetDelete (47).
pub const OFFSET_DELETE: i16 = 47;
/// DescribeClientQuotas (48).
pub const DESCRIBE_CLIENT_QUOTAS: i16 = 48;
/// AlterClientQuotas (49).
pub const ALTER_CLIENT_QUOTAS: i16 = 49;
/// DescribeUserScramCredentials (50).
pub const DESCRIBE_USER_SCRAM_CREDENTIALS: i16 = 50;
/// AlterUserScramCredentials (51).
pub const ALTER_USER_SCRAM_CREDENTIALS: i16 = 51;
/// UpdateFeatures (57).
pub const UPDATE_FEATURES: i16 = 57;
/// DescribeCluster (60).
pub const DESCRIBE_CLUSTER: i16 = 60;
/// DescribeProducers (61).
pub const DESCRIBE_PRODUCERS: i16 = 61;
/// UnregisterBroker (64).
pub const UNREGISTER_BROKER: i16 = 64;
/// DescribeTransactions (65).
pub const DESCRIBE_TRANSACTIONS: i16 = 65;
/// ListTransactions (66).
pub const LIST_TRANSACTIONS: i16 = 66;
/// AllocateProducerIds (67).
pub const ALLOCATE_PRODUCER_IDS: i16 = 67;
/// ConsumerGroupHeartbeat (68).
pub const CONSUMER_GROUP_HEARTBEAT: i16 = 68;
/// ConsumerGroupDescribe (69).
pub const CONSUMER_GROUP_DESCRIBE: i16 = 69;
/// GetTelemetrySubscriptions (71).
pub const GET_TELEMETRY_SUBSCRIPTIONS: i16 = 71;
/// PushTelemetry (72).
pub const PUSH_TELEMETRY: i16 = 72;
/// AssignReplicasToDirs (73).
pub const ASSIGN_REPLICAS_TO_DIRS: i16 = 73;
/// ListConfigResources (74).
pub const LIST_CONFIG_RESOURCES: i16 = 74;
/// DescribeTopicPartitions (75).
pub const DESCRIBE_TOPIC_PARTITIONS: i16 = 75;
/// ShareGroupHeartbeat (76).
pub const SHARE_GROUP_HEARTBEAT: i16 = 76;
/// ShareGroupDescribe (77).
pub const SHARE_GROUP_DESCRIBE: i16 = 77;
/// ShareFetch (78).
pub const SHARE_FETCH: i16 = 78;
/// ShareAcknowledge (79).
pub const SHARE_ACKNOWLEDGE: i16 = 79;
/// DescribeShareGroupOffsets (90).
pub const DESCRIBE_SHARE_GROUP_OFFSETS: i16 = 90;
/// AlterShareGroupOffsets (91).
pub const ALTER_SHARE_GROUP_OFFSETS: i16 = 91;
/// DeleteShareGroupOffsets (92).
pub const DELETE_SHARE_GROUP_OFFSETS: i16 = 92;

/// Highest version in both the broker range and the client range, if they overlap.
pub fn pick_version(
    broker_min: i16,
    broker_max: i16,
    client_min: i16,
    client_max: i16,
) -> Option<i16> {
    let lo = broker_min.max(client_min);
    let hi = broker_max.min(client_max);
    (lo <= hi).then_some(hi)
}
