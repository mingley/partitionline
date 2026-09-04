//! Kafka request api keys (`ApiKeys.java`) and version negotiation.

use crate::error::{Error, Result};

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
/// SaslHandshake (17). Kafka 4.0 `validVersions` is `0-1`.
pub const SASL_HANDSHAKE: i16 = 17;
/// ApiVersions (18). Kafka 4.0 `validVersions` is `0-4`.
pub const API_VERSIONS: i16 = 18;
/// CreateTopics (19). Kafka 4.0 `validVersions` is `2-7`.
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
/// SaslAuthenticate (36). Kafka 4.0 `validVersions` is `0-2`.
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
/// ElectLeaders (43). Kafka 4.0 `validVersions` is `0-2`.
pub const ELECT_LEADERS: i16 = 43;
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
/// ListTransactions (66). Kafka 4.0 `validVersions` is `0-1`.
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
/// ListConfigResources (74). Java 4.0 `ApiKeys` enum name is
/// `LIST_CLIENT_METRICS_RESOURCES`.
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

/// Java `ApiKeys` enum constant name for `id`.
///
/// Kafka 4.0 `ApiKeys.toString` is the enum name (`PRODUCE`), not
/// `ApiMessageType.name` (`Produce`). Api 74 is Java 4.0
/// `LIST_CLIENT_METRICS_RESOURCES` (this crate's [`LIST_CONFIG_RESOURCES`]).
/// Broker-only and named STATUS-hole keys are included so
/// [`crate::protocol::header::RequestHeader`] Display matches Java. Apis
/// 90–92 are this crate's 4.1-oriented share-offset keys.
#[must_use]
pub const fn name(id: i16) -> Option<&'static str> {
    match id {
        PRODUCE => Some("PRODUCE"),
        FETCH => Some("FETCH"),
        LIST_OFFSETS => Some("LIST_OFFSETS"),
        METADATA => Some("METADATA"),
        4 => Some("LEADER_AND_ISR"),
        5 => Some("STOP_REPLICA"),
        6 => Some("UPDATE_METADATA"),
        7 => Some("CONTROLLED_SHUTDOWN"),
        OFFSET_COMMIT => Some("OFFSET_COMMIT"),
        OFFSET_FETCH => Some("OFFSET_FETCH"),
        FIND_COORDINATOR => Some("FIND_COORDINATOR"),
        JOIN_GROUP => Some("JOIN_GROUP"),
        HEARTBEAT => Some("HEARTBEAT"),
        LEAVE_GROUP => Some("LEAVE_GROUP"),
        SYNC_GROUP => Some("SYNC_GROUP"),
        DESCRIBE_GROUPS => Some("DESCRIBE_GROUPS"),
        LIST_GROUPS => Some("LIST_GROUPS"),
        SASL_HANDSHAKE => Some("SASL_HANDSHAKE"),
        API_VERSIONS => Some("API_VERSIONS"),
        CREATE_TOPICS => Some("CREATE_TOPICS"),
        DELETE_TOPICS => Some("DELETE_TOPICS"),
        DELETE_RECORDS => Some("DELETE_RECORDS"),
        INIT_PRODUCER_ID => Some("INIT_PRODUCER_ID"),
        OFFSET_FOR_LEADER_EPOCH => Some("OFFSET_FOR_LEADER_EPOCH"),
        ADD_PARTITIONS_TO_TXN => Some("ADD_PARTITIONS_TO_TXN"),
        ADD_OFFSETS_TO_TXN => Some("ADD_OFFSETS_TO_TXN"),
        END_TXN => Some("END_TXN"),
        WRITE_TXN_MARKERS => Some("WRITE_TXN_MARKERS"),
        TXN_OFFSET_COMMIT => Some("TXN_OFFSET_COMMIT"),
        DESCRIBE_ACLS => Some("DESCRIBE_ACLS"),
        CREATE_ACLS => Some("CREATE_ACLS"),
        DELETE_ACLS => Some("DELETE_ACLS"),
        DESCRIBE_CONFIGS => Some("DESCRIBE_CONFIGS"),
        ALTER_CONFIGS => Some("ALTER_CONFIGS"),
        ALTER_REPLICA_LOG_DIRS => Some("ALTER_REPLICA_LOG_DIRS"),
        DESCRIBE_LOG_DIRS => Some("DESCRIBE_LOG_DIRS"),
        SASL_AUTHENTICATE => Some("SASL_AUTHENTICATE"),
        CREATE_PARTITIONS => Some("CREATE_PARTITIONS"),
        CREATE_DELEGATION_TOKEN => Some("CREATE_DELEGATION_TOKEN"),
        RENEW_DELEGATION_TOKEN => Some("RENEW_DELEGATION_TOKEN"),
        EXPIRE_DELEGATION_TOKEN => Some("EXPIRE_DELEGATION_TOKEN"),
        DESCRIBE_DELEGATION_TOKEN => Some("DESCRIBE_DELEGATION_TOKEN"),
        DELETE_GROUPS => Some("DELETE_GROUPS"),
        ELECT_LEADERS => Some("ELECT_LEADERS"),
        INCREMENTAL_ALTER_CONFIGS => Some("INCREMENTAL_ALTER_CONFIGS"),
        ALTER_PARTITION_REASSIGNMENTS => Some("ALTER_PARTITION_REASSIGNMENTS"),
        LIST_PARTITION_REASSIGNMENTS => Some("LIST_PARTITION_REASSIGNMENTS"),
        OFFSET_DELETE => Some("OFFSET_DELETE"),
        DESCRIBE_CLIENT_QUOTAS => Some("DESCRIBE_CLIENT_QUOTAS"),
        ALTER_CLIENT_QUOTAS => Some("ALTER_CLIENT_QUOTAS"),
        DESCRIBE_USER_SCRAM_CREDENTIALS => Some("DESCRIBE_USER_SCRAM_CREDENTIALS"),
        ALTER_USER_SCRAM_CREDENTIALS => Some("ALTER_USER_SCRAM_CREDENTIALS"),
        52 => Some("VOTE"),
        53 => Some("BEGIN_QUORUM_EPOCH"),
        54 => Some("END_QUORUM_EPOCH"),
        55 => Some("DESCRIBE_QUORUM"),
        56 => Some("ALTER_PARTITION"),
        UPDATE_FEATURES => Some("UPDATE_FEATURES"),
        58 => Some("ENVELOPE"),
        59 => Some("FETCH_SNAPSHOT"),
        DESCRIBE_CLUSTER => Some("DESCRIBE_CLUSTER"),
        DESCRIBE_PRODUCERS => Some("DESCRIBE_PRODUCERS"),
        62 => Some("BROKER_REGISTRATION"),
        63 => Some("BROKER_HEARTBEAT"),
        UNREGISTER_BROKER => Some("UNREGISTER_BROKER"),
        DESCRIBE_TRANSACTIONS => Some("DESCRIBE_TRANSACTIONS"),
        LIST_TRANSACTIONS => Some("LIST_TRANSACTIONS"),
        ALLOCATE_PRODUCER_IDS => Some("ALLOCATE_PRODUCER_IDS"),
        CONSUMER_GROUP_HEARTBEAT => Some("CONSUMER_GROUP_HEARTBEAT"),
        CONSUMER_GROUP_DESCRIBE => Some("CONSUMER_GROUP_DESCRIBE"),
        70 => Some("CONTROLLER_REGISTRATION"),
        GET_TELEMETRY_SUBSCRIPTIONS => Some("GET_TELEMETRY_SUBSCRIPTIONS"),
        PUSH_TELEMETRY => Some("PUSH_TELEMETRY"),
        ASSIGN_REPLICAS_TO_DIRS => Some("ASSIGN_REPLICAS_TO_DIRS"),
        LIST_CONFIG_RESOURCES => Some("LIST_CLIENT_METRICS_RESOURCES"),
        DESCRIBE_TOPIC_PARTITIONS => Some("DESCRIBE_TOPIC_PARTITIONS"),
        SHARE_GROUP_HEARTBEAT => Some("SHARE_GROUP_HEARTBEAT"),
        SHARE_GROUP_DESCRIBE => Some("SHARE_GROUP_DESCRIBE"),
        SHARE_FETCH => Some("SHARE_FETCH"),
        SHARE_ACKNOWLEDGE => Some("SHARE_ACKNOWLEDGE"),
        80 => Some("ADD_RAFT_VOTER"),
        81 => Some("REMOVE_RAFT_VOTER"),
        82 => Some("UPDATE_RAFT_VOTER"),
        83 => Some("INITIALIZE_SHARE_GROUP_STATE"),
        84 => Some("READ_SHARE_GROUP_STATE"),
        85 => Some("WRITE_SHARE_GROUP_STATE"),
        86 => Some("DELETE_SHARE_GROUP_STATE"),
        87 => Some("READ_SHARE_GROUP_STATE_SUMMARY"),
        DESCRIBE_SHARE_GROUP_OFFSETS => Some("DESCRIBE_SHARE_GROUP_OFFSETS"),
        ALTER_SHARE_GROUP_OFFSETS => Some("ALTER_SHARE_GROUP_OFFSETS"),
        DELETE_SHARE_GROUP_OFFSETS => Some("DELETE_SHARE_GROUP_OFFSETS"),
        _ => None,
    }
}

/// Java `ApiKeys.hasId`.
#[must_use]
pub const fn has_id(id: i16) -> bool {
    name(id).is_some()
}

/// Java `ApiKeys.forId`. Unknown ids are [`crate::Error::protocol`]
/// (`Unexpected api key: {id}`).
pub fn for_id(id: i16) -> Result<&'static str> {
    name(id).ok_or_else(|| Error::protocol(format!("Unexpected api key: {id}")))
}

/// Java `ApiKeys.clusterAction` (inter-broker ClusterAction APIs).
///
/// Kafka 4.0.0 constructors. Unknown ids and this crate's 4.1-oriented
/// share-offset keys (90–92) are `false`.
#[must_use]
pub const fn cluster_action(id: i16) -> bool {
    matches!(
        id,
        4 | 5
            | 6
            | 7
            | WRITE_TXN_MARKERS
            | 52
            | 53
            | 54
            | 55
            | 56
            | UPDATE_FEATURES
            | 58
            | 62
            | 63
            | ALLOCATE_PRODUCER_IDS
            | 83
            | 84
            | 85
            | 86
            | 87
    )
}

/// Java `ApiKeys.forwardable`.
///
/// Kafka 4.0.0 constructors. Unknown ids and this crate's 4.1-oriented
/// share-offset keys (90–92) are `false`.
#[must_use]
pub const fn forwardable(id: i16) -> bool {
    matches!(
        id,
        CREATE_TOPICS
            | DELETE_TOPICS
            | CREATE_ACLS
            | DELETE_ACLS
            | ALTER_CONFIGS
            | CREATE_PARTITIONS
            | CREATE_DELEGATION_TOKEN
            | RENEW_DELEGATION_TOKEN
            | EXPIRE_DELEGATION_TOKEN
            | ELECT_LEADERS
            | INCREMENTAL_ALTER_CONFIGS
            | ALTER_PARTITION_REASSIGNMENTS
            | LIST_PARTITION_REASSIGNMENTS
            | ALTER_CLIENT_QUOTAS
            | ALTER_USER_SCRAM_CREDENTIALS
            | 55
            | UPDATE_FEATURES
            | UNREGISTER_BROKER
            | ALLOCATE_PRODUCER_IDS
            | 80
            | 81
    )
}

/// Java `ApiKeys.minRequiredInterBrokerMagic`.
///
/// Kafka 4.0.0 constructors. AddPartitionsToTxn, AddOffsetsToTxn, EndTxn,
/// WriteTxnMarkers, and TxnOffsetCommit are
/// [`crate::RecordBatch::MAGIC_VALUE_V2`]; every other id (including unknown
/// and this crate's 4.1-oriented share-offset keys 90–92) is
/// [`crate::RecordBatch::MAGIC_VALUE_V0`].
#[must_use]
pub const fn min_required_inter_broker_magic(id: i16) -> i8 {
    match id {
        ADD_PARTITIONS_TO_TXN
        | ADD_OFFSETS_TO_TXN
        | END_TXN
        | WRITE_TXN_MARKERS
        | TXN_OFFSET_COMMIT => 2,
        _ => 0,
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_id_and_for_id_match_java() {
        assert!(has_id(PRODUCE));
        assert!(has_id(43));
        assert!(has_id(LIST_CONFIG_RESOURCES));
        assert!(has_id(DESCRIBE_SHARE_GROUP_OFFSETS));
        assert!(!has_id(999));
        assert!(!has_id(-1));
        assert_eq!(for_id(PRODUCE).unwrap(), "PRODUCE");
        assert_eq!(for_id(43).unwrap(), "ELECT_LEADERS");
        assert_eq!(
            for_id(LIST_CONFIG_RESOURCES).unwrap(),
            "LIST_CLIENT_METRICS_RESOURCES"
        );
        let err = for_id(999).unwrap_err();
        assert!(err.to_string().contains("Unexpected api key: 999"), "{err}");
    }

    #[test]
    fn cluster_action_and_forwardable_match_java_4_0() {
        assert!(!cluster_action(PRODUCE));
        assert!(!forwardable(PRODUCE));
        assert!(cluster_action(WRITE_TXN_MARKERS));
        assert!(!forwardable(WRITE_TXN_MARKERS));
        assert!(!cluster_action(CREATE_TOPICS));
        assert!(forwardable(CREATE_TOPICS));
        assert!(cluster_action(UPDATE_FEATURES));
        assert!(forwardable(UPDATE_FEATURES));
        assert!(!cluster_action(UNREGISTER_BROKER));
        assert!(forwardable(UNREGISTER_BROKER));
        assert!(cluster_action(ALLOCATE_PRODUCER_IDS));
        assert!(forwardable(ALLOCATE_PRODUCER_IDS));
        assert!(!cluster_action(43));
        assert!(forwardable(43));
        assert!(cluster_action(55));
        assert!(forwardable(55));
        assert!(cluster_action(4));
        assert!(!forwardable(4));
        assert!(!cluster_action(DESCRIBE_SHARE_GROUP_OFFSETS));
        assert!(!forwardable(DESCRIBE_SHARE_GROUP_OFFSETS));
        assert!(!cluster_action(999));
        assert!(!forwardable(999));
    }

    #[test]
    fn min_required_inter_broker_magic_match_java_4_0() {
        use crate::RecordBatch;
        assert_eq!(
            min_required_inter_broker_magic(ADD_PARTITIONS_TO_TXN),
            RecordBatch::MAGIC_VALUE_V2
        );
        assert_eq!(
            min_required_inter_broker_magic(ADD_OFFSETS_TO_TXN),
            RecordBatch::MAGIC_VALUE_V2
        );
        assert_eq!(
            min_required_inter_broker_magic(END_TXN),
            RecordBatch::MAGIC_VALUE_V2
        );
        assert_eq!(
            min_required_inter_broker_magic(WRITE_TXN_MARKERS),
            RecordBatch::MAGIC_VALUE_V2
        );
        assert_eq!(
            min_required_inter_broker_magic(TXN_OFFSET_COMMIT),
            RecordBatch::MAGIC_VALUE_V2
        );
        assert_eq!(
            min_required_inter_broker_magic(PRODUCE),
            RecordBatch::MAGIC_VALUE_V0
        );
        assert_eq!(
            min_required_inter_broker_magic(CREATE_TOPICS),
            RecordBatch::MAGIC_VALUE_V0
        );
        assert_eq!(
            min_required_inter_broker_magic(UNREGISTER_BROKER),
            RecordBatch::MAGIC_VALUE_V0
        );
        assert_eq!(
            min_required_inter_broker_magic(ALLOCATE_PRODUCER_IDS),
            RecordBatch::MAGIC_VALUE_V0
        );
        assert_eq!(
            min_required_inter_broker_magic(55),
            RecordBatch::MAGIC_VALUE_V0
        );
        assert_eq!(
            min_required_inter_broker_magic(DESCRIBE_SHARE_GROUP_OFFSETS),
            RecordBatch::MAGIC_VALUE_V0
        );
        assert_eq!(
            min_required_inter_broker_magic(999),
            RecordBatch::MAGIC_VALUE_V0
        );
    }
}
