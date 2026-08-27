#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

pub const PRODUCE: i16 = 0;
pub const FETCH: i16 = 1;
pub const LIST_OFFSETS: i16 = 2;
pub const METADATA: i16 = 3;
pub const OFFSET_COMMIT: i16 = 8;
pub const OFFSET_FETCH: i16 = 9;
pub const FIND_COORDINATOR: i16 = 10;
pub const JOIN_GROUP: i16 = 11;
pub const HEARTBEAT: i16 = 12;
pub const LEAVE_GROUP: i16 = 13;
pub const SYNC_GROUP: i16 = 14;
pub const SASL_HANDSHAKE: i16 = 17;
pub const API_VERSIONS: i16 = 18;
pub const CREATE_TOPICS: i16 = 19;
pub const DELETE_TOPICS: i16 = 20;
pub const DELETE_RECORDS: i16 = 21;
pub const INIT_PRODUCER_ID: i16 = 22;
pub const OFFSET_FOR_LEADER_EPOCH: i16 = 23;
pub const ADD_PARTITIONS_TO_TXN: i16 = 24;
pub const ADD_OFFSETS_TO_TXN: i16 = 25;
pub const END_TXN: i16 = 26;
pub const TXN_OFFSET_COMMIT: i16 = 28;
pub const DESCRIBE_ACLS: i16 = 29;
pub const CREATE_ACLS: i16 = 30;
pub const DELETE_ACLS: i16 = 31;
pub const DESCRIBE_CONFIGS: i16 = 32;
pub const ALTER_CONFIGS: i16 = 33;
pub const SASL_AUTHENTICATE: i16 = 36;
pub const CREATE_PARTITIONS: i16 = 37;
pub const INCREMENTAL_ALTER_CONFIGS: i16 = 44;
pub const OFFSET_DELETE: i16 = 47;
pub const DESCRIBE_CLUSTER: i16 = 60;
pub const CONSUMER_GROUP_HEARTBEAT: i16 = 68;
pub const SHARE_GROUP_HEARTBEAT: i16 = 76;
pub const SHARE_FETCH: i16 = 78;
pub const SHARE_ACKNOWLEDGE: i16 = 79;

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
