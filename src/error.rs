//! Kafka and client error types.

use std::fmt;
use std::io;

/// Client, protocol, or broker failure.
#[derive(Debug)]
pub enum Error {
    /// I/O or TLS failure.
    Io(io::Error),
    /// Client-side protocol or usage error.
    Protocol(String),
    /// Broker `error_code` plus a short context string.
    Broker {
        /// Kafka `error_code`.
        code: i16,
        /// Api name or `topic-partition`.
        message: String,
    },
    /// Metadata did not list this topic.
    UnknownTopic(String),
    /// Metadata has no leader for this partition.
    NoLeader {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
    },
    /// Broker does not support a required API version.
    Unsupported(String),
    /// The producer (or connection) is shut down.
    Closed,
    /// A request exceeded [`crate::ProducerConfig::request_timeout`] or similar.
    Timeout,
    /// `try_send` could not queue (metadata, connection, or `buffer.memory`).
    QueueFull,
    /// Serialized record size exceeds [`crate::ProducerConfig::max_request_size`]
    /// or [`crate::ProducerConfig::buffer_memory`].
    ///
    /// Java `KafkaProducer.ensureValidRecordSize` checks `max.request.size`
    /// first, then `buffer.memory`. [`Display`] is Java
    /// `RecordTooLargeException`. For [`Self::MAX_REQUEST_SIZE_CONFIG`]:
    /// `The message is {size} bytes when serialized which is larger than {max},
    /// which is the value of the max.request.size configuration.` For
    /// [`Self::BUFFER_MEMORY_CONFIG`]: `The message is {size} bytes when
    /// serialized which is larger than the total memory buffer you have
    /// configured with the buffer.memory configuration.` `size` is Java
    /// `AbstractRecords.estimateSizeInBytesUpperBound`.
    RecordTooLarge {
        /// Java `AbstractRecords.estimateSizeInBytesUpperBound` of the record.
        size: u64,
        /// Configured cap that was exceeded (`max.request.size` or
        /// `buffer.memory`).
        max: u64,
        /// Java config name: [`Self::MAX_REQUEST_SIZE_CONFIG`] or
        /// [`Self::BUFFER_MEMORY_CONFIG`].
        config: &'static str,
    },
    /// [`crate::ConsumerGroup::poll`] was not called within `max.poll.interval.ms`.
    MaxPollInterval,
    /// [`crate::Consumer::wakeup`] interrupted fetch or poll.
    Wakeup,
}

impl Error {
    /// Java `ProducerConfig.MAX_REQUEST_SIZE_CONFIG`.
    pub const MAX_REQUEST_SIZE_CONFIG: &str = "max.request.size";
    /// Java `ProducerConfig.BUFFER_MEMORY_CONFIG`.
    pub const BUFFER_MEMORY_CONFIG: &str = "buffer.memory";

    /// Wrap a protocol / client-side failure.
    pub fn protocol(msg: impl Into<String>) -> Self {
        Self::Protocol(msg.into())
    }

    /// Java `RecordTooLargeException` when `estimateSizeInBytesUpperBound`
    /// exceeds `max.request.size`.
    #[must_use]
    pub(crate) fn record_too_large_max_request_size(size: u64, max: u64) -> Self {
        Self::RecordTooLarge {
            size,
            max,
            config: Self::MAX_REQUEST_SIZE_CONFIG,
        }
    }

    /// Java `RecordTooLargeException` when `estimateSizeInBytesUpperBound`
    /// exceeds `buffer.memory`.
    #[must_use]
    pub(crate) fn record_too_large_buffer_memory(size: u64, max: u64) -> Self {
        Self::RecordTooLarge {
            size,
            max,
            config: Self::BUFFER_MEMORY_CONFIG,
        }
    }

    /// Broker `error_code` plus a short context string (api or `topic-partition`).
    pub fn broker(code: i16, message: impl Into<String>) -> Self {
        Self::Broker {
            code,
            message: message.into(),
        }
    }

    /// Kafka error code when this is a broker error.
    #[must_use]
    pub fn broker_code(&self) -> Option<i16> {
        match self {
            Self::Broker { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Kafka transient errors (`NOT_LEADER`, coordinator move, timeout) plus I/O.
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Broker { code, .. } => matches!(
                *code,
                NOT_LEADER_OR_FOLLOWER
                    | LEADER_NOT_AVAILABLE
                    | NOT_ENOUGH_REPLICAS
                    | NOT_ENOUGH_REPLICAS_AFTER_APPEND
                    | REQUEST_TIMED_OUT
                    | COORDINATOR_LOAD_IN_PROGRESS
                    | COORDINATOR_NOT_AVAILABLE
                    | NOT_COORDINATOR
                    | NOT_CONTROLLER
                    | UNKNOWN_TOPIC_OR_PARTITION
                    | SHARE_SESSION_NOT_FOUND
                    | INVALID_SHARE_SESSION_EPOCH
            ),
            Self::Io(_) | Self::Timeout => true,
            _ => false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Protocol(m) => write!(f, "protocol: {m}"),
            Self::Broker { code, message } => {
                let name = error_name(*code).unwrap_or("unknown");
                if message.is_empty() {
                    write!(f, "broker error {code} ({name})")
                } else {
                    write!(f, "broker error {code} ({name}): {message}")
                }
            }
            Self::UnknownTopic(t) => write!(f, "unknown topic {t}"),
            Self::NoLeader { topic, partition } => {
                write!(f, "no leader for {topic}-{partition}")
            }
            Self::Unsupported(m) => write!(f, "unsupported: {m}"),
            Self::Closed => write!(f, "producer closed"),
            Self::Timeout => write!(f, "timeout"),
            Self::QueueFull => write!(f, "producer queue full"),
            Self::RecordTooLarge {
                size,
                max: _,
                config,
            } if *config == Self::BUFFER_MEMORY_CONFIG => write!(
                f,
                "The message is {size} bytes when serialized which is larger than the total memory buffer you have configured with the {config} configuration."
            ),
            Self::RecordTooLarge { size, max, config } => {
                write!(
                    f,
                    "The message is {size} bytes when serialized which is larger than {max}, which is the value of the {config} configuration."
                )
            }
            Self::MaxPollInterval => write!(f, "max.poll.interval.ms exceeded"),
            Self::Wakeup => write!(f, "wakeup"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Self::Io(e) => Self::Io(io::Error::new(e.kind(), e.to_string())),
            Self::Protocol(m) => Self::Protocol(m.clone()),
            Self::Broker { code, message } => Self::Broker {
                code: *code,
                message: message.clone(),
            },
            Self::UnknownTopic(t) => Self::UnknownTopic(t.clone()),
            Self::NoLeader { topic, partition } => Self::NoLeader {
                topic: topic.clone(),
                partition: *partition,
            },
            Self::Unsupported(m) => Self::Unsupported(m.clone()),
            Self::Closed => Self::Closed,
            Self::Timeout => Self::Timeout,
            Self::QueueFull => Self::QueueFull,
            Self::RecordTooLarge { size, max, config } => Self::RecordTooLarge {
                size: *size,
                max: *max,
                config,
            },
            Self::MaxPollInterval => Self::MaxPollInterval,
            Self::Wakeup => Self::Wakeup,
        }
    }
}

/// Client result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Kafka `UNKNOWN_SERVER_ERROR` (-1). Java `Errors.forCode` uses this for
/// a code this crate (or Kafka) does not name.
pub const UNKNOWN_SERVER_ERROR: i16 = -1;
/// Kafka `NONE` (0).
pub const NONE: i16 = 0;
/// Kafka `OFFSET_OUT_OF_RANGE` (1).
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
/// Kafka `CORRUPT_MESSAGE` (2).
pub const CORRUPT_MESSAGE: i16 = 2;
/// Kafka `UNKNOWN_TOPIC_OR_PARTITION` (3).
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// Kafka `INVALID_FETCH_SIZE` (4).
pub const INVALID_FETCH_SIZE: i16 = 4;
/// Kafka `LEADER_NOT_AVAILABLE` (5).
pub const LEADER_NOT_AVAILABLE: i16 = 5;
/// Kafka `NOT_LEADER_OR_FOLLOWER` (6).
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// Kafka `REQUEST_TIMED_OUT` (7).
pub const REQUEST_TIMED_OUT: i16 = 7;
/// Kafka `BROKER_NOT_AVAILABLE` (8).
pub const BROKER_NOT_AVAILABLE: i16 = 8;
/// Kafka `REPLICA_NOT_AVAILABLE` (9).
pub const REPLICA_NOT_AVAILABLE: i16 = 9;
/// Kafka `MESSAGE_TOO_LARGE` (10).
pub const MESSAGE_TOO_LARGE: i16 = 10;
/// Kafka `STALE_CONTROLLER_EPOCH` (11).
pub const STALE_CONTROLLER_EPOCH: i16 = 11;
/// Kafka `OFFSET_METADATA_TOO_LARGE` (12).
pub const OFFSET_METADATA_TOO_LARGE: i16 = 12;
/// Kafka `NETWORK_EXCEPTION` (13).
pub const NETWORK_EXCEPTION: i16 = 13;
/// Kafka `COORDINATOR_LOAD_IN_PROGRESS` (14).
pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
/// Kafka `COORDINATOR_NOT_AVAILABLE` (15).
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// Kafka `NOT_COORDINATOR` (16).
pub const NOT_COORDINATOR: i16 = 16;
/// Kafka `INVALID_TOPIC_EXCEPTION` (17).
pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
/// Kafka `RECORD_LIST_TOO_LARGE` (18).
pub const RECORD_LIST_TOO_LARGE: i16 = 18;
/// Kafka `NOT_ENOUGH_REPLICAS` (19).
pub const NOT_ENOUGH_REPLICAS: i16 = 19;
/// Kafka `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20).
pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
/// Kafka `INVALID_REQUIRED_ACKS` (21).
pub const INVALID_REQUIRED_ACKS: i16 = 21;
/// Kafka `ILLEGAL_GENERATION` (22).
pub const ILLEGAL_GENERATION: i16 = 22;
/// Kafka `INCONSISTENT_GROUP_PROTOCOL` (23).
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
/// Kafka `INVALID_GROUP_ID` (24).
pub const INVALID_GROUP_ID: i16 = 24;
/// Kafka `UNKNOWN_MEMBER_ID` (25).
pub const UNKNOWN_MEMBER_ID: i16 = 25;
/// Kafka `INVALID_SESSION_TIMEOUT` (26).
pub const INVALID_SESSION_TIMEOUT: i16 = 26;
/// Kafka `REBALANCE_IN_PROGRESS` (27).
pub const REBALANCE_IN_PROGRESS: i16 = 27;
/// Kafka `INVALID_COMMIT_OFFSET_SIZE` (28).
pub const INVALID_COMMIT_OFFSET_SIZE: i16 = 28;
/// Kafka `TOPIC_AUTHORIZATION_FAILED` (29).
pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
/// Kafka `GROUP_AUTHORIZATION_FAILED` (30).
pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
/// Kafka `CLUSTER_AUTHORIZATION_FAILED` (31).
pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
/// Kafka `INVALID_TIMESTAMP` (32).
pub const INVALID_TIMESTAMP: i16 = 32;
/// Kafka `UNSUPPORTED_SASL_MECHANISM` (33).
pub const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
/// Kafka `ILLEGAL_SASL_STATE` (34).
pub const ILLEGAL_SASL_STATE: i16 = 34;
/// Kafka `UNSUPPORTED_VERSION` (35).
pub const UNSUPPORTED_VERSION: i16 = 35;
/// Kafka `TOPIC_ALREADY_EXISTS` (36).
pub const TOPIC_ALREADY_EXISTS: i16 = 36;
/// Kafka `INVALID_PARTITIONS` (37).
pub const INVALID_PARTITIONS: i16 = 37;
/// Kafka `INVALID_REPLICATION_FACTOR` (38).
pub const INVALID_REPLICATION_FACTOR: i16 = 38;
/// Kafka `INVALID_REPLICA_ASSIGNMENT` (39).
pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
/// Kafka `INVALID_CONFIG` (40).
pub const INVALID_CONFIG: i16 = 40;
/// Kafka `NOT_CONTROLLER` (41).
pub const NOT_CONTROLLER: i16 = 41;
/// Kafka `INVALID_REQUEST` (42).
pub const INVALID_REQUEST: i16 = 42;
/// Kafka `UNSUPPORTED_FOR_MESSAGE_FORMAT` (43).
pub const UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 = 43;
/// Kafka `POLICY_VIOLATION` (44).
pub const POLICY_VIOLATION: i16 = 44;
/// Kafka `OUT_OF_ORDER_SEQUENCE_NUMBER` (45).
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
/// Kafka `DUPLICATE_SEQUENCE_NUMBER` (46).
pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
/// Kafka `INVALID_PRODUCER_EPOCH` (47).
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
/// Kafka `INVALID_TXN_STATE` (48).
pub const INVALID_TXN_STATE: i16 = 48;
/// Kafka `INVALID_PRODUCER_ID_MAPPING` (49).
pub const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
/// Kafka `INVALID_TRANSACTION_TIMEOUT` (50).
pub const INVALID_TRANSACTION_TIMEOUT: i16 = 50;
/// Kafka `CONCURRENT_TRANSACTIONS` (51).
pub const CONCURRENT_TRANSACTIONS: i16 = 51;
/// Kafka `TRANSACTION_COORDINATOR_FENCED` (52).
pub const TRANSACTION_COORDINATOR_FENCED: i16 = 52;
/// Kafka `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53).
pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
/// Kafka `SECURITY_DISABLED` (54).
pub const SECURITY_DISABLED: i16 = 54;
/// Kafka `OPERATION_NOT_ATTEMPTED` (55).
pub const OPERATION_NOT_ATTEMPTED: i16 = 55;
/// Kafka `KAFKA_STORAGE_ERROR` (56).
pub const KAFKA_STORAGE_ERROR: i16 = 56;
/// Kafka `LOG_DIR_NOT_FOUND` (57).
pub const LOG_DIR_NOT_FOUND: i16 = 57;
/// Kafka `SASL_AUTHENTICATION_FAILED` (58).
pub const SASL_AUTHENTICATION_FAILED: i16 = 58;
/// Kafka `UNKNOWN_PRODUCER_ID` (59).
pub const UNKNOWN_PRODUCER_ID: i16 = 59;
/// Kafka `REASSIGNMENT_IN_PROGRESS` (60).
pub const REASSIGNMENT_IN_PROGRESS: i16 = 60;
/// Kafka `DELEGATION_TOKEN_AUTH_DISABLED` (61).
pub const DELEGATION_TOKEN_AUTH_DISABLED: i16 = 61;
/// Kafka `DELEGATION_TOKEN_NOT_FOUND` (62).
pub const DELEGATION_TOKEN_NOT_FOUND: i16 = 62;
/// Kafka `DELEGATION_TOKEN_OWNER_MISMATCH` (63).
pub const DELEGATION_TOKEN_OWNER_MISMATCH: i16 = 63;
/// Kafka `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64). Official Java
/// `KafkaApis.handleCreateTokenRequest` / `handleRenewTokenRequest` /
/// `handleExpireTokenRequest` / `handleDescribeTokensRequest` write this
/// when the connection is not allowed to issue, renew, expire, or describe
/// a delegation token (PLAINTEXT / one-way SSL / already token-authenticated).
pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
/// Kafka `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65).
pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: i16 = 65;
/// Kafka `DELEGATION_TOKEN_EXPIRED` (66).
pub const DELEGATION_TOKEN_EXPIRED: i16 = 66;
/// Kafka `INVALID_PRINCIPAL_TYPE` (67).
pub const INVALID_PRINCIPAL_TYPE: i16 = 67;
/// Kafka `NON_EMPTY_GROUP` (68).
pub const NON_EMPTY_GROUP: i16 = 68;
/// Kafka `GROUP_ID_NOT_FOUND` (69).
pub const GROUP_ID_NOT_FOUND: i16 = 69;
/// Kafka `FETCH_SESSION_ID_NOT_FOUND` (70).
pub const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
/// Kafka `INVALID_FETCH_SESSION_EPOCH` (71).
pub const INVALID_FETCH_SESSION_EPOCH: i16 = 71;
/// Kafka `LISTENER_NOT_FOUND` (72).
pub const LISTENER_NOT_FOUND: i16 = 72;
/// Kafka `TOPIC_DELETION_DISABLED` (73).
pub const TOPIC_DELETION_DISABLED: i16 = 73;
/// Kafka `FENCED_LEADER_EPOCH` (74).
pub const FENCED_LEADER_EPOCH: i16 = 74;
/// Kafka `UNKNOWN_LEADER_EPOCH` (75).
pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
/// Kafka `UNSUPPORTED_COMPRESSION_TYPE` (76).
pub const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
/// Kafka `STALE_BROKER_EPOCH` (77).
pub const STALE_BROKER_EPOCH: i16 = 77;
/// Kafka `OFFSET_NOT_AVAILABLE` (78).
pub const OFFSET_NOT_AVAILABLE: i16 = 78;
/// Kafka `MEMBER_ID_REQUIRED` (79).
pub const MEMBER_ID_REQUIRED: i16 = 79;
/// Kafka `PREFERRED_LEADER_NOT_AVAILABLE` (80).
pub const PREFERRED_LEADER_NOT_AVAILABLE: i16 = 80;
/// Kafka `GROUP_MAX_SIZE_REACHED` (81).
pub const GROUP_MAX_SIZE_REACHED: i16 = 81;
/// Kafka `FENCED_INSTANCE_ID` (82).
pub const FENCED_INSTANCE_ID: i16 = 82;
/// Kafka `ELIGIBLE_LEADERS_NOT_AVAILABLE` (83).
pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: i16 = 83;
/// Kafka `ELECTION_NOT_NEEDED` (84).
pub const ELECTION_NOT_NEEDED: i16 = 84;
/// Kafka `NO_REASSIGNMENT_IN_PROGRESS` (85).
pub const NO_REASSIGNMENT_IN_PROGRESS: i16 = 85;
/// Kafka `GROUP_SUBSCRIBED_TO_TOPIC` (86).
pub const GROUP_SUBSCRIBED_TO_TOPIC: i16 = 86;
/// Kafka `INVALID_RECORD` (87).
pub const INVALID_RECORD: i16 = 87;
/// Kafka `UNSTABLE_OFFSET_COMMIT` (88).
pub const UNSTABLE_OFFSET_COMMIT: i16 = 88;
/// Kafka `THROTTLING_QUOTA_EXCEEDED` (89).
pub const THROTTLING_QUOTA_EXCEEDED: i16 = 89;
/// Kafka `PRODUCER_FENCED` (90).
pub const PRODUCER_FENCED: i16 = 90;
/// Kafka `RESOURCE_NOT_FOUND` (91).
pub const RESOURCE_NOT_FOUND: i16 = 91;
/// Kafka `DUPLICATE_RESOURCE` (92).
pub const DUPLICATE_RESOURCE: i16 = 92;
/// Kafka `UNACCEPTABLE_CREDENTIAL` (93).
pub const UNACCEPTABLE_CREDENTIAL: i16 = 93;
/// Kafka `INCONSISTENT_VOTER_SET` (94).
pub const INCONSISTENT_VOTER_SET: i16 = 94;
/// Kafka `INVALID_UPDATE_VERSION` (95).
pub const INVALID_UPDATE_VERSION: i16 = 95;
/// Kafka `FEATURE_UPDATE_FAILED` (96).
pub const FEATURE_UPDATE_FAILED: i16 = 96;
/// Kafka `PRINCIPAL_DESERIALIZATION_FAILURE` (97).
pub const PRINCIPAL_DESERIALIZATION_FAILURE: i16 = 97;
/// Kafka `SNAPSHOT_NOT_FOUND` (98).
pub const SNAPSHOT_NOT_FOUND: i16 = 98;
/// Kafka `POSITION_OUT_OF_RANGE` (99).
pub const POSITION_OUT_OF_RANGE: i16 = 99;
/// Kafka `UNKNOWN_TOPIC_ID` (100).
pub const UNKNOWN_TOPIC_ID: i16 = 100;
/// Kafka `DUPLICATE_BROKER_REGISTRATION` (101).
pub const DUPLICATE_BROKER_REGISTRATION: i16 = 101;
/// Kafka `BROKER_ID_NOT_REGISTERED` (102).
pub const BROKER_ID_NOT_REGISTERED: i16 = 102;
/// Kafka `INCONSISTENT_TOPIC_ID` (103).
pub const INCONSISTENT_TOPIC_ID: i16 = 103;
/// Kafka `INCONSISTENT_CLUSTER_ID` (104).
pub const INCONSISTENT_CLUSTER_ID: i16 = 104;
/// Kafka `TRANSACTIONAL_ID_NOT_FOUND` (105).
pub const TRANSACTIONAL_ID_NOT_FOUND: i16 = 105;
/// Kafka `FETCH_SESSION_TOPIC_ID_ERROR` (106).
pub const FETCH_SESSION_TOPIC_ID_ERROR: i16 = 106;
/// Kafka `INELIGIBLE_REPLICA` (107).
pub const INELIGIBLE_REPLICA: i16 = 107;
/// Kafka `NEW_LEADER_ELECTED` (108).
pub const NEW_LEADER_ELECTED: i16 = 108;
/// Kafka `OFFSET_MOVED_TO_TIERED_STORAGE` (109).
pub const OFFSET_MOVED_TO_TIERED_STORAGE: i16 = 109;
/// Kafka `FENCED_MEMBER_EPOCH` (110).
pub const FENCED_MEMBER_EPOCH: i16 = 110;
/// Kafka `UNRELEASED_INSTANCE_ID` (111).
pub const UNRELEASED_INSTANCE_ID: i16 = 111;
/// Kafka `UNSUPPORTED_ASSIGNOR` (112).
pub const UNSUPPORTED_ASSIGNOR: i16 = 112;
/// Kafka `STALE_MEMBER_EPOCH` (113).
pub const STALE_MEMBER_EPOCH: i16 = 113;
/// Kafka `MISMATCHED_ENDPOINT_TYPE` (114).
pub const MISMATCHED_ENDPOINT_TYPE: i16 = 114;
/// Kafka `UNSUPPORTED_ENDPOINT_TYPE` (115).
pub const UNSUPPORTED_ENDPOINT_TYPE: i16 = 115;
/// Kafka `UNKNOWN_CONTROLLER_ID` (116).
pub const UNKNOWN_CONTROLLER_ID: i16 = 116;
/// Kafka `UNKNOWN_SUBSCRIPTION_ID` (117).
pub const UNKNOWN_SUBSCRIPTION_ID: i16 = 117;
/// Kafka `TELEMETRY_TOO_LARGE` (118).
pub const TELEMETRY_TOO_LARGE: i16 = 118;
/// Kafka `INVALID_REGISTRATION` (119).
pub const INVALID_REGISTRATION: i16 = 119;
/// Kafka `TRANSACTION_ABORTABLE` (120).
pub const TRANSACTION_ABORTABLE: i16 = 120;
/// Kafka `INVALID_RECORD_STATE` (121).
pub const INVALID_RECORD_STATE: i16 = 121;
/// Kafka `SHARE_SESSION_NOT_FOUND` (122).
pub const SHARE_SESSION_NOT_FOUND: i16 = 122;
/// Kafka `INVALID_SHARE_SESSION_EPOCH` (123).
pub const INVALID_SHARE_SESSION_EPOCH: i16 = 123;
/// Kafka `FENCED_STATE_EPOCH` (124).
pub const FENCED_STATE_EPOCH: i16 = 124;
/// Kafka `INVALID_VOTER_KEY` (125).
pub const INVALID_VOTER_KEY: i16 = 125;
/// Kafka `DUPLICATE_VOTER` (126).
pub const DUPLICATE_VOTER: i16 = 126;
/// Kafka `VOTER_NOT_FOUND` (127).
pub const VOTER_NOT_FOUND: i16 = 127;
/// Kafka `INVALID_REGULAR_EXPRESSION` (128).
pub const INVALID_REGULAR_EXPRESSION: i16 = 128;
/// Kafka `REBOOTSTRAP_REQUIRED` (129).
pub const REBOOTSTRAP_REQUIRED: i16 = 129;
/// Kafka `SHARE_SESSION_LIMIT_REACHED` (133). Not in Kafka 4.0.0 `Errors`
/// (4.0.0 ends at [`REBOOTSTRAP_REQUIRED`]); later Kafka names 133 for the
/// share session cap.
pub const SHARE_SESSION_LIMIT_REACHED: i16 = 133;

/// Coordinator is loading, missing, or not this node. Rediscover and retry.
pub fn coordinator_retriable(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
    )
}

/// Per-group ConsumerGroupDescribe codes that Java `describeConsumerGroups`
/// retries with DescribeGroups (api 15).
#[must_use]
pub fn consumer_group_describe_classic_fallback(code: i16) -> bool {
    matches!(code, UNSUPPORTED_VERSION | GROUP_ID_NOT_FOUND)
}

/// Kafka `Errors` enum name for `code`, when this crate names it.
///
/// The table is Kafka 4.0.0 `Errors` (`-1` through `129`) plus
/// [`SHARE_SESSION_LIMIT_REACHED`] (133).
#[must_use]
pub fn error_name(code: i16) -> Option<&'static str> {
    Some(match code {
        UNKNOWN_SERVER_ERROR => "UNKNOWN_SERVER_ERROR",
        NONE => "NONE",
        OFFSET_OUT_OF_RANGE => "OFFSET_OUT_OF_RANGE",
        CORRUPT_MESSAGE => "CORRUPT_MESSAGE",
        UNKNOWN_TOPIC_OR_PARTITION => "UNKNOWN_TOPIC_OR_PARTITION",
        INVALID_FETCH_SIZE => "INVALID_FETCH_SIZE",
        LEADER_NOT_AVAILABLE => "LEADER_NOT_AVAILABLE",
        NOT_LEADER_OR_FOLLOWER => "NOT_LEADER_OR_FOLLOWER",
        REQUEST_TIMED_OUT => "REQUEST_TIMED_OUT",
        BROKER_NOT_AVAILABLE => "BROKER_NOT_AVAILABLE",
        REPLICA_NOT_AVAILABLE => "REPLICA_NOT_AVAILABLE",
        MESSAGE_TOO_LARGE => "MESSAGE_TOO_LARGE",
        STALE_CONTROLLER_EPOCH => "STALE_CONTROLLER_EPOCH",
        OFFSET_METADATA_TOO_LARGE => "OFFSET_METADATA_TOO_LARGE",
        NETWORK_EXCEPTION => "NETWORK_EXCEPTION",
        COORDINATOR_LOAD_IN_PROGRESS => "COORDINATOR_LOAD_IN_PROGRESS",
        COORDINATOR_NOT_AVAILABLE => "COORDINATOR_NOT_AVAILABLE",
        NOT_COORDINATOR => "NOT_COORDINATOR",
        INVALID_TOPIC_EXCEPTION => "INVALID_TOPIC_EXCEPTION",
        RECORD_LIST_TOO_LARGE => "RECORD_LIST_TOO_LARGE",
        NOT_ENOUGH_REPLICAS => "NOT_ENOUGH_REPLICAS",
        NOT_ENOUGH_REPLICAS_AFTER_APPEND => "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        INVALID_REQUIRED_ACKS => "INVALID_REQUIRED_ACKS",
        ILLEGAL_GENERATION => "ILLEGAL_GENERATION",
        INCONSISTENT_GROUP_PROTOCOL => "INCONSISTENT_GROUP_PROTOCOL",
        INVALID_GROUP_ID => "INVALID_GROUP_ID",
        UNKNOWN_MEMBER_ID => "UNKNOWN_MEMBER_ID",
        INVALID_SESSION_TIMEOUT => "INVALID_SESSION_TIMEOUT",
        REBALANCE_IN_PROGRESS => "REBALANCE_IN_PROGRESS",
        INVALID_COMMIT_OFFSET_SIZE => "INVALID_COMMIT_OFFSET_SIZE",
        TOPIC_AUTHORIZATION_FAILED => "TOPIC_AUTHORIZATION_FAILED",
        GROUP_AUTHORIZATION_FAILED => "GROUP_AUTHORIZATION_FAILED",
        CLUSTER_AUTHORIZATION_FAILED => "CLUSTER_AUTHORIZATION_FAILED",
        INVALID_TIMESTAMP => "INVALID_TIMESTAMP",
        UNSUPPORTED_SASL_MECHANISM => "UNSUPPORTED_SASL_MECHANISM",
        ILLEGAL_SASL_STATE => "ILLEGAL_SASL_STATE",
        UNSUPPORTED_VERSION => "UNSUPPORTED_VERSION",
        TOPIC_ALREADY_EXISTS => "TOPIC_ALREADY_EXISTS",
        INVALID_PARTITIONS => "INVALID_PARTITIONS",
        INVALID_REPLICATION_FACTOR => "INVALID_REPLICATION_FACTOR",
        INVALID_REPLICA_ASSIGNMENT => "INVALID_REPLICA_ASSIGNMENT",
        INVALID_CONFIG => "INVALID_CONFIG",
        NOT_CONTROLLER => "NOT_CONTROLLER",
        INVALID_REQUEST => "INVALID_REQUEST",
        UNSUPPORTED_FOR_MESSAGE_FORMAT => "UNSUPPORTED_FOR_MESSAGE_FORMAT",
        POLICY_VIOLATION => "POLICY_VIOLATION",
        OUT_OF_ORDER_SEQUENCE_NUMBER => "OUT_OF_ORDER_SEQUENCE_NUMBER",
        DUPLICATE_SEQUENCE_NUMBER => "DUPLICATE_SEQUENCE_NUMBER",
        INVALID_PRODUCER_EPOCH => "INVALID_PRODUCER_EPOCH",
        INVALID_TXN_STATE => "INVALID_TXN_STATE",
        INVALID_PRODUCER_ID_MAPPING => "INVALID_PRODUCER_ID_MAPPING",
        INVALID_TRANSACTION_TIMEOUT => "INVALID_TRANSACTION_TIMEOUT",
        CONCURRENT_TRANSACTIONS => "CONCURRENT_TRANSACTIONS",
        TRANSACTION_COORDINATOR_FENCED => "TRANSACTION_COORDINATOR_FENCED",
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED => "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        SECURITY_DISABLED => "SECURITY_DISABLED",
        OPERATION_NOT_ATTEMPTED => "OPERATION_NOT_ATTEMPTED",
        KAFKA_STORAGE_ERROR => "KAFKA_STORAGE_ERROR",
        LOG_DIR_NOT_FOUND => "LOG_DIR_NOT_FOUND",
        SASL_AUTHENTICATION_FAILED => "SASL_AUTHENTICATION_FAILED",
        UNKNOWN_PRODUCER_ID => "UNKNOWN_PRODUCER_ID",
        REASSIGNMENT_IN_PROGRESS => "REASSIGNMENT_IN_PROGRESS",
        DELEGATION_TOKEN_AUTH_DISABLED => "DELEGATION_TOKEN_AUTH_DISABLED",
        DELEGATION_TOKEN_NOT_FOUND => "DELEGATION_TOKEN_NOT_FOUND",
        DELEGATION_TOKEN_OWNER_MISMATCH => "DELEGATION_TOKEN_OWNER_MISMATCH",
        DELEGATION_TOKEN_REQUEST_NOT_ALLOWED => "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
        DELEGATION_TOKEN_AUTHORIZATION_FAILED => "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
        DELEGATION_TOKEN_EXPIRED => "DELEGATION_TOKEN_EXPIRED",
        INVALID_PRINCIPAL_TYPE => "INVALID_PRINCIPAL_TYPE",
        NON_EMPTY_GROUP => "NON_EMPTY_GROUP",
        GROUP_ID_NOT_FOUND => "GROUP_ID_NOT_FOUND",
        FETCH_SESSION_ID_NOT_FOUND => "FETCH_SESSION_ID_NOT_FOUND",
        INVALID_FETCH_SESSION_EPOCH => "INVALID_FETCH_SESSION_EPOCH",
        LISTENER_NOT_FOUND => "LISTENER_NOT_FOUND",
        TOPIC_DELETION_DISABLED => "TOPIC_DELETION_DISABLED",
        FENCED_LEADER_EPOCH => "FENCED_LEADER_EPOCH",
        UNKNOWN_LEADER_EPOCH => "UNKNOWN_LEADER_EPOCH",
        UNSUPPORTED_COMPRESSION_TYPE => "UNSUPPORTED_COMPRESSION_TYPE",
        STALE_BROKER_EPOCH => "STALE_BROKER_EPOCH",
        OFFSET_NOT_AVAILABLE => "OFFSET_NOT_AVAILABLE",
        MEMBER_ID_REQUIRED => "MEMBER_ID_REQUIRED",
        PREFERRED_LEADER_NOT_AVAILABLE => "PREFERRED_LEADER_NOT_AVAILABLE",
        GROUP_MAX_SIZE_REACHED => "GROUP_MAX_SIZE_REACHED",
        FENCED_INSTANCE_ID => "FENCED_INSTANCE_ID",
        ELIGIBLE_LEADERS_NOT_AVAILABLE => "ELIGIBLE_LEADERS_NOT_AVAILABLE",
        ELECTION_NOT_NEEDED => "ELECTION_NOT_NEEDED",
        NO_REASSIGNMENT_IN_PROGRESS => "NO_REASSIGNMENT_IN_PROGRESS",
        GROUP_SUBSCRIBED_TO_TOPIC => "GROUP_SUBSCRIBED_TO_TOPIC",
        INVALID_RECORD => "INVALID_RECORD",
        UNSTABLE_OFFSET_COMMIT => "UNSTABLE_OFFSET_COMMIT",
        THROTTLING_QUOTA_EXCEEDED => "THROTTLING_QUOTA_EXCEEDED",
        PRODUCER_FENCED => "PRODUCER_FENCED",
        RESOURCE_NOT_FOUND => "RESOURCE_NOT_FOUND",
        DUPLICATE_RESOURCE => "DUPLICATE_RESOURCE",
        UNACCEPTABLE_CREDENTIAL => "UNACCEPTABLE_CREDENTIAL",
        INCONSISTENT_VOTER_SET => "INCONSISTENT_VOTER_SET",
        INVALID_UPDATE_VERSION => "INVALID_UPDATE_VERSION",
        FEATURE_UPDATE_FAILED => "FEATURE_UPDATE_FAILED",
        PRINCIPAL_DESERIALIZATION_FAILURE => "PRINCIPAL_DESERIALIZATION_FAILURE",
        SNAPSHOT_NOT_FOUND => "SNAPSHOT_NOT_FOUND",
        POSITION_OUT_OF_RANGE => "POSITION_OUT_OF_RANGE",
        UNKNOWN_TOPIC_ID => "UNKNOWN_TOPIC_ID",
        DUPLICATE_BROKER_REGISTRATION => "DUPLICATE_BROKER_REGISTRATION",
        BROKER_ID_NOT_REGISTERED => "BROKER_ID_NOT_REGISTERED",
        INCONSISTENT_TOPIC_ID => "INCONSISTENT_TOPIC_ID",
        INCONSISTENT_CLUSTER_ID => "INCONSISTENT_CLUSTER_ID",
        TRANSACTIONAL_ID_NOT_FOUND => "TRANSACTIONAL_ID_NOT_FOUND",
        FETCH_SESSION_TOPIC_ID_ERROR => "FETCH_SESSION_TOPIC_ID_ERROR",
        INELIGIBLE_REPLICA => "INELIGIBLE_REPLICA",
        NEW_LEADER_ELECTED => "NEW_LEADER_ELECTED",
        OFFSET_MOVED_TO_TIERED_STORAGE => "OFFSET_MOVED_TO_TIERED_STORAGE",
        FENCED_MEMBER_EPOCH => "FENCED_MEMBER_EPOCH",
        UNRELEASED_INSTANCE_ID => "UNRELEASED_INSTANCE_ID",
        UNSUPPORTED_ASSIGNOR => "UNSUPPORTED_ASSIGNOR",
        STALE_MEMBER_EPOCH => "STALE_MEMBER_EPOCH",
        MISMATCHED_ENDPOINT_TYPE => "MISMATCHED_ENDPOINT_TYPE",
        UNSUPPORTED_ENDPOINT_TYPE => "UNSUPPORTED_ENDPOINT_TYPE",
        UNKNOWN_CONTROLLER_ID => "UNKNOWN_CONTROLLER_ID",
        UNKNOWN_SUBSCRIPTION_ID => "UNKNOWN_SUBSCRIPTION_ID",
        TELEMETRY_TOO_LARGE => "TELEMETRY_TOO_LARGE",
        INVALID_REGISTRATION => "INVALID_REGISTRATION",
        TRANSACTION_ABORTABLE => "TRANSACTION_ABORTABLE",
        INVALID_RECORD_STATE => "INVALID_RECORD_STATE",
        SHARE_SESSION_NOT_FOUND => "SHARE_SESSION_NOT_FOUND",
        INVALID_SHARE_SESSION_EPOCH => "INVALID_SHARE_SESSION_EPOCH",
        FENCED_STATE_EPOCH => "FENCED_STATE_EPOCH",
        INVALID_VOTER_KEY => "INVALID_VOTER_KEY",
        DUPLICATE_VOTER => "DUPLICATE_VOTER",
        VOTER_NOT_FOUND => "VOTER_NOT_FOUND",
        INVALID_REGULAR_EXPRESSION => "INVALID_REGULAR_EXPRESSION",
        REBOOTSTRAP_REQUIRED => "REBOOTSTRAP_REQUIRED",
        SHARE_SESSION_LIMIT_REACHED => "SHARE_SESSION_LIMIT_REACHED",
        _ => return None,
    })
}

/// Java `Errors.forCode` then the enum name.
///
/// Names match Kafka 4.0.0 `Errors`. Unknown codes are
/// `UNKNOWN_SERVER_ERROR`. [`SHARE_SESSION_LIMIT_REACHED`] (133) is named
/// for later Kafka; 4.0.0 `forCode` for 133 is `UNKNOWN_SERVER_ERROR`.
#[must_use]
pub fn for_code(code: i16) -> &'static str {
    error_name(code).unwrap_or("UNKNOWN_SERVER_ERROR")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_code_matches_java_errors() {
        assert_eq!(for_code(NONE), "NONE");
        assert_eq!(for_code(UNKNOWN_SERVER_ERROR), "UNKNOWN_SERVER_ERROR");
        assert_eq!(for_code(-1), "UNKNOWN_SERVER_ERROR");
        assert_eq!(
            for_code(UNKNOWN_TOPIC_OR_PARTITION),
            "UNKNOWN_TOPIC_OR_PARTITION"
        );
        assert_eq!(for_code(999), "UNKNOWN_SERVER_ERROR");
        assert_eq!(error_name(NONE), Some("NONE"));
        assert_eq!(
            error_name(UNKNOWN_SERVER_ERROR),
            Some("UNKNOWN_SERVER_ERROR")
        );
        assert_eq!(error_name(999), None);
        assert_eq!(CORRUPT_MESSAGE, 2);
        assert_eq!(for_code(CORRUPT_MESSAGE), "CORRUPT_MESSAGE");
        assert_eq!(INVALID_FETCH_SIZE, 4);
        assert_eq!(for_code(INVALID_FETCH_SIZE), "INVALID_FETCH_SIZE");
        assert_eq!(RECORD_LIST_TOO_LARGE, 18);
        assert_eq!(for_code(RECORD_LIST_TOO_LARGE), "RECORD_LIST_TOO_LARGE");
        assert_eq!(INVALID_REQUEST, 42);
        assert_eq!(INVALID_TXN_STATE, 48);
        assert_eq!(for_code(42), "INVALID_REQUEST");
        assert_eq!(for_code(INVALID_TXN_STATE), "INVALID_TXN_STATE");
        assert_eq!(SECURITY_DISABLED, 54);
        assert_eq!(UNKNOWN_PRODUCER_ID, 59);
        assert_eq!(for_code(54), "SECURITY_DISABLED");
        assert_eq!(for_code(UNKNOWN_PRODUCER_ID), "UNKNOWN_PRODUCER_ID");
        assert_eq!(UNKNOWN_LEADER_EPOCH, 75);
        assert_eq!(STALE_BROKER_EPOCH, 77);
        assert_eq!(for_code(75), "UNKNOWN_LEADER_EPOCH");
        assert_eq!(for_code(77), "STALE_BROKER_EPOCH");
        assert_eq!(GROUP_SUBSCRIBED_TO_TOPIC, 86);
        assert_eq!(PRODUCER_FENCED, 90);
        assert_eq!(for_code(86), "GROUP_SUBSCRIBED_TO_TOPIC");
        assert_eq!(for_code(PRODUCER_FENCED), "PRODUCER_FENCED");
        assert_eq!(REBOOTSTRAP_REQUIRED, 129);
        assert_eq!(for_code(REBOOTSTRAP_REQUIRED), "REBOOTSTRAP_REQUIRED");
        assert_eq!(error_name(130), None);
        assert_eq!(for_code(130), "UNKNOWN_SERVER_ERROR");
        assert_eq!(for_code(131), "UNKNOWN_SERVER_ERROR");
        assert_eq!(for_code(132), "UNKNOWN_SERVER_ERROR");
        assert_eq!(SHARE_SESSION_LIMIT_REACHED, 133);
        assert_eq!(
            for_code(SHARE_SESSION_LIMIT_REACHED),
            "SHARE_SESSION_LIMIT_REACHED"
        );
        assert_eq!(for_code(134), "UNKNOWN_SERVER_ERROR");
        let mut names = std::collections::HashSet::new();
        for code in -1_i16..=129 {
            let name = error_name(code).unwrap_or("");
            assert!(!name.is_empty(), "unnamed Kafka 4.0.0 error {code}");
            assert_eq!(for_code(code), name);
            assert!(names.insert(name), "duplicate Errors name {name}");
        }
        assert_eq!(names.len(), 131);
    }
}
