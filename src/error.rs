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
    /// [`crate::ConsumerGroup::poll`] was not called within `max.poll.interval.ms`.
    MaxPollInterval,
    /// [`crate::Consumer::wakeup`] interrupted fetch or poll.
    Wakeup,
}

impl Error {
    /// Wrap a protocol / client-side failure.
    pub fn protocol(msg: impl Into<String>) -> Self {
        Self::Protocol(msg.into())
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
            Self::MaxPollInterval => Self::MaxPollInterval,
            Self::Wakeup => Self::Wakeup,
        }
    }
}

/// Client result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Kafka `NONE` (0).
pub const NONE: i16 = 0;
/// Kafka `OFFSET_OUT_OF_RANGE` (1).
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
/// Kafka `UNKNOWN_TOPIC_OR_PARTITION` (3).
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// Kafka `LEADER_NOT_AVAILABLE` (5).
pub const LEADER_NOT_AVAILABLE: i16 = 5;
/// Kafka `NOT_LEADER_OR_FOLLOWER` (6).
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// Kafka `REQUEST_TIMED_OUT` (7).
pub const REQUEST_TIMED_OUT: i16 = 7;
/// Kafka `NOT_ENOUGH_REPLICAS` (19).
pub const NOT_ENOUGH_REPLICAS: i16 = 19;
/// Kafka `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20).
pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
/// Kafka `INVALID_REQUIRED_ACKS` (21).
pub const INVALID_REQUIRED_ACKS: i16 = 21;
/// Kafka `COORDINATOR_LOAD_IN_PROGRESS` (14).
pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
/// Kafka `COORDINATOR_NOT_AVAILABLE` (15).
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// Kafka `NOT_COORDINATOR` (16).
pub const NOT_COORDINATOR: i16 = 16;
/// Kafka `NOT_CONTROLLER` (41).
pub const NOT_CONTROLLER: i16 = 41;
/// Official Java `KafkaApis.handleCreateTokenRequest` /
/// `handleRenewTokenRequest` / `handleExpireTokenRequest` /
/// `handleDescribeTokensRequest` write this when the connection is
/// not allowed to issue, renew, expire, or describe a delegation
/// token (PLAINTEXT / one-way SSL / already token-authenticated).
pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
/// Kafka `INVALID_TOPIC_EXCEPTION` (17).
pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
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
/// Kafka `INVALID_TXN_STATE` (42).
pub const INVALID_TXN_STATE: i16 = 42;
/// Kafka `TOPIC_AUTHORIZATION_FAILED` (29).
pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
/// Kafka `CLUSTER_AUTHORIZATION_FAILED` (31).
pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
/// Kafka `MESSAGE_TOO_LARGE` (10).
pub const MESSAGE_TOO_LARGE: i16 = 10;
/// Kafka `SASL_AUTHENTICATION_FAILED` (58).
pub const SASL_AUTHENTICATION_FAILED: i16 = 58;
/// Kafka `REBALANCE_IN_PROGRESS` (27).
pub const REBALANCE_IN_PROGRESS: i16 = 27;
/// Kafka `MEMBER_ID_REQUIRED` (79).
pub const MEMBER_ID_REQUIRED: i16 = 79;
/// Kafka `OUT_OF_ORDER_SEQUENCE_NUMBER` (45).
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
/// Kafka `DUPLICATE_SEQUENCE_NUMBER` (46).
pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
/// Kafka `FENCED_LEADER_EPOCH` (74).
pub const FENCED_LEADER_EPOCH: i16 = 74;
/// Kafka `UNKNOWN_LEADER_EPOCH` (77).
pub const UNKNOWN_LEADER_EPOCH: i16 = 77;
/// Kafka `INVALID_RECORD_STATE` (121).
pub const INVALID_RECORD_STATE: i16 = 121;
/// Kafka `SHARE_SESSION_NOT_FOUND` (122).
pub const SHARE_SESSION_NOT_FOUND: i16 = 122;
/// Kafka `INVALID_SHARE_SESSION_EPOCH` (123).
pub const INVALID_SHARE_SESSION_EPOCH: i16 = 123;
/// Kafka `SHARE_SESSION_LIMIT_REACHED` (133).
pub const SHARE_SESSION_LIMIT_REACHED: i16 = 133;

/// Coordinator is loading, missing, or not this node. Rediscover and retry.
pub fn coordinator_retriable(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
    )
}

/// Kafka error code name, when this crate knows it.
#[must_use]
pub fn error_name(code: i16) -> Option<&'static str> {
    Some(match code {
        NONE => "NONE",
        OFFSET_OUT_OF_RANGE => "OFFSET_OUT_OF_RANGE",
        UNKNOWN_TOPIC_OR_PARTITION => "UNKNOWN_TOPIC_OR_PARTITION",
        LEADER_NOT_AVAILABLE => "LEADER_NOT_AVAILABLE",
        NOT_LEADER_OR_FOLLOWER => "NOT_LEADER_OR_FOLLOWER",
        REQUEST_TIMED_OUT => "REQUEST_TIMED_OUT",
        COORDINATOR_LOAD_IN_PROGRESS => "COORDINATOR_LOAD_IN_PROGRESS",
        COORDINATOR_NOT_AVAILABLE => "COORDINATOR_NOT_AVAILABLE",
        NOT_ENOUGH_REPLICAS => "NOT_ENOUGH_REPLICAS",
        NOT_ENOUGH_REPLICAS_AFTER_APPEND => "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        INVALID_REQUIRED_ACKS => "INVALID_REQUIRED_ACKS",
        NOT_COORDINATOR => "NOT_COORDINATOR",
        NOT_CONTROLLER => "NOT_CONTROLLER",
        DELEGATION_TOKEN_REQUEST_NOT_ALLOWED => "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
        INVALID_TOPIC_EXCEPTION => "INVALID_TOPIC_EXCEPTION",
        UNSUPPORTED_VERSION => "UNSUPPORTED_VERSION",
        TOPIC_ALREADY_EXISTS => "TOPIC_ALREADY_EXISTS",
        INVALID_PARTITIONS => "INVALID_PARTITIONS",
        INVALID_REPLICATION_FACTOR => "INVALID_REPLICATION_FACTOR",
        INVALID_REPLICA_ASSIGNMENT => "INVALID_REPLICA_ASSIGNMENT",
        INVALID_CONFIG => "INVALID_CONFIG",
        INVALID_TXN_STATE => "INVALID_TXN_STATE",
        TOPIC_AUTHORIZATION_FAILED => "TOPIC_AUTHORIZATION_FAILED",
        CLUSTER_AUTHORIZATION_FAILED => "CLUSTER_AUTHORIZATION_FAILED",
        MESSAGE_TOO_LARGE => "MESSAGE_TOO_LARGE",
        SASL_AUTHENTICATION_FAILED => "SASL_AUTHENTICATION_FAILED",
        REBALANCE_IN_PROGRESS => "REBALANCE_IN_PROGRESS",
        MEMBER_ID_REQUIRED => "MEMBER_ID_REQUIRED",
        OUT_OF_ORDER_SEQUENCE_NUMBER => "OUT_OF_ORDER_SEQUENCE_NUMBER",
        DUPLICATE_SEQUENCE_NUMBER => "DUPLICATE_SEQUENCE_NUMBER",
        FENCED_LEADER_EPOCH => "FENCED_LEADER_EPOCH",
        UNKNOWN_LEADER_EPOCH => "UNKNOWN_LEADER_EPOCH",
        INVALID_RECORD_STATE => "INVALID_RECORD_STATE",
        SHARE_SESSION_NOT_FOUND => "SHARE_SESSION_NOT_FOUND",
        INVALID_SHARE_SESSION_EPOCH => "INVALID_SHARE_SESSION_EPOCH",
        SHARE_SESSION_LIMIT_REACHED => "SHARE_SESSION_LIMIT_REACHED",
        _ => return None,
    })
}
