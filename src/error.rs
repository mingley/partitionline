use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Protocol(String),
    Broker { code: i16, message: String },
    UnknownTopic(String),
    NoLeader { topic: String, partition: i32 },
    Unsupported(String),
    Closed,
    Timeout,
    QueueFull,
}

impl Error {
    pub fn protocol(msg: impl Into<String>) -> Self {
        Self::Protocol(msg.into())
    }

    pub fn broker(code: i16, message: impl Into<String>) -> Self {
        Self::Broker {
            code,
            message: message.into(),
        }
    }

    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Broker { code, .. } => matches!(
                *code,
                NOT_LEADER_OR_FOLLOWER
                    | LEADER_NOT_AVAILABLE
                    | NOT_ENOUGH_REPLICAS
                    | NOT_ENOUGH_REPLICAS_AFTER_APPEND
                    | REQUEST_TIMED_OUT
                    | NOT_COORDINATOR
                    | UNKNOWN_TOPIC_OR_PARTITION
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
                write!(
                    f,
                    "broker error {code} ({}){suffix}",
                    error_name(*code).unwrap_or("unknown"),
                    suffix = if message.is_empty() {
                        String::new()
                    } else {
                        format!(": {message}")
                    }
                )
            }
            Self::UnknownTopic(t) => write!(f, "unknown topic {t}"),
            Self::NoLeader { topic, partition } => {
                write!(f, "no leader for {topic}-{partition}")
            }
            Self::Unsupported(m) => write!(f, "unsupported: {m}"),
            Self::Closed => write!(f, "producer closed"),
            Self::Timeout => write!(f, "timeout"),
            Self::QueueFull => write!(f, "producer queue full"),
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

pub type Result<T> = std::result::Result<T, Error>;

pub const NONE: i16 = 0;
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const LEADER_NOT_AVAILABLE: i16 = 5;
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const REQUEST_TIMED_OUT: i16 = 7;
pub const NOT_ENOUGH_REPLICAS: i16 = 19;
pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
pub const INVALID_REQUIRED_ACKS: i16 = 21;
pub const NOT_COORDINATOR: i16 = 16;
pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const TOPIC_ALREADY_EXISTS: i16 = 36;
pub const INVALID_PARTITIONS: i16 = 37;
pub const INVALID_REPLICATION_FACTOR: i16 = 38;
pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
pub const INVALID_CONFIG: i16 = 40;
pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
pub const MESSAGE_TOO_LARGE: i16 = 10;
pub const SASL_AUTHENTICATION_FAILED: i16 = 58;
pub const MEMBER_ID_REQUIRED: i16 = 79;
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;

pub fn error_name(code: i16) -> Option<&'static str> {
    Some(match code {
        NONE => "NONE",
        OFFSET_OUT_OF_RANGE => "OFFSET_OUT_OF_RANGE",
        UNKNOWN_TOPIC_OR_PARTITION => "UNKNOWN_TOPIC_OR_PARTITION",
        LEADER_NOT_AVAILABLE => "LEADER_NOT_AVAILABLE",
        NOT_LEADER_OR_FOLLOWER => "NOT_LEADER_OR_FOLLOWER",
        REQUEST_TIMED_OUT => "REQUEST_TIMED_OUT",
        NOT_ENOUGH_REPLICAS => "NOT_ENOUGH_REPLICAS",
        NOT_ENOUGH_REPLICAS_AFTER_APPEND => "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        INVALID_REQUIRED_ACKS => "INVALID_REQUIRED_ACKS",
        NOT_COORDINATOR => "NOT_COORDINATOR",
        INVALID_TOPIC_EXCEPTION => "INVALID_TOPIC_EXCEPTION",
        UNSUPPORTED_VERSION => "UNSUPPORTED_VERSION",
        TOPIC_ALREADY_EXISTS => "TOPIC_ALREADY_EXISTS",
        INVALID_PARTITIONS => "INVALID_PARTITIONS",
        INVALID_REPLICATION_FACTOR => "INVALID_REPLICATION_FACTOR",
        INVALID_REPLICA_ASSIGNMENT => "INVALID_REPLICA_ASSIGNMENT",
        INVALID_CONFIG => "INVALID_CONFIG",
        TOPIC_AUTHORIZATION_FAILED => "TOPIC_AUTHORIZATION_FAILED",
        MESSAGE_TOO_LARGE => "MESSAGE_TOO_LARGE",
        SASL_AUTHENTICATION_FAILED => "SASL_AUTHENTICATION_FAILED",
        MEMBER_ID_REQUIRED => "MEMBER_ID_REQUIRED",
        OUT_OF_ORDER_SEQUENCE_NUMBER => "OUT_OF_ORDER_SEQUENCE_NUMBER",
        DUPLICATE_SEQUENCE_NUMBER => "DUPLICATE_SEQUENCE_NUMBER",
        _ => return None,
    })
}
