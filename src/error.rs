//! Client errors. Broker `error_code` values come from `kafka_protocol::error`.

use kafka_protocol::error::ResponseError;

/// Partitionline result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Recoverable client failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// I/O on a broker connection.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// kafka-protocol encode/decode (`anyhow` inside that crate).
    #[error("protocol: {0}")]
    Protocol(String),

    /// Broker returned a non-zero error code.
    #[error("broker {code}: {kind:?}")]
    Broker {
        /// Raw Kafka error code.
        code: i16,
        /// Mapped `kafka_protocol` error, if known.
        kind: Option<ResponseError>,
    },

    /// Size prefix larger than we will allocate.
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(i32),

    /// Correlation id on a response did not match the in-flight request.
    #[error("correlation mismatch: expected {expected}, got {got}")]
    Correlation {
        /// Id we sent.
        expected: i32,
        /// Id the broker returned.
        got: i32,
    },

    /// No bootstrap address could be parsed or reached.
    #[error("no bootstrap brokers")]
    NoBootstrap,

    /// Topic or partition missing from metadata.
    #[error("unknown topic partition {topic}/{partition}")]
    UnknownPartition {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: i32,
    },

    /// Feature is documented as a gap (see PROTOCOL.md).
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

impl Error {
    /// Wrap a kafka-protocol encode/decode failure.
    pub fn protocol(err: impl std::fmt::Display) -> Self {
        Self::Protocol(err.to_string())
    }

    /// Map a broker `error_code`. Zero is success.
    pub fn broker(code: i16) -> Self {
        Self::Broker {
            code,
            kind: ResponseError::try_from_code(code),
        }
    }

    /// `Ok` if `code` is 0, else `Err(Error::broker(code))`.
    pub fn check(code: i16) -> Result<()> {
        if code == 0 {
            Ok(())
        } else {
            Err(Self::broker(code))
        }
    }

    /// Coordinator still coming up after broker start (InitProducerId, etc.).
    pub fn coordinator_loading(code: i16) -> bool {
        matches!(
            ResponseError::try_from_code(code),
            Some(
                ResponseError::CoordinatorLoadInProgress
                    | ResponseError::CoordinatorNotAvailable
                    | ResponseError::NotCoordinator
            )
        )
    }
}
