//! Bounded Confluent Protobuf framing and an explicit caller-selected codec.
//!
//! The message-index path follows the five-byte schema header, before the
//! Protobuf payload. `[0]` is encoded as one zero byte; other paths use signed
//! zigzag varints for their length and nonnegative indexes. This vendor framing
//! is separate from Kafka's record-batch protocol.
//!
//! No serializer, descriptor parser, schema registration or recursive reference
//! fetch is implicit. Construct a [`Codec`] for a known writer schema and its
//! references, then an [`Adapter`] for that schema ID. Input/frame/index storage
//! is bounded here; decoded objects and any codec-internal allocation must be
//! bounded by the caller's chosen codec.
//!
//! ```
//! use partitionline_schema::protobuf::{decode, encode, Limits};
//! let limits = Limits::new(1024, 8)?;
//! let frame = encode(42, &[1, 0], &[0x08, 0x96, 0x01], limits)?;
//! let message = decode(&frame, limits)?;
//! assert_eq!(message.indexes, [1, 0]);
//! assert_eq!(message.payload, [0x08, 0x96, 0x01]);
//! # Ok::<(), partitionline_schema::protobuf::Error>(())
//! ```

use crate::{wire, DecodeError};

/// Validated frame and message-index limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_frame_bytes: usize,
    max_indexes: usize,
}

impl Limits {
    /// Configure 6 bytes–64 MiB per complete frame and 1–1024 indexes.
    pub fn new(max_frame_bytes: usize, max_indexes: usize) -> Result<Self, Error> {
        if !(6..=64 * 1024 * 1024).contains(&max_frame_bytes) || !(1..=1024).contains(&max_indexes)
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_frame_bytes,
            max_indexes,
        })
    }

    /// Maximum complete frame length, including header and indexes.
    pub fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }

    /// Maximum number of indexes in a message path.
    pub fn max_indexes(self) -> usize {
        self.max_indexes
    }
}

impl Default for Limits {
    /// One MiB per complete frame, at most 32 message indexes.
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_indexes: 32,
        }
    }
}

/// Parsed framing, with a borrowed payload and bounded owned index path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<'a> {
    /// Writer schema ID from the generic five-byte header.
    pub schema_id: u32,
    /// Top-level index followed by nested message indexes.
    pub indexes: Vec<u32>,
    /// Actual Protobuf data, excluding the message-index encoding.
    pub payload: &'a [u8],
}

/// Structured framing or selected-schema failure, without payload contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Invalid frame/depth limit configuration.
    InvalidLimits,
    /// Generic magic-byte/schema-header failure.
    Header(DecodeError),
    /// Complete frame exceeds the byte limit or its length overflows.
    TooLarge,
    /// Path length exceeds the configured bound.
    IndexLimitExceeded,
    /// Encode path is empty or contains an index greater than `i32::MAX`.
    InvalidPath,
    /// Index count or an index varint ends before its terminator.
    TruncatedIndexes,
    /// More than five bytes, continuation on byte five, or overflowing bits.
    InvalidVarint,
    /// Decoded signed count is negative.
    NegativeLength,
    /// Decoded signed message index is negative.
    NegativeIndex,
    /// Bounded output/index reservation failed.
    AllocationFailed,
    /// Frame refers to a different schema than the selected adapter.
    UnknownSchema {
        /// Configured writer schema.
        expected: u32,
        /// Received writer schema.
        got: u32,
    },
    /// Frame selects a different message within the writer schema.
    UnexpectedMessage,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Header(error) => write!(f, "protobuf framing: {error}"),
            Self::UnknownSchema { expected, got } => {
                write!(f, "protobuf framing: expected schema {expected}, got {got}")
            }
            other => write!(f, "protobuf framing: {other:?}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Header(error) => Some(error),
            _ => None,
        }
    }
}

fn read_index(bytes: &[u8], cursor: &mut usize) -> Result<i32, Error> {
    let mut value = 0u32;
    for byte_index in 0..5 {
        let byte = *bytes.get(*cursor).ok_or(Error::TruncatedIndexes)?;
        *cursor += 1;
        if byte_index == 4 && byte & 0xf0 != 0 {
            return Err(Error::InvalidVarint);
        }
        value |= u32::from(byte & 0x7f) << (byte_index * 7);
        if byte & 0x80 == 0 {
            return Ok(((value >> 1) as i32) ^ -((value & 1) as i32));
        }
    }
    Err(Error::InvalidVarint)
}

/// Decode a complete Confluent Protobuf frame without copying its payload.
///
/// Noncanonical but valid zigzag varints and explicit length-one `[0]` are
/// accepted, like the reference reader. Negative/overflowing values are errors.
/// This parses indexes; descriptor existence is enforced by the selected codec
/// contract or by [`Adapter`]'s fixed expected path.
pub fn decode(bytes: &[u8], limits: Limits) -> Result<Message<'_>, Error> {
    if bytes.len() > limits.max_frame_bytes {
        return Err(Error::TooLarge);
    }
    let header = wire::decode(bytes).map_err(Error::Header)?;
    let mut cursor = 0;
    let count = read_index(header.payload, &mut cursor)?;
    if count < 0 {
        return Err(Error::NegativeLength);
    }
    let count = count as usize;
    if count > limits.max_indexes {
        return Err(Error::IndexLimitExceeded);
    }
    // Every explicit index needs at least one byte, before any reservation.
    if header.payload.len() - cursor < count {
        return Err(Error::TruncatedIndexes);
    }
    let mut indexes = Vec::new();
    indexes
        .try_reserve_exact(count.max(1))
        .map_err(|_| Error::AllocationFailed)?;
    if count == 0 {
        indexes.push(0);
    } else {
        for _ in 0..count {
            let index = read_index(header.payload, &mut cursor)?;
            if index < 0 {
                return Err(Error::NegativeIndex);
            }
            indexes.push(index as u32);
        }
    }
    Ok(Message {
        schema_id: header.schema_id,
        indexes,
        payload: &header.payload[cursor..],
    })
}

fn varint_size(mut value: u32) -> usize {
    let mut size = 1;
    while value >= 128 {
        value >>= 7;
        size += 1;
    }
    size
}

fn header_size(indexes: &[u32], limits: Limits) -> Result<usize, Error> {
    if indexes.is_empty() {
        return Err(Error::InvalidPath);
    }
    if indexes.len() > limits.max_indexes {
        return Err(Error::IndexLimitExceeded);
    }
    if indexes.iter().any(|&index| index > i32::MAX as u32) {
        return Err(Error::InvalidPath);
    }
    Ok(5 + if indexes == [0] {
        1
    } else {
        varint_size((indexes.len() as u32) << 1)
            + indexes
                .iter()
                .map(|&index| varint_size(index << 1))
                .sum::<usize>()
    })
}

fn write_index(mut value: u32, output: &mut [u8], cursor: &mut usize) {
    while value >= 128 {
        output[*cursor] = (value as u8 & 0x7f) | 0x80;
        *cursor += 1;
        value >>= 7;
    }
    output[*cursor] = value as u8;
    *cursor += 1;
}

fn frame_buffer(
    schema_id: u32,
    indexes: &[u32],
    payload_len: usize,
    limits: Limits,
) -> Result<(Vec<u8>, usize), Error> {
    let header_len = header_size(indexes, limits)?;
    let size = header_len.checked_add(payload_len).ok_or(Error::TooLarge)?;
    if size > limits.max_frame_bytes {
        return Err(Error::TooLarge);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(size)
        .map_err(|_| Error::AllocationFailed)?;
    output.resize(size, 0);
    output[1..5].copy_from_slice(&schema_id.to_be_bytes());
    let mut cursor = 5;
    if indexes == [0] {
        cursor += 1; // Buffer is already zeroed.
    } else {
        write_index((indexes.len() as u32) << 1, &mut output, &mut cursor);
        for &index in indexes {
            write_index(index << 1, &mut output, &mut cursor);
        }
    }
    debug_assert_eq!(cursor, header_len);
    Ok((output, header_len))
}

/// Encode known schema ID, validated message path and already-serialized data.
/// All size checks precede output allocation. No registry mutation occurs.
pub fn encode(
    schema_id: u32,
    indexes: &[u32],
    payload: &[u8],
    limits: Limits,
) -> Result<Vec<u8>, Error> {
    let (mut output, header_len) = frame_buffer(schema_id, indexes, payload.len(), limits)?;
    output[header_len..].copy_from_slice(payload);
    Ok(output)
}

/// Caller-selected serialization library and writer-descriptor contract.
///
/// Bind the codec to one message and its resolved imported schemas. Paths use
/// declaration order within each containing message, not field numbers. The
/// caller chooses its library/dependencies explicitly; this crate adds none.
/// `encoded_len` must be exact, `encode_into` must fill the supplied slice or
/// fail. Bound decoded objects, recursion and codec-internal scratch separately.
pub trait Codec {
    /// Decoded/encoded application message type.
    type Message;
    /// Serializer-specific structured failure type.
    type Error;
    /// Descriptor's top-level/nested message-index path.
    fn message_indexes(&self) -> &[u32];
    /// Exact payload size, without allocating an encoded payload.
    fn encoded_len(&self, message: &Self::Message) -> Result<usize, Self::Error>;
    /// Serialize into the exact-size, bounded slice provided by the adapter.
    fn encode_into(&self, message: &Self::Message, output: &mut [u8]) -> Result<(), Self::Error>;
    /// Decode data according to this writer descriptor and resolved references.
    fn decode(&self, payload: &[u8]) -> Result<Self::Message, Self::Error>;
}

/// Adapter failure retains the distinction between framing and codec errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterError<E> {
    /// Framing, size, selected schema or selected message failure.
    Frame(Error),
    /// Caller-selected serializer failure.
    Codec(E),
}
impl<E: std::fmt::Display> std::fmt::Display for AdapterError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Frame(error) => error.fmt(f),
            Self::Codec(error) => error.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for AdapterError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Frame(error) => Some(error),
            Self::Codec(error) => Some(error),
        }
    }
}

/// An explicitly selected codec, writer schema ID and immutable message path.
///
/// Construct only after looking up/resolving the known writer schema. An unknown
/// ID or different message is rejected before invoking the payload decoder.
/// No network call, implicit registration or schema-ID guessing is performed.
pub struct Adapter<C> {
    schema_id: u32,
    indexes: Vec<u32>,
    limits: Limits,
    codec: C,
}
impl<C: Codec> Adapter<C> {
    /// Validate and capture a codec's message path for a known writer schema ID.
    pub fn new(schema_id: u32, codec: C, limits: Limits) -> Result<Self, Error> {
        let path = codec.message_indexes();
        let size = header_size(path, limits)?;
        if size > limits.max_frame_bytes {
            return Err(Error::TooLarge);
        }
        let mut indexes = Vec::new();
        indexes
            .try_reserve_exact(path.len())
            .map_err(|_| Error::AllocationFailed)?;
        indexes.extend_from_slice(path);
        Ok(Self {
            schema_id,
            indexes,
            limits,
            codec,
        })
    }

    /// Encode into one bounded frame; the codec receives only the payload slice.
    pub fn encode(&self, message: &C::Message) -> Result<Vec<u8>, AdapterError<C::Error>> {
        let len = self
            .codec
            .encoded_len(message)
            .map_err(AdapterError::Codec)?;
        let (mut output, header_len) =
            frame_buffer(self.schema_id, &self.indexes, len, self.limits)
                .map_err(AdapterError::Frame)?;
        self.codec
            .encode_into(message, &mut output[header_len..])
            .map_err(AdapterError::Codec)?;
        Ok(output)
    }

    /// Check bounded framing and selected writer/message before codec decoding.
    pub fn decode(&self, frame: &[u8]) -> Result<C::Message, AdapterError<C::Error>> {
        let message = decode(frame, self.limits).map_err(AdapterError::Frame)?;
        if message.schema_id != self.schema_id {
            return Err(AdapterError::Frame(Error::UnknownSchema {
                expected: self.schema_id,
                got: message.schema_id,
            }));
        }
        if message.indexes != self.indexes {
            return Err(AdapterError::Frame(Error::UnexpectedMessage));
        }
        self.codec
            .decode(message.payload)
            .map_err(AdapterError::Codec)
    }
}
