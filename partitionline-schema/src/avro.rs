//! Opt-in Avro datum adapter with explicit writer/reader schema resolution.
//!
//! Confluent Avro framing is the generic five-byte header followed by one Avro
//! binary datum, without an object-container header or single-object fingerprint.
//! A [`Codec`] supplies the serialization library. This module supplies no
//! built-in JSON schema parser or Avro serializer and adds no dependencies.
//!
//! [`Adapter::new`] checks borrowed schema/reference input bounds, then asks the
//! codec to resolve the explicitly supplied writer and reader schemas. The codec
//! must implement Avro defaults, unions/nulls, named references and resolution
//! rules, and separately bound parsing, recursion, scratch and decoded objects.
//! Frame/schema input limits are not process RSS or decoded-object limits.
//! No registry lookup, registration, recursive fetch or schema-ID guessing occurs.
//!
//! ```
//! use partitionline_schema::avro::{decode, encode, Limits};
//! // This datum encodes the Avro long value 1; the writer ID is already known.
//! let frame = encode(42, &[2], Limits::default())?;
//! let message = decode(&frame, Limits::default())?;
//! assert_eq!(message.schema_id, 42);
//! assert_eq!(message.payload, [2]);
//! # Ok::<(), partitionline_schema::avro::Error>(())
//! ```

use crate::{wire, DecodeError, WireMessage};

/// Validated complete-frame and combined writer/reader schema input limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_frame_bytes: usize,
    max_schema_bytes: usize,
    max_references: usize,
}

impl Limits {
    /// Configure 5 bytes–64 MiB frames, 1 byte–64 MiB schema input, and 0–1024 refs.
    ///
    /// Schema bytes include both root JSON texts and every reference name/JSON;
    /// references are counted across writer and reader, including duplicates
    /// between those two independent sets. All checks precede codec resolution.
    pub fn new(
        max_frame_bytes: usize,
        max_schema_bytes: usize,
        max_references: usize,
    ) -> Result<Self, Error> {
        if !(5..=64 * 1024 * 1024).contains(&max_frame_bytes)
            || !(1..=64 * 1024 * 1024).contains(&max_schema_bytes)
            || max_references > 1024
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_frame_bytes,
            max_schema_bytes,
            max_references,
        })
    }

    /// Maximum complete frame length, including the five-byte header.
    pub fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }

    /// Combined UTF-8 bytes in writer/reader root schemas and reference inputs.
    pub fn max_schema_bytes(self) -> usize {
        self.max_schema_bytes
    }

    /// Combined count of explicitly resolved writer/reader references.
    pub fn max_references(self) -> usize {
        self.max_references
    }
}

impl Default for Limits {
    /// One MiB per frame, two MiB combined schema input and at most 64 references.
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_schema_bytes: 2 * 1024 * 1024,
            max_references: 64,
        }
    }
}

/// One already-fetched named schema; no implicit reference traversal occurs.
#[derive(Clone, Copy)]
pub struct Reference<'a> {
    /// Avro full name, interpreted and validated by the selected codec.
    pub name: &'a str,
    /// The referenced Avro JSON schema, interpreted by the selected codec.
    pub json: &'a str,
}

/// Borrowed root schema and its explicit, flattened named-reference set.
///
/// Supply every needed reference, including transitive dependencies, explicitly.
/// The codec must resolve names independently for writer and reader; reference
/// order is not a compatibility policy. Duplicate names within one set fail.
#[derive(Clone, Copy)]
pub struct Schema<'a> {
    /// Root Avro JSON schema, interpreted and validated by the selected codec.
    pub json: &'a str,
    /// Already-resolved named schemas, with no duplicate names in this set.
    pub references: &'a [Reference<'a>],
}

/// Structured adapter failure; messages never include schemas or payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Invalid frame/schema/reference limit configuration.
    InvalidLimits,
    /// Invalid generic magic-byte/schema-ID header.
    Header(DecodeError),
    /// Complete frame exceeds its bound or its size overflows.
    TooLarge,
    /// Combined schema/reference text bytes exceed their bound or overflow.
    SchemaBytesExceeded,
    /// Combined resolved-reference count exceeds its bound or overflows.
    ReferenceLimitExceeded,
    /// A root/reference schema or reference name is empty or whitespace-only.
    EmptySchemaInput,
    /// One writer or reader reference set repeats a name.
    DuplicateReference,
    /// Bounded output reservation failed.
    AllocationFailed,
    /// Frame writer schema differs from the explicitly selected writer.
    UnknownSchema {
        /// Configured writer ID.
        expected: u32,
        /// Received writer ID.
        got: u32,
    },
    /// Codec reported an encoding/decoding length different from the exact datum.
    PayloadLengthMismatch {
        /// Supplied payload slice length.
        expected: usize,
        /// Bytes the codec reports writing or consuming.
        got: usize,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Header(error) => write!(f, "avro adapter: {error}"),
            other => write!(f, "avro adapter: {other:?}"),
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

/// Decode bounded Confluent Avro framing, borrowing the binary datum.
///
/// This checks framing only. [`Adapter`] enforces the selected writer schema;
/// the caller-selected codec validates the datum and resolves its reader schema.
pub fn decode(bytes: &[u8], limits: Limits) -> Result<WireMessage<'_>, Error> {
    if bytes.len() > limits.max_frame_bytes {
        return Err(Error::TooLarge);
    }
    wire::decode(bytes).map_err(Error::Header)
}

fn frame_buffer(schema_id: u32, payload_len: usize, limits: Limits) -> Result<Vec<u8>, Error> {
    let len = 5usize.checked_add(payload_len).ok_or(Error::TooLarge)?;
    if len > limits.max_frame_bytes {
        return Err(Error::TooLarge);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(len)
        .map_err(|_| Error::AllocationFailed)?;
    output.resize(len, 0);
    output[1..5].copy_from_slice(&schema_id.to_be_bytes());
    Ok(output)
}

/// Frame an already-serialized Avro datum with a known writer schema ID.
/// All size checks precede output allocation; no registry mutation occurs.
pub fn encode(schema_id: u32, payload: &[u8], limits: Limits) -> Result<Vec<u8>, Error> {
    let mut output = frame_buffer(schema_id, payload.len(), limits)?;
    output[5..].copy_from_slice(payload);
    Ok(output)
}

fn check_schemas(writer: Schema<'_>, reader: Schema<'_>, limits: Limits) -> Result<(), Error> {
    let count = writer
        .references
        .len()
        .checked_add(reader.references.len())
        .ok_or(Error::ReferenceLimitExceeded)?;
    if count > limits.max_references {
        return Err(Error::ReferenceLimitExceeded);
    }
    let mut bytes = 0usize;
    for schema in [writer, reader] {
        bytes = bytes
            .checked_add(schema.json.len())
            .ok_or(Error::SchemaBytesExceeded)?;
        for reference in schema.references {
            bytes = bytes
                .checked_add(reference.name.len())
                .and_then(|n| n.checked_add(reference.json.len()))
                .ok_or(Error::SchemaBytesExceeded)?;
        }
    }
    if bytes > limits.max_schema_bytes {
        return Err(Error::SchemaBytesExceeded);
    }
    // Text traversal/name comparisons happen only after the aggregate bounds.
    for schema in [writer, reader] {
        if schema.json.trim().is_empty() {
            return Err(Error::EmptySchemaInput);
        }
        for (index, reference) in schema.references.iter().enumerate() {
            if reference.name.trim().is_empty() || reference.json.trim().is_empty() {
                return Err(Error::EmptySchemaInput);
            }
            if schema.references[..index]
                .iter()
                .any(|previous| previous.name == reference.name)
            {
                return Err(Error::DuplicateReference);
            }
        }
    }
    Ok(())
}

/// Caller-selected Avro serialization library and resolved-schema contract.
///
/// [`resolve`](Codec::resolve) is called once at adapter construction after input
/// bounds are checked. Validate both JSON schemas and named references; retain
/// immutable writer/reader selections. Encoding uses the writer, decoding uses
/// writer-to-reader resolution (including defaults, unions and promotions).
/// Reject incompatible data with the codec's structured error type.
///
/// The caller must configure limits for schema parsing/recursion, scratch and
/// decoded-object allocations. [`Limits`] bounds input and adapter-owned output,
/// and is supplied to resolution; it cannot constrain arbitrary codec internals.
/// Neither schema registration nor network access is part of this contract.
pub trait Codec {
    /// Application datum representation used for both encoding and decoding.
    type Value;
    /// Serializer-specific structured failure type.
    type Error;

    /// Validate and select explicit writer/reader schemas and their references.
    fn resolve(
        &mut self,
        writer: Schema<'_>,
        reader: Schema<'_>,
        limits: Limits,
    ) -> Result<(), Self::Error>;

    /// Exact writer-schema encoded size, without allocating an encoded payload.
    fn encoded_len(&self, value: &Self::Value) -> Result<usize, Self::Error>;

    /// Write the writer-schema binary datum into the exact-size supplied slice.
    /// Return the written byte count; success must fill the entire slice.
    fn encode_into(&self, value: &Self::Value, output: &mut [u8]) -> Result<usize, Self::Error>;

    /// Resolve a writer-schema binary datum into the configured reader value.
    /// Return the consumed byte count; the adapter rejects trailing data.
    fn decode(&self, payload: &[u8]) -> Result<(Self::Value, usize), Self::Error>;
}

/// Failure distinguishes adapter framing/input contracts from codec failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterError<E> {
    /// Frame, selected schema, input bounds or payload-length contract failure.
    Input(Error),
    /// Caller-selected schema resolution or payload serialization failure.
    Codec(E),
}
impl<E: std::fmt::Display> std::fmt::Display for AdapterError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Input(error) => error.fmt(f),
            Self::Codec(error) => error.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for AdapterError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Input(error) => Some(error),
            Self::Codec(error) => Some(error),
        }
    }
}

/// An explicitly selected writer schema ID, writer/reader schema pair and codec.
///
/// Resolve/fetch schema references before construction. Unknown schema IDs fail
/// before invoking the decoder. The adapter does not retain/copy input schemas;
/// its codec owns its resolved selections. No registry connection is required.
pub struct Adapter<C> {
    schema_id: u32,
    codec: C,
    limits: Limits,
}
impl<C: Codec> Adapter<C> {
    /// Bound schema inputs, then resolve the explicit writer/reader pair once.
    pub fn new(
        schema_id: u32,
        writer: Schema<'_>,
        reader: Schema<'_>,
        mut codec: C,
        limits: Limits,
    ) -> Result<Self, AdapterError<C::Error>> {
        check_schemas(writer, reader, limits).map_err(AdapterError::Input)?;
        codec
            .resolve(writer, reader, limits)
            .map_err(AdapterError::Codec)?;
        Ok(Self {
            schema_id,
            codec,
            limits,
        })
    }

    /// Encode one bounded frame using the selected writer schema and known ID.
    pub fn encode(&self, value: &C::Value) -> Result<Vec<u8>, AdapterError<C::Error>> {
        let size = self.codec.encoded_len(value).map_err(AdapterError::Codec)?;
        let mut output =
            frame_buffer(self.schema_id, size, self.limits).map_err(AdapterError::Input)?;
        let written = self
            .codec
            .encode_into(value, &mut output[5..])
            .map_err(AdapterError::Codec)?;
        if written != size {
            return Err(AdapterError::Input(Error::PayloadLengthMismatch {
                expected: size,
                got: written,
            }));
        }
        Ok(output)
    }

    /// Check the bounded frame/known writer ID, then resolve into the reader.
    pub fn decode(&self, frame: &[u8]) -> Result<C::Value, AdapterError<C::Error>> {
        let message = decode(frame, self.limits).map_err(AdapterError::Input)?;
        if message.schema_id != self.schema_id {
            return Err(AdapterError::Input(Error::UnknownSchema {
                expected: self.schema_id,
                got: message.schema_id,
            }));
        }
        let (value, consumed) = self
            .codec
            .decode(message.payload)
            .map_err(AdapterError::Codec)?;
        if consumed != message.payload.len() {
            return Err(AdapterError::Input(Error::PayloadLengthMismatch {
                expected: message.payload.len(),
                got: consumed,
            }));
        }
        Ok(value)
    }
}
