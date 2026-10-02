//! Opt-in, versioned JSON Schema validation and serialization contract.
//!
//! Profile [`PROFILE`] uses Draft 2020-12 and ordinary UTF-8 JSON after the
//! Confluent five-byte schema-ID header. A caller-selected [`Codec`] parses
//! schemas, resolves the explicit references, serializes values and validates
//! documents. This module supplies no built-in JSON parser or validator and
//! adds no dependency, network lookup, schema registration or mutation.
//!
//! Encoding validates the serialized document against the selected writer.
//! Decoding validates it against both writer and reader, without coercion,
//! default insertion or rewriting. JSON Schema compatibility means that the
//! same instance satisfies both selections; construction does not prove that
//! every possible writer instance satisfies the reader. `format` is an
//! annotation in this profile, not an implicit format-assertion vocabulary.
//!
//! Limits cover borrowed schema/reference bytes and complete frames. The codec
//! must separately bound schema parsing, reference resolution, recursion,
//! scratch and decoded-object allocation, and reject unsupported required
//! vocabularies or a `$schema` declaration inconsistent with this profile.
//!
//! ```
//! use partitionline_schema::json_schema::{decode, encode, Limits};
//! // Framing alone does not validate a document against a JSON Schema.
//! let frame = encode(42, "null", Limits::default())?;
//! let message = decode(&frame, Limits::default())?;
//! assert_eq!(message.schema_id, 42);
//! assert_eq!(message.payload, b"null");
//! # Ok::<(), partitionline_schema::json_schema::Error>(())
//! ```

use crate::{wire, DecodeError, WireMessage};

/// Versioned adapter semantics, independent of Schema Registry schema versions.
pub const PROFILE: &str = "partitionline.json-schema.draft2020-12.v1";
/// Canonical supported dialect URI; other dialects fail explicitly.
pub const DIALECT_URI: &str = "https://json-schema.org/draft/2020-12/schema";

/// Explicit supported schema dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// Draft 2020-12 with format annotations and explicit offline references.
    Draft202012,
}

impl Dialect {
    /// Select the supported canonical URI; no automatic dialect fallback.
    pub fn from_uri(uri: &str) -> Result<Self, Error> {
        if uri == DIALECT_URI {
            Ok(Self::Draft202012)
        } else {
            Err(Error::UnsupportedDialect)
        }
    }

    /// Canonical URI required in the explicit schema selection.
    pub fn uri(self) -> &'static str {
        match self {
            Self::Draft202012 => DIALECT_URI,
        }
    }
}

/// Complete frame, combined schema text and flattened reference input limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_frame_bytes: usize,
    max_schema_bytes: usize,
    max_references: usize,
}

impl Limits {
    /// Configure 6 bytes–64 MiB frames, 1 byte–64 MiB schema input, 0–1024 refs.
    ///
    /// Schema bytes include both roots, their dialect URIs, and every reference
    /// URI/dialect/schema, counting references in both independent selections.
    pub fn new(
        max_frame_bytes: usize,
        max_schema_bytes: usize,
        max_references: usize,
    ) -> Result<Self, Error> {
        if !(6..=64 * 1024 * 1024).contains(&max_frame_bytes)
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

    /// Maximum complete frame bytes, including the five-byte header.
    pub fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }

    /// Maximum combined root/reference schema and URI bytes.
    pub fn max_schema_bytes(self) -> usize {
        self.max_schema_bytes
    }

    /// Maximum combined explicit writer/reader reference count.
    pub fn max_references(self) -> usize {
        self.max_references
    }
}

impl Default for Limits {
    /// One MiB frames, two MiB combined schema input, at most 64 references.
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_schema_bytes: 2 * 1024 * 1024,
            max_references: 64,
        }
    }
}

/// One already-fetched schema resource, supplied to the codec without I/O.
#[derive(Clone, Copy)]
pub struct Reference<'a> {
    /// Absolute resource URI; URI syntax, `$id` and anchors are codec-validated.
    pub uri: &'a str,
    /// Explicit canonical dialect URI; unsupported dialects fail before codec use.
    pub dialect_uri: &'a str,
    /// Referenced JSON Schema text; boolean schemas are also permitted by draft.
    pub json: &'a str,
}

/// Explicit root schema and its complete flattened resource set.
///
/// Supply all transitive references before construction. The codec must resolve
/// relative `$ref` against `$id`, anchors and dynamic references using only this
/// set and the root. Missing resources fail instead of invoking a network fetch.
#[derive(Clone, Copy)]
pub struct Schema<'a> {
    /// Explicit canonical supported dialect URI.
    pub dialect_uri: &'a str,
    /// Root JSON Schema text; parsing and meta-schema checks belong to the codec.
    pub json: &'a str,
    /// Offline resources with unique URIs within this selection.
    pub references: &'a [Reference<'a>],
}

/// Structured input/framing error; diagnostics contain no schema or JSON data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Invalid frame/schema/reference limit configuration.
    InvalidLimits,
    /// Explicit dialect URI is unsupported; no fallback is performed.
    UnsupportedDialect,
    /// Invalid generic magic-byte/schema-ID header.
    Header(DecodeError),
    /// Complete frame is over budget or its length overflows.
    TooLarge,
    /// Combined schema/reference bytes are over budget or overflow.
    SchemaBytesExceeded,
    /// Combined flattened reference count is over budget or overflows.
    ReferenceLimitExceeded,
    /// A root/reference schema or reference URI is empty or whitespace-only.
    EmptySchemaInput,
    /// A reference URI is repeated within one writer or reader set.
    DuplicateReference,
    /// Payload is not UTF-8 JSON text.
    InvalidUtf8,
    /// Payload has no non-whitespace JSON input; JSON null is the text `null`.
    EmptyPayload,
    /// Bounded output reservation failed.
    AllocationFailed,
    /// Frame writer ID differs from the explicitly selected writer.
    UnknownSchema {
        /// Configured writer ID.
        expected: u32,
        /// Received writer ID.
        got: u32,
    },
    /// Codec reported a different serialization or complete consumption length.
    PayloadLengthMismatch {
        /// Payload slice length.
        expected: usize,
        /// Reported written or consumed bytes.
        got: usize,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Header(error) => write!(f, "json schema adapter: {error}"),
            other => write!(f, "json schema adapter: {other:?}"),
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

fn payload_text(payload: &[u8]) -> Result<&str, Error> {
    let text = std::str::from_utf8(payload).map_err(|_| Error::InvalidUtf8)?;
    if text.trim().is_empty() {
        return Err(Error::EmptyPayload);
    }
    Ok(text)
}

/// Borrow bounded framing and check nonempty UTF-8; this is not JSON validation.
pub fn decode(bytes: &[u8], limits: Limits) -> Result<WireMessage<'_>, Error> {
    if bytes.len() > limits.max_frame_bytes {
        return Err(Error::TooLarge);
    }
    let message = wire::decode(bytes).map_err(Error::Header)?;
    payload_text(message.payload)?;
    Ok(message)
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

/// Frame nonempty UTF-8 text with a known ID; JSON semantics require [`Adapter`].
pub fn encode(schema_id: u32, payload: &str, limits: Limits) -> Result<Vec<u8>, Error> {
    if payload.len() > limits.max_frame_bytes.saturating_sub(5) {
        return Err(Error::TooLarge);
    }
    payload_text(payload.as_bytes())?;
    let mut output = frame_buffer(schema_id, payload.len(), limits)?;
    output[5..].copy_from_slice(payload.as_bytes());
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
            .and_then(|n| n.checked_add(schema.dialect_uri.len()))
            .ok_or(Error::SchemaBytesExceeded)?;
        for reference in schema.references {
            bytes = bytes
                .checked_add(reference.uri.len())
                .and_then(|n| n.checked_add(reference.dialect_uri.len()))
                .and_then(|n| n.checked_add(reference.json.len()))
                .ok_or(Error::SchemaBytesExceeded)?;
        }
    }
    if bytes > limits.max_schema_bytes {
        return Err(Error::SchemaBytesExceeded);
    }
    // Text traversal and URI comparisons occur only after aggregate bounds.
    for schema in [writer, reader] {
        Dialect::from_uri(schema.dialect_uri)?;
        if schema.json.trim().is_empty() {
            return Err(Error::EmptySchemaInput);
        }
        for (index, reference) in schema.references.iter().enumerate() {
            Dialect::from_uri(reference.dialect_uri)?;
            if reference.uri.trim().is_empty() || reference.json.trim().is_empty() {
                return Err(Error::EmptySchemaInput);
            }
            if schema.references[..index]
                .iter()
                .any(|previous| previous.uri == reference.uri)
            {
                return Err(Error::DuplicateReference);
            }
        }
    }
    Ok(())
}

/// One of the immutable schema selections prepared by the chosen codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    /// Producing writer schema.
    Writer,
    /// Consuming reader schema; validation does not transform the JSON instance.
    Reader,
}

/// Caller-selected JSON serializer, parser and Draft 2020-12 validator contract.
///
/// Validate root/reference schemas and declared `$schema`/`$id` values in
/// [`resolve`](Codec::resolve), retaining immutable selections. Resolve only the
/// explicit resources, reject missing references and unsupported required
/// vocabularies, and disable implicit network retrieval. `validate` must parse
/// exactly one complete JSON instance, reject non-JSON numbers (NaN/infinity),
/// and preserve numeric boundary precision sufficient for the schemas.
///
/// No coercion or default insertion is permitted. The codec must separately
/// cap recursion, parsing work, scratch, retained schemas and decoded values;
/// [`Limits`] cannot constrain arbitrary implementation internals. A validation
/// error remains a codec-specific structured error, distinct from framing.
pub trait Codec {
    /// Application value representation.
    type Value;
    /// Codec-specific structured failure.
    type Error;

    /// Prepare explicit offline writer/reader schema selections after input bounds.
    fn resolve(
        &mut self,
        writer: Schema<'_>,
        reader: Schema<'_>,
        limits: Limits,
    ) -> Result<(), Self::Error>;

    /// Exact UTF-8 JSON serialization length without a second encoded buffer.
    fn encoded_len(&self, value: &Self::Value) -> Result<usize, Self::Error>;

    /// Serialize into the bounded exact-size slice and report all written bytes.
    fn encode_into(&self, value: &Self::Value, output: &mut [u8]) -> Result<usize, Self::Error>;

    /// Parse and validate one complete JSON instance against the selected schema.
    fn validate(&self, payload: &str, selection: Selection) -> Result<(), Self::Error>;

    /// Deserialize validated JSON; report consumed bytes, including whitespace.
    fn decode(&self, payload: &str) -> Result<(Self::Value, usize), Self::Error>;
}

/// Failure separates input/framing contracts from caller-selected codec failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterError<E> {
    /// Framing, selected ID, dialect or input-budget failure.
    Input(Error),
    /// Schema preparation, instance validation or serialization failure.
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

/// Known writer schema ID and explicitly prepared writer/reader codec selections.
///
/// The adapter owns no schema copies. Its codec owns prepared selections and
/// receives only bounded output slices and frame payloads. Registry I/O is absent.
pub struct Adapter<C> {
    schema_id: u32,
    codec: C,
    limits: Limits,
}

impl<C: Codec> Adapter<C> {
    /// Bound inputs and reject unsupported dialects before preparing the codec.
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

    /// Serialize one bounded frame, then validate its actual JSON against writer.
    pub fn encode(&self, value: &C::Value) -> Result<Vec<u8>, AdapterError<C::Error>> {
        let len = self.codec.encoded_len(value).map_err(AdapterError::Codec)?;
        let mut frame =
            frame_buffer(self.schema_id, len, self.limits).map_err(AdapterError::Input)?;
        let written = self
            .codec
            .encode_into(value, &mut frame[5..])
            .map_err(AdapterError::Codec)?;
        if written != len {
            return Err(AdapterError::Input(Error::PayloadLengthMismatch {
                expected: len,
                got: written,
            }));
        }
        let payload = payload_text(&frame[5..]).map_err(AdapterError::Input)?;
        self.codec
            .validate(payload, Selection::Writer)
            .map_err(AdapterError::Codec)?;
        Ok(frame)
    }

    /// Check known ID, validate both schemas, then deserialize without rewriting.
    pub fn decode(&self, frame: &[u8]) -> Result<C::Value, AdapterError<C::Error>> {
        if frame.len() > self.limits.max_frame_bytes {
            return Err(AdapterError::Input(Error::TooLarge));
        }
        let message = wire::decode(frame)
            .map_err(Error::Header)
            .map_err(AdapterError::Input)?;
        if message.schema_id != self.schema_id {
            return Err(AdapterError::Input(Error::UnknownSchema {
                expected: self.schema_id,
                got: message.schema_id,
            }));
        }
        let payload = payload_text(message.payload).map_err(AdapterError::Input)?;
        self.codec
            .validate(payload, Selection::Writer)
            .map_err(AdapterError::Codec)?;
        self.codec
            .validate(payload, Selection::Reader)
            .map_err(AdapterError::Codec)?;
        let (value, consumed) = self.codec.decode(payload).map_err(AdapterError::Codec)?;
        if consumed != payload.len() {
            return Err(AdapterError::Input(Error::PayloadLengthMismatch {
                expected: payload.len(),
                got: consumed,
            }));
        }
        Ok(value)
    }
}
