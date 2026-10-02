//! Bounded Kafka request headers and implementation-specific ApiVersions negotiation.
//!
//! Apache Kafka 4.1.2/4.2.1/4.3.1 golden bytes pin this initial API18/0..4
//! contract. ApiVersions always uses response header0, including flexible bodies.
//! Unknown APIs close the connection through the transport handler error path.
//! The composed metadata router selects the expanded compiled registry; standalone
//! negotiation advertises only API18. Inventory alone never confers support.
//!
//! Parsing borrows a transport-bounded request; it never allocates from peer
//! string, tag or count fields. Strings use strict UTF8, nullable strings accept
//! only -1, tags must increase strictly, and supported bodies must be consumed.
//! These deliberate checks are stricter than some permissive Apache Java readers;
//! the fixtures retain both Java observations and this broker's policies.

use crate::transport::Handler;

/// One actually implemented and tested API/version range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersion {
    /// Kafka API key.
    pub api_key: i16,
    /// Lowest supported version, inclusive.
    pub min_version: i16,
    /// Highest supported version, inclusive.
    pub max_version: i16,
}

/// Composed-router advertisement; standalone negotiation selects only API18.
/// Upstream inventory alone does not confer support.
pub const IMPLEMENTED_API_VERSIONS: [ApiVersion; 4] = [
    ApiVersion {
        api_key: 3,
        min_version: 0,
        max_version: 13,
    },
    ApiVersion {
        api_key: 18,
        min_version: 0,
        max_version: 4,
    },
    ApiVersion {
        api_key: 19,
        min_version: 2,
        max_version: 4,
    },
    ApiVersion {
        api_key: 20,
        min_version: 1,
        max_version: 6,
    },
];
const NEGOTIATION_ONLY: [ApiVersion; 1] = [IMPLEMENTED_API_VERSIONS[1]];

/// Positive request bound and per-block tag-count bound.
///
/// Request bytes exclude the transport's four-byte frame prefix. Align this cap
/// with transport configuration. Tag payload lengths are bounded by remaining
/// request bytes, and strings by Kafka's 32767-byte string bound. Work is linear
/// in bounded bytes/tags; no resolver, task, lock or background work is started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_request_bytes: usize,
    max_tagged_fields: usize,
}

impl Limits {
    /// Validate a 1..=64MiB request cap and a 0..=65536 tag cap per block.
    pub fn new(max_request_bytes: usize, max_tagged_fields: usize) -> Result<Self, Error> {
        if !(1..=64 * 1024 * 1024).contains(&max_request_bytes) || max_tagged_fields > 65_536 {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_request_bytes,
            max_tagged_fields,
        })
    }

    /// Maximum request payload bytes.
    pub fn max_request_bytes(self) -> usize {
        self.max_request_bytes
    }
    /// Maximum tagged fields in each header or body block; zero disallows tags.
    pub fn max_tagged_fields(self) -> usize {
        self.max_tagged_fields
    }
}

impl Default for Limits {
    /// 8MiB requests and at most1024 tags per block.
    fn default() -> Self {
        Self {
            max_request_bytes: 8 * 1024 * 1024,
            max_tagged_fields: 1024,
        }
    }
}

/// A borrowed classic or flexible request header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader<'a> {
    /// API key; parsing a header alone does not establish handler support.
    pub api_key: i16,
    /// Requested API version.
    pub api_version: i16,
    /// Signed correlation ID, echoed without normalization.
    pub correlation_id: i32,
    /// Classic nullable int16-length UTF8 string in both header versions.
    pub client_id: Option<&'a str>,
    /// Header version1(classic) or2(flexible).
    pub header_version: i16,
    /// Validated unknown fields skipped without allocating their payloads.
    pub tagged_fields: usize,
}

impl<'a> RequestHeader<'a> {
    /// Parse a known header version, returning the unconsumed body slice.
    ///
    /// The API-specific dispatcher selects header version: ApiVersions uses1
    /// below API version3 and2 from3 onward, including unsupported future versions.
    /// Flexible headers retain the classic nullable ClientId encoding.
    pub fn parse(
        input: &'a [u8],
        header_version: i16,
        limits: Limits,
    ) -> Result<(Self, &'a [u8]), Error> {
        if input.len() > limits.max_request_bytes {
            return Err(Error::RequestTooLarge);
        }
        if !matches!(header_version, 1 | 2) {
            return Err(Error::UnsupportedHeaderVersion);
        }
        let mut reader = Reader {
            remaining: input,
            limits,
        };
        let api_key = reader.i16()?;
        let api_version = reader.i16()?;
        let correlation_id =
            i32::from_be_bytes(reader.take(4)?.try_into().map_err(|_| Error::Truncated)?);
        let length = reader.i16()?;
        let client_id = match length {
            -1 => None,
            0.. => Some(reader.string(usize::try_from(length).map_err(|_| Error::InvalidLength)?)?),
            _ => return Err(Error::InvalidLength),
        };
        let tagged_fields = if header_version == 2 {
            reader.tags()?
        } else {
            0
        };
        Ok((
            Self {
                api_key,
                api_version,
                correlation_id,
                client_id,
                header_version,
                tagged_fields,
            },
            reader.remaining,
        ))
    }
}

/// Malformed requests fail closed; no universal Kafka error body is fabricated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// A local parsing cap is outside its documented range.
    InvalidLimits,
    /// Payload exceeds the validated local request cap.
    RequestTooLarge,
    /// Only request header versions1 and2 are implemented.
    UnsupportedHeaderVersion,
    /// A field extends beyond the supplied request.
    Truncated,
    /// A null sentinel or string length violates its schema.
    InvalidLength,
    /// A wire string contains malformed UTF8.
    InvalidUtf8,
    /// An unsigned varint exceeds five bytes or overflows u32.
    InvalidVarint,
    /// Tag count exceeds the configured bound.
    TooManyTags,
    /// Tag IDs repeat or descend.
    InvalidTagOrder,
    /// A supported request schema left bytes unconsumed.
    TrailingBytes,
    /// No handler is implemented for this API key.
    UnimplementedApi(i16),
    /// Reservation of the fixed small response buffer failed.
    Allocation,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Kafka protocol: {self:?}")
    }
}
impl std::error::Error for Error {}

struct Reader<'a> {
    remaining: &'a [u8],
    limits: Limits,
}
impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let value = self.remaining.get(..length).ok_or(Error::Truncated)?;
        self.remaining = &self.remaining[length..];
        Ok(value)
    }
    fn i16(&mut self) -> Result<i16, Error> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| Error::Truncated)?,
        ))
    }
    fn varint(&mut self) -> Result<u32, Error> {
        let mut value = 0u32;
        for shift in [0, 7, 14, 21, 28] {
            let byte = self.take(1)?[0];
            if shift == 28 && byte > 15 {
                return Err(Error::InvalidVarint);
            }
            value |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err(Error::InvalidVarint)
    }
    fn string(&mut self, length: usize) -> Result<&'a str, Error> {
        if length > 32767 {
            return Err(Error::InvalidLength);
        }
        std::str::from_utf8(self.take(length)?).map_err(|_| Error::InvalidUtf8)
    }
    fn compact_string(&mut self) -> Result<&'a str, Error> {
        let length = self.varint()?.checked_sub(1).ok_or(Error::InvalidLength)?;
        self.string(usize::try_from(length).map_err(|_| Error::InvalidLength)?)
    }
    fn tags(&mut self) -> Result<usize, Error> {
        let count = usize::try_from(self.varint()?).map_err(|_| Error::TooManyTags)?;
        if count > self.limits.max_tagged_fields {
            return Err(Error::TooManyTags);
        }
        let mut previous = None;
        for _ in 0..count {
            let tag = self.varint()?;
            if previous.is_some_and(|id| tag <= id) {
                return Err(Error::InvalidTagOrder);
            }
            previous = Some(tag);
            let length = usize::try_from(self.varint()?).map_err(|_| Error::InvalidLength)?;
            let _ = self.take(length)?;
        }
        Ok(count)
    }
}

/// Header/ApiVersions-only handler; no other API is advertised or dispatched.
#[derive(Debug, Clone, Copy)]
pub struct ApiVersionsHandler {
    limits: Limits,
    advertised: &'static [ApiVersion],
}
impl Default for ApiVersionsHandler {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}
impl ApiVersionsHandler {
    /// Use validated parsing caps, independently of transport admission caps.
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            advertised: &NEGOTIATION_ONLY,
        }
    }
    pub(crate) fn composed(limits: Limits) -> Self {
        Self {
            limits,
            advertised: &IMPLEMENTED_API_VERSIONS,
        }
    }
    /// Parsing caps used by this handler.
    pub fn limits(self) -> Limits {
        self.limits
    }

    /// Decode one payload and return its correlation header plus response body.
    ///
    /// Unsupported ApiVersions versions use the pinned v0/error35 fallback and
    /// ignore the unknown body. For supported v3/4 software fields, semantic
    /// invalidity returns error42 in the requested body version with an empty
    /// API list. Structural failures close the affected connection instead.
    pub fn respond(&self, request: &[u8]) -> Result<Vec<u8>, Error> {
        if request.len() > self.limits.max_request_bytes {
            return Err(Error::RequestTooLarge);
        }
        let mut prefix = Reader {
            remaining: request,
            limits: self.limits,
        };
        let key = prefix.i16()?;
        if key != 18 {
            return Err(Error::UnimplementedApi(key));
        }
        let version = prefix.i16()?;
        let (header, body) =
            RequestHeader::parse(request, if version >= 3 { 2 } else { 1 }, self.limits)?;
        if !(0..=4).contains(&version) {
            return response(header.correlation_id, 0, 35, self.advertised);
        }
        let mut reader = Reader {
            remaining: body,
            limits: self.limits,
        };
        let valid = if version >= 3 {
            let name = reader.compact_string()?;
            let version = reader.compact_string()?;
            let _ = reader.tags()?;
            valid_software(name) && valid_software(version)
        } else {
            true
        };
        if !reader.remaining.is_empty() {
            return Err(Error::TrailingBytes);
        }
        response(
            header.correlation_id,
            version,
            if valid { 0 } else { 42 },
            if valid { self.advertised } else { &[] },
        )
    }
}
impl Handler for ApiVersionsHandler {
    type Error = Error;
    async fn handle(&self, request: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        self.respond(&request).map(Some)
    }
}

fn valid_software(value: &str) -> bool {
    let bytes = value.as_bytes();
    match (bytes.first(), bytes.last()) {
        (Some(first), Some(last))
            if first.is_ascii_alphanumeric() && last.is_ascii_alphanumeric() =>
        {
            bytes
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
        }
        _ => false,
    }
}

fn response(
    correlation: i32,
    version: i16,
    error: i16,
    advertised: &[ApiVersion],
) -> Result<Vec<u8>, Error> {
    // At most four locally compiled entries; peer fields cannot enlarge this.
    // Every ApiVersions response uses header0 (correlation ID only).
    let mut output = Vec::new();
    output
        .try_reserve_exact(48)
        .map_err(|_| Error::Allocation)?;
    output.extend_from_slice(&correlation.to_be_bytes());
    output.extend_from_slice(&error.to_be_bytes());
    if version >= 3 {
        output.push((advertised.len() + 1) as u8);
    } else {
        output.extend_from_slice(&(advertised.len() as i32).to_be_bytes());
    }
    {
        for api in advertised {
            output.extend_from_slice(&api.api_key.to_be_bytes());
            output.extend_from_slice(&api.min_version.to_be_bytes());
            output.extend_from_slice(&api.max_version.to_be_bytes());
            if version >= 3 {
                output.push(0);
            }
        }
    }
    if version >= 1 {
        output.extend_from_slice(&0i32.to_be_bytes());
    }
    if version >= 3 {
        output.push(0);
    }
    Ok(output)
}
