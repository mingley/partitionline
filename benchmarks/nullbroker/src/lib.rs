//! Validating null-broker Produce server (KL09-06).
//!
//! A standalone, zero-dependency Kafka-protocol server answering
//! `ApiVersions`, `Metadata`, `InitProducerId` and `Produce` immediately
//! after validating every batch, so the measured client's CPU — not broker
//! I/O — bounds throughput. Accepted records/bytes and validation failures
//! go to a result artifact labeled `client-ceiling`.
//!
//! This crate is workspace-excluded test infrastructure. It must never
//! become a dependency of `partitionline`.
//!
//! Spoken versions (negotiated by the client within the advertised range):
//! `ApiVersions` v0–v4, `Metadata` v13, `Produce` v12, `InitProducerId` v5,
//! `FindCoordinator` v6 (required by `Producer::new`; the plain and
//! idempotent produce paths never send it), `Fetch` v17 and `ListOffsets`
//! v10 (KL09-07 seeded synthetic log).

pub mod synth;

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// API key: Produce.
pub const API_KEY_PRODUCE: i16 = 0;
/// API key: Fetch.
pub const API_KEY_FETCH: i16 = 1;
/// API key: ListOffsets.
pub const API_KEY_LIST_OFFSETS: i16 = 2;
/// API key: Metadata.
pub const API_KEY_METADATA: i16 = 3;
/// API key: FindCoordinator.
pub const API_KEY_FIND_COORDINATOR: i16 = 10;
/// API key: InitProducerId.
pub const API_KEY_INIT_PRODUCER_ID: i16 = 22;
/// API key: ApiVersions.
pub const API_KEY_API_VERSIONS: i16 = 18;

/// Error code: none.
pub const ERR_NONE: i16 = 0;
/// Error code: `OFFSET_OUT_OF_RANGE`.
pub const ERR_OFFSET_OUT_OF_RANGE: i16 = 1;
/// Error code: `UNKNOWN_TOPIC_OR_PARTITION`.
pub const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// Error code: `NOT_LEADER_OR_FOLLOWER`.
pub const ERR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// Error code: `UNKNOWN_TOPIC_ID`.
pub const ERR_UNKNOWN_TOPIC_ID: i16 = 100;
/// Error code: `UNSUPPORTED_VERSION`.
pub const ERR_UNSUPPORTED_VERSION: i16 = 35;
/// Error code: `OUT_OF_ORDER_SEQUENCE_NUMBER`.
pub const ERR_OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
/// Error code: `INVALID_TXN_STATE`.
pub const ERR_INVALID_TXN_STATE: i16 = 48;
/// Error code: `INVALID_RECORD`.
pub const ERR_INVALID_RECORD: i16 = 87;

/// Maximum request frame accepted (128 MiB, above any sane
/// `max.request.size` but bounded against OOM).
pub const MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;

/// Decode failure: truncated input or invalid bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// Input ended mid-field.
    Truncated,
    /// Structurally invalid value (bad UTF-8, varint overflow, ...).
    Invalid(&'static str),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("truncated input"),
            Self::Invalid(reason) => write!(f, "invalid input: {reason}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Cursor over a request frame. All slicing funnels through [`Cursor::take`].
#[derive(Debug)]
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if n > self.remaining() {
            return Err(DecodeError::Truncated);
        }
        let start = self.pos;
        self.pos += n;
        self.buf.get(start..start + n).ok_or(DecodeError::Truncated)
    }

    fn get_u8(&mut self) -> Result<u8, DecodeError> {
        self.take(1).map(|b| b[0])
    }

    fn get_i16(&mut self) -> Result<i16, DecodeError> {
        self.take(2).map(|b| i16::from_be_bytes([b[0], b[1]]))
    }

    fn get_i32(&mut self) -> Result<i32, DecodeError> {
        self.take(4)
            .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn get_u32(&mut self) -> Result<u32, DecodeError> {
        self.take(4)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn get_i64(&mut self) -> Result<i64, DecodeError> {
        self.take(8)
            .map(|b| i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    fn get_uuid(&mut self) -> Result<[u8; 16], DecodeError> {
        let b = self.take(16)?;
        let mut out = [0u8; 16];
        out.copy_from_slice(b);
        Ok(out)
    }

    /// Unsigned varint (flexible protocol), at most 5 bytes.
    fn get_uvarint(&mut self) -> Result<u32, DecodeError> {
        let mut value: u32 = 0;
        for shift in (0..35).step_by(7) {
            let byte = self.get_u8()?;
            let low = u32::from(byte & 0x7f);
            if shift == 28 && low > 0x0f {
                return Err(DecodeError::Invalid("uvarint overflow"));
            }
            value |= low << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(DecodeError::Invalid("uvarint too long"))
    }

    /// Zig-zag signed varint (record batch), at most 5 bytes.
    fn get_varint(&mut self) -> Result<i32, DecodeError> {
        let raw = self.get_uvarint()?;
        Ok(((raw >> 1) as i32) ^ -((raw & 1) as i32))
    }

    /// Zig-zag signed varlong (record batch), at most 10 bytes.
    fn get_varlong(&mut self) -> Result<i64, DecodeError> {
        let mut value: u64 = 0;
        for shift in (0..70).step_by(7) {
            let byte = self.get_u8()?;
            let low = u64::from(byte & 0x7f);
            if shift == 63 && low > 1 {
                return Err(DecodeError::Invalid("varlong overflow"));
            }
            value |= low << shift;
            if byte & 0x80 == 0 {
                return Ok(((value >> 1) as i64) ^ -((value & 1) as i64));
            }
        }
        Err(DecodeError::Invalid("varlong too long"))
    }

    /// Compact nullable string (`uvarint n+1`, `0` is null).
    fn get_compact_string(&mut self) -> Result<Option<String>, DecodeError> {
        let n = self.get_uvarint()?;
        if n == 0 {
            return Ok(None);
        }
        let len = n.saturating_sub(1) as usize;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(|s| Some(s.to_owned()))
            .map_err(|_| DecodeError::Invalid("string is not UTF-8"))
    }

    /// Compact nullable bytes (`uvarint n+1`, `0` is null).
    fn get_compact_bytes(&mut self) -> Result<Option<&'a [u8]>, DecodeError> {
        let n = self.get_uvarint()?;
        if n == 0 {
            return Ok(None);
        }
        let len = n.saturating_sub(1) as usize;
        if len > self.remaining() {
            return Err(DecodeError::Truncated);
        }
        self.take(len).map(Some)
    }

    /// Array length: compact `n+1` when `flexible`, else `i32` (`-1` null).
    fn get_array_len(&mut self, flexible: bool) -> Result<Option<usize>, DecodeError> {
        if flexible {
            let n = self.get_uvarint()?;
            if n == 0 {
                return Ok(None);
            }
            let len = n.saturating_sub(1) as usize;
            if len > self.remaining() {
                return Err(DecodeError::Invalid("array length exceeds frame"));
            }
            Ok(Some(len))
        } else {
            let n = self.get_i32()?;
            if n < 0 {
                return Ok(None);
            }
            let len = usize::try_from(n).map_err(|_| DecodeError::Invalid("array length"))?;
            if len > self.remaining() {
                return Err(DecodeError::Invalid("array length exceeds frame"));
            }
            Ok(Some(len))
        }
    }

    /// Skip flexible tagged fields. Unknown tags are discarded.
    fn skip_tagged_fields(&mut self) -> Result<(), DecodeError> {
        let n = self.get_uvarint()? as usize;
        if n > self.remaining() {
            return Err(DecodeError::Invalid("tagged field count exceeds frame"));
        }
        for _ in 0..n {
            let _tag = self.get_uvarint()?;
            let size = self.get_uvarint()? as usize;
            let _ = self.take(size)?;
        }
        Ok(())
    }
}

/// Append helpers for response encoding.
trait PutExt {
    /// Append an unsigned varint.
    fn put_uvarint(&mut self, value: u32);
    /// Append a compact nullable string (`uvarint n+1`, `0` is null).
    fn put_compact_string(&mut self, s: Option<&str>);
    /// Append compact nullable bytes (`uvarint n+1`, `0` is null).
    fn put_compact_bytes(&mut self, bytes: Option<&[u8]>);
    /// Append a compact array length (`uvarint n+1`).
    fn put_compact_array_len(&mut self, len: usize);
    /// Append an empty tagged-fields section.
    fn put_empty_tagged_fields(&mut self);
}

impl PutExt for Vec<u8> {
    fn put_uvarint(&mut self, mut value: u32) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            self.push(byte);
            if value == 0 {
                return;
            }
        }
    }

    fn put_compact_string(&mut self, s: Option<&str>) {
        match s {
            None => self.put_uvarint(0),
            Some(text) => {
                self.put_uvarint(text.len() as u32 + 1);
                self.extend_from_slice(text.as_bytes());
            }
        }
    }

    fn put_compact_bytes(&mut self, bytes: Option<&[u8]>) {
        match bytes {
            None => self.put_uvarint(0),
            Some(b) => {
                self.put_uvarint(b.len() as u32 + 1);
                self.extend_from_slice(b);
            }
        }
    }

    fn put_compact_array_len(&mut self, len: usize) {
        self.put_uvarint(len as u32 + 1);
    }

    fn put_empty_tagged_fields(&mut self) {
        self.put_uvarint(0);
    }
}

/// CRC32-C (Castagnoli, polynomial `0x1EDC6F41` reflected) table.
fn crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// CRC32-C (Castagnoli) over `data`, table-driven and dependency-free.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(crc32c_table);
    let mut crc: u32 = 0xffff_ffff;
    for &byte in data {
        let index = ((crc ^ u32::from(byte)) & 0xff) as usize;
        crc = table[index] ^ (crc >> 8);
    }
    !crc
}

/// Decoded request header: `(api_key, api_version, correlation_id)`.
///
/// Client id is always a classic nullable string (`i16` length); header
/// `v2` only appends tagged fields after it. `ApiVersions` v0–v2 requests
/// use header `v1` (no tags); all other spoken versions use `v2`.
fn decode_request_header(frame: &[u8]) -> Result<(i16, i16, i32, usize), DecodeError> {
    let mut cur = Cursor::new(frame);
    let api_key = cur.get_i16()?;
    let api_version = cur.get_i16()?;
    let correlation_id = cur.get_i32()?;
    let flexible = match api_key {
        API_KEY_API_VERSIONS => api_version >= 3,
        API_KEY_PRODUCE => api_version >= 9,
        API_KEY_METADATA => api_version >= 9,
        API_KEY_INIT_PRODUCER_ID => api_version >= 2,
        API_KEY_FIND_COORDINATOR => api_version >= 3,
        API_KEY_FETCH => api_version >= 12,
        API_KEY_LIST_OFFSETS => api_version >= 6,
        _ => return Err(DecodeError::Invalid("unknown api key")),
    };
    let len = cur.get_i16()?;
    if len >= 0 {
        let n = usize::try_from(len).map_err(|_| DecodeError::Invalid("client id length"))?;
        let bytes = cur.take(n)?;
        if std::str::from_utf8(bytes).is_err() {
            return Err(DecodeError::Invalid("client id is not UTF-8"));
        }
    }
    if flexible {
        cur.skip_tagged_fields()?;
    }
    Ok((api_key, api_version, correlation_id, cur.pos))
}

/// Encode a response header: correlation id plus tagged fields when flexible.
///
/// `ApiVersions` responses always use header `v0` (KIP-482).
fn encode_response_header(out: &mut Vec<u8>, api_key: i16, api_version: i16, correlation_id: i32) {
    out.extend_from_slice(&correlation_id.to_be_bytes());
    let flexible = match api_key {
        API_KEY_API_VERSIONS => false,
        API_KEY_PRODUCE => api_version >= 9,
        API_KEY_METADATA => api_version >= 9,
        API_KEY_INIT_PRODUCER_ID => api_version >= 2,
        API_KEY_FIND_COORDINATOR => api_version >= 3,
        API_KEY_FETCH => api_version >= 12,
        API_KEY_LIST_OFFSETS => api_version >= 6,
        _ => false,
    };
    if flexible {
        out.put_empty_tagged_fields();
    }
}

/// API versions this server advertises: `(api_key, min, max)`.
#[must_use]
pub const fn advertised_apis() -> [(i16, i16, i16); 7] {
    [
        (API_KEY_PRODUCE, 9, 12),
        (API_KEY_METADATA, 12, 13),
        (API_KEY_INIT_PRODUCER_ID, 5, 5),
        (API_KEY_API_VERSIONS, 0, 4),
        (API_KEY_FIND_COORDINATOR, 1, 6),
        (API_KEY_FETCH, 15, 17),
        (API_KEY_LIST_OFFSETS, 7, 10),
    ]
}

/// Decode an `ApiVersions` request body: `(software_name, software_version)`.
/// Empty on v0–v2.
fn decode_api_versions_request(body: &[u8], version: i16) -> Result<(String, String), DecodeError> {
    if !(0..=4).contains(&version) {
        return Err(DecodeError::Invalid("ApiVersions version"));
    }
    if version < 3 {
        return Ok((String::new(), String::new()));
    }
    let mut cur = Cursor::new(body);
    let name = cur.get_compact_string()?.unwrap_or_default();
    let software_version = cur.get_compact_string()?.unwrap_or_default();
    cur.skip_tagged_fields()?;
    Ok((name, software_version))
}

/// Encode an `ApiVersions` response body (flexible v3–v4, classic v0–v2).
fn encode_api_versions_response(out: &mut Vec<u8>, version: i16, error_code: i16) {
    out.extend_from_slice(&error_code.to_be_bytes());
    let apis = advertised_apis();
    if version >= 3 {
        out.put_compact_array_len(apis.len());
        for (key, min, max) in apis {
            out.extend_from_slice(&key.to_be_bytes());
            out.extend_from_slice(&min.to_be_bytes());
            out.extend_from_slice(&max.to_be_bytes());
            out.put_empty_tagged_fields();
        }
    } else {
        out.extend_from_slice(&(apis.len() as i32).to_be_bytes());
        for (key, min, max) in apis {
            out.extend_from_slice(&key.to_be_bytes());
            out.extend_from_slice(&min.to_be_bytes());
            out.extend_from_slice(&max.to_be_bytes());
        }
    }
    if version >= 1 {
        out.extend_from_slice(&0i32.to_be_bytes());
    }
    if version >= 3 {
        out.put_empty_tagged_fields();
    }
}

/// One topic in a `Metadata` v12–v13 request: `(topic_id, name)`.
#[derive(Debug, Clone)]
struct MetadataRequestTopic {
    name: Option<String>,
}

/// Decode a `Metadata` v12–v13 request: `(topics_or_null_for_all, allow_auto)`.
fn decode_metadata_request(
    body: &[u8],
) -> Result<(Option<Vec<MetadataRequestTopic>>, bool), DecodeError> {
    let mut cur = Cursor::new(body);
    let topics = match cur.get_array_len(true)? {
        None => None,
        Some(n) => {
            let mut topics = Vec::with_capacity(n);
            for _ in 0..n {
                let _topic_id = cur.get_uuid()?;
                let name = cur.get_compact_string()?;
                cur.skip_tagged_fields()?;
                topics.push(MetadataRequestTopic { name });
            }
            Some(topics)
        }
    };
    let allow_auto = cur.get_u8()? != 0;
    // v12–v13 are outside 8..=10, so no IncludeClusterAuthorizedOperations byte.
    let _include_topic_authorized = cur.get_u8()?;
    cur.skip_tagged_fields()?;
    Ok((topics, allow_auto))
}

/// Encode a `Metadata` v12–v13 response: every node, `partitions` per topic.
fn encode_metadata_response(
    out: &mut Vec<u8>,
    version: i16,
    topics: &[MetadataRequestTopic],
    partitions: i32,
    leaders: &[i32],
    nodes: &[NodeInfo],
) {
    out.extend_from_slice(&0i32.to_be_bytes()); // throttle_ms
    out.put_compact_array_len(nodes.len());
    for node in nodes {
        out.extend_from_slice(&node.id.to_be_bytes());
        out.put_compact_string(Some(&node.host));
        out.extend_from_slice(&node.port.to_be_bytes());
        out.put_compact_string(None); // rack
        out.put_empty_tagged_fields();
    }
    out.put_compact_string(Some("nullbroker")); // cluster_id
    out.extend_from_slice(&0i32.to_be_bytes()); // controller_id
    out.put_compact_array_len(topics.len());
    for topic in topics {
        match topic.name.as_deref() {
            Some(name) if !name.is_empty() => {
                out.extend_from_slice(&ERR_NONE.to_be_bytes());
                out.put_compact_string(Some(name));
                // Deterministic per-name id so Fetch-by-ID stays unambiguous.
                out.extend_from_slice(&synth::topic_id(name));
                out.push(0); // is_internal
                let n = usize::try_from(partitions.max(0)).unwrap_or(0);
                out.put_compact_array_len(n);
                for index in 0..n {
                    let leader = leaders.get(index).copied().unwrap_or(0);
                    out.extend_from_slice(&ERR_NONE.to_be_bytes());
                    out.extend_from_slice(&(index as i32).to_be_bytes());
                    out.extend_from_slice(&leader.to_be_bytes());
                    out.extend_from_slice(&0i32.to_be_bytes()); // leader_epoch
                    out.put_compact_array_len(1); // replica_nodes
                    out.extend_from_slice(&leader.to_be_bytes());
                    out.put_compact_array_len(1); // isr_nodes
                    out.extend_from_slice(&leader.to_be_bytes());
                    out.put_compact_array_len(0); // offline_replicas
                    out.put_empty_tagged_fields();
                }
                out.extend_from_slice(&0i32.to_be_bytes()); // topic_authorized_operations
            }
            _ => {
                out.extend_from_slice(&ERR_UNKNOWN_TOPIC_OR_PARTITION.to_be_bytes());
                out.put_compact_string(topic.name.as_deref());
                out.extend_from_slice(&[0u8; 16]);
                out.push(0);
                out.put_compact_array_len(0);
                out.extend_from_slice(&0i32.to_be_bytes());
            }
        }
        out.put_empty_tagged_fields();
    }
    if version >= 13 {
        out.extend_from_slice(&ERR_NONE.to_be_bytes()); // v13 top-level error_code
    }
    out.put_empty_tagged_fields();
}

/// Decode an `InitProducerId` v5 request: `(transactional_id, timeout_ms)`.
fn decode_init_producer_id_request(body: &[u8]) -> Result<(Option<String>, i32), DecodeError> {
    let mut cur = Cursor::new(body);
    let transactional_id = cur.get_compact_string()?;
    let timeout_ms = cur.get_i32()?;
    let _producer_id = cur.get_i64()?;
    let _producer_epoch = cur.get_i16()?;
    cur.skip_tagged_fields()?;
    Ok((transactional_id, timeout_ms))
}

/// Decode a `FindCoordinator` v1–v6 request: `(key_type, keys)`.
fn decode_find_coordinator_request(
    body: &[u8],
    version: i16,
) -> Result<(i8, Vec<String>), DecodeError> {
    let mut cur = Cursor::new(body);
    let (key_type, keys) = if version < 4 {
        let key = if version == 3 {
            cur.get_compact_string()?
                .ok_or(DecodeError::Invalid("null coordinator key"))?
        } else {
            let len = cur.get_i16()?;
            if len < 0 {
                return Err(DecodeError::Invalid("null coordinator key"));
            }
            std::str::from_utf8(cur.take(len as usize)?)
                .map_err(|_| DecodeError::Invalid("coordinator key UTF-8"))?
                .to_owned()
        };
        (cur.get_u8()? as i8, vec![key])
    } else {
        let key_type = cur.get_u8()? as i8;
        let n = cur
            .get_array_len(true)?
            .ok_or(DecodeError::Invalid("null coordinator keys"))?;
        let mut keys = Vec::with_capacity(n);
        for _ in 0..n {
            keys.push(
                cur.get_compact_string()?
                    .ok_or(DecodeError::Invalid("null coordinator key"))?,
            );
        }
        (key_type, keys)
    };
    if version >= 3 {
        cur.skip_tagged_fields()?;
    }
    if !cur.is_empty() {
        return Err(DecodeError::Invalid("coordinator trailing bytes"));
    }
    Ok((key_type, keys))
}

/// Encode a `FindCoordinator` v1–v6 response: every key maps to one node.
fn encode_find_coordinator_response(
    out: &mut Vec<u8>,
    version: i16,
    keys: &[String],
    node_id: i32,
    host: &str,
    port: i32,
) {
    out.extend_from_slice(&0i32.to_be_bytes()); // throttle_ms
    if version < 4 {
        out.extend_from_slice(&ERR_NONE.to_be_bytes());
        if version == 3 {
            out.put_compact_string(None);
        } else {
            out.extend_from_slice(&(-1i16).to_be_bytes());
        }
        out.extend_from_slice(&node_id.to_be_bytes());
        if version == 3 {
            out.put_compact_string(Some(host));
        } else {
            out.extend_from_slice(&(host.len() as i16).to_be_bytes());
            out.extend_from_slice(host.as_bytes());
        }
        out.extend_from_slice(&port.to_be_bytes());
        if version == 3 {
            out.put_empty_tagged_fields();
        }
        return;
    }
    out.put_compact_array_len(keys.len());
    for key in keys {
        out.put_compact_string(Some(key));
        out.extend_from_slice(&node_id.to_be_bytes());
        out.put_compact_string(Some(host));
        out.extend_from_slice(&port.to_be_bytes());
        out.extend_from_slice(&ERR_NONE.to_be_bytes());
        out.put_compact_string(None); // error_message
        out.put_empty_tagged_fields();
    }
    out.put_empty_tagged_fields();
}

/// One partition in a `Fetch` v15–v17 request.
#[derive(Debug, Clone)]
struct FetchPartitionData {
    partition: i32,
    fetch_offset: i64,
    partition_max_bytes: i32,
}

/// One topic in a `Fetch` v15–v17 request (identity is the topic id).
#[derive(Debug, Clone)]
struct FetchTopicData {
    topic_id: [u8; 16],
    partitions: Vec<FetchPartitionData>,
}

/// Decode a `Fetch` v15–v17 request: `(max_bytes, topics)`.
///
/// Session, forgotten topics, rack and tagged fields are parsed and
/// ignored: every response is full (`session_id` 0). `min_bytes`/`max_wait`
/// are parsed and ignored: the null broker answers immediately.
fn decode_fetch_request(body: &[u8]) -> Result<(i32, Vec<FetchTopicData>), DecodeError> {
    let mut cur = Cursor::new(body);
    // v15+: no untagged ReplicaId.
    let _max_wait_ms = cur.get_i32()?;
    let _min_bytes = cur.get_i32()?;
    let max_bytes = cur.get_i32()?;
    let _isolation = cur.get_u8()?;
    let _session_id = cur.get_i32()?;
    let _session_epoch = cur.get_i32()?;
    let topic_count = cur.get_array_len(true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let topic_id = cur.get_uuid()?;
        let part_count = cur.get_array_len(true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let partition = cur.get_i32()?;
            let _current_leader_epoch = cur.get_i32()?;
            let fetch_offset = cur.get_i64()?;
            let _last_fetched_epoch = cur.get_i32()?;
            let _log_start_offset = cur.get_i64()?;
            let partition_max_bytes = cur.get_i32()?;
            cur.skip_tagged_fields()?;
            partitions.push(FetchPartitionData {
                partition,
                fetch_offset,
                partition_max_bytes,
            });
        }
        cur.skip_tagged_fields()?;
        topics.push(FetchTopicData {
            topic_id,
            partitions,
        });
    }
    // Forgotten topics (parsed, ignored).
    let forgotten = cur.get_array_len(true)?.unwrap_or(0);
    for _ in 0..forgotten {
        let _topic_id = cur.get_uuid()?;
        let n = cur.get_array_len(true)?.unwrap_or(0);
        for _ in 0..n {
            let _partition = cur.get_i32()?;
        }
        cur.skip_tagged_fields()?;
    }
    let _rack_id = cur.get_compact_string()?;
    cur.skip_tagged_fields()?;
    Ok((max_bytes, topics))
}

/// One partition's data in a `Fetch` v15–v17 response.
#[derive(Debug, Clone)]
struct FetchPartitionResult {
    partition: i32,
    error_code: i16,
    high_watermark: i64,
    records: Vec<u8>,
    aborted: Vec<(i64, i64)>,
}

/// Encode a `Fetch` v15–v17 response: full data, `session_id` 0.
/// Log start is always 0 and last-stable-offset always equals the high
/// watermark (everything served is stable; aborts are listed explicitly).
fn encode_fetch_response(out: &mut Vec<u8>, topics: &[([u8; 16], Vec<FetchPartitionResult>)]) {
    out.extend_from_slice(&0i32.to_be_bytes()); // throttle_ms
    out.extend_from_slice(&ERR_NONE.to_be_bytes()); // top-level error
    out.extend_from_slice(&0i32.to_be_bytes()); // session_id: none, full data
    out.put_compact_array_len(topics.len());
    for (topic_id, partitions) in topics {
        out.extend_from_slice(topic_id);
        out.put_compact_array_len(partitions.len());
        for p in partitions {
            out.extend_from_slice(&p.partition.to_be_bytes());
            out.extend_from_slice(&p.error_code.to_be_bytes());
            out.extend_from_slice(&p.high_watermark.to_be_bytes());
            out.extend_from_slice(&p.high_watermark.to_be_bytes()); // LSO = HW
            out.extend_from_slice(&0i64.to_be_bytes()); // log_start_offset
            out.put_compact_array_len(p.aborted.len());
            for (pid, first) in &p.aborted {
                out.extend_from_slice(&pid.to_be_bytes());
                out.extend_from_slice(&first.to_be_bytes());
                out.put_empty_tagged_fields();
            }
            out.extend_from_slice(&(-1i32).to_be_bytes()); // preferred replica
            if p.records.is_empty() {
                out.put_uvarint(1); // empty, non-null
            } else {
                out.put_compact_bytes(Some(&p.records));
            }
            out.put_empty_tagged_fields();
        }
        out.put_empty_tagged_fields();
    }
    out.put_empty_tagged_fields();
}

/// One partition in a `ListOffsets` v10 request.
#[derive(Debug, Clone)]
struct ListOffsetsPartitionData {
    partition: i32,
    timestamp: i64,
}

/// One topic in a `ListOffsets` v10 request.
#[derive(Debug, Clone)]
struct ListOffsetsTopicData {
    name: String,
    partitions: Vec<ListOffsetsPartitionData>,
}

/// Decode a `ListOffsets` v10 request.
fn decode_list_offsets_request(body: &[u8]) -> Result<Vec<ListOffsetsTopicData>, DecodeError> {
    let mut cur = Cursor::new(body);
    let _replica_id = cur.get_i32()?;
    let _isolation = cur.get_u8()?;
    let topic_count = cur.get_array_len(true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = cur.get_compact_string()?.unwrap_or_default();
        let part_count = cur.get_array_len(true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let partition = cur.get_i32()?;
            let _leader_epoch = cur.get_i32()?;
            let timestamp = cur.get_i64()?;
            cur.skip_tagged_fields()?;
            partitions.push(ListOffsetsPartitionData {
                partition,
                timestamp,
            });
        }
        cur.skip_tagged_fields()?;
        topics.push(ListOffsetsTopicData { name, partitions });
    }
    let _timeout_ms = cur.get_i32()?;
    cur.skip_tagged_fields()?;
    Ok(topics)
}

/// Encode a `ListOffsets` v10 response from `(topic, partition, error,
/// timestamp, offset)` rows.
fn encode_list_offsets_response(out: &mut Vec<u8>, rows: &[(String, i32, i16, i64, i64)]) {
    out.extend_from_slice(&0i32.to_be_bytes()); // throttle_ms
    let mut order: Vec<&str> = Vec::new();
    for (topic, _, _, _, _) in rows {
        if !order.contains(&topic.as_str()) {
            order.push(topic.as_str());
        }
    }
    out.put_compact_array_len(order.len());
    for topic in &order {
        out.put_compact_string(Some(topic));
        let grouped: Vec<&(String, i32, i16, i64, i64)> =
            rows.iter().filter(|r| &r.0 == topic).collect();
        out.put_compact_array_len(grouped.len());
        for (_, partition, error, timestamp, offset) in grouped {
            out.extend_from_slice(&partition.to_be_bytes());
            out.extend_from_slice(&error.to_be_bytes());
            out.extend_from_slice(&timestamp.to_be_bytes());
            out.extend_from_slice(&offset.to_be_bytes());
            out.extend_from_slice(&0i32.to_be_bytes()); // leader_epoch
            out.put_empty_tagged_fields();
        }
        out.put_empty_tagged_fields();
    }
    out.put_empty_tagged_fields();
}

/// Encode an `InitProducerId` v5 response.
fn encode_init_producer_id_response(
    out: &mut Vec<u8>,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
) {
    out.extend_from_slice(&0i32.to_be_bytes()); // throttle_ms
    out.extend_from_slice(&error_code.to_be_bytes());
    out.extend_from_slice(&producer_id.to_be_bytes());
    out.extend_from_slice(&producer_epoch.to_be_bytes());
    out.put_empty_tagged_fields();
}

/// One partition's records in a `Produce` v12 request.
#[derive(Debug)]
struct ProducePartitionData<'a> {
    index: i32,
    records: &'a [u8],
}

/// One topic in a `Produce` v12 request.
#[derive(Debug)]
struct ProduceTopicData<'a> {
    topic: String,
    partitions: Vec<ProducePartitionData<'a>>,
}

/// Decode a `Produce` v12 request: `(transactional_id, acks, topics)`.
fn decode_produce_request(
    body: &[u8],
) -> Result<(Option<String>, i16, Vec<ProduceTopicData<'_>>), DecodeError> {
    let mut cur = Cursor::new(body);
    let transactional_id = cur.get_compact_string()?;
    let acks = cur.get_i16()?;
    let _timeout_ms = cur.get_i32()?;
    let topic_count = cur.get_array_len(true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let topic = cur.get_compact_string()?.unwrap_or_default();
        let part_count = cur.get_array_len(true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let index = cur.get_i32()?;
            let records = cur.get_compact_bytes()?.unwrap_or(&[]);
            cur.skip_tagged_fields()?;
            partitions.push(ProducePartitionData { index, records });
        }
        cur.skip_tagged_fields()?;
        topics.push(ProduceTopicData { topic, partitions });
    }
    cur.skip_tagged_fields()?;
    Ok((transactional_id, acks, topics))
}

/// One partition result in a `Produce` v12 response.
#[derive(Debug, Clone)]
struct ProducePartitionResult {
    topic: String,
    partition: i32,
    error_code: i16,
    base_offset: i64,
}

/// Encode a `Produce` v12 response, grouping partitions by first-seen topic.
fn encode_produce_response(out: &mut Vec<u8>, parts: &[ProducePartitionResult]) {
    let mut order: Vec<&str> = Vec::new();
    for p in parts {
        if !order.contains(&p.topic.as_str()) {
            order.push(p.topic.as_str());
        }
    }
    out.put_compact_array_len(order.len());
    for topic in &order {
        out.put_compact_string(Some(topic));
        let grouped: Vec<&ProducePartitionResult> =
            parts.iter().filter(|p| &p.topic == topic).collect();
        out.put_compact_array_len(grouped.len());
        for p in grouped {
            out.extend_from_slice(&p.partition.to_be_bytes());
            out.extend_from_slice(&p.error_code.to_be_bytes());
            out.extend_from_slice(&p.base_offset.to_be_bytes());
            out.extend_from_slice(&(-1i64).to_be_bytes()); // log_append_time_ms
            out.extend_from_slice(&0i64.to_be_bytes()); // log_start_offset
            out.put_compact_array_len(0); // record_errors
            out.put_compact_string(None); // error_message
            out.put_empty_tagged_fields(); // no CurrentLeader tag
        }
        out.put_empty_tagged_fields();
    }
    out.extend_from_slice(&0i32.to_be_bytes()); // throttle_ms
    out.put_empty_tagged_fields();
}

/// Why a batch was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationFailure {
    /// Truncated frame, bad magic, length mismatch, malformed record.
    Framing,
    /// CRC32-C mismatch.
    Crc,
    /// `numRecords` / `lastOffsetDelta` inconsistent with walked records.
    Count,
    /// Base sequence breaks per-producer/partition continuity.
    Sequence,
    /// Transactional produce, which the null broker does not run.
    Transactional,
}

/// Fixed header length of a v2 record batch (through `numRecords`).
const BATCH_HEADER_LEN: usize = 61;
/// Offset of the CRC field within a batch.
const CRC_OFFSET: usize = 17;
/// First byte covered by the batch CRC (attributes).
const CRC_START: usize = 21;

/// A structurally valid batch, before sequence checks.
///
/// The client-stated base offset is not returned: like a real broker (and
/// the in-repo mock), the null broker ignores it — `partitionline` always
/// sends `0` — and assigns offsets unconditionally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedBatch {
    /// Records in the batch.
    pub records: u32,
    /// Batch producer id (`-1` for plain produce).
    pub producer_id: i64,
    /// Batch base sequence (idempotent produce only).
    pub base_sequence: i32,
}

/// Parse and structurally validate one record batch: framing, magic,
/// CRC32-C, `numRecords` / `lastOffsetDelta` consistency and an exact
/// record walk for uncompressed batches.
///
/// Compressed batches cannot be walked without a codec; their framing,
/// CRC and count header are still checked and the header count is trusted.
pub fn parse_batch(bytes: &[u8]) -> Result<ParsedBatch, ValidationFailure> {
    if bytes.is_empty() {
        return Ok(ParsedBatch {
            records: 0,
            producer_id: -1,
            base_sequence: 0,
        });
    }
    let mut cur = Cursor::new(bytes);
    let _base_offset = cur.get_i64().map_err(|_| ValidationFailure::Framing)?;
    let batch_len = cur.get_i32().map_err(|_| ValidationFailure::Framing)?;
    let total = usize::try_from(batch_len)
        .ok()
        .and_then(|n| n.checked_add(12))
        .unwrap_or(usize::MAX);
    if total != bytes.len() {
        return Err(ValidationFailure::Framing);
    }
    let _leader_epoch = cur.get_i32().map_err(|_| ValidationFailure::Framing)?;
    let magic = cur.get_u8().map_err(|_| ValidationFailure::Framing)?;
    if magic != 2 {
        return Err(ValidationFailure::Framing);
    }
    debug_assert_eq!(cur.pos, CRC_OFFSET);
    let stored = cur.get_u32().map_err(|_| ValidationFailure::Framing)?;
    if crc32c(&bytes[CRC_START..]) != stored {
        return Err(ValidationFailure::Crc);
    }
    let attributes = cur.get_i16().map_err(|_| ValidationFailure::Framing)?;
    if attributes & 0x20 != 0 {
        // Control batch: a producer client must never send one.
        return Err(ValidationFailure::Framing);
    }
    let last_offset_delta = cur.get_i32().map_err(|_| ValidationFailure::Framing)?;
    let _base_timestamp = cur.get_i64().map_err(|_| ValidationFailure::Framing)?;
    let _max_timestamp = cur.get_i64().map_err(|_| ValidationFailure::Framing)?;
    let producer_id = cur.get_i64().map_err(|_| ValidationFailure::Framing)?;
    let _producer_epoch = cur.get_i16().map_err(|_| ValidationFailure::Framing)?;
    let base_sequence = cur.get_i32().map_err(|_| ValidationFailure::Framing)?;
    let num_records = cur.get_i32().map_err(|_| ValidationFailure::Framing)?;
    debug_assert_eq!(cur.pos, BATCH_HEADER_LEN);
    if num_records < 0 || last_offset_delta != num_records - 1 {
        return Err(ValidationFailure::Count);
    }
    let count = u32::try_from(num_records).map_err(|_| ValidationFailure::Count)?;
    if attributes & 0x07 == 0 {
        walk_records(&bytes[BATCH_HEADER_LEN..], count)?;
    } else if num_records == 0 {
        return Err(ValidationFailure::Count);
    }
    Ok(ParsedBatch {
        records: count,
        producer_id,
        base_sequence,
    })
}

/// Walk `count` records, checking declared lengths, sequential offsets and
/// exact framing. Returns [`ValidationFailure::Framing`] on any mismatch.
fn walk_records(bytes: &[u8], count: u32) -> Result<(), ValidationFailure> {
    let mut cur = Cursor::new(bytes);
    for index in 0..count {
        let start = cur.pos;
        let declared = cur.get_varint().map_err(|_| ValidationFailure::Framing)?;
        if declared < 0 {
            return Err(ValidationFailure::Framing);
        }
        let body_start = cur.pos;
        let _attributes = cur.get_u8().map_err(|_| ValidationFailure::Framing)?;
        let _timestamp_delta = cur.get_varlong().map_err(|_| ValidationFailure::Framing)?;
        let offset_delta = cur.get_varint().map_err(|_| ValidationFailure::Framing)?;
        if offset_delta != index as i32 {
            return Err(ValidationFailure::Framing);
        }
        skip_sized_bytes(&mut cur)?;
        skip_sized_bytes(&mut cur)?;
        let headers = cur.get_varint().map_err(|_| ValidationFailure::Framing)?;
        if headers < -1 {
            return Err(ValidationFailure::Framing);
        }
        for _ in 0..headers.max(0) {
            let key_len = cur.get_varint().map_err(|_| ValidationFailure::Framing)?;
            if key_len < 0 {
                return Err(ValidationFailure::Framing);
            }
            cur.take(key_len as usize)
                .map_err(|_| ValidationFailure::Framing)?;
            skip_sized_bytes(&mut cur)?;
        }
        let consumed = cur.pos.saturating_sub(body_start);
        if consumed != declared as usize {
            return Err(ValidationFailure::Framing);
        }
        let _ = start;
    }
    if !cur.is_empty() {
        return Err(ValidationFailure::Framing);
    }
    Ok(())
}

/// Skip one length-prefixed (`varint`, `-1` null) byte run.
fn skip_sized_bytes(cur: &mut Cursor<'_>) -> Result<(), ValidationFailure> {
    let len = cur.get_varint().map_err(|_| ValidationFailure::Framing)?;
    if len < -1 {
        return Err(ValidationFailure::Framing);
    }
    if len > 0 {
        cur.take(len as usize)
            .map_err(|_| ValidationFailure::Framing)?;
    }
    Ok(())
}

/// Per-cause validation failure counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FailureCounts {
    /// Framing rejections.
    pub framing: u64,
    /// CRC32-C rejections.
    pub crc: u64,
    /// Record-count rejections.
    pub count: u64,
    /// Sequence-continuity rejections.
    pub sequence: u64,
    /// Transactional-produce rejections.
    pub transactional: u64,
}

impl FailureCounts {
    fn record(&mut self, failure: ValidationFailure) {
        match failure {
            ValidationFailure::Framing => self.framing += 1,
            ValidationFailure::Crc => self.crc += 1,
            ValidationFailure::Count => self.count += 1,
            ValidationFailure::Sequence => self.sequence += 1,
            ValidationFailure::Transactional => self.transactional += 1,
        }
    }

    /// Total failures across all causes.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.framing + self.crc + self.count + self.sequence + self.transactional
    }
}

/// Error code sent for a rejected batch.
///
/// Sequence mismatches report `OUT_OF_ORDER_SEQUENCE_NUMBER` like the
/// in-repo mock; exact retries are deduplicated as successes, never errors.
fn error_for(failure: ValidationFailure) -> i16 {
    match failure {
        ValidationFailure::Framing | ValidationFailure::Crc | ValidationFailure::Count => {
            ERR_INVALID_RECORD
        }
        ValidationFailure::Sequence => ERR_OUT_OF_ORDER_SEQUENCE_NUMBER,
        ValidationFailure::Transactional => ERR_INVALID_TXN_STATE,
    }
}

/// Mutable broker state: log end offsets, idempotent sequences, counters.
#[derive(Debug, Default)]
struct State {
    /// Request counts, populated only when explicitly enabled.
    api_versions: HashMap<(i16, i16), u64>,
    /// Next offset per `(topic, partition)`.
    next_offset: HashMap<(String, i32), i64>,
    /// Expected base sequence per `(producer_id, topic, partition)`.
    sequences: HashMap<(i64, String, i32), i32>,
    /// Last committed `(base_sequence, count, assigned_base)` per
    /// `(producer_id, topic, partition)` for retry deduplication.
    last_committed: HashMap<(i64, String, i32), (i32, u32, i64)>,
    /// Records accepted.
    accepted_records: u64,
    /// Validated batch wire bytes accepted.
    accepted_wire_bytes: u64,
    /// Produce requests handled (including `acks=0`).
    produce_requests: u64,
    /// Rejections by cause.
    failures: FailureCounts,
    /// Next producer id handed out by `InitProducerId`.
    next_producer_id: i64,
    /// Topic id to name, learned from `Metadata` responses (Fetch is by id).
    topic_ids: HashMap<[u8; 16], String>,
    /// Fetch requests handled.
    fetch_requests: u64,
    /// Synthetic records served.
    fetched_records: u64,
    /// Synthetic batch wire bytes served.
    fetched_wire_bytes: u64,
    /// Injected fault responses (fail-fast, never appended).
    injected_errors: u64,
    /// Faulted requests (one per `inject()` hit; `injected_errors`
    /// counts faulted partitions).
    injected_requests: u64,
    /// Metadata requests handled.
    metadata_requests: u64,
    /// Requests that hit a non-leader node.
    leader_mismatches: u64,
}

/// One advertised broker: live (has a listener) or dead (refused).
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Node id.
    pub id: i32,
    /// Advertised host.
    pub host: String,
    /// Advertised port.
    pub port: i32,
    /// Whether a listener serves this node.
    pub live: bool,
}

/// Seeded per-request fault decisions.
#[derive(Debug)]
pub struct FaultState {
    seed: u64,
    rate_per_million: u32,
    counter: std::sync::atomic::AtomicU64,
}

impl FaultState {
    fn new(seed: u64, rate_per_million: u32) -> Self {
        Self {
            seed,
            rate_per_million,
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Whether request `counter` (claimed atomically) is a fault.
    fn inject(&self) -> bool {
        if self.rate_per_million == 0 {
            return false;
        }
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let roll = (synth::splitmix64(self.seed.wrapping_add(n)) % 1_000_000) as u32;
        roll < self.rate_per_million.min(1_000_000)
    }
}

/// Server configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Record request API keys and versions. Disabled for timing runs.
    pub trace_api_versions: bool,
    /// Address to bind, e.g. `127.0.0.1:19092` (port `0` for ephemeral).
    pub bind: String,
    /// Partitions advertised per topic.
    pub partitions: i32,
    /// How long to serve before shutting down and writing the artifact.
    pub serve_for: Duration,
    /// Where to write the `client-ceiling` result artifact.
    pub artifact: PathBuf,
    /// Synthetic Fetch log configuration.
    pub synth: synth::SynthConfig,
    /// Live node listeners: node `i` binds `bind` port + `i`. One by default.
    pub nodes: u16,
    /// Extra advertised node ids with no listener (connection refused).
    pub dead_nodes: u16,
    /// Per-partition leader node ids (`partitions` long); empty means
    /// round-robin over live nodes.
    pub leaders: Vec<i32>,
    /// Node id whose responses are delayed (slow-node mode).
    pub slow_node: Option<i32>,
    /// Fixed response delay for the slow node.
    pub slow_delay: Duration,
    /// Fault-injection seed (seeded per-request decisions).
    pub fault_seed: u64,
    /// Injected `NOT_LEADER_OR_FOLLOWER` rate, per million requests.
    pub fault_rate_per_million: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            trace_api_versions: false,
            bind: "127.0.0.1:19092".to_owned(),
            partitions: 6,
            serve_for: Duration::from_secs(30),
            artifact: PathBuf::from("nullbroker-ceiling.json"),
            synth: synth::SynthConfig::default(),
            nodes: 1,
            dead_nodes: 0,
            leaders: Vec::new(),
            slow_node: None,
            slow_delay: Duration::ZERO,
            fault_seed: 0x5EED_0001,
            fault_rate_per_million: 0,
        }
    }
}

/// The served run's outcome, also written to the artifact file.
#[derive(Debug, Clone)]
pub struct RunReport {
    /// Observed `(api_key, version, request_count)` when tracing is enabled.
    pub api_versions: Vec<(i16, i16, u64)>,
    /// Records accepted.
    pub accepted_records: u64,
    /// Validated batch wire bytes accepted.
    pub accepted_wire_bytes: u64,
    /// Produce requests handled.
    pub produce_requests: u64,
    /// Rejections by cause.
    pub failures: FailureCounts,
    /// Log end offset per `(topic, partition)`.
    pub end_offsets: Vec<(String, i32, i64)>,
    /// Fetch requests handled.
    pub fetch_requests: u64,
    /// Synthetic records served.
    pub fetched_records: u64,
    /// Synthetic batch wire bytes served.
    pub fetched_wire_bytes: u64,
    /// Injected fault responses served.
    pub injected_errors: u64,
    /// Faulted requests (KL09-10 retry accounting).
    pub injected_requests: u64,
    /// Metadata requests handled (KL09-10 retry accounting).
    pub metadata_requests: u64,
    /// Requests that hit a non-leader node.
    pub leader_mismatches: u64,
    /// Active modes, recorded for the artifact.
    pub modes: Modes,
}

/// Active null-broker modes (KL09-08). All are off by default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Modes {
    /// Live node listeners.
    pub nodes: u16,
    /// Advertised-but-dead node ids.
    pub dead_nodes: u16,
    /// Slow node id, if any.
    pub slow_node: Option<i32>,
    /// Slow-node fixed response delay.
    pub slow_delay: Duration,
    /// Fault-injection seed.
    pub fault_seed: u64,
    /// Injected-error rate, per million requests.
    pub fault_rate_per_million: u32,
}

impl Default for Modes {
    fn default() -> Self {
        Self {
            nodes: 1,
            dead_nodes: 0,
            slow_node: None,
            slow_delay: Duration::ZERO,
            fault_seed: 0x5EED_0001,
            fault_rate_per_million: 0,
        }
    }
}

/// A validating null-broker Produce server.
#[derive(Debug)]
pub struct NullBroker {
    trace_api_versions: bool,
    state: Arc<Mutex<State>>,
    shutdown: Arc<AtomicBool>,
    /// One handle per live connection; shutdown closes them all to unblock
    /// readers. Connections use blocking I/O so large frames can never hit
    /// a mid-frame timeout.
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    partitions: i32,
    synth: synth::SynthConfig,
    /// Advertised brokers: live first, then dead.
    nodes: Vec<NodeInfo>,
    /// Leader node id per partition.
    leadership: Vec<i32>,
    faults: Arc<FaultState>,
    modes: Modes,
}

/// Leader map: explicit config when its length matches `partitions`,
/// else round-robin over `live` nodes.
fn resolve_leadership(config_leaders: &[i32], partitions: i32, live: u16) -> Vec<i32> {
    let n = usize::try_from(partitions.max(0)).unwrap_or(0);
    if config_leaders.len() == n && n > 0 {
        return config_leaders.to_vec();
    }
    let live = live.max(1) as i32;
    (0..n).map(|p| (p as i32) % live).collect()
}

impl NullBroker {
    /// Serve until `config.serve_for` elapses, then write the
    /// `client-ceiling` artifact and return the run report.
    ///
    /// Node `i` binds `config.bind` port + `i` (port `0` anchors node 0 to
    /// an ephemeral port and continues from there).
    pub fn run(config: &Config) -> io::Result<RunReport> {
        let bound = Self::bind_all(config)?;
        let broker = Self::with_bound(&bound, config)?;
        let broker = Arc::new(broker);
        let mut handles = Vec::new();
        for (node_id, listener) in &bound {
            let broker = Arc::clone(&broker);
            // `TcpListener` is shared by reference: the accept loop needs
            // only `&self`, so each node serves on its own thread.
            let listener = listener.try_clone()?;
            let serve_for = config.serve_for;
            let node_id = *node_id;
            handles.push(std::thread::spawn(move || {
                broker.serve(node_id, &listener, serve_for)
            }));
        }
        let mut result = Ok(());
        for handle in handles {
            result = result.and(handle.join().unwrap_or(Ok(())));
        }
        result?;
        let report = broker.report();
        write_artifact(&config.artifact, &report)?;
        Ok(report)
    }

    /// Bind one listener per live node.
    pub fn bind_all(config: &Config) -> io::Result<Vec<(i32, TcpListener)>> {
        let count = config.nodes.max(1);
        let base: std::net::SocketAddr = config
            .bind
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let mut bound = Vec::with_capacity(count as usize);
        let mut port = base.port();
        for id in 0..count {
            let addr = std::net::SocketAddr::new(base.ip(), port);
            let listener = TcpListener::bind(addr)?;
            if id == 0 {
                port = listener.local_addr()?.port();
            }
            bound.push((i32::from(id), listener));
            port = port.wrapping_add(1);
        }
        Ok(bound)
    }

    /// Build a broker around already-bound listeners (tests serve these).
    pub fn with_bound(bound: &[(i32, TcpListener)], config: &Config) -> io::Result<Self> {
        let mut nodes = Vec::new();
        for (id, listener) in bound {
            let addr = listener.local_addr()?;
            nodes.push(NodeInfo {
                id: *id,
                host: addr.ip().to_string(),
                port: i32::from(addr.port()),
                live: true,
            });
        }
        let live = bound.len() as u16;
        if let Some(first) = nodes.first().cloned() {
            let live_ports: Vec<i32> = nodes.iter().map(|n| n.port).collect();
            for d in 0..config.dead_nodes {
                // Dead ids need ports no live listener holds; ephemeral
                // allocation is near-sequential, so skip collisions.
                let mut port = first.port + i32::from(live) + i32::from(d);
                while live_ports.contains(&port) {
                    port += 1;
                }
                nodes.push(NodeInfo {
                    id: i32::from(live) + i32::from(d),
                    host: first.host.clone(),
                    port,
                    live: false,
                });
            }
        }
        let leadership = resolve_leadership(&config.leaders, config.partitions, live.max(1));
        Ok(Self {
            trace_api_versions: config.trace_api_versions,
            state: Arc::new(Mutex::new(State::default())),
            shutdown: Arc::new(AtomicBool::new(false)),
            sockets: Arc::new(Mutex::new(Vec::new())),
            partitions: config.partitions,
            synth: config.synth.clone(),
            nodes,
            leadership,
            faults: Arc::new(FaultState::new(
                config.fault_seed,
                config.fault_rate_per_million,
            )),
            modes: Modes {
                nodes: live,
                dead_nodes: config.dead_nodes,
                slow_node: config.slow_node,
                slow_delay: config.slow_delay,
                fault_seed: config.fault_seed,
                fault_rate_per_million: config.fault_rate_per_million,
            },
        })
    }

    /// Build a single-node broker around an already-bound listener (tests).
    pub fn with_listener(
        listener: &TcpListener,
        partitions: i32,
        synth: synth::SynthConfig,
    ) -> io::Result<Self> {
        let config = Config {
            partitions,
            synth,
            ..Config::default()
        };
        Self::with_bound(&[(0, listener.try_clone()?)], &config)
    }

    /// Ask the server to stop: the flag stops the accept loop and every
    /// live connection is shut down to unblock its reader.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        if let Ok(sockets) = self.sockets.lock() {
            for socket in sockets.iter() {
                let _ = socket.shutdown(std::net::Shutdown::Both);
            }
        }
    }

    /// Serve `listener` as `node_id` until `serve_for` elapses or
    /// [`Self::shutdown`].
    pub fn serve(
        &self,
        node_id: i32,
        listener: &TcpListener,
        serve_for: Duration,
    ) -> io::Result<()> {
        listener.set_nonblocking(true)?;
        let deadline = Instant::now() + serve_for;
        let mut workers = Vec::new();
        while Instant::now() < deadline && !self.shutdown.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => {
                    // Accepted sockets inherit nonblocking mode; workers
                    // need blocking reads.
                    if stream.set_nonblocking(false).is_err() {
                        continue;
                    }
                    if let Ok(handle) = stream.try_clone() {
                        if let Ok(mut sockets) = self.sockets.lock() {
                            sockets.push(handle);
                        }
                    }
                    let worker = Worker {
                        trace_api_versions: self.trace_api_versions,
                        state: Arc::clone(&self.state),
                        node_id,
                        nodes: self.nodes.clone(),
                        leadership: self.leadership.clone(),
                        faults: Arc::clone(&self.faults),
                        slow: self.modes.slow_node,
                        slow_delay: self.modes.slow_delay,
                        partitions: self.partitions,
                        synth: self.synth.clone(),
                    };
                    workers.push(std::thread::spawn(move || worker.serve_conn(stream)));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => return Err(e),
            }
        }
        self.shutdown();
        for worker in workers {
            let _ = worker.join();
        }
        Ok(())
    }

    /// Snapshot the run report.
    #[must_use]
    pub fn report(&self) -> RunReport {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut end_offsets: Vec<(String, i32, i64)> = state
            .next_offset
            .iter()
            .map(|((topic, partition), offset)| (topic.clone(), *partition, *offset))
            .collect();
        end_offsets.sort();
        let mut api_versions: Vec<_> = state
            .api_versions
            .iter()
            .map(|(&(key, version), &count)| (key, version, count))
            .collect();
        api_versions.sort_unstable();
        RunReport {
            api_versions,
            accepted_records: state.accepted_records,
            accepted_wire_bytes: state.accepted_wire_bytes,
            produce_requests: state.produce_requests,
            failures: state.failures,
            end_offsets,
            fetch_requests: state.fetch_requests,
            fetched_records: state.fetched_records,
            fetched_wire_bytes: state.fetched_wire_bytes,
            injected_errors: state.injected_errors,
            injected_requests: state.injected_requests,
            metadata_requests: state.metadata_requests,
            leader_mismatches: state.leader_mismatches,
            modes: self.modes.clone(),
        }
    }
}

/// One connection's request loop (blocking I/O; shutdown unblocks reads).
#[derive(Debug)]
struct Worker {
    trace_api_versions: bool,
    state: Arc<Mutex<State>>,
    node_id: i32,
    nodes: Vec<NodeInfo>,
    leadership: Vec<i32>,
    faults: Arc<FaultState>,
    slow: Option<i32>,
    slow_delay: Duration,
    partitions: i32,
    synth: synth::SynthConfig,
}

impl Worker {
    /// Leader of `partition` (0 when the map has no entry).
    fn leader_of(&self, partition: i32) -> i32 {
        usize::try_from(partition)
            .ok()
            .and_then(|p| self.leadership.get(p).copied())
            .unwrap_or(0)
    }
}

impl Worker {
    fn serve_conn(&self, stream: TcpStream) {
        let mut stream = stream;
        let mut len_buf = [0u8; 4];
        loop {
            if stream.read_exact(&mut len_buf).is_err() {
                return;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if !(8..=MAX_FRAME_BYTES).contains(&len) {
                return;
            }
            let mut frame = vec![0u8; len];
            if stream.read_exact(&mut frame).is_err() {
                return;
            }
            match self.handle_frame(&frame) {
                Ok(Some(response)) => {
                    if self.slow == Some(self.node_id) && !self.slow_delay.is_zero() {
                        std::thread::sleep(self.slow_delay);
                    }
                    let out_len = u32::try_from(response.len()).unwrap_or(u32::MAX);
                    if stream.write_all(&out_len.to_be_bytes()).is_err() {
                        return;
                    }
                    if stream.write_all(&response).is_err() {
                        return;
                    }
                }
                Ok(None) => {}     // acks=0: no response
                Err(()) => return, // undecodable frame: close, client will surface it
            }
        }
    }

    /// Handle one frame. `Ok(None)` means "no response" (`acks=0`);
    /// `Err` means the connection must close.
    fn handle_frame(&self, frame: &[u8]) -> Result<Option<Vec<u8>>, ()> {
        let (api_key, api_version, correlation_id, header_len) =
            decode_request_header(frame).map_err(|_| ())?;
        // Refuse versions we do not advertise or implement. Never select behavior
        // by client identity; all peers use the same handlers and validation.
        if !advertised_apis()
            .iter()
            .any(|&(key, min, max)| key == api_key && (min..=max).contains(&api_version))
        {
            return Err(());
        }
        if self.trace_api_versions {
            let mut state = self.lock_state().map_err(|_| ())?;
            *state
                .api_versions
                .entry((api_key, api_version))
                .or_default() += 1;
        }
        let body = frame.get(header_len..).ok_or(())?;
        let mut out = Vec::new();
        match api_key {
            API_KEY_API_VERSIONS => {
                let _ = decode_api_versions_request(body, api_version).map_err(|_| ())?;
                encode_response_header(&mut out, api_key, api_version, correlation_id);
                encode_api_versions_response(&mut out, api_version, ERR_NONE);
                Ok(Some(out))
            }
            API_KEY_METADATA if (12..=13).contains(&api_version) => {
                let (topics, _allow_auto) = decode_metadata_request(body).map_err(|_| ())?;
                if let Ok(mut state) = self.state.lock() {
                    state.metadata_requests += 1;
                }
                encode_response_header(&mut out, api_key, api_version, correlation_id);
                let topics = topics.as_deref().unwrap_or(&[]);
                // Remember id -> name so Fetch-by-ID resolves.
                if let Ok(mut state) = self.state.lock() {
                    for topic in topics {
                        if let Some(name) = topic.name.as_deref() {
                            if !name.is_empty() {
                                state
                                    .topic_ids
                                    .insert(synth::topic_id(name), name.to_owned());
                            }
                        }
                    }
                }
                encode_metadata_response(
                    &mut out,
                    api_version,
                    topics,
                    self.partitions,
                    &self.leadership,
                    &self.nodes,
                );
                Ok(Some(out))
            }
            API_KEY_INIT_PRODUCER_ID if api_version == 5 => {
                let (transactional_id, _timeout) =
                    decode_init_producer_id_request(body).map_err(|_| ())?;
                encode_response_header(&mut out, api_key, api_version, correlation_id);
                if transactional_id.is_some() {
                    encode_init_producer_id_response(&mut out, ERR_INVALID_TXN_STATE, -1, -1);
                } else {
                    let pid = {
                        let mut state = self.lock_state().map_err(|_| ())?;
                        state.next_producer_id += 1;
                        state.next_producer_id
                    };
                    encode_init_producer_id_response(&mut out, ERR_NONE, pid, 0);
                }
                Ok(Some(out))
            }
            API_KEY_FIND_COORDINATOR if (1..=6).contains(&api_version) => {
                let (_key_type, keys) =
                    decode_find_coordinator_request(body, api_version).map_err(|_| ())?;
                encode_response_header(&mut out, api_key, api_version, correlation_id);
                let (node_id, host, port) = self
                    .nodes
                    .iter()
                    .find(|n| n.live)
                    .map(|n| (n.id, n.host.as_str(), n.port))
                    .unwrap_or((0, "127.0.0.1", 19092));
                encode_find_coordinator_response(&mut out, api_version, &keys, node_id, host, port);
                Ok(Some(out))
            }
            API_KEY_PRODUCE if (9..=12).contains(&api_version) => {
                let (transactional_id, acks, topics) =
                    decode_produce_request(body).map_err(|_| ())?;
                {
                    let mut state = self.lock_state().map_err(|_| ())?;
                    state.produce_requests += 1;
                }
                // Injected faults fail fast (nothing appended, so a retry
                // preserves acked == accepted). Never on acks=0: a dropped
                // fire-and-forget batch is invisible loss.
                if acks != 0 && self.faults.inject() {
                    let parts = topics
                        .iter()
                        .flat_map(|t| {
                            t.partitions.iter().map(|p| ProducePartitionResult {
                                topic: t.topic.clone(),
                                partition: p.index,
                                error_code: ERR_NOT_LEADER_OR_FOLLOWER,
                                base_offset: -1,
                            })
                        })
                        .collect::<Vec<_>>();
                    if let Ok(mut state) = self.state.lock() {
                        state.injected_errors += parts.len() as u64;
                        state.injected_requests += 1;
                    }
                    encode_response_header(&mut out, api_key, api_version, correlation_id);
                    encode_produce_response(&mut out, &parts);
                    return Ok(Some(out));
                }
                if acks == 0 {
                    // No response, but still validate and count.
                    let _ = self.append(topics.iter(), transactional_id.is_some());
                    return Ok(None);
                }
                let parts = self.append(topics.iter(), transactional_id.is_some());
                encode_response_header(&mut out, api_key, api_version, correlation_id);
                encode_produce_response(&mut out, &parts);
                Ok(Some(out))
            }
            API_KEY_FETCH if (15..=17).contains(&api_version) => {
                let (max_bytes, topics) = decode_fetch_request(body).map_err(|_| ())?;
                {
                    let mut state = self.lock_state().map_err(|_| ())?;
                    state.fetch_requests += 1;
                }
                if self.faults.inject() {
                    let served = topics
                        .iter()
                        .map(|t| {
                            let parts = t
                                .partitions
                                .iter()
                                .map(|p| FetchPartitionResult {
                                    partition: p.partition,
                                    error_code: ERR_NOT_LEADER_OR_FOLLOWER,
                                    high_watermark: self.synth.records_per_partition as i64,
                                    records: Vec::new(),
                                    aborted: Vec::new(),
                                })
                                .collect::<Vec<_>>();
                            (t.topic_id, parts)
                        })
                        .collect::<Vec<_>>();
                    let n: usize = served.iter().map(|(_, p)| p.len()).sum();
                    if let Ok(mut state) = self.state.lock() {
                        state.injected_errors += n as u64;
                        state.injected_requests += 1;
                    }
                    encode_response_header(&mut out, api_key, api_version, correlation_id);
                    encode_fetch_response(&mut out, &served);
                    return Ok(Some(out));
                }
                let served = self.serve_fetch(max_bytes, &topics);
                encode_response_header(&mut out, api_key, api_version, correlation_id);
                encode_fetch_response(&mut out, &served);
                Ok(Some(out))
            }
            API_KEY_LIST_OFFSETS if (7..=10).contains(&api_version) => {
                let topics = decode_list_offsets_request(body).map_err(|_| ())?;
                let rows = self.serve_list_offsets(&topics);
                encode_response_header(&mut out, api_key, api_version, correlation_id);
                encode_list_offsets_response(&mut out, &rows);
                Ok(Some(out))
            }
            _ => Err(()),
        }
    }

    /// Validate every batch, assign offsets, count outcomes.
    ///
    /// Structural validation (CRC, record walk) runs lock-free; the state
    /// lock is held only for the sequence/offset commit.
    fn append<'a, I>(&self, topics: I, transactional: bool) -> Vec<ProducePartitionResult>
    where
        I: IntoIterator<Item = &'a ProduceTopicData<'a>>,
    {
        /// One partition's parsed batch, ready to commit.
        struct Pending {
            topic: String,
            partition: i32,
            wire_len: u64,
            parsed: Result<ParsedBatch, ValidationFailure>,
        }
        let mut pending = Vec::new();
        for topic in topics {
            for part in &topic.partitions {
                let parsed = if transactional {
                    Err(ValidationFailure::Transactional)
                } else {
                    parse_batch(part.records)
                };
                pending.push(Pending {
                    topic: topic.topic.clone(),
                    partition: part.index,
                    wire_len: part.records.len() as u64,
                    parsed,
                });
            }
        }
        let mut parts = Vec::with_capacity(pending.len());
        let mut state = match self.lock_state() {
            Ok(guard) => guard,
            Err(()) => return parts,
        };
        for item in pending {
            if self.leader_of(item.partition) != self.node_id {
                state.leader_mismatches += 1;
                parts.push(ProducePartitionResult {
                    topic: item.topic,
                    partition: item.partition,
                    error_code: ERR_NOT_LEADER_OR_FOLLOWER,
                    base_offset: -1,
                });
                continue;
            }
            if item.parsed == Err(ValidationFailure::Transactional) {
                state.failures.record(ValidationFailure::Transactional);
                parts.push(ProducePartitionResult {
                    topic: item.topic,
                    partition: item.partition,
                    error_code: ERR_INVALID_TXN_STATE,
                    base_offset: -1,
                });
                continue;
            }
            let parsed = match item.parsed {
                Ok(parsed) => parsed,
                Err(failure) => {
                    state.failures.record(failure);
                    parts.push(ProducePartitionResult {
                        topic: item.topic,
                        partition: item.partition,
                        error_code: error_for(failure),
                        base_offset: -1,
                    });
                    continue;
                }
            };
            // Sequence continuity for idempotent batches. An exact retry of
            // the last committed batch succeeds with its original base and
            // is not double-counted (broker-faithful deduplication).
            if parsed.producer_id >= 0 {
                let key = (parsed.producer_id, item.topic.clone(), item.partition);
                let expected = state
                    .sequences
                    .entry(key.clone())
                    .or_insert(parsed.base_sequence);
                if parsed.base_sequence != *expected {
                    let duplicate =
                        state
                            .last_committed
                            .get(&key)
                            .is_some_and(|&(seq, count, _)| {
                                seq == parsed.base_sequence && count == parsed.records
                            });
                    if duplicate {
                        let original = state
                            .last_committed
                            .get(&key)
                            .map(|&(_, _, base)| base)
                            .unwrap_or(-1);
                        parts.push(ProducePartitionResult {
                            topic: item.topic,
                            partition: item.partition,
                            error_code: ERR_NONE,
                            base_offset: original,
                        });
                        continue;
                    }
                    state.failures.record(ValidationFailure::Sequence);
                    parts.push(ProducePartitionResult {
                        topic: item.topic,
                        partition: item.partition,
                        error_code: error_for(ValidationFailure::Sequence),
                        base_offset: -1,
                    });
                    continue;
                }
                *expected = expected.wrapping_add(parsed.records as i32);
            }
            // Offsets are always assigned; the client-stated base is ignored.
            let next = state
                .next_offset
                .entry((item.topic.clone(), item.partition))
                .or_insert(0);
            let assigned = *next;
            *next = next.wrapping_add(i64::from(parsed.records));
            if parsed.producer_id >= 0 {
                state.last_committed.insert(
                    (parsed.producer_id, item.topic.clone(), item.partition),
                    (parsed.base_sequence, parsed.records, assigned),
                );
            }
            state.accepted_records += u64::from(parsed.records);
            state.accepted_wire_bytes += item.wire_len;
            parts.push(ProducePartitionResult {
                topic: item.topic,
                partition: item.partition,
                error_code: ERR_NONE,
                base_offset: assigned,
            });
        }
        parts
    }

    /// Serve one `Fetch` v15–v17 request from the synthetic log.
    ///
    /// Whole batches while both budgets allow, with a one-batch progress
    /// guarantee per partition below log end (mirroring Kafka, which can
    /// exceed `max_bytes` by one batch). Unknown topic ids and negative
    /// offsets get protocol errors, not panics.
    fn serve_fetch(
        &self,
        max_bytes: i32,
        topics: &[FetchTopicData],
    ) -> Vec<([u8; 16], Vec<FetchPartitionResult>)> {
        let known: Vec<bool> = {
            let state = match self.state.lock() {
                Ok(guard) => guard,
                Err(_) => return Vec::new(),
            };
            topics
                .iter()
                .map(|t| state.topic_ids.contains_key(&t.topic_id))
                .collect()
        };
        let end = self.synth.records_per_partition as i64;
        let total_budget = usize::try_from(max_bytes.max(0)).unwrap_or(0);
        let mut total_used = 0usize;
        let mut served_records = 0u64;
        let mut served_bytes = 0u64;
        let mut out = Vec::with_capacity(topics.len());
        for (topic, is_known) in topics.iter().zip(known.iter()) {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for part in &topic.partitions {
                if !is_known {
                    partitions.push(FetchPartitionResult {
                        partition: part.partition,
                        error_code: ERR_UNKNOWN_TOPIC_ID,
                        high_watermark: -1,
                        records: Vec::new(),
                        aborted: Vec::new(),
                    });
                    continue;
                }
                if part.fetch_offset < 0 {
                    partitions.push(FetchPartitionResult {
                        partition: part.partition,
                        error_code: ERR_OFFSET_OUT_OF_RANGE,
                        high_watermark: end,
                        records: Vec::new(),
                        aborted: Vec::new(),
                    });
                    continue;
                }
                if self.leader_of(part.partition) != self.node_id {
                    if let Ok(mut state) = self.state.lock() {
                        state.leader_mismatches += 1;
                    }
                    partitions.push(FetchPartitionResult {
                        partition: part.partition,
                        error_code: ERR_NOT_LEADER_OR_FOLLOWER,
                        high_watermark: end,
                        records: Vec::new(),
                        aborted: Vec::new(),
                    });
                    continue;
                }
                let part_budget = usize::try_from(part.partition_max_bytes.max(0)).unwrap_or(0);
                let mut records = Vec::new();
                let mut aborted = Vec::new();
                let mut part_records = 0u64;
                if self.synth.records_per_batch > 0 {
                    let mut batch_index =
                        (part.fetch_offset as u64) / u64::from(self.synth.records_per_batch);
                    while let Some(batch) =
                        synth::encode_batch(&self.synth, part.partition, batch_index)
                    {
                        let fits_part = records.len() + batch.bytes.len() <= part_budget;
                        let fits_total = total_used + batch.bytes.len() <= total_budget;
                        if !records.is_empty() && (!fits_part || !fits_total) {
                            break;
                        }
                        if batch.aborted {
                            aborted.push((batch.producer_id, batch.base_offset as i64));
                        }
                        part_records += u64::from(batch.count);
                        total_used += batch.bytes.len();
                        records.extend_from_slice(&batch.bytes);
                        batch_index += 1;
                    }
                }
                served_records += part_records;
                served_bytes += records.len() as u64;
                partitions.push(FetchPartitionResult {
                    partition: part.partition,
                    error_code: ERR_NONE,
                    high_watermark: end,
                    records,
                    aborted,
                });
            }
            out.push((topic.topic_id, partitions));
        }
        if let Ok(mut state) = self.state.lock() {
            state.fetched_records += served_records;
            state.fetched_wire_bytes += served_bytes;
        }
        out
    }

    /// Serve one `ListOffsets` v10 request against the synthetic log.
    ///
    /// Timestamp `-2` is earliest (`0`), `-1` is latest (log end); explicit
    /// millis map through the deterministic batch timestamps.
    fn serve_list_offsets(
        &self,
        topics: &[ListOffsetsTopicData],
    ) -> Vec<(String, i32, i16, i64, i64)> {
        let end = self.synth.records_per_partition as i64;
        let mut rows = Vec::new();
        for topic in topics {
            for part in &topic.partitions {
                if topic.name.is_empty() {
                    rows.push((
                        topic.name.clone(),
                        part.partition,
                        ERR_UNKNOWN_TOPIC_OR_PARTITION,
                        -1,
                        -1,
                    ));
                    continue;
                }
                let offset = match part.timestamp {
                    -2 => 0,
                    -1 => end,
                    ts => (ts - 1_700_000_000_000).clamp(0, end),
                };
                // Timestamp of the returned offset; -1 at log end (no batch).
                let ts = if offset >= end {
                    -1
                } else {
                    1_700_000_000_000 + offset
                };
                rows.push((topic.name.clone(), part.partition, ERR_NONE, ts, offset));
            }
        }
        rows
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, State>, ()> {
        self.state.lock().map_err(|_| ())
    }
}

/// Escape a string for the JSON artifact.
fn json_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Current Unix time in seconds (0 when the clock is unavailable).
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Write the `client-ceiling` result artifact atomically (tmp + rename).
pub fn write_artifact(path: &Path, report: &RunReport) -> io::Result<()> {
    let mut body = String::new();
    body.push_str("{\n");
    body.push_str("  \"label\": \"client-ceiling\",\n");
    body.push_str(&format!("  \"unix_time\": {},\n", unix_now()));
    body.push_str(&format!(
        "  \"accepted_records\": {},\n",
        report.accepted_records
    ));
    body.push_str(&format!(
        "  \"accepted_wire_bytes\": {},\n",
        report.accepted_wire_bytes
    ));
    body.push_str(&format!(
        "  \"produce_requests\": {},\n",
        report.produce_requests
    ));
    body.push_str(&format!(
        "  \"fetch_requests\": {},\n",
        report.fetch_requests
    ));
    body.push_str(&format!(
        "  \"fetched_records\": {},\n",
        report.fetched_records
    ));
    body.push_str(&format!(
        "  \"fetched_wire_bytes\": {},\n",
        report.fetched_wire_bytes
    ));
    body.push_str(&format!(
        "  \"injected_errors\": {},\n",
        report.injected_errors
    ));
    body.push_str(&format!(
        "  \"injected_requests\": {},\n",
        report.injected_requests
    ));
    body.push_str(&format!(
        "  \"metadata_requests\": {},\n",
        report.metadata_requests
    ));
    body.push_str(&format!(
        "  \"leader_mismatches\": {},\n",
        report.leader_mismatches
    ));
    body.push_str("  \"api_versions\": [");
    for (i, (key, version, count)) in report.api_versions.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&format!(
            "{{\"api_key\":{key},\"version\":{version},\"requests\":{count}}}"
        ));
    }
    body.push_str("],\n");
    body.push_str("  \"modes\": {\n");
    body.push_str(&format!("    \"nodes\": {},\n", report.modes.nodes));
    body.push_str(&format!(
        "    \"dead_nodes\": {},\n",
        report.modes.dead_nodes
    ));
    match report.modes.slow_node {
        Some(id) => body.push_str(&format!("    \"slow_node\": {id},\n")),
        None => body.push_str("    \"slow_node\": null,\n"),
    }
    body.push_str(&format!(
        "    \"slow_delay_ms\": {},\n",
        report.modes.slow_delay.as_millis()
    ));
    body.push_str(&format!(
        "    \"fault_seed\": {},\n",
        report.modes.fault_seed
    ));
    body.push_str(&format!(
        "    \"fault_rate_per_million\": {}\n",
        report.modes.fault_rate_per_million
    ));
    body.push_str("  },\n");
    body.push_str("  \"validation_failures\": {\n");
    body.push_str(&format!("    \"framing\": {},\n", report.failures.framing));
    body.push_str(&format!("    \"crc\": {},\n", report.failures.crc));
    body.push_str(&format!("    \"count\": {},\n", report.failures.count));
    body.push_str(&format!(
        "    \"sequence\": {},\n",
        report.failures.sequence
    ));
    body.push_str(&format!(
        "    \"transactional\": {}\n",
        report.failures.transactional
    ));
    body.push_str("  },\n");
    body.push_str("  \"end_offsets\": {\n");
    for (i, (topic, partition, offset)) in report.end_offsets.iter().enumerate() {
        let comma = if i + 1 == report.end_offsets.len() {
            ""
        } else {
            ","
        };
        body.push_str(&format!(
            "    \"{}/{}\": {}{}\n",
            json_escape(topic),
            partition,
            offset,
            comma
        ));
    }
    body.push_str("  }\n");
    body.push_str("}\n");
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn put_zigzag32(out: &mut Vec<u8>, value: i32) {
        let mut v = ((value << 1) ^ (value >> 31)) as u32;
        loop {
            let mut byte = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if v == 0 {
                return;
            }
        }
    }

    fn put_zigzag64(out: &mut Vec<u8>, value: i64) {
        let mut v = ((value << 1) ^ (value >> 63)) as u64;
        loop {
            let mut byte = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if v == 0 {
                return;
            }
        }
    }

    struct Rec {
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    }

    fn encode_record(out: &mut Vec<u8>, index: i32, rec: &Rec) {
        let mut body = Vec::new();
        body.push(0);
        put_zigzag64(&mut body, 0);
        put_zigzag32(&mut body, index);
        match &rec.key {
            None => put_zigzag32(&mut body, -1),
            Some(k) => {
                put_zigzag32(&mut body, k.len() as i32);
                body.extend_from_slice(k);
            }
        }
        match &rec.value {
            None => put_zigzag32(&mut body, -1),
            Some(v) => {
                put_zigzag32(&mut body, v.len() as i32);
                body.extend_from_slice(v);
            }
        }
        put_zigzag32(&mut body, rec.headers.len() as i32);
        for (k, v) in &rec.headers {
            put_zigzag32(&mut body, k.len() as i32);
            body.extend_from_slice(k);
            match v {
                None => put_zigzag32(&mut body, -1),
                Some(v) => {
                    put_zigzag32(&mut body, v.len() as i32);
                    body.extend_from_slice(v);
                }
            }
        }
        put_zigzag32(out, body.len() as i32);
        out.extend_from_slice(&body);
    }

    #[allow(clippy::too_many_arguments)]
    fn build_batch(
        base_offset: i64,
        attributes: i16,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        recs: &[Rec],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&base_offset.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes()); // batch_len placeholder
        out.extend_from_slice(&(-1i32).to_be_bytes()); // leader epoch
        out.push(2); // magic
        out.extend_from_slice(&0u32.to_be_bytes()); // crc placeholder
        out.extend_from_slice(&attributes.to_be_bytes());
        out.extend_from_slice(&(recs.len() as i32 - 1).to_be_bytes()); // last_offset_delta
        out.extend_from_slice(&1_700_000_000_000i64.to_be_bytes());
        out.extend_from_slice(&1_700_000_000_000i64.to_be_bytes());
        out.extend_from_slice(&producer_id.to_be_bytes());
        out.extend_from_slice(&producer_epoch.to_be_bytes());
        out.extend_from_slice(&base_sequence.to_be_bytes());
        out.extend_from_slice(&(recs.len() as i32).to_be_bytes());
        for (i, rec) in recs.iter().enumerate() {
            encode_record(&mut out, i as i32, rec);
        }
        let total = out.len();
        let batch_len = (total - 12) as i32;
        out[8..12].copy_from_slice(&batch_len.to_be_bytes());
        let crc = crc32c(&out[CRC_START..]);
        out[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
        out
    }

    /// Recompute the CRC after patching non-CRC fields for tamper tests.
    fn restamp_crc(batch: &mut [u8]) {
        let crc = crc32c(&batch[CRC_START..]);
        batch[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
    }

    fn sample_recs() -> Vec<Rec> {
        vec![
            Rec {
                key: None,
                value: Some(vec![b'x'; 100]),
                headers: Vec::new(),
            },
            Rec {
                key: Some(b"0123456789abcdef".to_vec()),
                value: Some(vec![b'y'; 10]),
                headers: vec![(b"h".to_vec(), Some(b"v".to_vec()))],
            },
            Rec {
                key: None,
                value: None,
                headers: vec![(b"n".to_vec(), None)],
            },
        ]
    }

    #[test]
    fn crc32c_known_vector() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn parse_valid_batch() {
        let batch = build_batch(-1, 0, -1, -1, 0, &sample_recs());
        let parsed = parse_batch(&batch).unwrap();
        assert_eq!(
            parsed,
            ParsedBatch {
                records: 3,
                producer_id: -1,
                base_sequence: 0,
            }
        );
    }

    #[test]
    fn parse_empty_batch_is_zero_records() {
        let parsed = parse_batch(&[]).unwrap();
        assert_eq!(parsed.records, 0);
    }

    #[test]
    fn parse_rejects_bad_crc() {
        let mut batch = build_batch(-1, 0, -1, -1, 0, &sample_recs());
        let last = batch.len() - 1;
        batch[last] ^= 0xff;
        assert_eq!(parse_batch(&batch), Err(ValidationFailure::Crc));
    }

    #[test]
    fn parse_rejects_truncation() {
        let batch = build_batch(-1, 0, -1, -1, 0, &sample_recs());
        assert_eq!(
            parse_batch(&batch[..batch.len() - 1]),
            Err(ValidationFailure::Framing)
        );
        assert_eq!(parse_batch(&batch[..10]), Err(ValidationFailure::Framing));
    }

    #[test]
    fn parse_rejects_trailing_garbage() {
        let mut batch = build_batch(-1, 0, -1, -1, 0, &sample_recs());
        batch.push(0);
        assert_eq!(parse_batch(&batch), Err(ValidationFailure::Framing));
    }

    #[test]
    fn parse_rejects_bad_magic() {
        let mut batch = build_batch(-1, 0, -1, -1, 0, &sample_recs());
        batch[16] = 1;
        restamp_crc(&mut batch);
        assert_eq!(parse_batch(&batch), Err(ValidationFailure::Framing));
    }

    #[test]
    fn parse_rejects_count_mismatch() {
        let mut batch = build_batch(-1, 0, -1, -1, 0, &sample_recs());
        // numRecords lives at bytes 57..61.
        batch[57..61].copy_from_slice(&9i32.to_be_bytes());
        restamp_crc(&mut batch);
        assert_eq!(parse_batch(&batch), Err(ValidationFailure::Count));
    }

    #[test]
    fn parse_rejects_control_batch() {
        let batch = build_batch(-1, 0x20, -1, -1, 0, &sample_recs());
        assert_eq!(parse_batch(&batch), Err(ValidationFailure::Framing));
    }

    #[test]
    fn parse_rejects_undeclared_record_bytes() {
        // Valid CRC and headers, but the record walk overruns: flip the
        // first record's length prefix, then restamp the CRC.
        let mut batch = build_batch(-1, 0, -1, -1, 0, &sample_recs());
        batch[BATCH_HEADER_LEN] = batch[BATCH_HEADER_LEN].wrapping_add(1);
        restamp_crc(&mut batch);
        assert_eq!(parse_batch(&batch), Err(ValidationFailure::Framing));
    }

    #[test]
    fn parse_accepts_compressed_header_without_walk() {
        // Codec bit set: header count is trusted, records are opaque.
        let batch = build_batch(-1, 2, -1, -1, 0, &sample_recs());
        let parsed = parse_batch(&batch).unwrap();
        assert_eq!(parsed.records, 3);
    }

    #[test]
    fn uvarint_roundtrip_and_overflow() {
        for value in [0u32, 1, 127, 128, 300, u32::MAX] {
            let mut out = Vec::new();
            out.put_uvarint(value);
            let mut cur = Cursor::new(&out);
            assert_eq!(cur.get_uvarint().unwrap(), value);
            assert!(cur.is_empty());
        }
        let mut cur = Cursor::new(&[0xff, 0xff, 0xff, 0xff, 0x7f]);
        assert!(cur.get_uvarint().is_err());
        let mut cur = Cursor::new(&[0x80]);
        assert!(cur.get_uvarint().is_err());
    }

    #[test]
    fn zigzag_varint_roundtrip() {
        // Encode with the test zig-zag writer, decode with the cursor.
        for value in [0i32, 1, -1, 63, -64, 8192, -8193, i32::MIN, i32::MAX] {
            let mut out = Vec::new();
            put_zigzag32(&mut out, value);
            let mut cur = Cursor::new(&out);
            assert_eq!(cur.get_varint().unwrap(), value);
        }
        for value in [0i64, -1, 1_700_000_000_000, i64::MIN, i64::MAX] {
            let mut out = Vec::new();
            put_zigzag64(&mut out, value);
            let mut cur = Cursor::new(&out);
            assert_eq!(cur.get_varlong().unwrap(), value);
        }
    }

    #[test]
    fn compact_string_and_bytes() {
        let mut out = Vec::new();
        out.put_compact_string(None);
        out.put_compact_string(Some("topic"));
        out.put_compact_bytes(None);
        out.put_compact_bytes(Some(b"data"));
        let mut cur = Cursor::new(&out);
        assert_eq!(cur.get_compact_string().unwrap(), None);
        assert_eq!(cur.get_compact_string().unwrap().as_deref(), Some("topic"));
        assert_eq!(cur.get_compact_bytes().unwrap(), None);
        assert_eq!(cur.get_compact_bytes().unwrap(), Some(b"data".as_slice()));
        assert!(cur.is_empty());
    }

    #[test]
    fn request_header_flexible_and_classic() {
        // Flexible header: key, version, correlation, CLASSIC client id, tags.
        let mut flex = Vec::new();
        flex.extend_from_slice(&API_KEY_PRODUCE.to_be_bytes());
        flex.extend_from_slice(&12i16.to_be_bytes());
        flex.extend_from_slice(&7i32.to_be_bytes());
        flex.extend_from_slice(&6i16.to_be_bytes());
        flex.extend_from_slice(b"client");
        flex.put_empty_tagged_fields();
        flex.push(0xaa);
        let (key, version, corr, len) = decode_request_header(&flex).unwrap();
        assert_eq!((key, version, corr), (API_KEY_PRODUCE, 12, 7));
        assert_eq!(&flex[len..], &[0xaa]);

        // Classic ApiVersions v0 header: i16 client id, no tags.
        let mut classic = Vec::new();
        classic.extend_from_slice(&API_KEY_API_VERSIONS.to_be_bytes());
        classic.extend_from_slice(&0i16.to_be_bytes());
        classic.extend_from_slice(&9i32.to_be_bytes());
        classic.extend_from_slice(&(-1i16).to_be_bytes());
        classic.push(0xbb);
        let (key, version, corr, len) = decode_request_header(&classic).unwrap();
        assert_eq!((key, version, corr), (API_KEY_API_VERSIONS, 0, 9));
        assert_eq!(&classic[len..], &[0xbb]);

        assert!(decode_request_header(&[0, 99, 0, 0, 0, 0, 0, 1]).is_err());
    }
}
