//! ApiVersions, Metadata, and Produce codecs.

use std::fmt;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::api_keys::{pick_version, API_VERSIONS};
use super::buf;
use super::records::{self, RecordBatch};
use crate::error::{Error, Result};
use crate::net::BrokerConn;

/// One key in an ApiVersions response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersion {
    /// Kafka api key.
    pub api_key: i16,
    /// Lowest version the broker speaks.
    pub min_version: i16,
    /// Highest version the broker speaks.
    pub max_version: i16,
}

/// One broker-supported feature in ApiVersions v3+ tagged field 0 (KIP-482).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportedFeatureKey {
    /// Feature name (for example `metadata.version`).
    pub name: String,
    /// Lowest version the broker supports.
    pub min_version: i16,
    /// Highest version the broker supports.
    pub max_version: i16,
}

/// One finalized feature in ApiVersions v3+ tagged field 2 (KIP-482).
///
/// Wire order is `max_version_level` then `min_version_level`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedFeatureKey {
    /// Feature name (for example `metadata.version`).
    pub name: String,
    /// Highest finalized version.
    pub max_version_level: i16,
    /// Lowest finalized version.
    pub min_version_level: i16,
}

/// ApiVersions response body.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersionsResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Supported api keys.
    pub api_keys: Vec<ApiVersion>,
    /// Throttle time (v1+).
    pub throttle_time_ms: i32,
    /// Supported features (v3+ tagged field 0). Empty when omitted.
    pub supported_features: Vec<SupportedFeatureKey>,
    /// Finalized-features epoch (v3+ tagged field 1). `None` when omitted or `-1`.
    pub finalized_features_epoch: Option<i64>,
    /// Finalized features (v3+ tagged field 2). Empty when omitted.
    pub finalized_features: Vec<FinalizedFeatureKey>,
    /// ZooKeeper migration ready (v3+ tagged field 3, KIP-866).
    pub zk_migration_ready: bool,
}

impl ApiVersionsResponse {
    /// Java `ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH`.
    pub const UNKNOWN_FINALIZED_FEATURES_EPOCH: i64 = -1;

    /// Java `ApiVersionsResponse.apiVersion` (`None` when the key is absent).
    #[must_use]
    pub fn api_version(&self, api_key: i16) -> Option<&ApiVersion> {
        self.api_keys.iter().find(|k| k.api_key == api_key)
    }

    /// Java `ApiVersionsResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 2
    }

    /// Java `ApiVersionsResponse.zkMigrationReady`.
    #[must_use]
    pub fn zk_migration_ready(&self) -> bool {
        self.zk_migration_ready
    }
}

/// Java `ApiVersionsRequest` helpers.
pub struct ApiVersionsRequest;

impl ApiVersionsRequest {
    /// Java `ApiVersionsRequest.isValid`.
    ///
    /// v3+ requires ClientSoftwareName and ClientSoftwareVersion to start and
    /// end with an ASCII alphanumeric, with interior `-` and `.` allowed.
    /// Empty is invalid. Below v3 this is always true. Encode does not reject
    /// invalid names; Java Builder does not either.
    #[must_use]
    pub fn is_valid(version: i16, software_name: &str, software_version: &str) -> bool {
        version < 3
            || (client_software_name_or_version_ok(software_name)
                && client_software_name_or_version_ok(software_version))
    }
}

/// Java `ApiVersionsRequest` `SOFTWARE_NAME_VERSION_PATTERN`.
fn client_software_name_or_version_ok(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    let Some(last) = chars.next_back() else {
        return true;
    };
    last.is_ascii_alphanumeric() && chars.all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.')
}

/// `true` when ApiVersions `version` is flexible (v3+).
///
/// Official Kafka 4.0 JSON: `validVersions: "0-4"`, `flexibleVersions: "3+"`.
/// v0–v2 are classic (empty request). v3–v4 send ClientSoftwareName /
/// ClientSoftwareVersion. v4 allows SupportedFeatures.MinVersion 0
/// (KAFKA-17011). v5+ is not spoken.
fn api_versions_flexible(version: i16) -> Result<bool> {
    match version {
        0..=2 => Ok(false),
        3..=4 => Ok(true),
        other => Err(Error::protocol(format!(
            "ApiVersions version {other} is not implemented"
        ))),
    }
}

/// Encode ApiVersions v0–v4.
///
/// Kafka 4.0 JSON: `validVersions: "0-4"`, `flexibleVersions: "3+"`.
/// v0–v2 are empty. v3 and v4 request match (ClientSoftwareName /
/// ClientSoftwareVersion). v4 lets the broker return features with
/// MinVersion 0 (KAFKA-17011). This crate speaks 0–4. v5+ is not spoken.
pub fn encode_api_versions_request(
    buf: &mut BytesMut,
    version: i16,
    software_name: &str,
    software_version: &str,
) -> crate::error::Result<()> {
    let flexible = api_versions_flexible(version)?;
    if version >= 3 {
        buf::put_string(buf, flexible, Some(software_name))?;
        buf::put_string(buf, flexible, Some(software_version))?;
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode ApiVersions v0–v4: `(software_name, software_version)`.
/// Empty on v0–v2.
pub fn decode_api_versions_request<B: Buf>(buf: &mut B, version: i16) -> Result<(String, String)> {
    let flexible = api_versions_flexible(version)?;
    if version >= 3 {
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let software_version = buf::get_string(buf, flexible)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        return Ok((name, software_version));
    }
    Ok((String::new(), String::new()))
}

/// Decode ApiVersions v0–v4 (classic v0–2, flexible v3–v4).
pub fn decode_api_versions_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ApiVersionsResponse> {
    let flexible = api_versions_flexible(version)?;
    let error_code = buf::get_i16(buf)?;
    let count = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut api_keys = Vec::with_capacity(count);
    for _ in 0..count {
        let api_key = buf::get_i16(buf)?;
        let min_version = buf::get_i16(buf)?;
        let max_version = buf::get_i16(buf)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        api_keys.push(ApiVersion {
            api_key,
            min_version,
            max_version,
        });
    }
    let throttle_time_ms = if version >= 1 { buf::get_i32(buf)? } else { 0 };
    let tagged = if flexible {
        decode_api_versions_tagged_fields(buf)?
    } else {
        ApiVersionsTaggedFields {
            supported: Vec::new(),
            epoch: None,
            finalized: Vec::new(),
            zk_migration_ready: false,
        }
    };
    Ok(ApiVersionsResponse {
        error_code,
        api_keys,
        throttle_time_ms,
        supported_features: tagged.supported,
        finalized_features_epoch: tagged.epoch,
        finalized_features: tagged.finalized,
        zk_migration_ready: tagged.zk_migration_ready,
    })
}

/// Parse an ApiVersions body, falling back to v0 when the sent version does
/// not leftover-empty (KIP-511: brokers 2.4+ answer an unsupported request
/// with a v0 UNSUPPORTED_VERSION body).
pub fn decode_api_versions_handshake(bytes: &[u8], version: i16) -> Result<ApiVersionsResponse> {
    let mut cur = bytes;
    if let Ok(resp) = decode_api_versions_response(&mut cur, version) {
        if !cur.has_remaining() {
            return Ok(resp);
        }
    }
    if version != 0 {
        let mut cur = bytes;
        if let Ok(resp) = decode_api_versions_response(&mut cur, 0) {
            if !cur.has_remaining() {
                return Ok(resp);
            }
        }
    }
    let mut cur = bytes;
    decode_api_versions_response(&mut cur, version)
}

/// Send ApiVersions at client max (v4) and retry once on
/// `UNSUPPORTED_VERSION` (KIP-511).
///
/// Brokers older than v4 respond with a v0 body listing the supported
/// ApiVersions range. The client then re-issues at
/// `pick_version(broker_min, broker_max, 0, 4)`.
pub async fn negotiate_api_versions(
    conn: &mut BrokerConn,
    request_timeout: Duration,
) -> Result<ApiVersionsResponse> {
    const SENT: i16 = 4;
    let body = conn
        .roundtrip(
            API_VERSIONS,
            SENT,
            |buf| encode_api_versions_request(buf, SENT, "partitionline", "0.1.0"),
            request_timeout,
        )
        .await?;
    let resp = decode_api_versions_handshake(body.as_ref(), SENT)?;
    if resp.error_code == 0 {
        return Ok(resp);
    }
    if resp.error_code != crate::error::UNSUPPORTED_VERSION {
        return Err(Error::broker(resp.error_code, "ApiVersions"));
    }
    let retry = resp
        .api_version(API_VERSIONS)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, SENT))
        .unwrap_or(0);
    if retry == SENT {
        return Err(Error::broker(resp.error_code, "ApiVersions"));
    }
    let body = conn
        .roundtrip(
            API_VERSIONS,
            retry,
            |buf| encode_api_versions_request(buf, retry, "partitionline", "0.1.0"),
            request_timeout,
        )
        .await?;
    let resp = decode_api_versions_handshake(body.as_ref(), retry)?;
    if resp.error_code != 0 {
        return Err(Error::broker(resp.error_code, "ApiVersions"));
    }
    Ok(resp)
}

/// Encode ApiVersions v0–v4 (used by the mock broker).
///
/// v0–v3 omit SupportedFeatures with MinVersion 0 (KAFKA-17011 / KAFKA-17492).
pub fn encode_api_versions_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &ApiVersionsResponse,
) -> crate::error::Result<()> {
    let flexible = api_versions_flexible(version)?;
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, flexible, Some(resp.api_keys.len()))?;
    for api in &resp.api_keys {
        buf.put_i16(api.api_key);
        buf.put_i16(api.min_version);
        buf.put_i16(api.max_version);
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if version >= 1 {
        buf.put_i32(resp.throttle_time_ms);
    }
    if flexible {
        encode_api_versions_tagged_fields(buf, version, resp)?;
    }
    Ok(())
}

struct ApiVersionsTaggedFields {
    supported: Vec<SupportedFeatureKey>,
    epoch: Option<i64>,
    finalized: Vec<FinalizedFeatureKey>,
    zk_migration_ready: bool,
}

fn leftover_empty<B: Buf>(buf: &B, what: &'static str) -> Result<()> {
    if buf.has_remaining() {
        Err(Error::protocol(format!("{what} leftover")))
    } else {
        Ok(())
    }
}

fn encode_supported_feature_keys(features: &[SupportedFeatureKey]) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    buf::put_array_len(&mut buf, true, Some(features.len()))?;
    for f in features {
        buf::put_compact_string(&mut buf, Some(&f.name))?;
        buf.put_i16(f.min_version);
        buf.put_i16(f.max_version);
        buf::put_empty_tagged_fields(&mut buf);
    }
    Ok(buf.freeze())
}

fn decode_supported_feature_keys<B: Buf>(buf: &mut B) -> Result<Vec<SupportedFeatureKey>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let min_version = buf::get_i16(buf)?;
        let max_version = buf::get_i16(buf)?;
        buf::skip_tagged_fields(buf)?;
        out.push(SupportedFeatureKey {
            name,
            min_version,
            max_version,
        });
    }
    Ok(out)
}

fn encode_finalized_feature_keys(features: &[FinalizedFeatureKey]) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    buf::put_array_len(&mut buf, true, Some(features.len()))?;
    for f in features {
        buf::put_compact_string(&mut buf, Some(&f.name))?;
        buf.put_i16(f.max_version_level);
        buf.put_i16(f.min_version_level);
        buf::put_empty_tagged_fields(&mut buf);
    }
    Ok(buf.freeze())
}

fn decode_finalized_feature_keys<B: Buf>(buf: &mut B) -> Result<Vec<FinalizedFeatureKey>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let max_version_level = buf::get_i16(buf)?;
        let min_version_level = buf::get_i16(buf)?;
        buf::skip_tagged_fields(buf)?;
        out.push(FinalizedFeatureKey {
            name,
            max_version_level,
            min_version_level,
        });
    }
    Ok(out)
}

fn encode_api_versions_tagged_fields(
    buf: &mut BytesMut,
    version: i16,
    resp: &ApiVersionsResponse,
) -> Result<()> {
    let mut tags: Vec<(u32, Bytes)> = Vec::new();
    let supported: Vec<SupportedFeatureKey> = resp
        .supported_features
        .iter()
        .filter(|f| version >= 4 || f.min_version != 0)
        .cloned()
        .collect();
    if !supported.is_empty() {
        tags.push((0, encode_supported_feature_keys(&supported)?));
    }
    if let Some(epoch) = resp.finalized_features_epoch {
        if epoch >= 0 {
            let mut b = BytesMut::new();
            b.put_i64(epoch);
            tags.push((1, b.freeze()));
        }
    }
    if !resp.finalized_features.is_empty() {
        tags.push((2, encode_finalized_feature_keys(&resp.finalized_features)?));
    }
    if resp.zk_migration_ready {
        tags.push((3, Bytes::from_static(&[1])));
    }
    buf::put_tagged_fields(buf, &tags)
}

fn decode_api_versions_tagged_fields<B: Buf>(buf: &mut B) -> Result<ApiVersionsTaggedFields> {
    let tags = buf::get_tagged_fields(buf)?;
    let mut supported = Vec::new();
    let mut epoch = None;
    let mut finalized = Vec::new();
    let mut zk = false;
    for (tag, value) in tags {
        match tag {
            0 => {
                let mut cur = value.as_ref();
                supported = decode_supported_feature_keys(&mut cur)?;
                leftover_empty(&cur, "supported_features")?;
            }
            1 => {
                let mut cur = value.as_ref();
                let v = buf::get_i64(&mut cur)?;
                leftover_empty(&cur, "finalized_features_epoch")?;
                epoch = (v != ApiVersionsResponse::UNKNOWN_FINALIZED_FEATURES_EPOCH && v >= 0)
                    .then_some(v);
            }
            2 => {
                let mut cur = value.as_ref();
                finalized = decode_finalized_feature_keys(&mut cur)?;
                leftover_empty(&cur, "finalized_features")?;
            }
            3 => {
                let mut cur = value.as_ref();
                zk = buf::get_bool(&mut cur)?;
                leftover_empty(&cur, "zk_migration_ready")?;
            }
            _ => {}
        }
    }
    Ok(ApiVersionsTaggedFields {
        supported,
        epoch,
        finalized,
        zk_migration_ready: zk,
    })
}

/// Java `Node.toString`: `{host}:{port} (id: {id} rack: {rack|null} isFenced: {bool})`.
pub(crate) fn format_java_node(
    f: &mut fmt::Formatter<'_>,
    host: &str,
    port: i32,
    id: i32,
    rack: Option<&str>,
    is_fenced: bool,
) -> fmt::Result {
    write!(
        f,
        "{}:{} (id: {} rack: {} isFenced: {})",
        host,
        port,
        id,
        rack.unwrap_or("null"),
        is_fenced
    )
}

/// One broker in a Metadata response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broker {
    /// Broker id.
    pub node_id: i32,
    /// Hostname or IP.
    pub host: String,
    /// Port.
    pub port: i32,
    /// Rack id (v1+).
    pub rack: Option<String>,
}

impl Broker {
    /// Construct [`Self`] (Java `Node(id, host, port, rack)`).
    pub fn new(node_id: i32, host: impl Into<String>, port: i32, rack: Option<String>) -> Self {
        Self {
            node_id,
            host: host.into(),
            port,
            rack,
        }
    }

    /// Java `Node.id`.
    #[must_use]
    pub fn id(&self) -> i32 {
        self.node_id
    }

    /// Java `Node.host`.
    #[must_use]
    pub fn host(&self) -> &str {
        self.host.as_str()
    }

    /// Java `Node.port`.
    #[must_use]
    pub fn port(&self) -> i32 {
        self.port
    }

    /// Java `Node.rack`.
    #[must_use]
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Java `Node.hasRack`.
    #[must_use]
    pub fn has_rack(&self) -> bool {
        self.rack.is_some()
    }

    /// Java `Node.isFenced`. Metadata brokers are never fenced.
    #[must_use]
    pub fn is_fenced(&self) -> bool {
        false
    }

    /// Java `Node.idString` (`Integer.toString(id)`).
    #[must_use]
    pub fn id_string(&self) -> String {
        self.node_id.to_string()
    }

    /// Java `Node.isEmpty` (empty host or negative port).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.host.is_empty() || self.port < 0
    }

    /// Java `Node.noNode` (`id` `-1`, empty host, port `-1`).
    #[must_use]
    pub fn no_node() -> Self {
        Self::new(-1, "", -1, None)
    }
}

impl fmt::Display for Broker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_java_node(
            f,
            self.host.as_str(),
            self.port,
            self.node_id,
            self.rack.as_deref(),
            false,
        )
    }
}

/// Produce v10+ / Fetch v16+ `NodeEndpoint` (KIP-951).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEndpoint {
    /// Broker id.
    pub node_id: i32,
    /// Hostname or IP.
    pub host: String,
    /// Port.
    pub port: i32,
    /// Rack id, or `None`.
    pub rack: Option<String>,
}

impl NodeEndpoint {
    /// Construct [`Self`] (Java `Node(id, host, port, rack)`).
    pub fn new(node_id: i32, host: impl Into<String>, port: i32, rack: Option<String>) -> Self {
        Self {
            node_id,
            host: host.into(),
            port,
            rack,
        }
    }

    /// Java `Node.id`.
    #[must_use]
    pub fn id(&self) -> i32 {
        self.node_id
    }

    /// Java `Node.host`.
    #[must_use]
    pub fn host(&self) -> &str {
        self.host.as_str()
    }

    /// Java `Node.port`.
    #[must_use]
    pub fn port(&self) -> i32 {
        self.port
    }

    /// Java `Node.rack`.
    #[must_use]
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Java `Node.hasRack`.
    #[must_use]
    pub fn has_rack(&self) -> bool {
        self.rack.is_some()
    }

    /// Java `Node.isFenced`. NodeEndpoints are never fenced.
    #[must_use]
    pub fn is_fenced(&self) -> bool {
        false
    }

    /// Java `Node.idString` (`Integer.toString(id)`).
    #[must_use]
    pub fn id_string(&self) -> String {
        self.node_id.to_string()
    }

    /// Java `Node.isEmpty` (empty host or negative port).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.host.is_empty() || self.port < 0
    }

    /// Java `Node.noNode` (`id` `-1`, empty host, port `-1`).
    #[must_use]
    pub fn no_node() -> Self {
        Self::new(-1, "", -1, None)
    }
}

impl fmt::Display for NodeEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_java_node(
            f,
            self.host.as_str(),
            self.port,
            self.node_id,
            self.rack.as_deref(),
            false,
        )
    }
}

impl From<Broker> for NodeEndpoint {
    fn from(b: Broker) -> Self {
        Self {
            node_id: b.node_id,
            host: b.host,
            port: b.port,
            rack: b.rack,
        }
    }
}

impl From<NodeEndpoint> for Broker {
    fn from(e: NodeEndpoint) -> Self {
        Self {
            node_id: e.node_id,
            host: e.host,
            port: e.port,
            rack: e.rack,
        }
    }
}

/// Compact array of NodeEndpoint (nested tagged fields). Leftover-empty.
pub(crate) fn encode_node_endpoints(endpoints: &[NodeEndpoint]) -> Result<Bytes> {
    let mut inner = BytesMut::new();
    buf::put_array_len(&mut inner, true, Some(endpoints.len()))?;
    for e in endpoints {
        inner.put_i32(e.node_id);
        buf::put_string(&mut inner, true, Some(e.host.as_str()))?;
        inner.put_i32(e.port);
        buf::put_string(&mut inner, true, e.rack.as_deref())?;
        buf::put_empty_tagged_fields(&mut inner);
    }
    Ok(inner.freeze())
}

/// Decode compact NodeEndpoints. Nested tagged fields must be leftover-empty.
pub(crate) fn decode_node_endpoints(value: &Bytes) -> Result<Vec<NodeEndpoint>> {
    let mut cur = value.as_ref();
    let n = buf::get_array_len(&mut cur, true)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let node_id = buf::get_i32(&mut cur)?;
        let host = buf::get_string(&mut cur, true)?.unwrap_or_default();
        let port = buf::get_i32(&mut cur)?;
        let rack = buf::get_string(&mut cur, true)?;
        buf::skip_tagged_fields(&mut cur)?;
        out.push(NodeEndpoint {
            node_id,
            host,
            port,
            rack,
        });
    }
    leftover_empty(&cur, "NodeEndpoints")?;
    Ok(out)
}

pub(crate) fn encode_top_level_node_endpoints(
    buf: &mut BytesMut,
    include: bool,
    endpoints: &[NodeEndpoint],
) -> Result<()> {
    if include && !endpoints.is_empty() {
        buf::put_tagged_fields(buf, &[(0, encode_node_endpoints(endpoints)?)])
    } else {
        buf::put_empty_tagged_fields(buf);
        Ok(())
    }
}

pub(crate) fn decode_top_level_node_endpoints<B: Buf>(
    buf: &mut B,
    include: bool,
) -> Result<Vec<NodeEndpoint>> {
    let tags = buf::get_tagged_fields(buf)?;
    let mut endpoints = Vec::new();
    for (tag, value) in tags {
        if include && tag == 0 {
            endpoints = decode_node_endpoints(&value)?;
        }
    }
    Ok(endpoints)
}

/// One partition in a Metadata response.
///
/// [`Self::without_leader_epoch`] is Java
/// `MetadataResponse.PartitionMetadata.withoutLeaderEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Partition index.
    pub partition_index: i32,
    /// Leader broker id, or [`MetadataResponse::NO_LEADER_ID`].
    pub leader_id: i32,
    /// Leader epoch (v7+), or [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub leader_epoch: i32,
    /// Replica broker ids.
    pub replica_nodes: Vec<i32>,
    /// In-sync replica broker ids.
    pub isr_nodes: Vec<i32>,
    /// Offline replica broker ids (v5+). Java `offlineReplicas`.
    pub offline_replicas: Vec<i32>,
}

impl PartitionMetadata {
    /// Java `MetadataResponse.PartitionMetadata.withoutLeaderEpoch`.
    ///
    /// Sets [`Self::leader_epoch`] to [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]
    /// (Java `Optional.empty`).
    #[must_use]
    pub fn without_leader_epoch(&self) -> Self {
        Self {
            leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            ..self.clone()
        }
    }
}

/// One topic in a Metadata response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Topic name.
    pub name: Option<String>,
    /// Topic id (v10+), or zeros.
    pub topic_id: [u8; 16],
    /// Internal topic (v1+).
    pub is_internal: bool,
    /// Partitions.
    pub partitions: Vec<PartitionMetadata>,
    /// Topic authorized operations (v8+).
    /// [`MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED`] when omitted.
    pub topic_authorized_operations: i32,
}

/// Metadata response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponse {
    /// Throttle time (v3+).
    pub throttle_time_ms: i32,
    /// Brokers in the cluster.
    pub brokers: Vec<Broker>,
    /// Cluster id (v2+).
    pub cluster_id: Option<String>,
    /// Controller broker id (v1+), or [`Self::NO_CONTROLLER_ID`].
    pub controller_id: i32,
    /// Topics.
    pub topics: Vec<TopicMetadata>,
    /// Top-level error (v13+). `0` on v1–v12.
    pub error_code: i16,
}

impl MetadataResponse {
    /// Java `MetadataResponse.NO_CONTROLLER_ID`.
    pub const NO_CONTROLLER_ID: i32 = -1;
    /// Java `MetadataResponse.NO_LEADER_ID`.
    pub const NO_LEADER_ID: i32 = -1;
    /// Java `MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED`.
    pub const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

    /// Java `MetadataResponse.hasReliableLeaderEpochs(short)` (private static).
    /// Public instance `hasReliableLeaderEpochs()` is this check at parse time.
    ///
    /// Prior to Metadata v9 (Kafka 2.4), brokers do not propagate leader epoch
    /// accurately while a reassignment is in progress. Clients must not retain
    /// those epochs for Fetch, ListOffsets, or OffsetsForLeaderEpoch.
    #[must_use]
    pub const fn has_reliable_leader_epochs(version: i16) -> bool {
        version >= 9
    }

    /// Java `MetadataResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 6
    }

    /// Fail when the v13+ top-level ErrorCode is non-zero.
    pub(crate) fn check(&self) -> Result<()> {
        if self.error_code == 0 {
            Ok(())
        } else {
            Err(Error::broker(self.error_code, "Metadata"))
        }
    }
}

/// One topic in a Metadata request (v10+ TopicId + nullable Name).
///
/// Java `describeTopics(Collection<String>)` sends [`Self::by_name`]
/// (Name set, TopicId zero). Java `describeTopics(TopicCollection.ofTopicIds)`
/// sends [`Self::by_id`] (Name null, TopicId set).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequestTopic {
    /// Topic name, or `None` when describing by TopicId.
    pub name: Option<String>,
    /// Topic UUID (v10+). Zero when describing by name.
    pub topic_id: [u8; 16],
}

impl MetadataRequestTopic {
    /// Name-based Metadata topic (TopicId zero).
    #[must_use]
    pub fn by_name(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            topic_id: [0; 16],
        }
    }

    /// Id-based Metadata topic (Name null). Requires Metadata v12+
    /// (Java `MetadataRequest.Builder`; v10 and v11 must not send TopicId
    /// or a null Name).
    #[must_use]
    pub fn by_id(topic_id: [u8; 16]) -> Self {
        Self {
            name: None,
            topic_id,
        }
    }
}

/// Encode Metadata. `topics = None` asks for all topics.
///
/// `IncludeTopicAuthorizedOperations` is false (Java default). Use
/// [`encode_metadata_request_with`] for `DescribeTopicsOptions`.
pub fn encode_metadata_request(
    buf: &mut BytesMut,
    version: i16,
    topics: Option<&[String]>,
    allow_auto: bool,
) -> crate::error::Result<()> {
    encode_metadata_request_with(buf, version, topics, allow_auto, false)
}

/// Encode Metadata with `IncludeTopicAuthorizedOperations` (v8+).
///
/// Below v8 the flag is omitted. Cluster authorized operations stay
/// unset (`false` on v8–v10). Name-based: v10+ sends TopicId zero.
/// For TopicId describes, use [`encode_metadata_request_topics`].
pub fn encode_metadata_request_with(
    buf: &mut BytesMut,
    version: i16,
    topics: Option<&[String]>,
    allow_auto: bool,
    include_topic_authorized_operations: bool,
) -> crate::error::Result<()> {
    let owned = topics.map(|names| {
        names
            .iter()
            .map(|name| MetadataRequestTopic::by_name(name.clone()))
            .collect::<Vec<_>>()
    });
    encode_metadata_request_topics(
        buf,
        version,
        owned.as_deref(),
        allow_auto,
        include_topic_authorized_operations,
    )
}

/// Encode Metadata Topics of Name and/or TopicId (v10+).
///
/// Java `describeTopics(TopicCollection.ofTopicIds)` sends Name null
/// and TopicId set. `topics = None` asks for all topics.
///
/// Java `MetadataRequest.Builder.build` rejects versions older than 1,
/// `allowAutoTopicCreation` false below v4, and a null Name or non-zero
/// TopicId below v12.
pub fn encode_metadata_request_topics(
    buf: &mut BytesMut,
    version: i16,
    topics: Option<&[MetadataRequestTopic]>,
    allow_auto: bool,
    include_topic_authorized_operations: bool,
) -> crate::error::Result<()> {
    if version < 1 {
        return Err(Error::Unsupported(
            "MetadataRequest versions older than 1 are not supported.".into(),
        ));
    }
    if !allow_auto && version < 4 {
        return Err(Error::Unsupported(
            "MetadataRequest versions older than 4 don't support the allowAutoTopicCreation field"
                .into(),
        ));
    }
    if let Some(topics) = topics {
        for t in topics {
            if t.name.is_none() && version < 12 {
                return Err(Error::Unsupported(format!(
                    "MetadataRequest version {version} does not support null topic names."
                )));
            }
            if t.topic_id != [0; 16] && version < 12 {
                return Err(Error::Unsupported(format!(
                    "MetadataRequest version {version} does not support non-zero topic IDs."
                )));
            }
        }
    }
    let flexible = version >= 9;
    match topics {
        None => buf::put_array_len(buf, flexible, None)?,
        Some(topics) => {
            buf::put_array_len(buf, flexible, Some(topics.len()))?;
            for t in topics {
                if version >= 10 {
                    buf.extend_from_slice(&t.topic_id);
                }
                buf::put_string(buf, flexible, t.name.as_deref())?;
                if flexible {
                    buf::put_empty_tagged_fields(buf);
                }
            }
        }
    }
    if version >= 4 {
        buf.put_u8(u8::from(allow_auto));
    }
    if (8..=10).contains(&version) {
        buf.put_u8(0);
    }
    if version >= 8 {
        buf.put_u8(u8::from(include_topic_authorized_operations));
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode Metadata request: topic names (`None` is all topics),
/// `allow.auto.create.topics`, and `IncludeTopicAuthorizedOperations`.
///
/// v10+ Topics entries with a null Name are skipped (id-based describes).
/// See [`decode_metadata_request_topics`] for every Topics entry.
pub fn decode_metadata_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Option<Vec<String>>, bool, bool)> {
    let (topics, allow_auto, include_topic_authorized) =
        decode_metadata_request_topics(buf, version)?;
    let names = topics.map(|ts| ts.into_iter().filter_map(|t| t.name).collect());
    Ok((names, allow_auto, include_topic_authorized))
}

/// Decode Metadata: every topic (Name and/or TopicId) plus flags.
pub fn decode_metadata_request_topics<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Option<Vec<MetadataRequestTopic>>, bool, bool)> {
    let flexible = version >= 9;
    let topics = match buf::get_array_len(buf, flexible)? {
        None => None,
        Some(n) => {
            let mut topics = Vec::with_capacity(n);
            for _ in 0..n {
                let topic_id = if version >= 10 {
                    buf::get_uuid(buf)?
                } else {
                    [0; 16]
                };
                let name = buf::get_string(buf, flexible)?;
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                topics.push(MetadataRequestTopic { name, topic_id });
            }
            Some(topics)
        }
    };
    let allow_auto = if version >= 4 {
        buf::need(buf, 1)?;
        buf.get_u8() != 0
    } else {
        false
    };
    if (8..=10).contains(&version) {
        buf::need(buf, 1)?;
        let _include_cluster = buf.get_u8();
    }
    let include_topic_authorized = if version >= 8 {
        buf::need(buf, 1)?;
        buf.get_u8() != 0
    } else {
        false
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((topics, allow_auto, include_topic_authorized))
}

fn get_int32_array<B: Buf>(buf: &mut B, flexible: bool) -> Result<Vec<i32>> {
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(buf::get_i32(buf)?);
    }
    Ok(out)
}

fn put_int32_array(buf: &mut BytesMut, flexible: bool, items: &[i32]) -> crate::error::Result<()> {
    buf::put_array_len(buf, flexible, Some(items.len()))?;
    for v in items {
        buf.put_i32(*v);
    }
    Ok(())
}

/// Decode Metadata.
pub fn decode_metadata_response<B: Buf>(buf: &mut B, version: i16) -> Result<MetadataResponse> {
    let flexible = version >= 9;
    let throttle_time_ms = if version >= 3 { buf::get_i32(buf)? } else { 0 };
    let broker_count = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut brokers = Vec::with_capacity(broker_count);
    for _ in 0..broker_count {
        let node_id = buf::get_i32(buf)?;
        let host =
            buf::get_string(buf, flexible)?.ok_or_else(|| Error::protocol("null broker host"))?;
        let port = buf::get_i32(buf)?;
        let rack = if version >= 1 {
            buf::get_string(buf, flexible)?
        } else {
            None
        };
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        brokers.push(Broker {
            node_id,
            host,
            port,
            rack,
        });
    }
    let cluster_id = if version >= 2 {
        buf::get_string(buf, flexible)?
    } else {
        None
    };
    let controller_id = if version >= 1 {
        buf::get_i32(buf)?
    } else {
        MetadataResponse::NO_CONTROLLER_ID
    };
    let topic_count = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let error_code = buf::get_i16(buf)?;
        let name = buf::get_string(buf, flexible)?;
        let topic_id = if version >= 10 {
            buf::get_uuid(buf)?
        } else {
            [0u8; 16]
        };
        let is_internal = if version >= 1 {
            buf::get_bool(buf)?
        } else {
            false
        };
        let part_count = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let error_code = buf::get_i16(buf)?;
            let partition_index = buf::get_i32(buf)?;
            let leader_id = buf::get_i32(buf)?;
            let leader_epoch = if version >= 7 {
                buf::get_i32(buf)?
            } else {
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            };
            let replica_nodes = get_int32_array(buf, flexible)?;
            let isr_nodes = get_int32_array(buf, flexible)?;
            let offline_replicas = if version >= 5 {
                get_int32_array(buf, flexible)?
            } else {
                Vec::new()
            };
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(PartitionMetadata {
                error_code,
                partition_index,
                leader_id,
                leader_epoch,
                replica_nodes,
                isr_nodes,
                offline_replicas,
            });
        }
        let topic_authorized_operations = if version >= 8 {
            buf::get_i32(buf)?
        } else {
            MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED
        };
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(TopicMetadata {
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
        });
    }
    if (8..=10).contains(&version) {
        let _cluster_authorized = buf::get_i32(buf)?;
    }
    let error_code = if version >= 13 { buf::get_i16(buf)? } else { 0 };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(MetadataResponse {
        throttle_time_ms,
        brokers,
        cluster_id,
        controller_id,
        topics,
        error_code,
    })
}

/// Encode Metadata (used by the mock broker).
pub fn encode_metadata_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &MetadataResponse,
) -> crate::error::Result<()> {
    let flexible = version >= 9;
    if version >= 3 {
        buf.put_i32(resp.throttle_time_ms);
    }
    buf::put_array_len(buf, flexible, Some(resp.brokers.len()))?;
    for b in &resp.brokers {
        buf.put_i32(b.node_id);
        buf::put_string(buf, flexible, Some(&b.host))?;
        buf.put_i32(b.port);
        if version >= 1 {
            buf::put_string(buf, flexible, b.rack.as_deref())?;
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if version >= 2 {
        buf::put_string(buf, flexible, resp.cluster_id.as_deref())?;
    }
    if version >= 1 {
        buf.put_i32(resp.controller_id);
    }
    buf::put_array_len(buf, flexible, Some(resp.topics.len()))?;
    for t in &resp.topics {
        buf.put_i16(t.error_code);
        buf::put_string(buf, flexible, t.name.as_deref())?;
        if version >= 10 {
            buf.extend_from_slice(&t.topic_id);
        }
        if version >= 1 {
            buf.put_u8(u8::from(t.is_internal));
        }
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i16(p.error_code);
            buf.put_i32(p.partition_index);
            buf.put_i32(p.leader_id);
            if version >= 7 {
                buf.put_i32(p.leader_epoch);
            }
            put_int32_array(buf, flexible, &p.replica_nodes)?;
            put_int32_array(buf, flexible, &p.isr_nodes)?;
            if version >= 5 {
                put_int32_array(buf, flexible, &p.offline_replicas)?;
            }
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if version >= 8 {
            buf.put_i32(t.topic_authorized_operations);
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if (8..=10).contains(&version) {
        buf.put_i32(MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);
    }
    if version >= 13 {
        buf.put_i16(resp.error_code);
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// One topic in a Produce request.
#[derive(Debug, Clone)]
pub struct ProduceTopicData {
    /// Topic name.
    pub topic: String,
    /// Partition batches.
    pub partitions: Vec<ProducePartitionData>,
}

/// One partition in a Produce request.
#[derive(Debug, Clone)]
pub struct ProducePartitionData {
    /// Partition index.
    pub index: i32,
    /// Record batch to produce.
    pub records: RecordBatch,
}

/// One partition in a Produce response.
///
/// [`Self::INVALID_OFFSET`] is Java `ProduceResponse.INVALID_OFFSET`.
/// [`Self::partition_response`] is Java `ProduceResponse.PartitionResponse(Errors)`
/// (`baseOffset` / `logStartOffset` [`Self::INVALID_OFFSET`], `logAppendTime`
/// [`RecordBatch::NO_TIMESTAMP`]). Decode below v2 fills
/// [`RecordBatch::NO_TIMESTAMP`]; decode below v5 fills
/// [`Self::INVALID_OFFSET`]. Omitted v10+ CurrentLeader fills
/// [`MetadataResponse::NO_LEADER_ID`] /
/// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`] (JSON defaults).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionResponse {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// First offset assigned to the batch, or [`Self::INVALID_OFFSET`].
    pub base_offset: i64,
    /// Log append time, or [`RecordBatch::NO_TIMESTAMP`].
    pub log_append_time_ms: i64,
    /// Log start offset, or [`Self::INVALID_OFFSET`] when omitted below v5.
    pub log_start_offset: i64,
    /// Produce v10+ CurrentLeader `LeaderId`, or
    /// [`MetadataResponse::NO_LEADER_ID`] when omitted (JSON default).
    pub current_leader_id: i32,
    /// Produce v10+ CurrentLeader `LeaderEpoch`, or
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`] when omitted (JSON default).
    pub current_leader_epoch: i32,
}

impl ProducePartitionResponse {
    /// Java `ProduceResponse.INVALID_OFFSET`.
    pub const INVALID_OFFSET: i64 = -1;

    /// Java `ProduceResponse.PartitionResponse(Errors)`.
    ///
    /// Sets [`Self::INVALID_OFFSET`] for `baseOffset` / `logStartOffset` and
    /// [`RecordBatch::NO_TIMESTAMP`] for `logAppendTime`. CurrentLeader is
    /// the Apache JSON default ([`MetadataResponse::NO_LEADER_ID`] /
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]). Java `PartitionResponse`
    /// has no topic name; this type does, so callers pass it.
    #[must_use]
    pub fn partition_response(topic: impl Into<String>, partition: i32, error_code: i16) -> Self {
        Self {
            topic: topic.into(),
            partition,
            error_code,
            base_offset: Self::INVALID_OFFSET,
            log_append_time_ms: RecordBatch::NO_TIMESTAMP,
            log_start_offset: Self::INVALID_OFFSET,
            current_leader_id: MetadataResponse::NO_LEADER_ID,
            current_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
        }
    }
}

/// Java `ProduceRequest` version helpers (KIP-890 transaction V2).
pub struct ProduceRequest;

impl ProduceRequest {
    /// Java `ProduceRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`.
    pub const LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2: i16 = 11;

    /// Java `ProduceRequest.isTransactionV2Requested`.
    #[must_use]
    pub const fn is_transaction_v2_requested(version: i16) -> bool {
        version > Self::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
    }
}

/// Java `ProduceResponse` helpers.
pub struct ProduceResponse;

impl ProduceResponse {
    /// Java `ProduceResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 6
    }
}

/// `true` when Produce `version` is flexible (v9+).
///
/// v3–v8 are classic. v9–v12 are compact arrays/strings/bytes plus tagged
/// fields (Apache JSON `flexibleVersions: "9+"`). v10+ adds partition
/// CurrentLeader tagged field 0 and top-level NodeEndpoints tagged field 0
/// (KIP-951). v11 is TRANSACTION_ABORTABLE
/// (same layout as v10). v12 is the same layout (KIP-890 Part 2
/// transaction V2: Produce also does AddPartitionsToTxn). Kafka 4.0
/// removed v0–v2. This crate speaks 3–12. v13+ (topic IDs) are not spoken.
fn produce_flexible(version: i16) -> Result<bool> {
    match version {
        3..=8 => Ok(false),
        9..=12 => Ok(true),
        other => Err(Error::protocol(format!(
            "Produce version {other} is not implemented"
        ))),
    }
}

fn encode_current_leader(leader_id: i32, leader_epoch: i32) -> Bytes {
    let mut inner = BytesMut::new();
    inner.put_i32(leader_id);
    inner.put_i32(leader_epoch);
    buf::put_empty_tagged_fields(&mut inner);
    inner.freeze()
}

fn decode_current_leader(value: &Bytes) -> Result<(i32, i32)> {
    let mut cur = value.as_ref();
    let leader_id = buf::get_i32(&mut cur)?;
    let leader_epoch = buf::get_i32(&mut cur)?;
    buf::skip_tagged_fields(&mut cur)?;
    leftover_empty(&cur, "CurrentLeader")?;
    Ok((leader_id, leader_epoch))
}

fn encode_produce_partition_tags(
    buf: &mut BytesMut,
    version: i16,
    current_leader_id: i32,
    current_leader_epoch: i32,
) -> Result<()> {
    if version >= 10 && current_leader_id >= 0 {
        buf::put_tagged_fields(
            buf,
            &[(
                0,
                encode_current_leader(current_leader_id, current_leader_epoch),
            )],
        )
    } else {
        buf::put_empty_tagged_fields(buf);
        Ok(())
    }
}

fn decode_produce_partition_tags<B: Buf>(buf: &mut B, version: i16) -> Result<(i32, i32)> {
    let tags = buf::get_tagged_fields(buf)?;
    let mut current_leader_id = MetadataResponse::NO_LEADER_ID;
    let mut current_leader_epoch = RecordBatch::NO_PARTITION_LEADER_EPOCH;
    if version >= 10 {
        for (tag, value) in tags {
            if tag == 0 {
                (current_leader_id, current_leader_epoch) = decode_current_leader(&value)?;
            }
        }
    }
    Ok((current_leader_id, current_leader_epoch))
}

/// Encode Produce v3–v8 (classic) or v9–v12 (flexible).
pub fn encode_produce_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: Option<&str>,
    acks: i16,
    timeout_ms: i32,
    topics: &[ProduceTopicData],
) -> Result<()> {
    let flexible = produce_flexible(version)?;
    if version >= 3 {
        buf::put_string(buf, flexible, transactional_id)?;
    }
    buf.put_i16(acks);
    buf.put_i32(timeout_ms);
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for topic in topics {
        if version <= 12 {
            buf::put_string(buf, flexible, Some(&topic.topic))?;
        } else {
            buf.extend_from_slice(&[0u8; 16]);
        }
        buf::put_array_len(buf, flexible, Some(topic.partitions.len()))?;
        for part in &topic.partitions {
            buf.put_i32(part.index);
            if flexible {
                let mut recs = BytesMut::new();
                records::encode_record_batch(&mut recs, &part.records)?;
                buf::put_bytes(buf, flexible, Some(&recs))?;
                buf::put_empty_tagged_fields(buf);
            } else {
                let len_pos = buf.len();
                buf.put_i32(0);
                records::encode_record_batch(buf, &part.records)?;
                let rec_len = buf::i32_from_usize(buf.len().saturating_sub(len_pos + 4))?;
                buf::patch_i32(buf, len_pos, rec_len)?;
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode Produce: `(transactional_id, acks, timeout_ms, topics)`.
pub fn decode_produce_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Option<String>, i16, i32, Vec<ProduceTopicData>)> {
    let flexible = produce_flexible(version)?;
    let transactional_id = if version >= 3 {
        buf::get_string(buf, flexible)?
    } else {
        None
    };
    let acks = buf::get_i16(buf)?;
    let timeout_ms = buf::get_i32(buf)?;
    let topic_count = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let topic = if version <= 12 {
            buf::get_string(buf, flexible)?.unwrap_or_default()
        } else {
            let _id = buf::get_uuid(buf)?;
            String::new()
        };
        let part_count = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let index = buf::get_i32(buf)?;
            let rec_bytes = buf::get_bytes(buf, flexible)?.unwrap_or_default();
            let mut rec_buf = &rec_bytes[..];
            let records = if rec_buf.is_empty() {
                RecordBatch::from_records(vec![])
            } else {
                records::decode_record_batch(&mut rec_buf)?
            };
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(ProducePartitionData { index, records });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(ProduceTopicData { topic, partitions });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((transactional_id, acks, timeout_ms, topics))
}

/// Encode Produce: one response per partition (mock broker).
pub fn encode_produce_response(
    buf: &mut BytesMut,
    version: i16,
    parts: &[ProducePartitionResponse],
) -> crate::error::Result<()> {
    encode_produce_response_with_endpoints(buf, version, parts, &[])
}

/// Encode Produce plus top-level NodeEndpoints (v10+ tagged field 0).
pub fn encode_produce_response_with_endpoints(
    buf: &mut BytesMut,
    version: i16,
    parts: &[ProducePartitionResponse],
    endpoints: &[NodeEndpoint],
) -> crate::error::Result<()> {
    let flexible = produce_flexible(version)?;
    // Group by topic, preserving first-seen order.
    let mut order: Vec<String> = Vec::new();
    for p in parts {
        if !order.iter().any(|t| t == &p.topic) {
            order.push(p.topic.clone());
        }
    }
    buf::put_array_len(buf, flexible, Some(order.len()))?;
    for topic in &order {
        if version <= 12 {
            buf::put_string(buf, flexible, Some(topic))?;
        } else {
            buf.extend_from_slice(&[0u8; 16]);
        }
        let grouped: Vec<&ProducePartitionResponse> =
            parts.iter().filter(|p| &p.topic == topic).collect();
        buf::put_array_len(buf, flexible, Some(grouped.len()))?;
        for p in grouped {
            buf.put_i32(p.partition);
            buf.put_i16(p.error_code);
            buf.put_i64(p.base_offset);
            if version >= 2 {
                buf.put_i64(p.log_append_time_ms);
            }
            if version >= 5 {
                buf.put_i64(p.log_start_offset);
            }
            if version >= 8 {
                buf::put_array_len(buf, flexible, Some(0))?;
                buf::put_string(buf, flexible, None)?;
            }
            if flexible {
                encode_produce_partition_tags(
                    buf,
                    version,
                    p.current_leader_id,
                    p.current_leader_epoch,
                )?;
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if version >= 1 {
        buf.put_i32(0);
    }
    if flexible {
        encode_top_level_node_endpoints(buf, version >= 10, endpoints)?;
    }
    Ok(())
}

/// Decode Produce into per-partition results and v10+ NodeEndpoints.
///
/// Versions below 2 fill [`RecordBatch::NO_TIMESTAMP`]. Versions below 5
/// fill [`ProducePartitionResponse::INVALID_OFFSET`] for log start.
pub fn decode_produce_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<ProducePartitionResponse>, Vec<NodeEndpoint>)> {
    let flexible = produce_flexible(version)?;
    let topic_count = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::new();
    for _ in 0..topic_count {
        let topic = if version <= 12 {
            buf::get_string(buf, flexible)?.unwrap_or_default()
        } else {
            let _id = buf::get_uuid(buf)?;
            String::new()
        };
        let part_count = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        for _ in 0..part_count {
            let partition = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let base_offset = buf::get_i64(buf)?;
            let log_append_time_ms = if version >= 2 {
                buf::get_i64(buf)?
            } else {
                RecordBatch::NO_TIMESTAMP
            };
            let log_start_offset = if version >= 5 {
                buf::get_i64(buf)?
            } else {
                ProducePartitionResponse::INVALID_OFFSET
            };
            if version >= 8 {
                let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
                for _ in 0..n {
                    let _idx = buf::get_i32(buf)?;
                    let _msg = buf::get_string(buf, flexible)?;
                    if flexible {
                        buf::skip_tagged_fields(buf)?;
                    }
                }
                let _err_msg = buf::get_string(buf, flexible)?;
            }
            let (current_leader_id, current_leader_epoch) = if flexible {
                decode_produce_partition_tags(buf, version)?
            } else {
                (
                    MetadataResponse::NO_LEADER_ID,
                    RecordBatch::NO_PARTITION_LEADER_EPOCH,
                )
            };
            out.push(ProducePartitionResponse {
                topic: topic.clone(),
                partition,
                error_code,
                base_offset,
                log_append_time_ms,
                log_start_offset,
                current_leader_id,
                current_leader_epoch,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
    }
    if version >= 1 {
        let _throttle = buf::get_i32(buf)?;
    }
    let endpoints = if flexible {
        decode_top_level_node_endpoints(buf, version >= 10)?
    } else {
        Vec::new()
    };
    Ok((out, endpoints))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::records::Record;
    use bytes::Bytes;

    #[test]
    fn produce_transaction_v2_version_cap_matches_java() {
        assert_eq!(
            ProduceRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2,
            11
        );
        assert!(!ProduceRequest::is_transaction_v2_requested(11));
        assert!(ProduceRequest::is_transaction_v2_requested(12));
        assert!(!ProduceRequest::is_transaction_v2_requested(10));
        assert!(!ProduceResponse::should_client_throttle(5));
        assert!(ProduceResponse::should_client_throttle(6));
    }

    #[test]
    fn produce_partition_response_matches_java() {
        assert_eq!(ProducePartitionResponse::INVALID_OFFSET, -1);
        assert_eq!(RecordBatch::NO_TIMESTAMP, -1);
        assert_eq!(MetadataResponse::NO_LEADER_ID, -1);
        assert_eq!(RecordBatch::NO_PARTITION_LEADER_EPOCH, -1);
        let none = ProducePartitionResponse::partition_response("t", 0, 0);
        assert_eq!(none.topic, "t");
        assert_eq!(none.partition, 0);
        assert_eq!(none.error_code, 0);
        assert_eq!(none.base_offset, ProducePartitionResponse::INVALID_OFFSET);
        assert_eq!(none.log_append_time_ms, RecordBatch::NO_TIMESTAMP);
        assert_eq!(
            none.log_start_offset,
            ProducePartitionResponse::INVALID_OFFSET
        );
        assert_eq!(none.current_leader_id, MetadataResponse::NO_LEADER_ID);
        assert_eq!(
            none.current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        let unknown = ProducePartitionResponse::partition_response(
            "missing",
            3,
            crate::error::UNKNOWN_TOPIC_OR_PARTITION,
        );
        assert_eq!(unknown.topic, "missing");
        assert_eq!(unknown.partition, 3);
        assert_eq!(unknown.error_code, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            unknown.base_offset,
            ProducePartitionResponse::INVALID_OFFSET
        );
        assert_eq!(unknown.log_append_time_ms, RecordBatch::NO_TIMESTAMP);
        assert_eq!(
            unknown.log_start_offset,
            ProducePartitionResponse::INVALID_OFFSET
        );
    }

    #[test]
    fn produce_v3_omitted_log_start_is_invalid_offset() {
        let parts = [ProducePartitionResponse {
            topic: "t".into(),
            partition: 0,
            error_code: 0,
            base_offset: 7,
            log_append_time_ms: RecordBatch::NO_TIMESTAMP,
            log_start_offset: 99,
            current_leader_id: -1,
            current_leader_epoch: -1,
        }];
        let mut buf = BytesMut::new();
        encode_produce_response(&mut buf, 3, &parts).unwrap();
        let mut cur = &buf[..];
        let (got, endpoints) = decode_produce_response(&mut cur, 3).unwrap();
        assert!(endpoints.is_empty());
        assert!(cur.is_empty());
        let part = got.first().expect("one partition");
        assert_eq!(part.base_offset, 7);
        assert_eq!(part.log_append_time_ms, RecordBatch::NO_TIMESTAMP);
        assert_eq!(
            part.log_start_offset,
            ProducePartitionResponse::INVALID_OFFSET,
            "Produce v3 omits LogStartOffset; decode fills INVALID_OFFSET"
        );
        assert_eq!(ProducePartitionResponse::INVALID_OFFSET, -1);
        assert_eq!(part.current_leader_id, MetadataResponse::NO_LEADER_ID);
        assert_eq!(
            part.current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
    }

    #[test]
    fn api_versions_v3_roundtrip() {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: 0,
                    min_version: 3,
                    max_version: 9,
                },
                ApiVersion {
                    api_key: 3,
                    min_version: 0,
                    max_version: 12,
                },
                ApiVersion {
                    api_key: 18,
                    min_version: 0,
                    max_version: 4,
                },
            ],
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_api_versions_response(&mut buf, 3, &resp).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_api_versions_response(&mut cur, 3).unwrap();
        assert_eq!(decoded, resp);
        assert!(
            !cur.has_remaining(),
            "ApiVersions v3 empty features must be leftover-empty"
        );
    }

    #[test]
    fn api_versions_v3_empty_features_is_zero_tagged_fields() {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: Vec::new(),
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_api_versions_response(&mut buf, 3, &resp).unwrap();
        assert_eq!(&buf[..], &[0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]);
        let mut cur = &buf[..];
        assert_eq!(decode_api_versions_response(&mut cur, 3).unwrap(), resp);
        assert!(!cur.has_remaining());
    }

    #[test]
    fn api_versions_v3_features_roundtrip_is_leftover_empty() {
        // KIP-482: tag 0 supported (name, min, max), tag 1 epoch INT64,
        // tag 2 finalized (name, max, min). Empty tags omitted.
        const BODY: &[u8] = &[
            0x00, 0x00, 0x02, 0x00, 0x12, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x03, 0x00, 0x17, 0x02, 0x11, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x2e,
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x01, 0x00, 0x14, 0x00, 0x01, 0x08,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x17, 0x02, 0x11, 0x6d, 0x65,
            0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
            0x00, 0x14, 0x00, 0x01, 0x00,
        ];
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
            }],
            throttle_time_ms: 0,
            supported_features: vec![SupportedFeatureKey {
                name: "metadata.version".into(),
                min_version: 1,
                max_version: 20,
            }],
            finalized_features_epoch: Some(1),
            finalized_features: vec![FinalizedFeatureKey {
                name: "metadata.version".into(),
                max_version_level: 20,
                min_version_level: 1,
            }],
            zk_migration_ready: false,
        };
        let mut buf = BytesMut::new();
        encode_api_versions_response(&mut buf, 3, &resp).unwrap();
        assert_eq!(&buf[..], BODY);
        let mut cur = &buf[..];
        let decoded = decode_api_versions_response(&mut cur, 3).unwrap();
        assert_eq!(decoded, resp);
        assert!(
            !cur.has_remaining(),
            "ApiVersions v3 features must be leftover-empty"
        );
    }

    #[test]
    fn api_versions_response_helpers_match_java() {
        assert_eq!(ApiVersionsResponse::UNKNOWN_FINALIZED_FEATURES_EPOCH, -1);
        assert!(!ApiVersionsResponse::should_client_throttle(1));
        assert!(ApiVersionsResponse::should_client_throttle(2));
        let resp = ApiVersionsResponse {
            api_keys: vec![ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
            }],
            zk_migration_ready: true,
            ..Default::default()
        };
        assert_eq!(resp.api_version(18).map(|v| v.max_version), Some(4));
        assert!(resp.api_version(1).is_none());
        assert!(resp.zk_migration_ready());
    }

    #[test]
    fn api_versions_request_is_valid_matches_java() {
        assert!(ApiVersionsRequest::is_valid(2, "", ""));
        assert!(ApiVersionsRequest::is_valid(2, "-invalid", "x."));
        assert!(ApiVersionsRequest::is_valid(
            3,
            crate::CLIENT_NAME,
            crate::CLIENT_VERSION
        ));
        assert!(ApiVersionsRequest::is_valid(4, "a", "1"));
        assert!(ApiVersionsRequest::is_valid(3, "a-b.c", "0.1.0"));
        assert!(!ApiVersionsRequest::is_valid(3, "", "0.1.0"));
        assert!(!ApiVersionsRequest::is_valid(3, "partitionline", ""));
        assert!(!ApiVersionsRequest::is_valid(3, "-x", "0.1.0"));
        assert!(!ApiVersionsRequest::is_valid(3, "x-", "0.1.0"));
        assert!(!ApiVersionsRequest::is_valid(3, ".x", "0.1.0"));
        assert!(!ApiVersionsRequest::is_valid(3, "x.", "0.1.0"));
        assert!(!ApiVersionsRequest::is_valid(3, "x_y", "0.1.0"));
    }

    #[test]
    fn api_versions_v3_matches_v4_and_does_not_speak_v5() {
        // Official Kafka 4.0 JSON: validVersions 0-4, flexibleVersions 3+.
        // v3 and v4 request match. v4 response includes SupportedFeatures
        // with MinVersion 0 (KAFKA-17011); v3 omits them. This crate
        // speaks 0–4. v5+ is not spoken.
        let mut v3 = BytesMut::new();
        encode_api_versions_request(&mut v3, 3, "partitionline", "0.1.0").unwrap();
        let mut v4 = BytesMut::new();
        encode_api_versions_request(&mut v4, 4, "partitionline", "0.1.0").unwrap();
        assert_eq!(v3.as_ref(), v4.as_ref(), "v3 and v4 request bodies match");
        let mut v0 = BytesMut::new();
        encode_api_versions_request(&mut v0, 0, "partitionline", "0.1.0").unwrap();
        assert!(v0.is_empty(), "v0–v2 request is empty");
        encode_api_versions_request(&mut v0, 2, "partitionline", "0.1.0").unwrap();
        assert!(v0.is_empty(), "v2 request is empty");
        let mut empty: &[u8] = &[];
        assert_eq!(
            decode_api_versions_request(&mut empty, 0).unwrap(),
            (String::new(), String::new())
        );
        let mut empty: &[u8] = &[];
        assert_eq!(
            decode_api_versions_request(&mut empty, 2).unwrap(),
            (String::new(), String::new())
        );
        let mut cur = v3.as_ref();
        assert_eq!(
            decode_api_versions_request(&mut cur, 3).unwrap(),
            ("partitionline".into(), "0.1.0".into())
        );
        assert!(!cur.has_remaining(), "v3 request leftover-empty");
        let mut cur = v4.as_ref();
        assert_eq!(
            decode_api_versions_request(&mut cur, 4).unwrap(),
            ("partitionline".into(), "0.1.0".into())
        );
        assert!(!cur.has_remaining(), "v4 request leftover-empty");
        let err = encode_api_versions_request(&mut BytesMut::new(), 5, "partitionline", "0.1.0")
            .unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v5 is not spoken, got {err}"
        );
        let mut empty: &[u8] = &[];
        let err = decode_api_versions_request(&mut empty, 5).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v5 decode is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 3, 0, 4), Some(3));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 4, 0, 4), Some(4));
        assert_eq!(crate::protocol::api_keys::pick_version(5, 5, 0, 4), None);

        let kraft = SupportedFeatureKey {
            name: "kraft.version".into(),
            min_version: 0,
            max_version: 1,
        };
        let meta = SupportedFeatureKey {
            name: "metadata.version".into(),
            min_version: 1,
            max_version: 20,
        };
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
            }],
            throttle_time_ms: 0,
            supported_features: vec![meta.clone(), kraft.clone()],
            finalized_features_epoch: None,
            finalized_features: Vec::new(),
            zk_migration_ready: false,
        };
        v3.clear();
        encode_api_versions_response(&mut v3, 3, &resp).unwrap();
        v4.clear();
        encode_api_versions_response(&mut v4, 4, &resp).unwrap();
        assert_ne!(
            v3.as_ref(),
            v4.as_ref(),
            "v3 omits SupportedFeatures with MinVersion 0"
        );
        let mut cur = v3.as_ref();
        let decoded = decode_api_versions_response(&mut cur, 3).unwrap();
        assert_eq!(decoded.supported_features, vec![meta.clone()]);
        assert!(!cur.has_remaining(), "v3 response leftover-empty");
        let mut cur = v4.as_ref();
        let decoded = decode_api_versions_response(&mut cur, 4).unwrap();
        assert_eq!(decoded.supported_features, vec![meta, kraft]);
        assert!(!cur.has_remaining(), "v4 response leftover-empty");
        v3.clear();
        let err = encode_api_versions_response(&mut v3, 5, &resp).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v5 response is not spoken, got {err}"
        );
    }

    #[test]
    fn api_versions_kip511_v0_unsupported_body_parses_when_sent_v4() {
        // Brokers 2.4+ answer an unsupported ApiVersions version with a
        // v0 body (KIP-511). Java ApiVersionsResponse.parse falls back
        // to v0 when the sent version does not leftover-empty.
        let resp = ApiVersionsResponse {
            error_code: crate::error::UNSUPPORTED_VERSION,
            api_keys: vec![ApiVersion {
                api_key: API_VERSIONS,
                min_version: 0,
                max_version: 3,
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_api_versions_response(&mut buf, 0, &resp).unwrap();
        let decoded = decode_api_versions_handshake(&buf, 4).unwrap();
        assert_eq!(decoded.error_code, crate::error::UNSUPPORTED_VERSION);
        assert_eq!(decoded.api_keys, resp.api_keys);
        assert_eq!(crate::protocol::api_keys::pick_version(0, 3, 0, 4), Some(3));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 4), Some(0));
    }

    #[test]
    fn produce_v3_transactional_id_is_not_null() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"x")),
            headers: vec![],
        };
        let topics = vec![ProduceTopicData {
            topic: "t".into(),
            partitions: vec![ProducePartitionData {
                index: 0,
                records: RecordBatch::from_records(vec![rec]),
            }],
        }];
        let mut buf = BytesMut::new();
        encode_produce_request(&mut buf, 3, Some("tx-1"), -1, 1000, &topics).unwrap();
        let (txn, acks, _, _) = decode_produce_request(&mut &buf[..], 3).unwrap();
        assert_eq!(txn.as_deref(), Some("tx-1"));
        assert_eq!(acks, -1);
    }

    #[test]
    fn produce_v9_roundtrip_is_leftover_empty() {
        let rec = Record {
            offset: 0,
            timestamp: 42,
            key: None,
            value: Some(Bytes::from_static(b"hi")),
            headers: vec![],
        };
        let topics = vec![ProduceTopicData {
            topic: "t".into(),
            partitions: vec![ProducePartitionData {
                index: 0,
                records: RecordBatch::from_records(vec![rec]),
            }],
        }];
        let mut buf = BytesMut::new();
        encode_produce_request(&mut buf, 9, None, 1, 1500, &topics).unwrap();
        let mut cur = &buf[..];
        let (txn, acks, timeout, decoded) = decode_produce_request(&mut cur, 9).unwrap();
        assert_eq!(txn, None);
        assert_eq!(acks, 1);
        assert_eq!(timeout, 1500);
        assert_eq!(decoded[0].topic, "t");
        assert_eq!(
            decoded[0].partitions[0].records.records[0].value.as_deref(),
            Some(&b"hi"[..])
        );
        assert!(
            cur.is_empty(),
            "Produce v9 request must consume compact tagged fields"
        );

        buf.clear();
        encode_produce_request(&mut buf, 12, None, 1, 1500, &topics).unwrap();
        let mut cur = &buf[..];
        let (txn, acks, timeout, decoded) = decode_produce_request(&mut cur, 12).unwrap();
        assert_eq!(txn, None);
        assert_eq!(acks, 1);
        assert_eq!(timeout, 1500);
        assert_eq!(decoded[0].topic, "t");
        assert!(
            cur.is_empty(),
            "Produce v12 request must consume compact tagged fields"
        );

        let mut txn_buf = BytesMut::new();
        encode_produce_request(&mut txn_buf, 8, Some("tx-1"), 1, 1500, &topics).unwrap();
        let mut cur = &txn_buf[..];
        let (txn, _, _, _) = decode_produce_request(&mut cur, 8).unwrap();
        assert_eq!(txn.as_deref(), Some("tx-1"));
        assert!(
            cur.is_empty(),
            "Produce v8 request leftover {} bytes",
            cur.len()
        );

        buf.clear();
        assert!(
            encode_produce_request(&mut buf, 13, None, 1, 1500, &topics).is_err(),
            "Produce v13+ (topic IDs) is not spoken"
        );
    }

    #[test]
    fn produce_v9_response_matches_compact_layout() {
        // Compact Topics {Name "t", compact Partitions {0, error 0,
        // base 0, logAppend -1, logStart 0, empty RecordErrors, null
        // ErrorMessage, tagged}, tagged}, throttle 0, tagged.
        const RESP: &[u8] = &[
            0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ];
        let parts = [ProducePartitionResponse {
            topic: "t".into(),
            partition: 0,
            error_code: 0,
            base_offset: 0,
            log_append_time_ms: RecordBatch::NO_TIMESTAMP,
            log_start_offset: 0,
            current_leader_id: -1,
            current_leader_epoch: -1,
        }];
        let mut buf = BytesMut::new();
        encode_produce_response(&mut buf, 9, &parts).unwrap();
        assert_eq!(&buf[..], RESP);
        let mut cur = &buf[..];
        let got = decode_produce_response(&mut cur, 9).unwrap();
        assert_eq!(got.0, parts);
        assert!(got.1.is_empty());
        assert!(
            cur.is_empty(),
            "Produce v9 response must consume compact tagged fields"
        );

        buf.clear();
        encode_produce_response(&mut buf, 12, &parts).unwrap();
        assert_eq!(
            &buf[..],
            RESP,
            "Produce v12 with omitted CurrentLeader matches v9 bytes"
        );
        let mut cur = &buf[..];
        let got = decode_produce_response(&mut cur, 12).unwrap();
        assert_eq!(got.0, parts);
        assert!(got.1.is_empty());
        assert!(
            cur.is_empty(),
            "Produce v12 empty CurrentLeader must consume compact tagged fields"
        );
    }

    #[test]
    fn produce_v11_current_leader_tagged_is_leftover_empty() {
        // Same as v9 compact response except partition tagged field 0:
        // LeaderId 2, LeaderEpoch 7, empty nested tagged fields (9 bytes).
        const RESP: &[u8] = &[
            0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x09, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let parts = [ProducePartitionResponse {
            topic: "t".into(),
            partition: 0,
            error_code: 0,
            base_offset: 0,
            log_append_time_ms: RecordBatch::NO_TIMESTAMP,
            log_start_offset: 0,
            current_leader_id: 2,
            current_leader_epoch: 7,
        }];
        let mut buf = BytesMut::new();
        encode_produce_response(&mut buf, 11, &parts).unwrap();
        assert_eq!(&buf[..], RESP);
        let mut cur = &buf[..];
        let got = decode_produce_response(&mut cur, 11).unwrap();
        assert_eq!(got.0, parts);
        assert!(got.1.is_empty());
        assert!(
            cur.is_empty(),
            "Produce v11 CurrentLeader must consume nested tagged fields"
        );
        buf.clear();
        encode_produce_response(&mut buf, 10, &parts).unwrap();
        assert_eq!(&buf[..], RESP, "Produce v10 CurrentLeader matches v11");
        buf.clear();
        encode_produce_response(&mut buf, 12, &parts).unwrap();
        assert_eq!(&buf[..], RESP, "Produce v12 CurrentLeader matches v11");
        buf.clear();
        assert!(
            encode_produce_response(&mut buf, 13, &parts).is_err(),
            "Produce v13+ (topic IDs) is not spoken"
        );
    }

    #[test]
    fn produce_v10_node_endpoints_tagged_is_leftover_empty() {
        let parts = [ProducePartitionResponse {
            topic: "t".into(),
            partition: 0,
            error_code: 6,
            base_offset: ProducePartitionResponse::INVALID_OFFSET,
            log_append_time_ms: RecordBatch::NO_TIMESTAMP,
            log_start_offset: 0,
            current_leader_id: 3,
            current_leader_epoch: 1,
        }];
        let endpoints = [NodeEndpoint {
            node_id: 3,
            host: "h".into(),
            port: 1,
            rack: None,
        }];
        let mut buf = BytesMut::new();
        encode_produce_response_with_endpoints(&mut buf, 10, &parts, &endpoints).unwrap();
        let mut cur = &buf[..];
        let (got, eps) = decode_produce_response(&mut cur, 10).unwrap();
        assert_eq!(got[0].current_leader_id, 3);
        assert_eq!(eps, endpoints);
        assert!(
            cur.is_empty(),
            "Produce NodeEndpoints tagged field 0 must consume nested tagged fields"
        );
        let mut omitted = BytesMut::new();
        encode_produce_response(&mut omitted, 10, &parts).unwrap();
        assert_ne!(
            &buf[..],
            &omitted[..],
            "NodeEndpoints tagged field 0 must not equal empty tags"
        );
        let mut v9 = BytesMut::new();
        encode_produce_response_with_endpoints(&mut v9, 9, &parts, &endpoints).unwrap();
        let mut empty = BytesMut::new();
        encode_produce_response(&mut empty, 9, &parts).unwrap();
        assert_eq!(&v9[..], &empty[..], "Produce v9 must omit NodeEndpoints");
    }

    #[test]
    fn metadata_broker_and_node_endpoint_match_java_node() {
        let broker = Broker::new(1, "127.0.0.1", 9092, Some("r".into()));
        assert_eq!(broker.id(), 1);
        assert_eq!(broker.id_string(), "1");
        assert_eq!(broker.host(), "127.0.0.1");
        assert_eq!(broker.port(), 9092);
        assert_eq!(broker.rack(), Some("r"));
        assert!(broker.has_rack());
        assert!(!broker.is_fenced());
        assert!(!broker.is_empty());
        assert_eq!(
            broker.to_string(),
            "127.0.0.1:9092 (id: 1 rack: r isFenced: false)"
        );
        let endpoint = NodeEndpoint::from(broker.clone());
        assert_eq!(endpoint.id(), 1);
        assert_eq!(endpoint.host(), "127.0.0.1");
        assert_eq!(endpoint.port(), 9092);
        assert_eq!(endpoint.rack(), Some("r"));
        assert!(endpoint.has_rack());
        assert!(!endpoint.is_fenced());
        assert_eq!(endpoint.to_string(), broker.to_string());
        assert_eq!(Broker::from(endpoint.clone()), broker);
        let empty = Broker::no_node();
        assert_eq!(empty.id(), -1);
        assert!(empty.is_empty());
        assert_eq!(empty.id_string(), "-1");
        assert_eq!(empty.to_string(), ":-1 (id: -1 rack: null isFenced: false)");
        let empty_ep = NodeEndpoint::no_node();
        assert!(empty_ep.is_empty());
        assert_eq!(empty_ep.to_string(), empty.to_string());
    }

    #[test]
    fn metadata_v12_roundtrip() {
        let resp = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![Broker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
            }],
            cluster_id: Some("cid".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("orders".into()),
                topic_id: [1u8; 16],
                is_internal: false,
                partitions: vec![PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 3,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    offline_replicas: vec![2],
                }],
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            error_code: 0,
        };
        let mut buf = BytesMut::new();
        encode_metadata_response(&mut buf, 12, &resp).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_metadata_response(&mut cur, 12).unwrap();
        assert_eq!(decoded, resp);
        leftover_empty(&cur, "Metadata v12").unwrap();

        let mut v13 = BytesMut::new();
        encode_metadata_response(&mut v13, 13, &resp).unwrap();
        let mut cur = &v13[..];
        let decoded = decode_metadata_response(&mut cur, 13).unwrap();
        assert_eq!(decoded, resp);
        leftover_empty(&cur, "Metadata v13").unwrap();
        assert_ne!(
            &buf[..],
            &v13[..],
            "Metadata v13 must write top-level ErrorCode before tagged fields"
        );
    }

    #[test]
    fn metadata_v13_top_error_fails_check() {
        let resp = MetadataResponse {
            throttle_time_ms: 0,
            brokers: Vec::new(),
            cluster_id: None,
            controller_id: MetadataResponse::NO_CONTROLLER_ID,
            topics: Vec::new(),
            error_code: crate::error::UNKNOWN_TOPIC_OR_PARTITION,
        };
        assert_eq!(resp.controller_id, MetadataResponse::NO_CONTROLLER_ID);
        assert_eq!(MetadataResponse::NO_CONTROLLER_ID, -1);
        assert_eq!(MetadataResponse::NO_LEADER_ID, -1);
        let mut buf = BytesMut::new();
        encode_metadata_response(&mut buf, 13, &resp).unwrap();
        let decoded = decode_metadata_response(&mut &buf[..], 13).unwrap();
        assert_eq!(decoded.error_code, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            decoded.check().unwrap_err().broker_code(),
            Some(crate::error::UNKNOWN_TOPIC_OR_PARTITION)
        );
    }

    #[test]
    fn metadata_has_reliable_leader_epochs_matches_java() {
        assert!(!MetadataResponse::has_reliable_leader_epochs(8));
        assert!(MetadataResponse::has_reliable_leader_epochs(9));
        assert!(MetadataResponse::has_reliable_leader_epochs(13));
        assert_eq!(MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED, i32::MIN);
        assert_eq!(
            MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            crate::AUTHORIZED_OPERATIONS_OMITTED
        );
        assert!(!MetadataResponse::should_client_throttle(5));
        assert!(MetadataResponse::should_client_throttle(6));
        let with_epoch = PartitionMetadata {
            error_code: 0,
            partition_index: 1,
            leader_id: 2,
            leader_epoch: 8,
            replica_nodes: vec![2, 3],
            isr_nodes: vec![2],
            offline_replicas: vec![3],
        };
        let stripped = with_epoch.without_leader_epoch();
        assert_eq!(
            stripped.leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(stripped.error_code, with_epoch.error_code);
        assert_eq!(stripped.partition_index, with_epoch.partition_index);
        assert_eq!(stripped.leader_id, with_epoch.leader_id);
        assert_eq!(stripped.replica_nodes, with_epoch.replica_nodes);
        assert_eq!(stripped.isr_nodes, with_epoch.isr_nodes);
        assert_eq!(stripped.offline_replicas, with_epoch.offline_replicas);
        assert_eq!(with_epoch.leader_epoch, 8);
    }

    #[test]
    fn metadata_v7_decodes_leader_epoch_and_omitted_authorized_ops() {
        let resp = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![Broker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
            }],
            cluster_id: Some("cid".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("orders".into()),
                topic_id: [0u8; 16],
                is_internal: false,
                partitions: vec![PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 3,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    offline_replicas: Vec::new(),
                }],
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            error_code: 0,
        };
        let mut buf = BytesMut::new();
        encode_metadata_response(&mut buf, 7, &resp).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_metadata_response(&mut cur, 7).unwrap();
        leftover_empty(&cur, "Metadata v7").unwrap();
        assert_eq!(decoded, resp);
        assert_eq!(decoded.topics[0].partitions[0].leader_epoch, 3);
        assert_eq!(
            decoded.topics[0].topic_authorized_operations,
            MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED
        );
        assert!(
            !MetadataResponse::has_reliable_leader_epochs(7),
            "v7 leader epochs are on the wire but must not be retained by the client"
        );
    }

    #[test]
    fn metadata_request_roundtrips_topics_and_allow_auto() {
        let topics = ["orders".to_string(), "payments".to_string()];
        let mut buf = BytesMut::new();
        encode_metadata_request(&mut buf, 12, Some(&topics), true).unwrap();
        let (got, allow, include_topic) = decode_metadata_request(&mut &buf[..], 12).unwrap();
        assert_eq!(got.as_deref(), Some(topics.as_slice()));
        assert!(allow);
        assert!(
            !include_topic,
            "encode_metadata_request must leave IncludeTopicAuthorizedOperations unset"
        );

        let mut all = BytesMut::new();
        encode_metadata_request(&mut all, 12, None, false).unwrap();
        let (got, allow, include_topic) = decode_metadata_request(&mut &all[..], 12).unwrap();
        assert!(got.is_none());
        assert!(!allow);
        assert!(!include_topic);

        let mut with = BytesMut::new();
        encode_metadata_request_with(&mut with, 12, Some(&topics), false, true).unwrap();
        let (got, allow, include_topic) = decode_metadata_request(&mut &with[..], 12).unwrap();
        assert_eq!(got.as_deref(), Some(topics.as_slice()));
        assert!(!allow);
        assert!(include_topic);
        assert_ne!(
            &buf[..],
            &with[..],
            "IncludeTopicAuthorizedOperations true must not match the default request"
        );
    }

    #[test]
    fn metadata_v12_topic_id_request_is_compact() {
        // Compact Topics[1] { TopicId "t" padded, Name null, tagged }
        // + AllowAutoTopicCreation false + IncludeTopicAuthorizedOperations
        // false + tagged. v12 is not in 8..=10, so no cluster-auth byte.
        const V12_ID: &[u8] = &[
            0x02, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let mut id = [0u8; 16];
        id[0] = b't';
        let topics = [MetadataRequestTopic::by_id(id)];
        let mut buf = BytesMut::new();
        encode_metadata_request_topics(&mut buf, 12, Some(&topics), false, false).unwrap();
        assert_eq!(&buf[..], V12_ID);
        let mut cur = &buf[..];
        let (got, allow, include_topic) = decode_metadata_request_topics(&mut cur, 12).unwrap();
        leftover_empty(&cur, "Metadata v12 TopicId request leftover").unwrap();
        let got = got.expect("Topics array");
        assert_eq!(got.as_slice(), topics.as_slice());
        assert!(!allow);
        assert!(!include_topic);
        let mut cur = &buf[..];
        let (names_only, _, _) = decode_metadata_request(&mut cur, 12).unwrap();
        leftover_empty(&cur, "Metadata v12 TopicId names-only leftover").unwrap();
        assert_eq!(
            names_only.as_deref(),
            Some(&[][..]),
            "name-only decode skips null-Name TopicId describes"
        );
    }

    #[test]
    fn metadata_builder_matches_java() {
        let names = ["t".to_string()];
        let err = encode_metadata_request(&mut BytesMut::new(), 0, Some(&names), true).unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "v0 is Java UnsupportedVersionException, got {err}"
        );
        assert!(err.to_string().contains("older than 1"), "got {err}");
        encode_metadata_request(&mut BytesMut::new(), 3, Some(&names), true).unwrap();
        let err =
            encode_metadata_request(&mut BytesMut::new(), 3, Some(&names), false).unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "allowAutoTopicCreation false below v4 is Java UnsupportedVersionException, got {err}"
        );
        assert!(
            err.to_string().contains("allowAutoTopicCreation"),
            "got {err}"
        );
        encode_metadata_request(&mut BytesMut::new(), 4, Some(&names), false).unwrap();

        let mut id = [0u8; 16];
        id[0] = 1;
        let by_id = [MetadataRequestTopic::by_id(id)];
        let err =
            encode_metadata_request_topics(&mut BytesMut::new(), 11, Some(&by_id), false, false)
                .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "null Name below v12 is Java UnsupportedVersionException, got {err}"
        );
        assert!(err.to_string().contains("null topic names"), "got {err}");
        encode_metadata_request_topics(&mut BytesMut::new(), 12, Some(&by_id), false, false)
            .unwrap();
        let named_id = [MetadataRequestTopic {
            name: Some("t".into()),
            topic_id: id,
        }];
        let err =
            encode_metadata_request_topics(&mut BytesMut::new(), 11, Some(&named_id), true, false)
                .unwrap_err();
        assert!(err.to_string().contains("non-zero topic IDs"), "got {err}");
    }
}
