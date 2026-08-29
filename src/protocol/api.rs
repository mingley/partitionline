//! ApiVersions, Metadata, and Produce codecs.

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
        .api_keys
        .iter()
        .find(|k| k.api_key == API_VERSIONS)
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
                epoch = (v >= 0).then_some(v);
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Partition index.
    pub partition_index: i32,
    /// Leader broker id, or `-1`.
    pub leader_id: i32,
    /// Leader epoch (v7+), or `-1`.
    pub leader_epoch: i32,
    /// Replica broker ids.
    pub replica_nodes: Vec<i32>,
    /// In-sync replica broker ids.
    pub isr_nodes: Vec<i32>,
    /// Offline replica broker ids (v5+). Java `offlineReplicas`.
    pub offline_replicas: Vec<i32>,
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
    /// Topic authorized operations (v8+). `i32::MIN` when omitted
    /// ([`crate::AUTHORIZED_OPERATIONS_OMITTED`]).
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
    /// Controller broker id (v1+), or `-1`.
    pub controller_id: i32,
    /// Topics.
    pub topics: Vec<TopicMetadata>,
    /// Top-level error (v13+). `0` on v1–v12.
    pub error_code: i16,
}

impl MetadataResponse {
    /// Fail when the v13+ top-level ErrorCode is non-zero.
    pub(crate) fn check(&self) -> Result<()> {
        if self.error_code == 0 {
            Ok(())
        } else {
            Err(Error::broker(self.error_code, "Metadata"))
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
/// unset (`false` on v8–v10).
pub fn encode_metadata_request_with(
    buf: &mut BytesMut,
    version: i16,
    topics: Option<&[String]>,
    allow_auto: bool,
    include_topic_authorized_operations: bool,
) -> crate::error::Result<()> {
    let flexible = version >= 9;
    match topics {
        None => buf::put_array_len(buf, flexible, None)?,
        Some(topics) => {
            buf::put_array_len(buf, flexible, Some(topics.len()))?;
            for name in topics {
                if version >= 10 {
                    buf.extend_from_slice(&[0u8; 16]);
                }
                buf::put_string(buf, flexible, Some(name))?;
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
pub fn decode_metadata_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Option<Vec<String>>, bool, bool)> {
    let flexible = version >= 9;
    let topics = match buf::get_array_len(buf, flexible)? {
        None => None,
        Some(n) => {
            let mut names = Vec::with_capacity(n);
            for _ in 0..n {
                if version >= 10 {
                    buf::need(buf, 16)?;
                    buf.advance(16);
                }
                let name = buf::get_string(buf, flexible)?;
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                if let Some(name) = name {
                    names.push(name);
                }
            }
            Some(names)
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
    let controller_id = if version >= 1 { buf::get_i32(buf)? } else { -1 };
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
            let leader_epoch = if version >= 7 { buf::get_i32(buf)? } else { -1 };
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
            i32::MIN
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
        buf.put_i32(-2147483648);
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionResponse {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// First offset assigned to the batch.
    pub base_offset: i64,
    /// Log append time, or `-1`.
    pub log_append_time_ms: i64,
    /// Log start offset.
    pub log_start_offset: i64,
    /// Produce v10+ CurrentLeader `LeaderId`, or `-1` when omitted.
    pub current_leader_id: i32,
    /// Produce v10+ CurrentLeader `LeaderEpoch`, or `-1` when omitted.
    pub current_leader_epoch: i32,
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
    let mut current_leader_id = -1;
    let mut current_leader_epoch = -1;
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
            let log_append_time_ms = if version >= 2 { buf::get_i64(buf)? } else { -1 };
            let log_start_offset = if version >= 5 { buf::get_i64(buf)? } else { -1 };
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
                (-1, -1)
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
            log_append_time_ms: -1,
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
            log_append_time_ms: -1,
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
            base_offset: -1,
            log_append_time_ms: -1,
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
                topic_authorized_operations: i32::MIN,
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
            controller_id: -1,
            topics: Vec::new(),
            error_code: crate::error::UNKNOWN_TOPIC_OR_PARTITION,
        };
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
}
