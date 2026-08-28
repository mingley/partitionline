//! ApiVersions, Metadata, and Produce codecs.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::buf;
use super::records::{self, RecordBatch};
use crate::error::{Error, Result};

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

/// Encode ApiVersions. v3+ sends `softwareName` / `softwareVersion`.
pub fn encode_api_versions_request(
    buf: &mut BytesMut,
    version: i16,
    software_name: &str,
    software_version: &str,
) -> crate::error::Result<()> {
    if version >= 3 {
        buf::put_compact_string(buf, Some(software_name))?;
        buf::put_compact_string(buf, Some(software_version))?;
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode ApiVersions (classic v0–2, flexible v3+).
pub fn decode_api_versions_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ApiVersionsResponse> {
    let flexible = version >= 3;
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

/// Encode ApiVersions (used by the mock broker).
pub fn encode_api_versions_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &ApiVersionsResponse,
) -> crate::error::Result<()> {
    let flexible = version >= 3;
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
        encode_api_versions_tagged_fields(buf, resp)?;
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

fn encode_api_versions_tagged_fields(buf: &mut BytesMut, resp: &ApiVersionsResponse) -> Result<()> {
    let mut tags: Vec<(u32, Bytes)> = Vec::new();
    if !resp.supported_features.is_empty() {
        tags.push((0, encode_supported_feature_keys(&resp.supported_features)?));
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
}

/// Encode Metadata. `topics = None` asks for all topics.
pub fn encode_metadata_request(
    buf: &mut BytesMut,
    version: i16,
    topics: Option<&[String]>,
    allow_auto: bool,
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
        buf.put_u8(0);
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode Metadata request: topic names (`None` is all topics) and `allow.auto.create.topics`.
pub fn decode_metadata_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Option<Vec<String>>, bool)> {
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
    if version >= 8 {
        buf::need(buf, 1)?;
        let _include_topic = buf.get_u8();
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((topics, allow_auto))
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
        if version >= 8 {
            let _authorized = buf::get_i32(buf)?;
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(TopicMetadata {
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
        });
    }
    if (8..=10).contains(&version) {
        let _cluster_authorized = buf::get_i32(buf)?;
    }
    if version >= 13 {
        let _top_error = buf::get_i16(buf)?;
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(MetadataResponse {
        throttle_time_ms,
        brokers,
        cluster_id,
        controller_id,
        topics,
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
            buf.put_i32(-2147483648);
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if (8..=10).contains(&version) {
        buf.put_i32(-2147483648);
    }
    if version >= 13 {
        buf.put_i16(0);
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
}

/// Encode Produce v3–8 (classic) or v9+ (flexible).
pub fn encode_produce_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: Option<&str>,
    acks: i16,
    timeout_ms: i32,
    topics: &[ProduceTopicData],
) -> Result<()> {
    let flexible = version >= 9;
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
    let flexible = version >= 9;
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
    let flexible = version >= 9;
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
                buf::put_empty_tagged_fields(buf);
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
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode Produce into per-partition results.
pub fn decode_produce_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<ProducePartitionResponse>> {
    let flexible = version >= 9;
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
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            out.push(ProducePartitionResponse {
                topic: topic.clone(),
                partition,
                error_code,
                base_offset,
                log_append_time_ms,
                log_start_offset,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
    }
    if version >= 1 {
        let _throttle = buf::get_i32(buf)?;
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
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
    fn produce_v9_roundtrip() {
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
        let (txn, acks, timeout, decoded) = decode_produce_request(&mut &buf[..], 9).unwrap();
        assert_eq!(txn, None);
        assert_eq!(acks, 1);
        assert_eq!(timeout, 1500);
        assert_eq!(decoded[0].topic, "t");
        assert_eq!(
            decoded[0].partitions[0].records.records[0].value.as_deref(),
            Some(&b"hi"[..])
        );

        let mut txn_buf = BytesMut::new();
        encode_produce_request(&mut txn_buf, 8, Some("tx-1"), 1, 1500, &topics).unwrap();
        let (txn, _, _, _) = decode_produce_request(&mut &txn_buf[..], 8).unwrap();
        assert_eq!(txn.as_deref(), Some("tx-1"));
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
            }],
        };
        let mut buf = BytesMut::new();
        encode_metadata_response(&mut buf, 12, &resp).unwrap();
        let decoded = decode_metadata_response(&mut &buf[..], 12).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn metadata_request_roundtrips_topics_and_allow_auto() {
        let topics = ["orders".to_string(), "payments".to_string()];
        let mut buf = BytesMut::new();
        encode_metadata_request(&mut buf, 12, Some(&topics), true).unwrap();
        let (got, allow) = decode_metadata_request(&mut &buf[..], 12).unwrap();
        assert_eq!(got.as_deref(), Some(topics.as_slice()));
        assert!(allow);

        let mut all = BytesMut::new();
        encode_metadata_request(&mut all, 12, None, false).unwrap();
        let (got, allow) = decode_metadata_request(&mut &all[..], 12).unwrap();
        assert!(got.is_none());
        assert!(!allow);
    }
}
