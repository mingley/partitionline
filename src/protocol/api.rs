#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use super::records::{self, RecordBatch};
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiVersion>,
    pub throttle_time_ms: i32,
}

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
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ApiVersionsResponse {
        error_code,
        api_keys,
        throttle_time_ms,
    })
}

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
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub name: Option<String>,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<Broker>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

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
            if version >= 5 {
                let _offline = get_int32_array(buf, flexible)?;
            }
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
                put_int32_array(buf, flexible, &[])?;
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

#[derive(Debug, Clone)]
pub struct ProduceTopicData {
    pub topic: String,
    pub partitions: Vec<ProducePartitionData>,
}

#[derive(Debug, Clone)]
pub struct ProducePartitionData {
    pub index: i32,
    pub records: RecordBatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionResponse {
    pub topic: String,
    pub partition: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

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

pub fn decode_produce_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, i32, Vec<ProduceTopicData>)> {
    let flexible = version >= 9;
    if version >= 3 {
        let _txn = buf::get_string(buf, flexible)?;
    }
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
    Ok((acks, timeout_ms, topics))
}

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
        };
        let mut buf = BytesMut::new();
        encode_api_versions_response(&mut buf, 3, &resp).unwrap();
        let decoded = decode_api_versions_response(&mut &buf[..], 3).unwrap();
        assert_eq!(decoded, resp);
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
        let (acks, timeout, decoded) = decode_produce_request(&mut &buf[..], 9).unwrap();
        assert_eq!(acks, 1);
        assert_eq!(timeout, 1500);
        assert_eq!(decoded[0].topic, "t");
        assert_eq!(
            decoded[0].partitions[0].records.records[0].value.as_deref(),
            Some(&b"hi"[..])
        );
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
                }],
            }],
        };
        let mut buf = BytesMut::new();
        encode_metadata_response(&mut buf, 12, &resp).unwrap();
        let decoded = decode_metadata_response(&mut &buf[..], 12).unwrap();
        assert_eq!(decoded, resp);
    }
}
