#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

pub const RESOURCE_TOPIC: i8 = 2;
pub const RESOURCE_BROKER: i8 = 4;
pub const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;
pub const CONFIG_SOURCE_DEFAULT: i8 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaAssignment {
    pub partition_index: i32,
    pub broker_ids: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfig {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub assignments: Vec<ReplicaAssignment>,
    pub configs: Vec<TopicConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsRequest {
    pub topics: Vec<CreatableTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicResult {
    pub name: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResource {
    pub resource_type: i8,
    pub name: String,
    pub keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSynonym {
    pub name: String,
    pub value: Option<String>,
    pub source: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    pub source: i8,
    pub is_sensitive: bool,
    pub synonyms: Vec<ConfigSynonym>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResult {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub name: String,
    pub entries: Vec<ConfigEntry>,
}

fn put_i32_array(buf: &mut BytesMut, items: &[i32]) -> crate::error::Result<()> {
    buf::put_array_len(buf, false, Some(items.len()))?;
    for v in items {
        buf.put_i32(*v);
    }
    Ok(())
}

fn get_i32_array<B: Buf>(buf: &mut B) -> Result<Vec<i32>> {
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(buf::get_i32(buf)?);
    }
    Ok(out)
}

fn get_string_array<B: Buf>(buf: &mut B) -> Result<Option<Vec<String>>> {
    let n = buf::get_array_len(buf, false)?;
    let Some(n) = n else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(
            buf::get_classic_nullable_string(buf)?
                .ok_or_else(|| Error::protocol("null string in array"))?,
        );
    }
    Ok(Some(out))
}

fn put_string_array(buf: &mut BytesMut, items: Option<&[String]>) -> crate::error::Result<()> {
    match items {
        None => buf::put_array_len(buf, false, None)?,
        Some(items) => {
            buf::put_array_len(buf, false, Some(items.len()))?;
            for s in items {
                buf::put_classic_nullable_string(buf, Some(s))?;
            }
        }
    }
    Ok(())
}

/// CreateTopics v0–4 (classic; flexible from v5).
pub fn encode_create_topics_request(
    buf: &mut BytesMut,
    version: i16,
    req: &CreateTopicsRequest,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, false, Some(req.topics.len()))?;
    for t in &req.topics {
        buf::put_classic_nullable_string(buf, Some(&t.name))?;
        buf.put_i32(t.num_partitions);
        buf.put_i16(t.replication_factor);
        buf::put_array_len(buf, false, Some(t.assignments.len()))?;
        for a in &t.assignments {
            buf.put_i32(a.partition_index);
            put_i32_array(buf, &a.broker_ids)?;
        }
        buf::put_array_len(buf, false, Some(t.configs.len()))?;
        for c in &t.configs {
            buf::put_classic_nullable_string(buf, Some(&c.name))?;
            buf::put_classic_nullable_string(buf, c.value.as_deref())?;
        }
    }
    buf.put_i32(req.timeout_ms);
    if version >= 1 {
        buf.put_u8(u8::from(req.validate_only));
    }
    Ok(())
}

pub fn decode_create_topics_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<CreateTopicsRequest> {
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let num_partitions = buf::get_i32(buf)?;
        let replication_factor = buf::get_i16(buf)?;
        let an = buf::get_array_len(buf, false)?.unwrap_or(0);
        let mut assignments = Vec::with_capacity(an);
        for _ in 0..an {
            let partition_index = buf::get_i32(buf)?;
            let broker_ids = get_i32_array(buf)?;
            assignments.push(ReplicaAssignment {
                partition_index,
                broker_ids,
            });
        }
        let cn = buf::get_array_len(buf, false)?.unwrap_or(0);
        let mut configs = Vec::with_capacity(cn);
        for _ in 0..cn {
            let name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
            let value = buf::get_classic_nullable_string(buf)?;
            configs.push(TopicConfig { name, value });
        }
        topics.push(CreatableTopic {
            name,
            num_partitions,
            replication_factor,
            assignments,
            configs,
        });
    }
    let timeout_ms = buf::get_i32(buf)?;
    let validate_only = if version >= 1 {
        buf::get_bool(buf)?
    } else {
        false
    };
    Ok(CreateTopicsRequest {
        topics,
        timeout_ms,
        validate_only,
    })
}

pub fn encode_create_topics_response(
    buf: &mut BytesMut,
    version: i16,
    results: &[TopicResult],
) -> crate::error::Result<()> {
    if version >= 2 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, false, Some(results.len()))?;
    for r in results {
        buf::put_classic_nullable_string(buf, Some(&r.name))?;
        buf.put_i16(r.error_code);
        if version >= 1 {
            buf::put_classic_nullable_string(buf, r.error_message.as_deref())?;
        }
    }
    Ok(())
}

pub fn decode_create_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<TopicResult>> {
    if version >= 2 {
        let _throttle = buf::get_i32(buf)?;
    }
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let error_code = buf::get_i16(buf)?;
        let error_message = if version >= 1 {
            buf::get_classic_nullable_string(buf)?
        } else {
            None
        };
        out.push(TopicResult {
            name,
            error_code,
            error_message,
        });
    }
    Ok(out)
}

/// DeleteTopics v0–3 (classic; flexible from v4).
pub fn encode_delete_topics_request(
    buf: &mut BytesMut,
    names: &[String],
    timeout_ms: i32,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, false, Some(names.len()))?;
    for name in names {
        buf::put_classic_nullable_string(buf, Some(name))?;
    }
    buf.put_i32(timeout_ms);
    Ok(())
}

pub fn decode_delete_topics_request<B: Buf>(buf: &mut B) -> Result<(Vec<String>, i32)> {
    let names = get_string_array(buf)?.unwrap_or_default();
    let timeout_ms = buf::get_i32(buf)?;
    Ok((names, timeout_ms))
}

pub fn encode_delete_topics_response(
    buf: &mut BytesMut,
    version: i16,
    results: &[TopicResult],
) -> crate::error::Result<()> {
    if version >= 1 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, false, Some(results.len()))?;
    for r in results {
        buf::put_classic_nullable_string(buf, Some(&r.name))?;
        buf.put_i16(r.error_code);
    }
    Ok(())
}

pub fn decode_delete_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<TopicResult>> {
    if version >= 1 {
        let _throttle = buf::get_i32(buf)?;
    }
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let error_code = buf::get_i16(buf)?;
        out.push(TopicResult {
            name,
            error_code,
            error_message: None,
        });
    }
    Ok(out)
}

/// DescribeConfigs v0–1 (classic; flexible from v4). v1 adds synonyms + config source.
pub fn encode_describe_configs_request(
    buf: &mut BytesMut,
    version: i16,
    resources: &[DescribeConfigsResource],
    include_synonyms: bool,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, false, Some(resources.len()))?;
    for r in resources {
        buf.put_i8(r.resource_type);
        buf::put_classic_nullable_string(buf, Some(&r.name))?;
        put_string_array(buf, r.keys.as_deref())?;
    }
    if version >= 1 {
        buf.put_u8(u8::from(include_synonyms));
    }
    Ok(())
}

pub fn decode_describe_configs_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<DescribeConfigsResource>, bool)> {
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut resources = Vec::with_capacity(n);
    for _ in 0..n {
        let resource_type = buf::get_i8(buf)?;
        let name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let keys = get_string_array(buf)?;
        resources.push(DescribeConfigsResource {
            resource_type,
            name,
            keys,
        });
    }
    let include_synonyms = if version >= 1 {
        buf::get_bool(buf)?
    } else {
        false
    };
    Ok((resources, include_synonyms))
}

pub fn encode_describe_configs_response(
    buf: &mut BytesMut,
    version: i16,
    results: &[DescribeConfigsResult],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(results.len()))?;
    for r in results {
        buf.put_i16(r.error_code);
        buf::put_classic_nullable_string(buf, r.error_message.as_deref())?;
        buf.put_i8(r.resource_type);
        buf::put_classic_nullable_string(buf, Some(&r.name))?;
        buf::put_array_len(buf, false, Some(r.entries.len()))?;
        for e in &r.entries {
            buf::put_classic_nullable_string(buf, Some(&e.name))?;
            buf::put_classic_nullable_string(buf, e.value.as_deref())?;
            buf.put_u8(u8::from(e.read_only));
            if version == 0 {
                buf.put_u8(u8::from(e.source == CONFIG_SOURCE_DEFAULT));
            } else {
                buf.put_i8(e.source);
            }
            buf.put_u8(u8::from(e.is_sensitive));
            if version >= 1 {
                buf::put_array_len(buf, false, Some(e.synonyms.len()))?;
                for s in &e.synonyms {
                    buf::put_classic_nullable_string(buf, Some(&s.name))?;
                    buf::put_classic_nullable_string(buf, s.value.as_deref())?;
                    buf.put_i8(s.source);
                }
            }
        }
    }
    Ok(())
}

pub fn decode_describe_configs_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<DescribeConfigsResult>> {
    let _throttle = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_classic_nullable_string(buf)?;
        let resource_type = buf::get_i8(buf)?;
        let name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let en = buf::get_array_len(buf, false)?.unwrap_or(0);
        let mut entries = Vec::with_capacity(en);
        for _ in 0..en {
            let ename = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
            let value = buf::get_classic_nullable_string(buf)?;
            let read_only = buf::get_bool(buf)?;
            let source = if version == 0 {
                if buf::get_bool(buf)? {
                    CONFIG_SOURCE_DEFAULT
                } else {
                    CONFIG_SOURCE_DYNAMIC_TOPIC
                }
            } else {
                buf::get_i8(buf)?
            };
            let is_sensitive = buf::get_bool(buf)?;
            let mut synonyms = Vec::new();
            if version >= 1 {
                let sn = buf::get_array_len(buf, false)?.unwrap_or(0);
                synonyms.reserve(sn);
                for _ in 0..sn {
                    let sname = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
                    let svalue = buf::get_classic_nullable_string(buf)?;
                    let ssource = buf::get_i8(buf)?;
                    synonyms.push(ConfigSynonym {
                        name: sname,
                        value: svalue,
                        source: ssource,
                    });
                }
            }
            entries.push(ConfigEntry {
                name: ename,
                value,
                read_only,
                source,
                is_sensitive,
                synonyms,
            });
        }
        out.push(DescribeConfigsResult {
            error_code,
            error_message,
            resource_type,
            name,
            entries,
        });
    }
    Ok(out)
}

pub const ALTER_CONFIG_SET: i8 = 0;
pub const ALTER_CONFIG_DELETE: i8 = 1;

pub fn encode_create_partitions_request(
    buf: &mut BytesMut,
    topics: &[(String, i32)],
    timeout_ms: i32,
    validate_only: bool,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, false, Some(topics.len()))?;
    for (name, count) in topics {
        buf::put_classic_nullable_string(buf, Some(name))?;
        buf.put_i32(*count);
        buf::put_array_len(buf, false, Some(0))?;
    }
    buf.put_i32(timeout_ms);
    buf.put_u8(u8::from(validate_only));
    Ok(())
}

pub fn decode_create_partitions_request<B: Buf>(buf: &mut B) -> Result<(Vec<(String, i32)>, bool)> {
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let count = buf::get_i32(buf)?;
        let an = buf::get_array_len(buf, false)?.unwrap_or(0);
        for _ in 0..an {
            let bn = buf::get_array_len(buf, false)?.unwrap_or(0);
            for _ in 0..bn {
                let _ = buf::get_i32(buf)?;
            }
        }
        topics.push((name, count));
    }
    let _timeout = buf::get_i32(buf)?;
    let validate_only = buf.get_u8() != 0;
    Ok((topics, validate_only))
}

pub fn encode_create_partitions_response(
    buf: &mut BytesMut,
    results: &[TopicResult],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(results.len()))?;
    for r in results {
        buf::put_classic_nullable_string(buf, Some(&r.name))?;
        buf.put_i16(r.error_code);
        buf::put_classic_nullable_string(buf, r.error_message.as_deref())?;
    }
    Ok(())
}

pub fn decode_create_partitions_response<B: Buf>(buf: &mut B) -> Result<Vec<TopicResult>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_classic_nullable_string(buf)?;
        out.push(TopicResult {
            name,
            error_code,
            error_message,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfig {
    pub name: String,
    pub op: i8,
    pub value: Option<String>,
}

pub fn encode_incremental_alter_configs_request(
    buf: &mut BytesMut,
    resource_type: i8,
    name: &str,
    configs: &[AlterConfig],
    validate_only: bool,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i8(resource_type);
    buf::put_classic_nullable_string(buf, Some(name))?;
    buf::put_array_len(buf, false, Some(configs.len()))?;
    for c in configs {
        buf::put_classic_nullable_string(buf, Some(&c.name))?;
        buf.put_i8(c.op);
        buf::put_classic_nullable_string(buf, c.value.as_deref())?;
    }
    buf.put_u8(u8::from(validate_only));
    Ok(())
}

pub fn decode_incremental_alter_configs_request<B: Buf>(
    buf: &mut B,
) -> Result<(i8, String, Vec<AlterConfig>, bool)> {
    let _n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let resource_type = buf::get_i8(buf)?;
    let name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let cn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut configs = Vec::with_capacity(cn);
    for _ in 0..cn {
        let cname = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let op = buf::get_i8(buf)?;
        let value = buf::get_classic_nullable_string(buf)?;
        configs.push(AlterConfig {
            name: cname,
            op,
            value,
        });
    }
    let validate_only = buf.get_u8() != 0;
    Ok((resource_type, name, configs, validate_only))
}

pub fn encode_incremental_alter_configs_response(
    buf: &mut BytesMut,
    error_code: i16,
    name: &str,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i16(error_code);
    buf::put_classic_nullable_string(buf, None)?;
    buf.put_i8(RESOURCE_TOPIC);
    buf::put_classic_nullable_string(buf, Some(name))?;
    Ok(())
}

pub fn decode_incremental_alter_configs_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    let _n = buf::get_array_len(buf, false)?.unwrap_or(0);
    buf::get_i16(buf)
}

pub fn encode_alter_configs_request(
    buf: &mut BytesMut,
    _version: i16,
    resource_type: i8,
    name: &str,
    configs: &[TopicConfig],
    validate_only: bool,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i8(resource_type);
    buf::put_classic_nullable_string(buf, Some(name))?;
    buf::put_array_len(buf, false, Some(configs.len()))?;
    for c in configs {
        buf::put_classic_nullable_string(buf, Some(&c.name))?;
        buf::put_classic_nullable_string(buf, c.value.as_deref())?;
    }
    buf.put_u8(u8::from(validate_only));
    Ok(())
}

pub fn decode_alter_configs_request<B: Buf>(
    buf: &mut B,
) -> Result<(i8, String, Vec<TopicConfig>, bool)> {
    let _n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let resource_type = buf::get_i8(buf)?;
    let name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let cn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut configs = Vec::with_capacity(cn);
    for _ in 0..cn {
        let cname = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let value = buf::get_classic_nullable_string(buf)?;
        configs.push(TopicConfig { name: cname, value });
    }
    let validate_only = buf.get_u8() != 0;
    Ok((resource_type, name, configs, validate_only))
}

pub fn encode_alter_configs_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    name: &str,
) -> crate::error::Result<()> {
    if version >= 1 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i16(error_code);
    buf::put_classic_nullable_string(buf, None)?;
    buf.put_i8(RESOURCE_TOPIC);
    buf::put_classic_nullable_string(buf, Some(name))?;
    Ok(())
}

pub fn decode_alter_configs_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    if version >= 1 {
        let _th = buf::get_i32(buf)?;
    }
    let _n = buf::get_array_len(buf, false)?.unwrap_or(0);
    buf::get_i16(buf)
}

pub fn encode_delete_records_request(
    buf: &mut BytesMut,
    topic: &str,
    partition: i32,
    offset: i64,
    timeout_ms: i32,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some(topic))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i32(partition);
    buf.put_i64(offset);
    buf.put_i32(timeout_ms);
    Ok(())
}

pub fn decode_delete_records_request<B: Buf>(buf: &mut B) -> Result<(String, i32, i64, i32)> {
    let _tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _pn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let partition = buf::get_i32(buf)?;
    let offset = buf::get_i64(buf)?;
    let timeout_ms = buf::get_i32(buf)?;
    Ok((topic, partition, offset, timeout_ms))
}

pub fn encode_delete_records_response(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    low_watermark: i64,
    error_code: i16,
) -> crate::error::Result<()> {
    if version >= 1 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some(topic))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i32(partition);
    buf.put_i64(low_watermark);
    buf.put_i16(error_code);
    Ok(())
}

pub fn decode_delete_records_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i32, i64, i16)> {
    if version >= 1 {
        let _th = buf::get_i32(buf)?;
    }
    let _tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let _topic = buf::get_classic_nullable_string(buf)?;
    let _pn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let partition = buf::get_i32(buf)?;
    let low_watermark = buf::get_i64(buf)?;
    let error_code = buf::get_i16(buf)?;
    Ok((partition, low_watermark, error_code))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDescription {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub brokers: Vec<super::api::Broker>,
}

pub fn encode_describe_cluster_request(
    buf: &mut BytesMut,
    include_authorized_operations: bool,
) -> crate::error::Result<()> {
    buf.put_u8(u8::from(include_authorized_operations));
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_cluster_request<B: Buf>(buf: &mut B) -> Result<bool> {
    let include = buf::get_bool(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(include)
}

pub fn encode_describe_cluster_response(
    buf: &mut BytesMut,
    desc: &ClusterDescription,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(desc.error_code);
    buf::put_compact_string(buf, desc.error_message.as_deref())?;
    buf::put_compact_string(buf, desc.cluster_id.as_deref())?;
    buf.put_i32(desc.controller_id);
    buf::put_array_len(buf, true, Some(desc.brokers.len()))?;
    for b in &desc.brokers {
        buf.put_i32(b.node_id);
        buf::put_compact_string(buf, Some(&b.host))?;
        buf.put_i32(b.port);
        buf::put_compact_string(buf, b.rack.as_deref())?;
        buf::put_empty_tagged_fields(buf);
    }
    buf.put_i32(i32::MIN);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_cluster_response<B: Buf>(buf: &mut B) -> Result<ClusterDescription> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let cluster_id = buf::get_compact_string(buf)?;
    let controller_id = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut brokers = Vec::with_capacity(n);
    for _ in 0..n {
        let node_id = buf::get_i32(buf)?;
        let host = buf::get_compact_string(buf)?.unwrap_or_default();
        let port = buf::get_i32(buf)?;
        let rack = buf::get_compact_string(buf)?;
        buf::skip_tagged_fields(buf)?;
        brokers.push(super::api::Broker {
            node_id,
            host,
            port,
            rack,
        });
    }
    let _ops = buf::get_i32(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(ClusterDescription {
        error_code,
        error_message,
        cluster_id,
        controller_id,
        brokers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_topics_v3_roundtrip() {
        let req = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "orders".into(),
                num_partitions: 6,
                replication_factor: 1,
                assignments: vec![ReplicaAssignment {
                    partition_index: 0,
                    broker_ids: vec![1],
                }],
                configs: vec![TopicConfig {
                    name: "cleanup.policy".into(),
                    value: Some("compact".into()),
                }],
            }],
            timeout_ms: 10_000,
            validate_only: false,
        };
        let mut buf = BytesMut::new();
        encode_create_topics_request(&mut buf, 3, &req).unwrap();
        let decoded = decode_create_topics_request(&mut &buf[..], 3).unwrap();
        assert_eq!(decoded, req);

        let results = vec![TopicResult {
            name: "orders".into(),
            error_code: 0,
            error_message: None,
        }];
        buf.clear();
        encode_create_topics_response(&mut buf, 3, &results).unwrap();
        assert_eq!(
            decode_create_topics_response(&mut &buf[..], 3).unwrap(),
            results
        );
    }

    #[test]
    fn create_topics_not_controller_is_not_at_byte_four() {
        let results = vec![TopicResult {
            name: "t".into(),
            error_code: crate::error::NOT_CONTROLLER,
            error_message: None,
        }];
        let mut buf = BytesMut::new();
        encode_create_topics_response(&mut buf, 4, &results).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "throttle + topic-array length must not look like error 41"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_create_topics_response(&mut cur, 4).unwrap(), results);
        assert!(
            !cur.has_remaining(),
            "CreateTopics v4 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn delete_topics_v3_roundtrip() {
        let names = vec!["orders".into(), "t".into()];
        let mut buf = BytesMut::new();
        encode_delete_topics_request(&mut buf, &names, 5000).unwrap();
        let (decoded, timeout) = decode_delete_topics_request(&mut &buf[..]).unwrap();
        assert_eq!(decoded, names);
        assert_eq!(timeout, 5000);

        let results = vec![
            TopicResult {
                name: "orders".into(),
                error_code: 0,
                error_message: None,
            },
            TopicResult {
                name: "t".into(),
                error_code: 3,
                error_message: None,
            },
        ];
        buf.clear();
        encode_delete_topics_response(&mut buf, 3, &results).unwrap();
        assert_eq!(
            decode_delete_topics_response(&mut &buf[..], 3).unwrap(),
            results
        );
    }

    #[test]
    fn create_partitions_not_controller_is_not_at_byte_four() {
        let results = vec![TopicResult {
            name: "t".into(),
            error_code: crate::error::NOT_CONTROLLER,
            error_message: None,
        }];
        let mut buf = BytesMut::new();
        encode_create_partitions_response(&mut buf, &results).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "throttle + topic-array length must not look like error 41"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_partitions_response(&mut cur).unwrap(),
            results
        );
        assert!(
            !cur.has_remaining(),
            "CreatePartitions v1 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn delete_topics_not_controller_is_not_at_byte_four() {
        let results = vec![TopicResult {
            name: "t".into(),
            error_code: crate::error::NOT_CONTROLLER,
            error_message: None,
        }];
        let mut buf = BytesMut::new();
        encode_delete_topics_response(&mut buf, 3, &results).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "throttle + topic-array length must not look like error 41"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_delete_topics_response(&mut cur, 3).unwrap(), results);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v3 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn describe_configs_v1_roundtrip() {
        let resources = vec![DescribeConfigsResource {
            resource_type: RESOURCE_TOPIC,
            name: "orders".into(),
            keys: Some(vec!["cleanup.policy".into()]),
        }];
        let mut buf = BytesMut::new();
        encode_describe_configs_request(&mut buf, 1, &resources, true).unwrap();
        let (decoded, syn) = decode_describe_configs_request(&mut &buf[..], 1).unwrap();
        assert_eq!(decoded, resources);
        assert!(syn);

        let results = vec![DescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: RESOURCE_TOPIC,
            name: "orders".into(),
            entries: vec![ConfigEntry {
                name: "cleanup.policy".into(),
                value: Some("compact".into()),
                read_only: false,
                source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
                synonyms: vec![ConfigSynonym {
                    name: "cleanup.policy".into(),
                    value: Some("delete".into()),
                    source: CONFIG_SOURCE_DEFAULT,
                }],
            }],
        }];
        buf.clear();
        encode_describe_configs_response(&mut buf, 1, &results).unwrap();
        assert_eq!(
            decode_describe_configs_response(&mut &buf[..], 1).unwrap(),
            results
        );
    }

    #[test]
    fn describe_configs_v0_is_default() {
        let results = vec![DescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: RESOURCE_BROKER,
            name: "1".into(),
            entries: vec![ConfigEntry {
                name: "log.retention.hours".into(),
                value: Some("168".into()),
                read_only: true,
                source: CONFIG_SOURCE_DEFAULT,
                is_sensitive: false,
                synonyms: vec![],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_describe_configs_response(&mut buf, 0, &results).unwrap();
        let decoded = decode_describe_configs_response(&mut &buf[..], 0).unwrap();
        assert_eq!(decoded[0].entries[0].source, CONFIG_SOURCE_DEFAULT);
        assert!(decoded[0].entries[0].synonyms.is_empty());
    }

    #[test]
    fn alter_configs_v1_roundtrip() {
        let mut buf = BytesMut::new();
        encode_alter_configs_request(
            &mut buf,
            1,
            RESOURCE_TOPIC,
            "t",
            &[TopicConfig {
                name: "retention.ms".into(),
                value: Some("1".into()),
            }],
            false,
        )
        .unwrap();
        let (rt, name, configs, validate) = decode_alter_configs_request(&mut &buf[..]).unwrap();
        assert_eq!(rt, RESOURCE_TOPIC);
        assert_eq!(name, "t");
        assert_eq!(
            configs,
            vec![TopicConfig {
                name: "retention.ms".into(),
                value: Some("1".into()),
            }]
        );
        assert!(!validate);
        buf.clear();
        encode_alter_configs_response(&mut buf, 1, 0, "t").unwrap();
        assert_eq!(decode_alter_configs_response(&mut &buf[..], 1).unwrap(), 0);
    }

    #[test]
    fn delete_records_v1_roundtrip() {
        let mut buf = BytesMut::new();
        encode_delete_records_request(&mut buf, "t", 0, 5, 1000).unwrap();
        let (topic, part, off, timeout) = decode_delete_records_request(&mut &buf[..]).unwrap();
        assert_eq!((topic.as_str(), part, off, timeout), ("t", 0, 5, 1000));
        buf.clear();
        encode_delete_records_response(&mut buf, 1, "t", 0, 5, 0).unwrap();
        let (p, low, err) = decode_delete_records_response(&mut &buf[..], 1).unwrap();
        assert_eq!((p, low, err), (0, 5, 0));
    }

    #[test]
    fn describe_cluster_v0_roundtrip() {
        let desc = ClusterDescription {
            error_code: 0,
            error_message: None,
            cluster_id: Some("mock".into()),
            controller_id: 1,
            brokers: vec![super::super::api::Broker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
            }],
        };
        let mut req = BytesMut::new();
        encode_describe_cluster_request(&mut req, false).unwrap();
        assert!(!decode_describe_cluster_request(&mut &req[..]).unwrap());
        let mut buf = BytesMut::new();
        encode_describe_cluster_response(&mut buf, &desc).unwrap();
        assert_eq!(
            decode_describe_cluster_response(&mut &buf[..]).unwrap(),
            desc
        );
    }
}
