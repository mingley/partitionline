#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Kafka SCRAM mechanism id (KIP-554 / `ScramMechanism`).
pub const SCRAM_SHA_256: i8 = 1;
/// Kafka SCRAM mechanism id (KIP-554 / `ScramMechanism`).
pub const SCRAM_SHA_512: i8 = 2;

pub const RESOURCE_TOPIC: i8 = 2;
pub const RESOURCE_BROKER: i8 = 4;
/// Config resource type for broker logger (KIP-1142 ListConfigResources).
pub const RESOURCE_BROKER_LOGGER: i8 = 8;
/// Config resource type for client metrics (KIP-714 / KIP-1142).
pub const RESOURCE_CLIENT_METRICS: i8 = 16;
/// Config resource type for consumer groups (KIP-1142).
pub const RESOURCE_GROUP: i8 = 32;
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
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let error_code = buf::get_i16(buf)?;
    if n > 0 {
        let _msg = buf::get_classic_nullable_string(buf)?;
        let _rt = buf::get_i8(buf)?;
        let _name = buf::get_classic_nullable_string(buf)?;
    }
    Ok(error_code)
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

/// One partition in AlterPartitionReassignments v0 (flexible).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignablePartition {
    pub partition_index: i32,
    pub replicas: Option<Vec<i32>>,
}

/// One topic in AlterPartitionReassignments v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignableTopic {
    pub name: String,
    pub partitions: Vec<ReassignablePartition>,
}

/// Per-partition result of AlterPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignmentPartitionResult {
    pub partition_index: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// Per-topic result of AlterPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignmentTopicResult {
    pub name: String,
    pub partitions: Vec<ReassignmentPartitionResult>,
}

/// AlterPartitionReassignments v0 response (top-level error after throttle).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub results: Vec<ReassignmentTopicResult>,
}

fn put_compact_nullable_i32_array(
    buf: &mut BytesMut,
    items: Option<&[i32]>,
) -> crate::error::Result<()> {
    match items {
        None => buf::put_array_len(buf, true, None)?,
        Some(items) => {
            buf::put_array_len(buf, true, Some(items.len()))?;
            for v in items {
                buf.put_i32(*v);
            }
        }
    }
    Ok(())
}

fn get_compact_nullable_i32_array<B: Buf>(buf: &mut B) -> Result<Option<Vec<i32>>> {
    let Some(n) = buf::get_array_len(buf, true)? else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(buf::get_i32(buf)?);
    }
    Ok(Some(out))
}

/// AlterPartitionReassignments v0 (flexible from v0; KIP-455).
pub fn encode_alter_partition_reassignments_request(
    buf: &mut BytesMut,
    timeout_ms: i32,
    topics: &[ReassignableTopic],
) -> crate::error::Result<()> {
    buf.put_i32(timeout_ms);
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for t in topics {
        buf::put_compact_string(buf, Some(&t.name))?;
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            put_compact_nullable_i32_array(buf, p.replicas.as_deref())?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_alter_partition_reassignments_request<B: Buf>(
    buf: &mut B,
) -> Result<(i32, Vec<ReassignableTopic>)> {
    let timeout_ms = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let replicas = get_compact_nullable_i32_array(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(ReassignablePartition {
                partition_index,
                replicas,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(ReassignableTopic { name, partitions });
    }
    buf::skip_tagged_fields(buf)?;
    Ok((timeout_ms, topics))
}

pub fn encode_alter_partition_reassignments_response(
    buf: &mut BytesMut,
    resp: &AlterPartitionReassignmentsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.results.len()))?;
    for t in &resp.results {
        buf::put_compact_string(buf, Some(&t.name))?;
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            buf.put_i16(p.error_code);
            buf::put_compact_string(buf, p.error_message.as_deref())?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_alter_partition_reassignments_response<B: Buf>(
    buf: &mut B,
) -> Result<AlterPartitionReassignmentsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let part_err = buf::get_i16(buf)?;
            let part_msg = buf::get_compact_string(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(ReassignmentPartitionResult {
                partition_index,
                error_code: part_err,
                error_message: part_msg,
            });
        }
        buf::skip_tagged_fields(buf)?;
        results.push(ReassignmentTopicResult { name, partitions });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(AlterPartitionReassignmentsResponse {
        error_code,
        error_message,
        results,
    })
}

/// One topic in ListPartitionReassignments v0 (flexible; topics nullable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListReassignmentTopic {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

/// One ongoing partition reassignment in the List response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OngoingPartitionReassignment {
    pub partition_index: i32,
    pub replicas: Vec<i32>,
    pub adding_replicas: Vec<i32>,
    pub removing_replicas: Vec<i32>,
}

/// One topic in ListPartitionReassignments response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OngoingTopicReassignment {
    pub name: String,
    pub partitions: Vec<OngoingPartitionReassignment>,
}

/// ListPartitionReassignments v0 response (top-level error after throttle).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub topics: Vec<OngoingTopicReassignment>,
}

fn put_compact_i32_array(buf: &mut BytesMut, items: &[i32]) -> crate::error::Result<()> {
    put_compact_nullable_i32_array(buf, Some(items))
}

fn get_compact_i32_array<B: Buf>(buf: &mut B) -> Result<Vec<i32>> {
    Ok(get_compact_nullable_i32_array(buf)?.unwrap_or_default())
}

/// ListPartitionReassignments v0 (flexible from v0; KIP-455).
///
/// `topics = None` lists every ongoing reassignment.
pub fn encode_list_partition_reassignments_request(
    buf: &mut BytesMut,
    timeout_ms: i32,
    topics: Option<&[ListReassignmentTopic]>,
) -> crate::error::Result<()> {
    buf.put_i32(timeout_ms);
    match topics {
        None => buf::put_array_len(buf, true, None)?,
        Some(topics) => {
            buf::put_array_len(buf, true, Some(topics.len()))?;
            for t in topics {
                buf::put_compact_string(buf, Some(&t.name))?;
                put_compact_i32_array(buf, &t.partition_indexes)?;
                buf::put_empty_tagged_fields(buf);
            }
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_list_partition_reassignments_request<B: Buf>(
    buf: &mut B,
) -> Result<(i32, Option<Vec<ListReassignmentTopic>>)> {
    let timeout_ms = buf::get_i32(buf)?;
    let topics = match buf::get_array_len(buf, true)? {
        None => None,
        Some(n) => {
            let mut topics = Vec::with_capacity(n);
            for _ in 0..n {
                let name = buf::get_compact_string(buf)?.unwrap_or_default();
                let partition_indexes = get_compact_i32_array(buf)?;
                buf::skip_tagged_fields(buf)?;
                topics.push(ListReassignmentTopic {
                    name,
                    partition_indexes,
                });
            }
            Some(topics)
        }
    };
    buf::skip_tagged_fields(buf)?;
    Ok((timeout_ms, topics))
}

pub fn encode_list_partition_reassignments_response(
    buf: &mut BytesMut,
    resp: &ListPartitionReassignmentsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for t in &resp.topics {
        buf::put_compact_string(buf, Some(&t.name))?;
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            put_compact_i32_array(buf, &p.replicas)?;
            put_compact_i32_array(buf, &p.adding_replicas)?;
            put_compact_i32_array(buf, &p.removing_replicas)?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_list_partition_reassignments_response<B: Buf>(
    buf: &mut B,
) -> Result<ListPartitionReassignmentsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let replicas = get_compact_i32_array(buf)?;
            let adding_replicas = get_compact_i32_array(buf)?;
            let removing_replicas = get_compact_i32_array(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(OngoingPartitionReassignment {
                partition_index,
                replicas,
                adding_replicas,
                removing_replicas,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(OngoingTopicReassignment { name, partitions });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ListPartitionReassignmentsResponse {
        error_code,
        error_message,
        topics,
    })
}

/// One finalized-feature update in UpdateFeatures v0 (flexible; KIP-584).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdateKey {
    pub name: String,
    pub max_version_level: i16,
    pub allow_downgrade: bool,
}

/// Per-feature result of UpdateFeatures v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatableFeatureResult {
    pub name: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// UpdateFeatures v0 response (top-level error after throttle).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFeaturesResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub results: Vec<UpdatableFeatureResult>,
}

/// UpdateFeatures v0 (flexible from v0; KIP-584).
///
/// Official Apache JSON (`validVersions: "0-2"`, `flexibleVersions: "0+"`)
/// and kafka-protocol 0.18.0: v0 encodes `AllowDowngrade` BOOLEAN; v1+
/// replaces it with `UpgradeType` and adds top-level `ValidateOnly`.
/// This crate targets v0.
pub fn encode_update_features_request(
    buf: &mut BytesMut,
    timeout_ms: i32,
    updates: &[FeatureUpdateKey],
) -> crate::error::Result<()> {
    buf.put_i32(timeout_ms);
    buf::put_array_len(buf, true, Some(updates.len()))?;
    for u in updates {
        buf::put_compact_string(buf, Some(&u.name))?;
        buf.put_i16(u.max_version_level);
        buf.put_u8(u8::from(u.allow_downgrade));
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_update_features_request<B: Buf>(buf: &mut B) -> Result<(i32, Vec<FeatureUpdateKey>)> {
    let timeout_ms = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut updates = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let max_version_level = buf::get_i16(buf)?;
        let allow_downgrade = buf::get_bool(buf)?;
        buf::skip_tagged_fields(buf)?;
        updates.push(FeatureUpdateKey {
            name,
            max_version_level,
            allow_downgrade,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok((timeout_ms, updates))
}

pub fn encode_update_features_response(
    buf: &mut BytesMut,
    resp: &UpdateFeaturesResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.results.len()))?;
    for r in &resp.results {
        buf::put_compact_string(buf, Some(&r.name))?;
        buf.put_i16(r.error_code);
        buf::put_compact_string(buf, r.error_message.as_deref())?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_update_features_response<B: Buf>(buf: &mut B) -> Result<UpdateFeaturesResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let feat_err = buf::get_i16(buf)?;
        let feat_msg = buf::get_compact_string(buf)?;
        buf::skip_tagged_fields(buf)?;
        results.push(UpdatableFeatureResult {
            name,
            error_code: feat_err,
            error_message: feat_msg,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(UpdateFeaturesResponse {
        error_code,
        error_message,
        results,
    })
}

/// One SCRAM credential to remove (AlterUserScramCredentials v0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialDeletion {
    pub name: String,
    pub mechanism: i8,
}

/// One SCRAM credential to insert or replace (AlterUserScramCredentials v0).
///
/// `salt` / `salted_password` are caller-supplied bytes. This type does not
/// hash a password. `Debug` redacts those fields.
#[derive(Clone, PartialEq, Eq)]
pub struct ScramCredentialUpsertion {
    pub name: String,
    pub mechanism: i8,
    pub iterations: i32,
    pub salt: Vec<u8>,
    pub salted_password: Vec<u8>,
}

impl fmt::Debug for ScramCredentialUpsertion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramCredentialUpsertion")
            .field("name", &self.name)
            .field("mechanism", &self.mechanism)
            .field("iterations", &self.iterations)
            .field("salt", &"<redacted>")
            .field("salted_password", &"<redacted>")
            .finish()
    }
}

/// Per-user result of AlterUserScramCredentials v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserScramCredentialsResult {
    pub user: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// AlterUserScramCredentials v0 (flexible from v0; KIP-554).
///
/// Official Apache JSON (`apiKey: 51`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`) and kafka-protocol 0.18.0: compact
/// `Deletions` of `{Name, Mechanism, tagged}`, compact `Upsertions` of
/// `{Name, Mechanism, Iterations, Salt, SaltedPassword, tagged}`, then
/// tagged. No timeout field. Response: `ThrottleTimeMs` INT32, compact
/// `Results` of `{User, ErrorCode, ErrorMessage, tagged}`, tagged. There
/// is no top-level `error_code` — 41 is on each result, after throttle,
/// the compact results length, and that result's compact `User`.
pub fn encode_alter_user_scram_credentials_request(
    buf: &mut BytesMut,
    deletions: &[ScramCredentialDeletion],
    upsertions: &[ScramCredentialUpsertion],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(deletions.len()))?;
    for d in deletions {
        buf::put_compact_string(buf, Some(&d.name))?;
        buf.put_i8(d.mechanism);
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_array_len(buf, true, Some(upsertions.len()))?;
    for u in upsertions {
        buf::put_compact_string(buf, Some(&u.name))?;
        buf.put_i8(u.mechanism);
        buf.put_i32(u.iterations);
        buf::put_compact_bytes(buf, Some(&u.salt))?;
        buf::put_compact_bytes(buf, Some(&u.salted_password))?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_alter_user_scram_credentials_request<B: Buf>(
    buf: &mut B,
) -> Result<(Vec<ScramCredentialDeletion>, Vec<ScramCredentialUpsertion>)> {
    let n_del = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut deletions = Vec::with_capacity(n_del);
    for _ in 0..n_del {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let mechanism = buf::get_i8(buf)?;
        buf::skip_tagged_fields(buf)?;
        deletions.push(ScramCredentialDeletion { name, mechanism });
    }
    let n_up = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut upsertions = Vec::with_capacity(n_up);
    for _ in 0..n_up {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let mechanism = buf::get_i8(buf)?;
        let iterations = buf::get_i32(buf)?;
        let salt = buf::get_compact_bytes(buf)?.unwrap_or_default();
        let salted_password = buf::get_compact_bytes(buf)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        upsertions.push(ScramCredentialUpsertion {
            name,
            mechanism,
            iterations,
            salt,
            salted_password,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok((deletions, upsertions))
}

pub fn encode_alter_user_scram_credentials_response(
    buf: &mut BytesMut,
    results: &[AlterUserScramCredentialsResult],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(results.len()))?;
    for r in results {
        buf::put_compact_string(buf, Some(&r.user))?;
        buf.put_i16(r.error_code);
        buf::put_compact_string(buf, r.error_message.as_deref())?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_alter_user_scram_credentials_response<B: Buf>(
    buf: &mut B,
) -> Result<Vec<AlterUserScramCredentialsResult>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let user = buf::get_compact_string(buf)?.unwrap_or_default();
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        buf::skip_tagged_fields(buf)?;
        results.push(AlterUserScramCredentialsResult {
            user,
            error_code,
            error_message,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(results)
}

/// One SCRAM mechanism + iteration count (DescribeUserScramCredentials v0).
///
/// Fixture metadata only. This is not a credential store and does not
/// carry salt or salted password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialInfo {
    pub mechanism: i8,
    pub iterations: i32,
}

/// Per-user result of DescribeUserScramCredentials v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUserScramCredentialsResult {
    pub user: String,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub credential_infos: Vec<ScramCredentialInfo>,
}

/// DescribeUserScramCredentials v0 body (api 50).
///
/// Official Apache JSON (`apiKey: 50`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`, listeners `broker` and `controller`) and
/// kafka-protocol 0.18.0: this crate targets v0. Request: nullable
/// compact `Users` of `{Name compact, tagged}`, tagged. Null or empty
/// means describe all users. Response: `ThrottleTimeMs` INT32, top-level
/// `ErrorCode` INT16, compact nullable `ErrorMessage`, compact `Results`
/// of `{User, ErrorCode, ErrorMessage, CredentialInfos[] of
/// {Mechanism INT8, Iterations INT32, tagged}, tagged}`, tagged.
/// Measured: **41 is the top-level ErrorCode at bytes 4–5**, after
/// throttle. Not a first-result field (AlterUserScramCredentials puts
/// 41 after compact User at bytes 11–12). Fixture users only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUserScramCredentialsResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub results: Vec<DescribeUserScramCredentialsResult>,
}

pub fn encode_describe_user_scram_credentials_request(
    buf: &mut BytesMut,
    users: Option<&[String]>,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, users.map(|u| u.len()))?;
    if let Some(users) = users {
        for name in users {
            buf::put_compact_string(buf, Some(name))?;
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_user_scram_credentials_request<B: Buf>(
    buf: &mut B,
) -> Result<Option<Vec<String>>> {
    let n = buf::get_array_len(buf, true)?;
    let users = match n {
        None => None,
        Some(n) => {
            let mut users = Vec::with_capacity(n);
            for _ in 0..n {
                let name = buf::get_compact_string(buf)?.unwrap_or_default();
                buf::skip_tagged_fields(buf)?;
                users.push(name);
            }
            Some(users)
        }
    };
    buf::skip_tagged_fields(buf)?;
    Ok(users)
}

pub fn encode_describe_user_scram_credentials_response(
    buf: &mut BytesMut,
    resp: &DescribeUserScramCredentialsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.results.len()))?;
    for r in &resp.results {
        buf::put_compact_string(buf, Some(&r.user))?;
        buf.put_i16(r.error_code);
        buf::put_compact_string(buf, r.error_message.as_deref())?;
        buf::put_array_len(buf, true, Some(r.credential_infos.len()))?;
        for c in &r.credential_infos {
            buf.put_i8(c.mechanism);
            buf.put_i32(c.iterations);
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_user_scram_credentials_response<B: Buf>(
    buf: &mut B,
) -> Result<DescribeUserScramCredentialsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let user = buf::get_compact_string(buf)?.unwrap_or_default();
        let user_error = buf::get_i16(buf)?;
        let user_message = buf::get_compact_string(buf)?;
        let cn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut credential_infos = Vec::with_capacity(cn);
        for _ in 0..cn {
            let mechanism = buf::get_i8(buf)?;
            let iterations = buf::get_i32(buf)?;
            buf::skip_tagged_fields(buf)?;
            credential_infos.push(ScramCredentialInfo {
                mechanism,
                iterations,
            });
        }
        buf::skip_tagged_fields(buf)?;
        results.push(DescribeUserScramCredentialsResult {
            user,
            error_code: user_error,
            error_message: user_message,
            credential_infos,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeUserScramCredentialsResponse {
        error_code,
        error_message,
        results,
    })
}

/// One quota entity component (type + optional name). Null name is the default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaEntity {
    pub entity_type: String,
    pub name: Option<String>,
}

impl ClientQuotaEntity {
    pub fn new(entity_type: impl Into<String>, name: Option<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            name,
        }
    }
}

/// MatchType 0: exact entity name (DescribeClientQuotas, KIP-219).
pub const QUOTA_MATCH_EXACT: i8 = 0;
/// MatchType 1: default entity (DescribeClientQuotas, KIP-219).
pub const QUOTA_MATCH_DEFAULT: i8 = 1;
/// MatchType 2: any specified name (DescribeClientQuotas, KIP-219).
pub const QUOTA_MATCH_ANY: i8 = 2;

/// One filter component in DescribeClientQuotas (api 48).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaFilterComponent {
    pub entity_type: String,
    pub match_type: i8,
    pub match_value: Option<String>,
}

impl ClientQuotaFilterComponent {
    pub fn new(
        entity_type: impl Into<String>,
        match_type: i8,
        match_value: Option<String>,
    ) -> Self {
        Self {
            entity_type: entity_type.into(),
            match_type,
            match_value,
        }
    }
}

/// One quota key/value in a DescribeClientQuotas entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaValue {
    pub key: String,
    pub value: f64,
}

impl ClientQuotaValue {
    pub fn new(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

/// One described quota entity plus its values.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaEntry {
    pub entity: Vec<ClientQuotaEntity>,
    pub values: Vec<ClientQuotaValue>,
}

impl ClientQuotaEntry {
    pub fn new(entity: Vec<ClientQuotaEntity>, values: Vec<ClientQuotaValue>) -> Self {
        Self { entity, values }
    }
}

/// DescribeClientQuotas v1 response body (top-level ErrorCode).
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClientQuotasResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub entries: Option<Vec<ClientQuotaEntry>>,
}

/// One quota key to set or remove (AlterClientQuotas).
///
/// `value` is ignored when `remove` is true. This is a fixture op, not a
/// live cluster quota store.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaOp {
    pub key: String,
    pub value: f64,
    pub remove: bool,
}

impl ClientQuotaOp {
    pub fn set(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
            remove: false,
        }
    }

    pub fn remove(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: 0.0,
            remove: true,
        }
    }
}

/// One entity plus its ops in AlterClientQuotas v1.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaAlteration {
    pub entity: Vec<ClientQuotaEntity>,
    pub ops: Vec<ClientQuotaOp>,
}

impl ClientQuotaAlteration {
    pub fn new(entity: Vec<ClientQuotaEntity>, ops: Vec<ClientQuotaOp>) -> Self {
        Self { entity, ops }
    }
}

/// Per-entry result of AlterClientQuotas v1. Error sits on the entry;
/// there is no top-level response error_code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaAlterationResult {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub entity: Vec<ClientQuotaEntity>,
}

/// AlterClientQuotas v1 (classic v0; flexible from v1; KIP-546 / KIP-599).
///
/// Official Apache JSON (`apiKey: 49`, `validVersions: "0-1"`,
/// `flexibleVersions: "1+"`) and kafka-protocol 0.18.0: this crate
/// targets v1, the version a client encodes (VERSIONS.max). v0 is not
/// flexible. Request: compact `Entries` of `{Entity compact
/// [{EntityType, EntityName nullable, tagged}], Ops compact [{Key,
/// Value FLOAT64, Remove BOOLEAN, tagged}], tagged}`, `ValidateOnly`
/// BOOLEAN, tagged. No timeout field. Response: `ThrottleTimeMs`
/// INT32, compact `Entries` of `{ErrorCode INT16, ErrorMessage
/// compact-nullable, Entity, tagged}`, tagged. There is no top-level
/// `error_code` — 41 is the first entry ErrorCode, after throttle and
/// the compact entries length (bytes 5–6 for a one-entry fixture).
pub fn encode_alter_client_quotas_request(
    buf: &mut BytesMut,
    entries: &[ClientQuotaAlteration],
    validate_only: bool,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(entries.len()))?;
    for e in entries {
        buf::put_array_len(buf, true, Some(e.entity.len()))?;
        for ent in &e.entity {
            buf::put_compact_string(buf, Some(&ent.entity_type))?;
            buf::put_compact_string(buf, ent.name.as_deref())?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_array_len(buf, true, Some(e.ops.len()))?;
        for op in &e.ops {
            buf::put_compact_string(buf, Some(&op.key))?;
            buf.put_f64(op.value);
            buf.put_u8(u8::from(op.remove));
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf.put_u8(u8::from(validate_only));
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_alter_client_quotas_request<B: Buf>(
    buf: &mut B,
) -> Result<(Vec<ClientQuotaAlteration>, bool)> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
        let en = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut entity = Vec::with_capacity(en);
        for _ in 0..en {
            let entity_type = buf::get_compact_string(buf)?.unwrap_or_default();
            let name = buf::get_compact_string(buf)?;
            buf::skip_tagged_fields(buf)?;
            entity.push(ClientQuotaEntity { entity_type, name });
        }
        let on = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut ops = Vec::with_capacity(on);
        for _ in 0..on {
            let key = buf::get_compact_string(buf)?.unwrap_or_default();
            let value = buf::get_f64(buf)?;
            let remove = buf::get_bool(buf)?;
            buf::skip_tagged_fields(buf)?;
            ops.push(ClientQuotaOp { key, value, remove });
        }
        buf::skip_tagged_fields(buf)?;
        entries.push(ClientQuotaAlteration { entity, ops });
    }
    let validate_only = buf::get_bool(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((entries, validate_only))
}

pub fn encode_alter_client_quotas_response(
    buf: &mut BytesMut,
    results: &[ClientQuotaAlterationResult],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(results.len()))?;
    for r in results {
        buf.put_i16(r.error_code);
        buf::put_compact_string(buf, r.error_message.as_deref())?;
        buf::put_array_len(buf, true, Some(r.entity.len()))?;
        for ent in &r.entity {
            buf::put_compact_string(buf, Some(&ent.entity_type))?;
            buf::put_compact_string(buf, ent.name.as_deref())?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_alter_client_quotas_response<B: Buf>(
    buf: &mut B,
) -> Result<Vec<ClientQuotaAlterationResult>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        let en = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut entity = Vec::with_capacity(en);
        for _ in 0..en {
            let entity_type = buf::get_compact_string(buf)?.unwrap_or_default();
            let name = buf::get_compact_string(buf)?;
            buf::skip_tagged_fields(buf)?;
            entity.push(ClientQuotaEntity { entity_type, name });
        }
        buf::skip_tagged_fields(buf)?;
        results.push(ClientQuotaAlterationResult {
            error_code,
            error_message,
            entity,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(results)
}

/// DescribeClientQuotas v1 (classic v0; flexible from v1; KIP-219).
///
/// Official Apache JSON (`apiKey: 48`, `validVersions: "0-1"`,
/// `flexibleVersions: "1+"`, listeners `broker` only) and
/// kafka-protocol 0.18.0: this crate targets v1, the version a client
/// encodes (VERSIONS.max). v0 is not flexible. Request: compact
/// `Components` of `{EntityType, MatchType INT8 (0 exact / 1 default /
/// 2 any), Match nullable, tagged}`, `Strict` BOOLEAN, tagged. Response:
/// `ThrottleTimeMs` INT32, **top-level `ErrorCode` INT16**, compact
/// nullable `ErrorMessage`, compact nullable `Entries` of `{Entity
/// compact [{EntityType, EntityName nullable, tagged}], Values compact
/// [{Key, Value FLOAT64, tagged}], tagged}`, tagged. Measured
/// independently from kafka-protocol 0.18.0 (`client` encodes the
/// request; `broker` encodes the response): **the top-level ErrorCode
/// is the INT16 at bytes 4–5**, after throttle — not a first-result
/// field (AlterClientQuotas puts the first-entry code at bytes 5–6).
/// This is not a controller hop.
pub fn encode_describe_client_quotas_request(
    buf: &mut BytesMut,
    components: &[ClientQuotaFilterComponent],
    strict: bool,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(components.len()))?;
    for c in components {
        buf::put_compact_string(buf, Some(&c.entity_type))?;
        buf.put_i8(c.match_type);
        buf::put_compact_string(buf, c.match_value.as_deref())?;
        buf::put_empty_tagged_fields(buf);
    }
    buf.put_u8(u8::from(strict));
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_client_quotas_request<B: Buf>(
    buf: &mut B,
) -> Result<(Vec<ClientQuotaFilterComponent>, bool)> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut components = Vec::with_capacity(n);
    for _ in 0..n {
        let entity_type = buf::get_compact_string(buf)?.unwrap_or_default();
        let match_type = buf::get_i8(buf)?;
        let match_value = buf::get_compact_string(buf)?;
        buf::skip_tagged_fields(buf)?;
        components.push(ClientQuotaFilterComponent {
            entity_type,
            match_type,
            match_value,
        });
    }
    let strict = buf::get_bool(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((components, strict))
}

pub fn encode_describe_client_quotas_response(
    buf: &mut BytesMut,
    resp: &DescribeClientQuotasResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    match &resp.entries {
        None => buf::put_array_len(buf, true, None)?,
        Some(entries) => {
            buf::put_array_len(buf, true, Some(entries.len()))?;
            for e in entries {
                buf::put_array_len(buf, true, Some(e.entity.len()))?;
                for ent in &e.entity {
                    buf::put_compact_string(buf, Some(&ent.entity_type))?;
                    buf::put_compact_string(buf, ent.name.as_deref())?;
                    buf::put_empty_tagged_fields(buf);
                }
                buf::put_array_len(buf, true, Some(e.values.len()))?;
                for v in &e.values {
                    buf::put_compact_string(buf, Some(&v.key))?;
                    buf.put_f64(v.value);
                    buf::put_empty_tagged_fields(buf);
                }
                buf::put_empty_tagged_fields(buf);
            }
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_client_quotas_response<B: Buf>(
    buf: &mut B,
) -> Result<DescribeClientQuotasResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let entries = match buf::get_array_len(buf, true)? {
        None => None,
        Some(n) => {
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                let en = buf::get_array_len(buf, true)?.unwrap_or(0);
                let mut entity = Vec::with_capacity(en);
                for _ in 0..en {
                    let entity_type = buf::get_compact_string(buf)?.unwrap_or_default();
                    let name = buf::get_compact_string(buf)?;
                    buf::skip_tagged_fields(buf)?;
                    entity.push(ClientQuotaEntity { entity_type, name });
                }
                let vn = buf::get_array_len(buf, true)?.unwrap_or(0);
                let mut values = Vec::with_capacity(vn);
                for _ in 0..vn {
                    let key = buf::get_compact_string(buf)?.unwrap_or_default();
                    let value = buf::get_f64(buf)?;
                    buf::skip_tagged_fields(buf)?;
                    values.push(ClientQuotaValue { key, value });
                }
                buf::skip_tagged_fields(buf)?;
                entries.push(ClientQuotaEntry { entity, values });
            }
            Some(entries)
        }
    };
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeClientQuotasResponse {
        error_code,
        error_message,
        entries,
    })
}

/// One active producer in DescribeProducers (api 61, KIP-360).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveProducer {
    pub producer_id: i64,
    pub producer_epoch: i32,
    pub last_sequence: i32,
    pub last_timestamp: i64,
    pub coordinator_epoch: i32,
    pub current_txn_start_offset: i64,
}

impl ActiveProducer {
    pub fn new(
        producer_id: i64,
        producer_epoch: i32,
        last_sequence: i32,
        last_timestamp: i64,
        coordinator_epoch: i32,
        current_txn_start_offset: i64,
    ) -> Self {
        Self {
            producer_id,
            producer_epoch,
            last_sequence,
            last_timestamp,
            coordinator_epoch,
            current_txn_start_offset,
        }
    }
}

/// Per-partition DescribeProducers result. ErrorCode sits here, not
/// at the top of the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersPartition {
    pub partition_index: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub active_producers: Vec<ActiveProducer>,
}

impl DescribeProducersPartition {
    pub fn new(
        partition_index: i32,
        error_code: i16,
        error_message: Option<String>,
        active_producers: Vec<ActiveProducer>,
    ) -> Self {
        Self {
            partition_index,
            error_code,
            error_message,
            active_producers,
        }
    }
}

/// One topic in a DescribeProducers v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersTopic {
    pub name: String,
    pub partitions: Vec<DescribeProducersPartition>,
}

impl DescribeProducersTopic {
    pub fn new(name: impl Into<String>, partitions: Vec<DescribeProducersPartition>) -> Self {
        Self {
            name: name.into(),
            partitions,
        }
    }
}

/// DescribeProducers v0 response body. There is no top-level ErrorCode
/// after throttle; the first-partition code is later in the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersResponse {
    pub topics: Vec<DescribeProducersTopic>,
}

impl DescribeProducersResponse {
    pub fn new(topics: Vec<DescribeProducersTopic>) -> Self {
        Self { topics }
    }
}

/// DescribeProducers v0 (flexible from v0; KIP-360).
///
/// Official Apache JSON (`apiKey: 61`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`, listeners `broker` only) and
/// kafka-protocol 0.18.0 (`DescribeProducersRequest` /
/// `DescribeProducersResponse`, `VERSIONS` min=max=0). This crate
/// targets v0, the only version. Request encode used
/// `features = ["client"]`; response encode used `broker`.
/// Request: compact `Topics` of `{Name, compact PartitionIndexes
/// INT32[], tagged}`, tagged. Response: `ThrottleTimeMs` INT32,
/// compact `Topics` of `{Name, compact Partitions of
/// {PartitionIndex INT32, ErrorCode INT16, compact nullable
/// ErrorMessage, compact ActiveProducers of {ProducerId INT64,
/// ProducerEpoch INT32, LastSequence INT32, LastTimestamp INT64,
/// CoordinatorEpoch INT32, CurrentTxnStartOffset INT64, tagged},
/// tagged}, tagged}`, tagged. **ErrorCode is per-partition**, not
/// top-level. Measured independently on leftover-empty fixture topic
/// `"t"` partition `0`: the first-partition ErrorCode is the INT16
/// at **bytes 12–13**, after throttle, compact topics len, compact
/// name `"t"`, compact partitions len, and PartitionIndex — not
/// bytes 4–5. This is a partition-leader hop, not a controller hop
/// and not a transaction-coordinator hop.
pub fn encode_describe_producers_request(
    buf: &mut BytesMut,
    topic: &str,
    partitions: &[i32],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(1))?;
    buf::put_compact_string(buf, Some(topic))?;
    buf::put_array_len(buf, true, Some(partitions.len()))?;
    for p in partitions {
        buf.put_i32(*p);
    }
    buf::put_empty_tagged_fields(buf);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_producers_request<B: Buf>(buf: &mut B) -> Result<(String, Vec<i32>)> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topic = String::new();
    let mut partitions = Vec::new();
    for i in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut idxs = Vec::with_capacity(pn);
        for _ in 0..pn {
            idxs.push(buf::get_i32(buf)?);
        }
        buf::skip_tagged_fields(buf)?;
        if i == 0 {
            topic = name;
            partitions = idxs;
        }
    }
    buf::skip_tagged_fields(buf)?;
    Ok((topic, partitions))
}

pub fn encode_describe_producers_response(
    buf: &mut BytesMut,
    resp: &DescribeProducersResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for topic in &resp.topics {
        buf::put_compact_string(buf, Some(&topic.name))?;
        buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
        for p in &topic.partitions {
            buf.put_i32(p.partition_index);
            buf.put_i16(p.error_code);
            buf::put_compact_string(buf, p.error_message.as_deref())?;
            buf::put_array_len(buf, true, Some(p.active_producers.len()))?;
            for prod in &p.active_producers {
                buf.put_i64(prod.producer_id);
                buf.put_i32(prod.producer_epoch);
                buf.put_i32(prod.last_sequence);
                buf.put_i64(prod.last_timestamp);
                buf.put_i32(prod.coordinator_epoch);
                buf.put_i64(prod.current_txn_start_offset);
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_producers_response<B: Buf>(
    buf: &mut B,
) -> Result<DescribeProducersResponse> {
    let _th = buf::get_i32(buf)?;
    let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let error_message = buf::get_compact_string(buf)?;
            let an = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut active_producers = Vec::with_capacity(an);
            for _ in 0..an {
                let producer_id = buf::get_i64(buf)?;
                let producer_epoch = buf::get_i32(buf)?;
                let last_sequence = buf::get_i32(buf)?;
                let last_timestamp = buf::get_i64(buf)?;
                let coordinator_epoch = buf::get_i32(buf)?;
                let current_txn_start_offset = buf::get_i64(buf)?;
                buf::skip_tagged_fields(buf)?;
                active_producers.push(ActiveProducer {
                    producer_id,
                    producer_epoch,
                    last_sequence,
                    last_timestamp,
                    coordinator_epoch,
                    current_txn_start_offset,
                });
            }
            buf::skip_tagged_fields(buf)?;
            partitions.push(DescribeProducersPartition {
                partition_index,
                error_code,
                error_message,
                active_producers,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(DescribeProducersTopic { name, partitions });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeProducersResponse { topics })
}

/// AllocateProducerIds v0 response (top-level error after throttle).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocateProducerIdsResponse {
    pub error_code: i16,
    pub producer_id_start: i64,
    pub producer_id_len: i32,
}

/// AllocateProducerIds v0 (flexible from v0; KIP-730).
///
/// Official Apache JSON (`apiKey: 67`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`) and kafka-protocol 0.18.0: this crate
/// targets v0, the only version a client encodes (`VERSIONS` min=max=0).
/// v0 is flexible. Request: `BrokerId` INT32, `BrokerEpoch` INT64,
/// tagged. No timeout field. Response: `ThrottleTimeMs` INT32,
/// `ErrorCode` INT16, `ProducerIdStart` INT64, `ProducerIdLen` INT32,
/// tagged. There is a top-level `error_code` — 41 is that INT16 after
/// throttle (bytes 4–5). Fixture broker id/epoch only; not a live
/// cluster PID allocator.
pub fn encode_allocate_producer_ids_request(
    buf: &mut BytesMut,
    broker_id: i32,
    broker_epoch: i64,
) -> crate::error::Result<()> {
    buf.put_i32(broker_id);
    buf.put_i64(broker_epoch);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_allocate_producer_ids_request<B: Buf>(buf: &mut B) -> Result<(i32, i64)> {
    let broker_id = buf::get_i32(buf)?;
    let broker_epoch = buf::get_i64(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((broker_id, broker_epoch))
}

pub fn encode_allocate_producer_ids_response(
    buf: &mut BytesMut,
    resp: &AllocateProducerIdsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf.put_i64(resp.producer_id_start);
    buf.put_i32(resp.producer_id_len);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_allocate_producer_ids_response<B: Buf>(
    buf: &mut B,
) -> Result<AllocateProducerIdsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let producer_id_start = buf::get_i64(buf)?;
    let producer_id_len = buf::get_i32(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(AllocateProducerIdsResponse {
        error_code,
        producer_id_start,
        producer_id_len,
    })
}

/// One topic in a DescribeTransactions v0 transaction state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

/// One transactional.id result from DescribeTransactions (api 65) v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionState {
    pub error_code: i16,
    pub transactional_id: String,
    pub transaction_state: String,
    pub transaction_timeout_ms: i32,
    pub transaction_start_time_ms: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub topics: Vec<TransactionTopic>,
}

/// DescribeTransactions v0 (flexible from v0; KIP-664).
///
/// Official Apache JSON (`apiKey: 65`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`) and kafka-protocol 0.18.0: this crate
/// targets v0, the only version a client encodes (`VERSIONS` min=max=0).
/// v0 is flexible. Request: compact `TransactionalIds` `[]string`,
/// tagged. No timeout field. Response: `ThrottleTimeMs` INT32, compact
/// `TransactionStates` of `{ErrorCode INT16, TransactionalId compact,
/// TransactionState compact, TransactionTimeoutMs INT32,
/// TransactionStartTimeMs INT64, ProducerId INT64, ProducerEpoch INT16,
/// Topics compact [{Topic compact, Partitions compact []INT32, tagged}],
/// tagged}`, tagged. There is no top-level `error_code` — 16 is the
/// first result ErrorCode, after throttle and the compact states length
/// (bytes 5–6 for a one-result fixture). Fixture transactional ids only.
pub fn encode_describe_transactions_request(
    buf: &mut BytesMut,
    transactional_ids: &[String],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(transactional_ids.len()))?;
    for id in transactional_ids {
        buf::put_compact_string(buf, Some(id))?;
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_transactions_request<B: Buf>(buf: &mut B) -> Result<Vec<String>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        ids.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ids)
}

pub fn encode_describe_transactions_response(
    buf: &mut BytesMut,
    states: &[TransactionState],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(states.len()))?;
    for s in states {
        buf.put_i16(s.error_code);
        buf::put_compact_string(buf, Some(&s.transactional_id))?;
        buf::put_compact_string(buf, Some(&s.transaction_state))?;
        buf.put_i32(s.transaction_timeout_ms);
        buf.put_i64(s.transaction_start_time_ms);
        buf.put_i64(s.producer_id);
        buf.put_i16(s.producer_epoch);
        buf::put_array_len(buf, true, Some(s.topics.len()))?;
        for t in &s.topics {
            buf::put_compact_string(buf, Some(&t.name))?;
            buf::put_array_len(buf, true, Some(t.partitions.len()))?;
            for p in &t.partitions {
                buf.put_i32(*p);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_transactions_response<B: Buf>(buf: &mut B) -> Result<Vec<TransactionState>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut states = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let transactional_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let transaction_state = buf::get_compact_string(buf)?.unwrap_or_default();
        let transaction_timeout_ms = buf::get_i32(buf)?;
        let transaction_start_time_ms = buf::get_i64(buf)?;
        let producer_id = buf::get_i64(buf)?;
        let producer_epoch = buf::get_i16(buf)?;
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let name = buf::get_compact_string(buf)?.unwrap_or_default();
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                partitions.push(buf::get_i32(buf)?);
            }
            buf::skip_tagged_fields(buf)?;
            topics.push(TransactionTopic { name, partitions });
        }
        buf::skip_tagged_fields(buf)?;
        states.push(TransactionState {
            error_code,
            transactional_id,
            transaction_state,
            transaction_timeout_ms,
            transaction_start_time_ms,
            producer_id,
            producer_epoch,
            topics,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(states)
}

/// One transactional.id listing from ListTransactions (api 66) v0.
///
/// This is not [`TransactionState`] (DescribeTransactions api 65).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionListing {
    pub transactional_id: String,
    pub producer_id: i64,
    pub transaction_state: String,
}

/// ListTransactions v0 body (api 66).
///
/// Official Apache JSON (`apiKey: 66`, `validVersions: "0-2"`,
/// `flexibleVersions: "0+"`) and kafka-protocol 0.18.0: this crate
/// targets v0. kafka-protocol `VERSIONS` is 0–2; v1 adds
/// `DurationFilter` and v2 adds `TransactionalIdPattern`. A client
/// encodes v0 when those fields are unset. v0 is flexible.
/// Request: compact `StateFilters` `[]string`, compact
/// `ProducerIdFilters` `[]INT64`, tagged.
/// Response: `ThrottleTimeMs` INT32, top-level `ErrorCode` INT16,
/// compact `UnknownStateFilters` `[]string`, compact
/// `TransactionStates` of `{TransactionalId compact, ProducerId INT64,
/// TransactionState compact, tagged}`, tagged.
/// Measured: **16 is the top-level ErrorCode at bytes 4–5**, after
/// throttle. Not a first-result field (DescribeTransactions puts 16
/// at bytes 5–6). Fixture transactional ids only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTransactionsResponse {
    pub error_code: i16,
    pub unknown_state_filters: Vec<String>,
    pub transaction_states: Vec<TransactionListing>,
}

pub fn encode_list_transactions_request(
    buf: &mut BytesMut,
    state_filters: &[String],
    producer_id_filters: &[i64],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(state_filters.len()))?;
    for state in state_filters {
        buf::put_compact_string(buf, Some(state))?;
    }
    buf::put_array_len(buf, true, Some(producer_id_filters.len()))?;
    for id in producer_id_filters {
        buf.put_i64(*id);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_list_transactions_request<B: Buf>(buf: &mut B) -> Result<(Vec<String>, Vec<i64>)> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut state_filters = Vec::with_capacity(n);
    for _ in 0..n {
        state_filters.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut producer_id_filters = Vec::with_capacity(pn);
    for _ in 0..pn {
        producer_id_filters.push(buf::get_i64(buf)?);
    }
    buf::skip_tagged_fields(buf)?;
    Ok((state_filters, producer_id_filters))
}

pub fn encode_list_transactions_response(
    buf: &mut BytesMut,
    resp: &ListTransactionsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, true, Some(resp.unknown_state_filters.len()))?;
    for state in &resp.unknown_state_filters {
        buf::put_compact_string(buf, Some(state))?;
    }
    buf::put_array_len(buf, true, Some(resp.transaction_states.len()))?;
    for t in &resp.transaction_states {
        buf::put_compact_string(buf, Some(&t.transactional_id))?;
        buf.put_i64(t.producer_id);
        buf::put_compact_string(buf, Some(&t.transaction_state))?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_list_transactions_response<B: Buf>(buf: &mut B) -> Result<ListTransactionsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let un = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut unknown_state_filters = Vec::with_capacity(un);
    for _ in 0..un {
        unknown_state_filters.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut transaction_states = Vec::with_capacity(n);
    for _ in 0..n {
        let transactional_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let producer_id = buf::get_i64(buf)?;
        let transaction_state = buf::get_compact_string(buf)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        transaction_states.push(TransactionListing {
            transactional_id,
            producer_id,
            transaction_state,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ListTransactionsResponse {
        error_code,
        unknown_state_filters,
        transaction_states,
    })
}

/// UnregisterBroker v0 body (api 64, KIP-500).
///
/// Official Apache JSON (`apiKey: 64`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`, listeners `broker` and `controller`) and
/// kafka-protocol 0.18.0: this crate targets v0, the only version a
/// client encodes (`VERSIONS` min=max=0). v0 is flexible. Request:
/// `BrokerId` INT32, tagged. Response: `ThrottleTimeMs` INT32, top-level
/// `ErrorCode` INT16, compact nullable `ErrorMessage`, tagged.
/// Measured independently from kafka-protocol 0.18.0 (`client` encodes
/// the request; `broker` encodes the response): **41 is the top-level
/// ErrorCode at bytes 4–5**, after throttle. Not a first-result field
/// (AlterUserScramCredentials puts 41 after compact User at bytes
/// 11–12; DescribeTransactions puts the first-result code at bytes
/// 5–6). Fixture broker id only; not a live KRaft unregistration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnregisterBrokerResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
}

pub fn encode_unregister_broker_request(
    buf: &mut BytesMut,
    broker_id: i32,
) -> crate::error::Result<()> {
    buf.put_i32(broker_id);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_unregister_broker_request<B: Buf>(buf: &mut B) -> Result<i32> {
    let broker_id = buf::get_i32(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(broker_id)
}

pub fn encode_unregister_broker_response(
    buf: &mut BytesMut,
    resp: &UnregisterBrokerResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_unregister_broker_response<B: Buf>(buf: &mut B) -> Result<UnregisterBrokerResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(UnregisterBrokerResponse {
        error_code,
        error_message,
    })
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

/// Omitted authorized-operations bitfield (`INT32` min). Official default.
pub const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

/// One assigned topic in ConsumerGroupDescribe (api 69) Assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupTopicPartitions {
    pub topic_id: [u8; 16],
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

impl ConsumerGroupTopicPartitions {
    pub fn new(topic_id: [u8; 16], topic_name: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            topic_id,
            topic_name: topic_name.into(),
            partitions,
        }
    }
}

/// Current or target assignment for one described member.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsumerGroupAssignment {
    pub topic_partitions: Vec<ConsumerGroupTopicPartitions>,
}

impl ConsumerGroupAssignment {
    pub fn new(topic_partitions: Vec<ConsumerGroupTopicPartitions>) -> Self {
        Self { topic_partitions }
    }
}

/// One member in a ConsumerGroupDescribe v1 group.
///
/// `member_type` is v1+ (`-1` unknown, `0` classic, `1` consumer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMember {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub member_epoch: i32,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub subscribed_topic_regex: Option<String>,
    pub assignment: ConsumerGroupAssignment,
    pub target_assignment: ConsumerGroupAssignment,
    pub member_type: i8,
}

impl ConsumerGroupMember {
    pub fn new(
        member_id: impl Into<String>,
        member_epoch: i32,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            instance_id: None,
            rack_id: None,
            member_epoch,
            client_id: client_id.into(),
            client_host: client_host.into(),
            subscribed_topic_names: Vec::new(),
            subscribed_topic_regex: None,
            assignment: ConsumerGroupAssignment::default(),
            target_assignment: ConsumerGroupAssignment::default(),
            member_type: -1,
        }
    }
}

/// One described group in ConsumerGroupDescribe (api 69) v1.
///
/// ErrorCode sits here, not at the top of the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedConsumerGroup {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub group_id: String,
    pub group_state: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignor_name: String,
    pub members: Vec<ConsumerGroupMember>,
    pub authorized_operations: i32,
}

impl DescribedConsumerGroup {
    pub fn new(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            group_id: group_id.into(),
            group_state: String::new(),
            group_epoch: 0,
            assignment_epoch: 0,
            assignor_name: String::new(),
            members: Vec::new(),
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }
}

/// ConsumerGroupDescribe v1 (flexible from v0; KIP-848 / KIP-1099).
///
/// Official Apache JSON (`apiKey: 69`, `validVersions: "0-1"`,
/// `flexibleVersions: "0+"`, request listeners `broker`) and
/// kafka-protocol 0.18.0 (`ConsumerGroupDescribeRequest` /
/// `ConsumerGroupDescribeResponse`, `VERSIONS` min=0 max=1). This crate
/// targets v1, the version a client encodes (`VERSIONS.max`). v0 is the
/// same layout minus `MemberType`. Request encode used
/// `features = ["client"]`; response encode used `broker`.
/// Request: compact `GroupIds`, `IncludeAuthorizedOperations` BOOLEAN,
/// tagged. Response: `ThrottleTimeMs` INT32, compact `Groups` of
/// `{ErrorCode INT16, compact nullable ErrorMessage, GroupId, GroupState,
/// GroupEpoch INT32, AssignmentEpoch INT32, AssignorName, compact
/// Members of {MemberId, compact nullable InstanceId, compact nullable
/// RackId, MemberEpoch INT32, ClientId, ClientHost, compact
/// SubscribedTopicNames, compact nullable SubscribedTopicRegex,
/// Assignment, TargetAssignment, MemberType INT8 (v1+), tagged},
/// AuthorizedOperations INT32, tagged}`, tagged. Assignment is compact
/// TopicPartitions of `{TopicId UUID, TopicName, compact Partitions
/// INT32[], tagged}`. **ErrorCode is per-group**, the first field of
/// each DescribedGroup — not a top-level code after throttle. Measured
/// independently on leftover-empty fixture group `"g"`: the first-group
/// ErrorCode is the INT16 at **bytes 5–6**, after throttle and the
/// compact groups length — not bytes 4–5 (DescribeClientQuotas) or
/// 12–13 (DescribeProducers first partition). Official response
/// supported errors include `NOT_COORDINATOR`. This is a
/// group-coordinator hop, not a controller hop and not a
/// partition-leader hop.
pub fn encode_consumer_group_describe_request(
    buf: &mut BytesMut,
    group_ids: &[String],
    include_authorized_operations: bool,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(group_ids.len()))?;
    for id in group_ids {
        buf::put_compact_string(buf, Some(id))?;
    }
    buf.put_u8(u8::from(include_authorized_operations));
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_consumer_group_describe_request<B: Buf>(buf: &mut B) -> Result<(Vec<String>, bool)> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut group_ids = Vec::with_capacity(n);
    for _ in 0..n {
        group_ids.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let include_authorized_operations = buf::get_bool(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((group_ids, include_authorized_operations))
}

fn encode_consumer_group_assignment(
    buf: &mut BytesMut,
    assignment: &ConsumerGroupAssignment,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(assignment.topic_partitions.len()))?;
    for tp in &assignment.topic_partitions {
        buf.extend_from_slice(&tp.topic_id);
        buf::put_compact_string(buf, Some(&tp.topic_name))?;
        buf::put_array_len(buf, true, Some(tp.partitions.len()))?;
        for p in &tp.partitions {
            buf.put_i32(*p);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

fn decode_consumer_group_assignment<B: Buf>(buf: &mut B) -> Result<ConsumerGroupAssignment> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topic_partitions = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        buf::skip_tagged_fields(buf)?;
        topic_partitions.push(ConsumerGroupTopicPartitions {
            topic_id,
            topic_name,
            partitions,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ConsumerGroupAssignment { topic_partitions })
}

fn encode_consumer_group_member(
    buf: &mut BytesMut,
    member: &ConsumerGroupMember,
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(&member.member_id))?;
    buf::put_compact_string(buf, member.instance_id.as_deref())?;
    buf::put_compact_string(buf, member.rack_id.as_deref())?;
    buf.put_i32(member.member_epoch);
    buf::put_compact_string(buf, Some(&member.client_id))?;
    buf::put_compact_string(buf, Some(&member.client_host))?;
    buf::put_array_len(buf, true, Some(member.subscribed_topic_names.len()))?;
    for name in &member.subscribed_topic_names {
        buf::put_compact_string(buf, Some(name))?;
    }
    buf::put_compact_string(buf, member.subscribed_topic_regex.as_deref())?;
    encode_consumer_group_assignment(buf, &member.assignment)?;
    encode_consumer_group_assignment(buf, &member.target_assignment)?;
    buf.put_i8(member.member_type);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

fn decode_consumer_group_member<B: Buf>(buf: &mut B) -> Result<ConsumerGroupMember> {
    let member_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let instance_id = buf::get_compact_string(buf)?;
    let rack_id = buf::get_compact_string(buf)?;
    let member_epoch = buf::get_i32(buf)?;
    let client_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let client_host = buf::get_compact_string(buf)?.unwrap_or_default();
    let sn = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut subscribed_topic_names = Vec::with_capacity(sn);
    for _ in 0..sn {
        subscribed_topic_names.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let subscribed_topic_regex = buf::get_compact_string(buf)?;
    let assignment = decode_consumer_group_assignment(buf)?;
    let target_assignment = decode_consumer_group_assignment(buf)?;
    let member_type = buf::get_i8(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(ConsumerGroupMember {
        member_id,
        instance_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscribed_topic_names,
        subscribed_topic_regex,
        assignment,
        target_assignment,
        member_type,
    })
}

pub fn encode_consumer_group_describe_response(
    buf: &mut BytesMut,
    groups: &[DescribedConsumerGroup],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(groups.len()))?;
    for g in groups {
        buf.put_i16(g.error_code);
        buf::put_compact_string(buf, g.error_message.as_deref())?;
        buf::put_compact_string(buf, Some(&g.group_id))?;
        buf::put_compact_string(buf, Some(&g.group_state))?;
        buf.put_i32(g.group_epoch);
        buf.put_i32(g.assignment_epoch);
        buf::put_compact_string(buf, Some(&g.assignor_name))?;
        buf::put_array_len(buf, true, Some(g.members.len()))?;
        for m in &g.members {
            encode_consumer_group_member(buf, m)?;
        }
        buf.put_i32(g.authorized_operations);
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_consumer_group_describe_response<B: Buf>(
    buf: &mut B,
) -> Result<Vec<DescribedConsumerGroup>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let group_state = buf::get_compact_string(buf)?.unwrap_or_default();
        let group_epoch = buf::get_i32(buf)?;
        let assignment_epoch = buf::get_i32(buf)?;
        let assignor_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let mn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut members = Vec::with_capacity(mn);
        for _ in 0..mn {
            members.push(decode_consumer_group_member(buf)?);
        }
        let authorized_operations = buf::get_i32(buf)?;
        buf::skip_tagged_fields(buf)?;
        groups.push(DescribedConsumerGroup {
            error_code,
            error_message,
            group_id,
            group_state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            members,
            authorized_operations,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(groups)
}

/// One member in a classic DescribeGroups (api 15) v6 group.
///
/// `group_instance_id` is v4+ (nullable). Metadata and assignment are
/// protocol bytes, not a parsed member store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub member_metadata: Vec<u8>,
    pub member_assignment: Vec<u8>,
}

impl DescribedGroupMember {
    pub fn new(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            group_instance_id: None,
            client_id: client_id.into(),
            client_host: client_host.into(),
            member_metadata: Vec::new(),
            member_assignment: Vec::new(),
        }
    }
}

/// One described group in DescribeGroups (api 15) v6.
///
/// ErrorCode sits here, not at the top of the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroup {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub group_id: String,
    pub group_state: String,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<DescribedGroupMember>,
    pub authorized_operations: i32,
}

impl DescribedGroup {
    pub fn new(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            group_id: group_id.into(),
            group_state: String::new(),
            protocol_type: String::new(),
            protocol_data: String::new(),
            members: Vec::new(),
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }
}

/// DescribeGroups v6 (classic through v4; flexible from v5; KIP-1043).
///
/// Official Apache JSON (`apiKey: 15`, request `listeners: ["broker"]`,
/// `validVersions: "0-6"`, `flexibleVersions: "5+"`) and
/// kafka-protocol 0.18.0 (`DescribeGroupsRequest` /
/// `DescribeGroupsResponse`, `VERSIONS` min=0 max=6). This crate
/// targets v6, the version a client encodes (`VERSIONS.max`). Request
/// encode used `features = ["client"]`; response encode used `broker`.
/// Request: compact `Groups`, `IncludeAuthorizedOperations` BOOLEAN
/// (v3+), tagged. Response: `ThrottleTimeMs` INT32 (v1+), compact
/// `Groups` of `{ErrorCode INT16, compact nullable ErrorMessage (v6+),
/// GroupId, GroupState, ProtocolType, ProtocolData, compact Members of
/// {MemberId, compact nullable GroupInstanceId (v4+), ClientId,
/// ClientHost, compact MemberMetadata BYTES, compact MemberAssignment
/// BYTES, tagged}, AuthorizedOperations INT32 (v3+), tagged}`, tagged.
/// **ErrorCode is per-group**, the first field of each DescribedGroup
/// — not a top-level code after throttle. Measured independently on
/// leftover-empty fixture group `"g"`: the first-group ErrorCode is
/// the INT16 at **bytes 5–6**, after throttle and the compact groups
/// length — not bytes 4–5 (DescribeClientQuotas) or 5–6 assumed from
/// ConsumerGroupDescribe or 12–13 (DescribeProducers first
/// partition). Official listed per-group errors include
/// `COORDINATOR_LOAD_IN_PROGRESS` (14), `COORDINATOR_NOT_AVAILABLE`
/// (15), `NOT_COORDINATOR` (16), `AUTHORIZATION_FAILED` (29);
/// version 6 also returns `GROUP_ID_NOT_FOUND`. This is a
/// group-coordinator hop, not a controller hop and not a
/// partition-leader hop.
pub fn encode_describe_groups_request(
    buf: &mut BytesMut,
    group_ids: &[String],
    include_authorized_operations: bool,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(group_ids.len()))?;
    for id in group_ids {
        buf::put_compact_string(buf, Some(id))?;
    }
    buf.put_u8(u8::from(include_authorized_operations));
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_groups_request<B: Buf>(buf: &mut B) -> Result<(Vec<String>, bool)> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut group_ids = Vec::with_capacity(n);
    for _ in 0..n {
        group_ids.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let include_authorized_operations = buf::get_bool(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((group_ids, include_authorized_operations))
}

fn encode_described_group_member(
    buf: &mut BytesMut,
    member: &DescribedGroupMember,
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(&member.member_id))?;
    buf::put_compact_string(buf, member.group_instance_id.as_deref())?;
    buf::put_compact_string(buf, Some(&member.client_id))?;
    buf::put_compact_string(buf, Some(&member.client_host))?;
    buf::put_compact_bytes(buf, Some(&member.member_metadata))?;
    buf::put_compact_bytes(buf, Some(&member.member_assignment))?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

fn decode_described_group_member<B: Buf>(buf: &mut B) -> Result<DescribedGroupMember> {
    let member_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let group_instance_id = buf::get_compact_string(buf)?;
    let client_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let client_host = buf::get_compact_string(buf)?.unwrap_or_default();
    let member_metadata = buf::get_compact_bytes(buf)?.unwrap_or_default();
    let member_assignment = buf::get_compact_bytes(buf)?.unwrap_or_default();
    buf::skip_tagged_fields(buf)?;
    Ok(DescribedGroupMember {
        member_id,
        group_instance_id,
        client_id,
        client_host,
        member_metadata,
        member_assignment,
    })
}

pub fn encode_describe_groups_response(
    buf: &mut BytesMut,
    groups: &[DescribedGroup],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(groups.len()))?;
    for g in groups {
        buf.put_i16(g.error_code);
        buf::put_compact_string(buf, g.error_message.as_deref())?;
        buf::put_compact_string(buf, Some(&g.group_id))?;
        buf::put_compact_string(buf, Some(&g.group_state))?;
        buf::put_compact_string(buf, Some(&g.protocol_type))?;
        buf::put_compact_string(buf, Some(&g.protocol_data))?;
        buf::put_array_len(buf, true, Some(g.members.len()))?;
        for m in &g.members {
            encode_described_group_member(buf, m)?;
        }
        buf.put_i32(g.authorized_operations);
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_groups_response<B: Buf>(buf: &mut B) -> Result<Vec<DescribedGroup>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let group_state = buf::get_compact_string(buf)?.unwrap_or_default();
        let protocol_type = buf::get_compact_string(buf)?.unwrap_or_default();
        let protocol_data = buf::get_compact_string(buf)?.unwrap_or_default();
        let mn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut members = Vec::with_capacity(mn);
        for _ in 0..mn {
            members.push(decode_described_group_member(buf)?);
        }
        let authorized_operations = buf::get_i32(buf)?;
        buf::skip_tagged_fields(buf)?;
        groups.push(DescribedGroup {
            error_code,
            error_message,
            group_id,
            group_state,
            protocol_type,
            protocol_data,
            members,
            authorized_operations,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(groups)
}

/// One listed group in ListGroups (api 16) v5.
///
/// There is no per-group ErrorCode. The response error sits at the top
/// of the body, after throttle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroup {
    pub group_id: String,
    pub protocol_type: String,
    pub group_state: String,
    pub group_type: String,
}

impl ListedGroup {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            protocol_type: String::new(),
            group_state: String::new(),
            group_type: String::new(),
        }
    }
}

/// ListGroups v5 body (classic through v2; flexible from v3; KIP-518 / KIP-848).
///
/// Official Apache JSON (`apiKey: 16`, request `listeners: ["broker"]`,
/// `validVersions: "0-5"`, `flexibleVersions: "3+"`) and
/// kafka-protocol 0.18.0 (`ListGroupsRequest` /
/// `ListGroupsResponse`, `VERSIONS` min=0 max=5). This crate
/// targets v5, the version a client encodes (`VERSIONS.max`). Request
/// encode used `features = ["client"]`; response encode used `broker`.
/// Official listed errors (`ListGroupsRequest.java`):
/// `COORDINATOR_LOAD_IN_PROGRESS` (14), `COORDINATOR_NOT_AVAILABLE`
/// (15), `AUTHORIZATION_FAILED` (29). `NOT_COORDINATOR` (16) is **not**
/// listed. Request: compact `StatesFilter` (v4+), compact `TypesFilter`
/// (v5+), tagged. Response: `ThrottleTimeMs` INT32 (v1+), top-level
/// `ErrorCode` INT16, compact `Groups` of `{GroupId, ProtocolType,
/// GroupState (v4+), GroupType (v5+), tagged}`, tagged. **ErrorCode is
/// top-level**, after throttle — not a first-group field. Measured
/// independently on leftover-empty fixture group `"g"`: the top-level
/// ErrorCode is the INT16 at **bytes 4–5** — not bytes 5–6
/// (DescribeGroups / ConsumerGroupDescribe first-group) or 12–13
/// (DescribeProducers first partition). This is broker-only: no
/// FindCoordinator hop, no controller hop, no partition-leader hop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsResponse {
    pub error_code: i16,
    pub groups: Vec<ListedGroup>,
}

pub fn encode_list_groups_request(
    buf: &mut BytesMut,
    states_filter: &[String],
    types_filter: &[String],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(states_filter.len()))?;
    for state in states_filter {
        buf::put_compact_string(buf, Some(state))?;
    }
    buf::put_array_len(buf, true, Some(types_filter.len()))?;
    for ty in types_filter {
        buf::put_compact_string(buf, Some(ty))?;
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_list_groups_request<B: Buf>(buf: &mut B) -> Result<(Vec<String>, Vec<String>)> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut states_filter = Vec::with_capacity(n);
    for _ in 0..n {
        states_filter.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut types_filter = Vec::with_capacity(tn);
    for _ in 0..tn {
        types_filter.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    buf::skip_tagged_fields(buf)?;
    Ok((states_filter, types_filter))
}

pub fn encode_list_groups_response(
    buf: &mut BytesMut,
    resp: &ListGroupsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, true, Some(resp.groups.len()))?;
    for g in &resp.groups {
        buf::put_compact_string(buf, Some(&g.group_id))?;
        buf::put_compact_string(buf, Some(&g.protocol_type))?;
        buf::put_compact_string(buf, Some(&g.group_state))?;
        buf::put_compact_string(buf, Some(&g.group_type))?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_list_groups_response<B: Buf>(buf: &mut B) -> Result<ListGroupsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let protocol_type = buf::get_compact_string(buf)?.unwrap_or_default();
        let group_state = buf::get_compact_string(buf)?.unwrap_or_default();
        let group_type = buf::get_compact_string(buf)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        groups.push(ListedGroup {
            group_id,
            protocol_type,
            group_state,
            group_type,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ListGroupsResponse { error_code, groups })
}

/// One deletion result in DeleteGroups (api 42) v2.
///
/// ErrorCode sits here after GroupId, not at the top of the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletableGroupResult {
    pub group_id: String,
    pub error_code: i16,
}

impl DeletableGroupResult {
    pub fn new(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            group_id: group_id.into(),
            error_code,
        }
    }
}

/// DeleteGroups v2 body (classic through v1; flexible from v2).
///
/// Official Apache JSON (`apiKey: 42`, request `listeners: ["broker"]`,
/// `validVersions: "0-2"`, `flexibleVersions: "2+"`; Kafka 4.1.0, the
/// release kafka-protocol 0.18.0 was generated against) and
/// kafka-protocol 0.18.0 (`DeleteGroupsRequest` /
/// `DeleteGroupsResponse`, `VERSIONS` min=0 max=2). This crate
/// targets v2, the version a client encodes (`VERSIONS.max`). Request
/// encode used `features = ["client"]`; response encode used `broker`.
/// Official listed errors (`DeleteGroupsResponse.java`):
/// `COORDINATOR_LOAD_IN_PROGRESS` (14), `COORDINATOR_NOT_AVAILABLE`
/// (15), `NOT_COORDINATOR` (16), `INVALID_GROUP_ID` (24),
/// `GROUP_AUTHORIZATION_FAILED` (30), `NON_EMPTY_GROUP` (68),
/// `GROUP_ID_NOT_FOUND` (69). Request: compact `GroupsNames`, tagged.
/// Response: `ThrottleTimeMs` INT32, compact `Results` of `{compact
/// GroupId, ErrorCode INT16, tagged}`, tagged. **ErrorCode is
/// per-group**, the second field of each DeletableGroupResult after
/// GroupId — not a top-level code after throttle. Measured
/// independently on leftover-empty fixture group `"g"`: the first-group
/// ErrorCode is the INT16 at **bytes 7–8**, after throttle, the compact
/// results length, and compact GroupId `"g"` — not bytes 4–5
/// (ListGroups / DescribeClientQuotas top-level) or 5–6 (DescribeGroups
/// / ConsumerGroupDescribe first-group first field) or 12–13
/// (DescribeProducers first partition). Because `NOT_COORDINATOR` (16)
/// is listed, this is a group-coordinator hop, not a controller hop
/// and not a partition-leader hop.
pub fn encode_delete_groups_request(
    buf: &mut BytesMut,
    group_ids: &[String],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(group_ids.len()))?;
    for id in group_ids {
        buf::put_compact_string(buf, Some(id))?;
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_delete_groups_request<B: Buf>(buf: &mut B) -> Result<Vec<String>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut group_ids = Vec::with_capacity(n);
    for _ in 0..n {
        group_ids.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    buf::skip_tagged_fields(buf)?;
    Ok(group_ids)
}

pub fn encode_delete_groups_response(
    buf: &mut BytesMut,
    results: &[DeletableGroupResult],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(results.len()))?;
    for r in results {
        buf::put_compact_string(buf, Some(&r.group_id))?;
        buf.put_i16(r.error_code);
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_delete_groups_response<B: Buf>(buf: &mut B) -> Result<Vec<DeletableGroupResult>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let error_code = buf::get_i16(buf)?;
        buf::skip_tagged_fields(buf)?;
        results.push(DeletableGroupResult {
            group_id,
            error_code,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(results)
}

/// One assigned topic in ShareGroupDescribe (api 77) Assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupTopicPartitions {
    pub topic_id: [u8; 16],
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

impl ShareGroupTopicPartitions {
    pub fn new(topic_id: [u8; 16], topic_name: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            topic_id,
            topic_name: topic_name.into(),
            partitions,
        }
    }
}

/// Current assignment for one described share-group member.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupAssignment {
    pub topic_partitions: Vec<ShareGroupTopicPartitions>,
}

impl ShareGroupAssignment {
    pub fn new(topic_partitions: Vec<ShareGroupTopicPartitions>) -> Self {
        Self { topic_partitions }
    }
}

/// One member in a ShareGroupDescribe v1 group.
///
/// Official member fields are MemberId, RackId, MemberEpoch, ClientId,
/// ClientHost, SubscribedTopicNames, Assignment. There is no InstanceId,
/// SubscribedTopicRegex, TargetAssignment, or MemberType.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMember {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub member_epoch: i32,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub assignment: ShareGroupAssignment,
}

impl ShareGroupMember {
    pub fn new(
        member_id: impl Into<String>,
        member_epoch: i32,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            rack_id: None,
            member_epoch,
            client_id: client_id.into(),
            client_host: client_host.into(),
            subscribed_topic_names: Vec::new(),
            assignment: ShareGroupAssignment::default(),
        }
    }
}

/// One described group in ShareGroupDescribe (api 77) v1.
///
/// ErrorCode sits here, not at the top of the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedShareGroup {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub group_id: String,
    pub group_state: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignor_name: String,
    pub members: Vec<ShareGroupMember>,
    pub authorized_operations: i32,
}

impl DescribedShareGroup {
    pub fn new(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            group_id: group_id.into(),
            group_state: String::new(),
            group_epoch: 0,
            assignment_epoch: 0,
            assignor_name: String::new(),
            members: Vec::new(),
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }
}

/// ShareGroupDescribe v1 (flexible from v0; KIP-932).
///
/// Official Apache JSON (`apiKey: 77`, request `listeners: ["broker"]`,
/// `validVersions: "1"`, `flexibleVersions: "0+"`) and kafka-protocol
/// 0.18.0 (`ShareGroupDescribeRequest` / `ShareGroupDescribeResponse`,
/// `VERSIONS` min=1 max=1). This crate targets v1, the version a client
/// encodes (`VERSIONS.max`). Version 0 was early-access in Kafka 4.0 and
/// was removed in 4.1; this crate does not speak it. Request encode
/// used `features = ["client"]`; response encode used `broker`.
/// Official listed errors (`ShareGroupDescribeResponse.json` /
/// `ShareGroupDescribeResponse.java`): `GROUP_AUTHORIZATION_FAILED`,
/// `TOPIC_AUTHORIZATION_FAILED` (v1+), `NOT_COORDINATOR` (16),
/// `COORDINATOR_NOT_AVAILABLE`, `COORDINATOR_LOAD_IN_PROGRESS`,
/// `INVALID_GROUP_ID`, `GROUP_ID_NOT_FOUND`, `INVALID_REQUEST`.
/// Request: compact `GroupIds`, `IncludeAuthorizedOperations` BOOLEAN,
/// tagged. Response: `ThrottleTimeMs` INT32, compact `Groups` of
/// `{ErrorCode INT16, compact nullable ErrorMessage, GroupId,
/// GroupState, GroupEpoch INT32, AssignmentEpoch INT32, AssignorName,
/// compact Members of {MemberId, compact nullable RackId, MemberEpoch
/// INT32, ClientId, ClientHost, compact SubscribedTopicNames,
/// Assignment, tagged}, AuthorizedOperations INT32, tagged}`, tagged.
/// Assignment is compact TopicPartitions of `{TopicId UUID, TopicName,
/// compact Partitions INT32[], tagged}`. **ErrorCode is per-group**,
/// the first field of each DescribedGroup — not a top-level code after
/// throttle. Measured independently from kafka-protocol 0.18.0
/// (`broker` encodes the response) on leftover-empty fixture group
/// `"g"`: the first-group ErrorCode is the INT16 at **bytes 5–6**,
/// after throttle and the compact groups length — not bytes 4–5
/// (ListGroups / DescribeClientQuotas top-level), 7–8 (DeleteGroups
/// after GroupId), or 12–13 (DescribeProducers first partition).
/// Official Java `DescribeShareGroupsHandler` looks up
/// `CoordinatorType.GROUP` (`FindCoordinator` `key_type=0`). Official
/// FindCoordinator JSON names SHARE (`key_type=2`) for the share-state
/// key `"groupId:topicId:partition"` (v6), which this API does not use.
/// Because `NOT_COORDINATOR` (16) is listed, this is a
/// share-group coordinator hop (group coordinator), not a controller
/// hop and not a partition-leader hop.
pub fn encode_share_group_describe_request(
    buf: &mut BytesMut,
    group_ids: &[String],
    include_authorized_operations: bool,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(group_ids.len()))?;
    for id in group_ids {
        buf::put_compact_string(buf, Some(id))?;
    }
    buf.put_u8(u8::from(include_authorized_operations));
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_share_group_describe_request<B: Buf>(buf: &mut B) -> Result<(Vec<String>, bool)> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut group_ids = Vec::with_capacity(n);
    for _ in 0..n {
        group_ids.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let include_authorized_operations = buf::get_bool(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((group_ids, include_authorized_operations))
}

fn encode_share_group_assignment(
    buf: &mut BytesMut,
    assignment: &ShareGroupAssignment,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(assignment.topic_partitions.len()))?;
    for tp in &assignment.topic_partitions {
        buf.extend_from_slice(&tp.topic_id);
        buf::put_compact_string(buf, Some(&tp.topic_name))?;
        buf::put_array_len(buf, true, Some(tp.partitions.len()))?;
        for p in &tp.partitions {
            buf.put_i32(*p);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

fn decode_share_group_assignment<B: Buf>(buf: &mut B) -> Result<ShareGroupAssignment> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topic_partitions = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        buf::skip_tagged_fields(buf)?;
        topic_partitions.push(ShareGroupTopicPartitions {
            topic_id,
            topic_name,
            partitions,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ShareGroupAssignment { topic_partitions })
}

fn encode_share_group_member(
    buf: &mut BytesMut,
    member: &ShareGroupMember,
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(&member.member_id))?;
    buf::put_compact_string(buf, member.rack_id.as_deref())?;
    buf.put_i32(member.member_epoch);
    buf::put_compact_string(buf, Some(&member.client_id))?;
    buf::put_compact_string(buf, Some(&member.client_host))?;
    buf::put_array_len(buf, true, Some(member.subscribed_topic_names.len()))?;
    for name in &member.subscribed_topic_names {
        buf::put_compact_string(buf, Some(name))?;
    }
    encode_share_group_assignment(buf, &member.assignment)?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

fn decode_share_group_member<B: Buf>(buf: &mut B) -> Result<ShareGroupMember> {
    let member_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let rack_id = buf::get_compact_string(buf)?;
    let member_epoch = buf::get_i32(buf)?;
    let client_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let client_host = buf::get_compact_string(buf)?.unwrap_or_default();
    let sn = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut subscribed_topic_names = Vec::with_capacity(sn);
    for _ in 0..sn {
        subscribed_topic_names.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let assignment = decode_share_group_assignment(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(ShareGroupMember {
        member_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscribed_topic_names,
        assignment,
    })
}

pub fn encode_share_group_describe_response(
    buf: &mut BytesMut,
    groups: &[DescribedShareGroup],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(groups.len()))?;
    for g in groups {
        buf.put_i16(g.error_code);
        buf::put_compact_string(buf, g.error_message.as_deref())?;
        buf::put_compact_string(buf, Some(&g.group_id))?;
        buf::put_compact_string(buf, Some(&g.group_state))?;
        buf.put_i32(g.group_epoch);
        buf.put_i32(g.assignment_epoch);
        buf::put_compact_string(buf, Some(&g.assignor_name))?;
        buf::put_array_len(buf, true, Some(g.members.len()))?;
        for m in &g.members {
            encode_share_group_member(buf, m)?;
        }
        buf.put_i32(g.authorized_operations);
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_share_group_describe_response<B: Buf>(
    buf: &mut B,
) -> Result<Vec<DescribedShareGroup>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let group_state = buf::get_compact_string(buf)?.unwrap_or_default();
        let group_epoch = buf::get_i32(buf)?;
        let assignment_epoch = buf::get_i32(buf)?;
        let assignor_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let mn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut members = Vec::with_capacity(mn);
        for _ in 0..mn {
            members.push(decode_share_group_member(buf)?);
        }
        let authorized_operations = buf::get_i32(buf)?;
        buf::skip_tagged_fields(buf)?;
        groups.push(DescribedShareGroup {
            error_code,
            error_message,
            group_id,
            group_state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            members,
            authorized_operations,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(groups)
}

/// One requested topic in DescribeShareGroupOffsets (api 90).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsTopic {
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

impl DescribeShareGroupOffsetsTopic {
    pub fn new(topic_name: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            topic_name: topic_name.into(),
            partitions,
        }
    }
}

/// One requested group in DescribeShareGroupOffsets (api 90) v0.
///
/// `topics = None` is official nullable Topics (all topic-partitions).
/// kafka-protocol 0.18.0 `Default` is `Some([])`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsGroup {
    pub group_id: String,
    pub topics: Option<Vec<DescribeShareGroupOffsetsTopic>>,
}

impl DescribeShareGroupOffsetsGroup {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            topics: Some(Vec::new()),
        }
    }
}

/// One partition in a described share-group offsets topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedShareGroupOffsetsPartition {
    pub partition_index: i32,
    pub start_offset: i64,
    pub leader_epoch: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// One topic in a described share-group offsets group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedShareGroupOffsetsTopic {
    pub topic_name: String,
    pub topic_id: [u8; 16],
    pub partitions: Vec<DescribedShareGroupOffsetsPartition>,
}

/// One described group in DescribeShareGroupOffsets (api 90) v0.
///
/// Group-level ErrorCode sits here after GroupId and Topics, not at the
/// top of the response body and not on the first partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedShareGroupOffsets {
    pub group_id: String,
    pub topics: Vec<DescribedShareGroupOffsetsTopic>,
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl DescribedShareGroupOffsets {
    pub fn new(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            group_id: group_id.into(),
            topics: Vec::new(),
            error_code,
            error_message: None,
        }
    }
}

/// DescribeShareGroupOffsets v0 (flexible from v0; KIP-932).
///
/// Official Apache JSON (`apiKey: 90`, request `listeners: ["broker"]`,
/// `validVersions: "0-1"`, `flexibleVersions: "0+"`) and kafka-protocol
/// 0.18.0 (`DescribeShareGroupOffsetsRequest` /
/// `DescribeShareGroupOffsetsResponse`, `VERSIONS` min=0 max=0). This
/// crate targets v0, the version a client encodes (`VERSIONS.max`).
/// Official trunk v1 adds `Lag` INT64 (KIP-1226); 0.18.0 does not
/// speak it and this crate does not invent it. Request encode used
/// `features = ["client"]`; response encode used `broker`.
/// Official listed errors (`DescribeShareGroupOffsetsResponse.json`):
/// `GROUP_AUTHORIZATION_FAILED`, `TOPIC_AUTHORIZATION_FAILED`,
/// `NOT_COORDINATOR` (16), `COORDINATOR_NOT_AVAILABLE`,
/// `COORDINATOR_LOAD_IN_PROGRESS`, `GROUP_ID_NOT_FOUND`,
/// `INVALID_REQUEST`, `UNKNOWN_SERVER_ERROR`.
/// Request: compact `Groups` of `{GroupId, compact nullable Topics of
/// {TopicName, compact Partitions INT32[], tagged}, tagged}`, tagged.
/// Response: `ThrottleTimeMs` INT32, compact `Groups` of `{GroupId,
/// compact Topics of {TopicName, TopicId UUID, compact Partitions of
/// {PartitionIndex INT32, StartOffset INT64, LeaderEpoch INT32,
/// ErrorCode INT16, compact nullable ErrorMessage, tagged}, tagged},
/// ErrorCode INT16, compact nullable ErrorMessage, tagged}`, tagged.
/// **Group-level ErrorCode is per-group**, after GroupId and Topics —
/// not a top-level code after throttle and not the first-partition
/// code. Measured independently from kafka-protocol 0.18.0 (`broker`
/// encodes the response) on leftover-empty fixture group `"g"`
/// (empty Topics): the first-group ErrorCode is the INT16 at
/// **bytes 8–9**, after throttle, the compact groups length, compact
/// GroupId `"g"`, and the compact empty Topics length — not bytes
/// 4–5 (ListGroups / DescribeClientQuotas top-level), 5–6
/// (ShareGroupDescribe / DescribeGroups first-group first field),
/// 7–8 (DeleteGroups after GroupId), or 12–13 (DescribeProducers
/// first partition). First-partition ErrorCode, when a leftover-empty
/// topic `"t"` partition `0` is present, is at **byte 43** and is not
/// the hop code. Official Java `ListShareGroupOffsetsHandler` (the
/// AdminClient handler for this RPC; there is no class named
/// `DescribeShareGroupOffsetsHandler`) looks up
/// `CoordinatorType.GROUP` (`FindCoordinator` `key_type=0`). Official
/// FindCoordinator JSON names SHARE (`key_type=2`) for the
/// share-state key `"groupId:topicId:partition"` (v6), which this
/// API does not use. Because `NOT_COORDINATOR` (16) is listed, this
/// is a share-group coordinator hop (group coordinator), not a
/// controller hop and not a partition-leader hop.
pub fn encode_describe_share_group_offsets_request(
    buf: &mut BytesMut,
    groups: &[DescribeShareGroupOffsetsGroup],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(groups.len()))?;
    for g in groups {
        buf::put_compact_string(buf, Some(&g.group_id))?;
        buf::put_array_len(buf, true, g.topics.as_ref().map(Vec::len))?;
        if let Some(topics) = &g.topics {
            for t in topics {
                buf::put_compact_string(buf, Some(&t.topic_name))?;
                buf::put_array_len(buf, true, Some(t.partitions.len()))?;
                for p in &t.partitions {
                    buf.put_i32(*p);
                }
                buf::put_empty_tagged_fields(buf);
            }
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_share_group_offsets_request<B: Buf>(
    buf: &mut B,
) -> Result<Vec<DescribeShareGroupOffsetsGroup>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let topics = match buf::get_array_len(buf, true)? {
            None => None,
            Some(tn) => {
                let mut topics = Vec::with_capacity(tn);
                for _ in 0..tn {
                    let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
                    let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
                    let mut partitions = Vec::with_capacity(pn);
                    for _ in 0..pn {
                        partitions.push(buf::get_i32(buf)?);
                    }
                    buf::skip_tagged_fields(buf)?;
                    topics.push(DescribeShareGroupOffsetsTopic {
                        topic_name,
                        partitions,
                    });
                }
                Some(topics)
            }
        };
        buf::skip_tagged_fields(buf)?;
        groups.push(DescribeShareGroupOffsetsGroup { group_id, topics });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(groups)
}

pub fn encode_describe_share_group_offsets_response(
    buf: &mut BytesMut,
    groups: &[DescribedShareGroupOffsets],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(groups.len()))?;
    for g in groups {
        buf::put_compact_string(buf, Some(&g.group_id))?;
        buf::put_array_len(buf, true, Some(g.topics.len()))?;
        for t in &g.topics {
            buf::put_compact_string(buf, Some(&t.topic_name))?;
            buf.extend_from_slice(&t.topic_id);
            buf::put_array_len(buf, true, Some(t.partitions.len()))?;
            for p in &t.partitions {
                buf.put_i32(p.partition_index);
                buf.put_i64(p.start_offset);
                buf.put_i32(p.leader_epoch);
                buf.put_i16(p.error_code);
                buf::put_compact_string(buf, p.error_message.as_deref())?;
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf.put_i16(g.error_code);
        buf::put_compact_string(buf, g.error_message.as_deref())?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_share_group_offsets_response<B: Buf>(
    buf: &mut B,
) -> Result<Vec<DescribedShareGroupOffsets>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
            let topic_id = buf::get_uuid(buf)?;
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                let partition_index = buf::get_i32(buf)?;
                let start_offset = buf::get_i64(buf)?;
                let leader_epoch = buf::get_i32(buf)?;
                let error_code = buf::get_i16(buf)?;
                let error_message = buf::get_compact_string(buf)?;
                buf::skip_tagged_fields(buf)?;
                partitions.push(DescribedShareGroupOffsetsPartition {
                    partition_index,
                    start_offset,
                    leader_epoch,
                    error_code,
                    error_message,
                });
            }
            buf::skip_tagged_fields(buf)?;
            topics.push(DescribedShareGroupOffsetsTopic {
                topic_name,
                topic_id,
                partitions,
            });
        }
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        buf::skip_tagged_fields(buf)?;
        groups.push(DescribedShareGroupOffsets {
            group_id,
            topics,
            error_code,
            error_message,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(groups)
}

/// One requested partition in AlterShareGroupOffsets (api 91).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsPartition {
    pub partition_index: i32,
    pub start_offset: i64,
}

impl AlterShareGroupOffsetsPartition {
    pub fn new(partition_index: i32, start_offset: i64) -> Self {
        Self {
            partition_index,
            start_offset,
        }
    }
}

/// One requested topic in AlterShareGroupOffsets (api 91) v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsTopic {
    pub topic_name: String,
    pub partitions: Vec<AlterShareGroupOffsetsPartition>,
}

impl AlterShareGroupOffsetsTopic {
    pub fn new(
        topic_name: impl Into<String>,
        partitions: Vec<AlterShareGroupOffsetsPartition>,
    ) -> Self {
        Self {
            topic_name: topic_name.into(),
            partitions,
        }
    }
}

/// One partition in an altered share-group offsets topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlteredShareGroupOffsetsPartition {
    pub partition_index: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// One topic in an AlterShareGroupOffsets (api 91) v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlteredShareGroupOffsetsTopic {
    pub topic_name: String,
    pub topic_id: [u8; 16],
    pub partitions: Vec<AlteredShareGroupOffsetsPartition>,
}

/// AlterShareGroupOffsets (api 91) v0 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-group field
/// and not the first-partition code. This API has a single GroupId on
/// the request and no Groups array on the response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlteredShareGroupOffsets {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub topics: Vec<AlteredShareGroupOffsetsTopic>,
}

impl AlteredShareGroupOffsets {
    pub fn new(error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            topics: Vec::new(),
        }
    }
}

/// AlterShareGroupOffsets v0 (flexible from v0; KIP-932).
///
/// Official Apache JSON (`apiKey: 91`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`) and kafka-protocol
/// 0.18.0 (`AlterShareGroupOffsetsRequest` /
/// `AlterShareGroupOffsetsResponse`, `VERSIONS` min=0 max=0). This
/// crate targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Official listed errors
/// (`AlterShareGroupOffsetsResponse.json`):
/// `GROUP_AUTHORIZATION_FAILED`, `TOPIC_AUTHORIZATION_FAILED`,
/// `NOT_COORDINATOR` (16), `COORDINATOR_NOT_AVAILABLE`,
/// `COORDINATOR_LOAD_IN_PROGRESS`, `GROUP_ID_NOT_FOUND`,
/// `NON_EMPTY_GROUP`, `KAFKA_STORAGE_ERROR`, `INVALID_REQUEST`,
/// `UNKNOWN_SERVER_ERROR`.
/// Request: compact `GroupId`, compact `Topics` of `{TopicName,
/// compact Partitions of {PartitionIndex INT32, StartOffset INT64,
/// tagged}, tagged}`, tagged.
/// Response: `ThrottleTimeMs` INT32, `ErrorCode` INT16, compact
/// nullable `ErrorMessage`, compact `Responses` of `{TopicName,
/// TopicId UUID, compact Partitions of {PartitionIndex INT32,
/// ErrorCode INT16, compact nullable ErrorMessage, tagged}, tagged}`,
/// tagged.
/// **ErrorCode is top-level**, after throttle — not a first-group
/// field and not the first-partition code. Measured independently
/// from kafka-protocol 0.18.0 (`broker` encodes the response) on
/// leftover-empty fixture group `"g"` (empty Responses): the
/// top-level ErrorCode is the INT16 at **bytes 4–5**, after throttle
/// — not bytes 5–6 (ShareGroupDescribe / DescribeGroups first-group
/// first field), 7–8 (DeleteGroups after GroupId), 8–9
/// (DescribeShareGroupOffsets first-group after GroupId and Topics),
/// or 12–13 (DescribeProducers first partition). The leftover-empty
/// body is 9 bytes, so those later offsets are not present. First-
/// partition ErrorCode, when a leftover-empty topic `"t"` partition
/// `0` is present, is at **bytes 31–32** and is not the hop code.
/// Official Java `AlterShareGroupOffsetsHandler` looks up
/// `CoordinatorType.GROUP` (`FindCoordinator` `key_type=0`). Official
/// FindCoordinator JSON names SHARE (`key_type=2`) for the
/// share-state key `"groupId:topicId:partition"` (v6), which this
/// API does not use. Because `NOT_COORDINATOR` (16) is listed, this
/// is a share-group coordinator hop (group coordinator), not a
/// controller hop and not a partition-leader hop.
pub fn encode_alter_share_group_offsets_request(
    buf: &mut BytesMut,
    group_id: &str,
    topics: &[AlterShareGroupOffsetsTopic],
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(group_id))?;
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for t in topics {
        buf::put_compact_string(buf, Some(&t.topic_name))?;
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            buf.put_i64(p.start_offset);
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_alter_share_group_offsets_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, Vec<AlterShareGroupOffsetsTopic>)> {
    let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let start_offset = buf::get_i64(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(AlterShareGroupOffsetsPartition {
                partition_index,
                start_offset,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(AlterShareGroupOffsetsTopic {
            topic_name,
            partitions,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok((group_id, topics))
}

pub fn encode_alter_share_group_offsets_response(
    buf: &mut BytesMut,
    resp: &AlteredShareGroupOffsets,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for t in &resp.topics {
        buf::put_compact_string(buf, Some(&t.topic_name))?;
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            buf.put_i16(p.error_code);
            buf::put_compact_string(buf, p.error_message.as_deref())?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_alter_share_group_offsets_response<B: Buf>(
    buf: &mut B,
) -> Result<AlteredShareGroupOffsets> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let error_message = buf::get_compact_string(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(AlteredShareGroupOffsetsPartition {
                partition_index,
                error_code,
                error_message,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(AlteredShareGroupOffsetsTopic {
            topic_name,
            topic_id,
            partitions,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(AlteredShareGroupOffsets {
        error_code,
        error_message,
        topics,
    })
}

/// One requested topic in DeleteShareGroupOffsets (api 92).
///
/// Official request topics are topic names only — no partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetsTopic {
    pub topic_name: String,
}

impl DeleteShareGroupOffsetsTopic {
    pub fn new(topic_name: impl Into<String>) -> Self {
        Self {
            topic_name: topic_name.into(),
        }
    }
}

/// One topic in a DeleteShareGroupOffsets (api 92) v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedShareGroupOffsetsTopic {
    pub topic_name: String,
    pub topic_id: [u8; 16],
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// DeleteShareGroupOffsets (api 92) v0 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-group field
/// and not the first-topic code. This API has a single GroupId on the
/// request and no Groups array on the response. Official request topics
/// have no partitions; the response ErrorCode after TopicName/TopicId
/// is topic-level, not partition-level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedShareGroupOffsets {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub topics: Vec<DeletedShareGroupOffsetsTopic>,
}

impl DeletedShareGroupOffsets {
    pub fn new(error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            topics: Vec::new(),
        }
    }
}

/// DeleteShareGroupOffsets v0 (flexible from v0; KIP-932).
///
/// Official Apache JSON (`apiKey: 92`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`) and kafka-protocol
/// 0.18.0 (`DeleteShareGroupOffsetsRequest` /
/// `DeleteShareGroupOffsetsResponse`, `VERSIONS` min=0 max=0). This
/// crate targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Official listed errors
/// (`DeleteShareGroupOffsetsResponse.json`):
/// `GROUP_AUTHORIZATION_FAILED`, `TOPIC_AUTHORIZATION_FAILED`,
/// `NOT_COORDINATOR` (16), `COORDINATOR_NOT_AVAILABLE`,
/// `COORDINATOR_LOAD_IN_PROGRESS`, `GROUP_ID_NOT_FOUND`,
/// `NON_EMPTY_GROUP`, `KAFKA_STORAGE_ERROR`, `INVALID_REQUEST`,
/// `UNKNOWN_SERVER_ERROR`, `UNKNOWN_TOPIC_OR_PARTITION`.
/// Request: compact `GroupId`, compact `Topics` of `{TopicName,
/// tagged}`, tagged. No partitions on the request.
/// Response: `ThrottleTimeMs` INT32, `ErrorCode` INT16, compact
/// nullable `ErrorMessage`, compact `Responses` of `{TopicName,
/// TopicId UUID, ErrorCode INT16, compact nullable ErrorMessage,
/// tagged}`, tagged.
/// **ErrorCode is top-level**, after throttle — not a first-group
/// field and not the first-topic code. Measured independently
/// from kafka-protocol 0.18.0 (`broker` encodes the response) on
/// leftover-empty fixture group `"g"` (empty Responses): the
/// top-level ErrorCode is the INT16 at **bytes 4–5**, after throttle
/// — not bytes 5–6 (ShareGroupDescribe / DescribeGroups first-group
/// first field), 7–8 (DeleteGroups after GroupId), 8–9
/// (DescribeShareGroupOffsets first-group after GroupId and Topics),
/// or 12–13 (DescribeProducers first partition). The leftover-empty
/// body is 9 bytes, so those later offsets are not present. First-
/// topic ErrorCode, when a leftover-empty topic `"t"` is present, is
/// at **bytes 26–27** and is not the hop code. This API has no
/// first-partition ErrorCode (official request topics are names only).
/// Official Java `DeleteShareGroupOffsetsHandler` looks up
/// `CoordinatorType.GROUP` (`FindCoordinator` `key_type=0`). Official
/// FindCoordinator JSON names SHARE (`key_type=2`) for the
/// share-state key `"groupId:topicId:partition"` (v6), which this
/// API does not use. Because `NOT_COORDINATOR` (16) is listed, this
/// is a share-group coordinator hop (group coordinator), not a
/// controller hop and not a partition-leader hop.
pub fn encode_delete_share_group_offsets_request(
    buf: &mut BytesMut,
    group_id: &str,
    topics: &[DeleteShareGroupOffsetsTopic],
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(group_id))?;
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for t in topics {
        buf::put_compact_string(buf, Some(&t.topic_name))?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_delete_share_group_offsets_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, Vec<DeleteShareGroupOffsetsTopic>)> {
    let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        topics.push(DeleteShareGroupOffsetsTopic { topic_name });
    }
    buf::skip_tagged_fields(buf)?;
    Ok((group_id, topics))
}

pub fn encode_delete_share_group_offsets_response(
    buf: &mut BytesMut,
    resp: &DeletedShareGroupOffsets,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for t in &resp.topics {
        buf::put_compact_string(buf, Some(&t.topic_name))?;
        buf.extend_from_slice(&t.topic_id);
        buf.put_i16(t.error_code);
        buf::put_compact_string(buf, t.error_message.as_deref())?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_delete_share_group_offsets_response<B: Buf>(
    buf: &mut B,
) -> Result<DeletedShareGroupOffsets> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let topic_id = buf::get_uuid(buf)?;
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        buf::skip_tagged_fields(buf)?;
        topics.push(DeletedShareGroupOffsetsTopic {
            topic_name,
            topic_id,
            error_code,
            error_message,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(DeletedShareGroupOffsets {
        error_code,
        error_message,
        topics,
    })
}

/// Cursor for DescribeTopicPartitions (api 75) pagination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionCursor {
    pub topic_name: String,
    pub partition_index: i32,
}

impl TopicPartitionCursor {
    pub fn new(topic_name: impl Into<String>, partition_index: i32) -> Self {
        Self {
            topic_name: topic_name.into(),
            partition_index,
        }
    }
}

/// One partition in a DescribeTopicPartitions (api 75) v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTopicPartition {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub eligible_leader_replicas: Option<Vec<i32>>,
    pub last_known_elr: Option<Vec<i32>>,
    pub offline_replicas: Vec<i32>,
}

impl DescribedTopicPartition {
    pub fn new(error_code: i16) -> Self {
        Self {
            error_code,
            partition_index: 0,
            leader_id: 0,
            leader_epoch: -1,
            replica_nodes: Vec::new(),
            isr_nodes: Vec::new(),
            eligible_leader_replicas: None,
            last_known_elr: None,
            offline_replicas: Vec::new(),
        }
    }
}

/// One topic in a DescribeTopicPartitions (api 75) v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTopicPartitions {
    pub error_code: i16,
    pub name: Option<String>,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: Vec<DescribedTopicPartition>,
    pub topic_authorized_operations: i32,
}

impl DescribedTopicPartitions {
    pub fn new(name: impl Into<String>, error_code: i16) -> Self {
        Self {
            error_code,
            name: Some(name.into()),
            topic_id: [0; 16],
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }
}

/// DescribeTopicPartitions (api 75) v0 response body.
///
/// **There is no top-level ErrorCode.** The first ErrorCode is the
/// first-topic INT16. A first-partition ErrorCode exists only when a
/// partition is present and is later in the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsResponse {
    pub topics: Vec<DescribedTopicPartitions>,
    pub next_cursor: Option<TopicPartitionCursor>,
}

impl DescribeTopicPartitionsResponse {
    pub fn new(topics: Vec<DescribedTopicPartitions>) -> Self {
        Self {
            topics,
            next_cursor: None,
        }
    }
}

fn put_compact_i32s(buf: &mut BytesMut, items: &[i32]) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(items.len()))?;
    for v in items {
        buf.put_i32(*v);
    }
    Ok(())
}

fn get_compact_i32s<B: Buf>(buf: &mut B) -> Result<Vec<i32>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(buf::get_i32(buf)?);
    }
    Ok(out)
}

fn put_compact_nullable_i32s(
    buf: &mut BytesMut,
    items: Option<&[i32]>,
) -> crate::error::Result<()> {
    match items {
        None => buf::put_array_len(buf, true, None),
        Some(items) => put_compact_i32s(buf, items),
    }
}

fn get_compact_nullable_i32s<B: Buf>(buf: &mut B) -> Result<Option<Vec<i32>>> {
    let Some(n) = buf::get_array_len(buf, true)? else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(buf::get_i32(buf)?);
    }
    Ok(Some(out))
}

fn put_nullable_cursor(
    buf: &mut BytesMut,
    cursor: Option<&TopicPartitionCursor>,
) -> crate::error::Result<()> {
    match cursor {
        None => buf.put_i8(-1),
        Some(c) => {
            buf.put_i8(1);
            buf::put_compact_string(buf, Some(&c.topic_name))?;
            buf.put_i32(c.partition_index);
            buf::put_empty_tagged_fields(buf);
        }
    }
    Ok(())
}

fn get_nullable_cursor<B: Buf>(buf: &mut B) -> Result<Option<TopicPartitionCursor>> {
    let marker = buf::get_i8(buf)?;
    if marker < 0 {
        return Ok(None);
    }
    let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
    let partition_index = buf::get_i32(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(Some(TopicPartitionCursor {
        topic_name,
        partition_index,
    }))
}

/// DescribeTopicPartitions v0 (flexible from v0; KIP-966).
///
/// Official Apache JSON (`apiKey: 75`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Official JSON lists
/// **no** `errorCodes`. Official Java
/// `DescribeTopicPartitionsRequestHandler` answers from the broker
/// `MetadataCache` (topic describe / `TOPIC_AUTHORIZATION_FAILED` /
/// `INVALID_REQUEST`); it does not look up a coordinator. Official
/// Java `DescribeTopicPartitionsRequest.getErrorResponse` writes the
/// exception code onto each topic. `NOT_COORDINATOR` (16) is **not**
/// listed. kafka-protocol 0.18.0 (`DescribeTopicPartitionsRequest` /
/// `DescribeTopicPartitionsResponse`, `VERSIONS` min=0 max=0). This
/// crate targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: compact `Topics` of `{Name, tagged}`,
/// `ResponsePartitionLimit` INT32 (default 2000), nullable `Cursor`
/// `{TopicName, PartitionIndex INT32, tagged}` (`0xff` null / `0x01`
/// present), tagged. Response: `ThrottleTimeMs` INT32, compact
/// `Topics` of `{ErrorCode INT16, compact nullable Name, TopicId UUID,
/// IsInternal BOOLEAN, compact Partitions of {ErrorCode INT16,
/// PartitionIndex INT32, LeaderId INT32, LeaderEpoch INT32, compact
/// ReplicaNodes, compact IsrNodes, compact nullable
/// EligibleLeaderReplicas, compact nullable LastKnownElr, compact
/// OfflineReplicas, tagged}, TopicAuthorizedOperations INT32,
/// tagged}`, nullable `NextCursor`, tagged.
/// **ErrorCode is first-topic**, the first field of the first topic
/// after throttle and the compact topics length — not a top-level
/// code after throttle. Measured independently from kafka-protocol
/// 0.18.0 (`broker` encodes the response) on leftover-empty fixture
/// topic `"t"` (empty Partitions): the first-topic ErrorCode is the
/// INT16 at **bytes 5–6** — not bytes 4–5 (DeleteShareGroupOffsets /
/// AlterShareGroupOffsets / ListGroups top-level), 7–8 (DeleteGroups
/// after GroupId), 8–9 (DescribeShareGroupOffsets first-group after
/// GroupId and Topics), or 12–13 (DescribeProducers first partition).
/// This offset happens to match ShareGroupDescribe / DescribeGroups
/// first-group first field (also bytes 5–6); it was measured on this
/// API's official first-topic field, not copied. First-partition
/// ErrorCode, when leftover-empty partition `0` is present, is at
/// **bytes 27–28** and is not the first ErrorCode. Because 16 is not
/// listed, this is broker-only: no FindCoordinator, no controller
/// hop, no partition-leader hop.
pub fn encode_describe_topic_partitions_request(
    buf: &mut BytesMut,
    topics: &[String],
    response_partition_limit: i32,
    cursor: Option<&TopicPartitionCursor>,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for name in topics {
        buf::put_compact_string(buf, Some(name))?;
        buf::put_empty_tagged_fields(buf);
    }
    buf.put_i32(response_partition_limit);
    put_nullable_cursor(buf, cursor)?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_topic_partitions_request<B: Buf>(
    buf: &mut B,
) -> Result<(Vec<String>, i32, Option<TopicPartitionCursor>)> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        topics.push(name);
    }
    let response_partition_limit = buf::get_i32(buf)?;
    let cursor = get_nullable_cursor(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((topics, response_partition_limit, cursor))
}

pub fn encode_describe_topic_partitions_response(
    buf: &mut BytesMut,
    resp: &DescribeTopicPartitionsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for t in &resp.topics {
        buf.put_i16(t.error_code);
        buf::put_compact_string(buf, t.name.as_deref())?;
        buf.extend_from_slice(&t.topic_id);
        buf.put_u8(u8::from(t.is_internal));
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i16(p.error_code);
            buf.put_i32(p.partition_index);
            buf.put_i32(p.leader_id);
            buf.put_i32(p.leader_epoch);
            put_compact_i32s(buf, &p.replica_nodes)?;
            put_compact_i32s(buf, &p.isr_nodes)?;
            put_compact_nullable_i32s(buf, p.eligible_leader_replicas.as_deref())?;
            put_compact_nullable_i32s(buf, p.last_known_elr.as_deref())?;
            put_compact_i32s(buf, &p.offline_replicas)?;
            buf::put_empty_tagged_fields(buf);
        }
        buf.put_i32(t.topic_authorized_operations);
        buf::put_empty_tagged_fields(buf);
    }
    put_nullable_cursor(buf, resp.next_cursor.as_ref())?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_topic_partitions_response<B: Buf>(
    buf: &mut B,
) -> Result<DescribeTopicPartitionsResponse> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let name = buf::get_compact_string(buf)?;
        let topic_id = buf::get_uuid(buf)?;
        let is_internal = buf::get_bool(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let error_code = buf::get_i16(buf)?;
            let partition_index = buf::get_i32(buf)?;
            let leader_id = buf::get_i32(buf)?;
            let leader_epoch = buf::get_i32(buf)?;
            let replica_nodes = get_compact_i32s(buf)?;
            let isr_nodes = get_compact_i32s(buf)?;
            let eligible_leader_replicas = get_compact_nullable_i32s(buf)?;
            let last_known_elr = get_compact_nullable_i32s(buf)?;
            let offline_replicas = get_compact_i32s(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(DescribedTopicPartition {
                error_code,
                partition_index,
                leader_id,
                leader_epoch,
                replica_nodes,
                isr_nodes,
                eligible_leader_replicas,
                last_known_elr,
                offline_replicas,
            });
        }
        let topic_authorized_operations = buf::get_i32(buf)?;
        buf::skip_tagged_fields(buf)?;
        topics.push(DescribedTopicPartitions {
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
        });
    }
    let next_cursor = get_nullable_cursor(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeTopicPartitionsResponse {
        topics,
        next_cursor,
    })
}

/// One listed resource in ListConfigResources (api 74) v1.
///
/// There is no per-resource ErrorCode. The response error sits at the
/// top of the body, after throttle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedConfigResource {
    pub resource_name: String,
    pub resource_type: i8,
}

impl ListedConfigResource {
    pub fn new(resource_name: impl Into<String>, resource_type: i8) -> Self {
        Self {
            resource_name: resource_name.into(),
            resource_type,
        }
    }
}

/// ListConfigResources (api 74) v1 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-resource
/// field and not a first-config field. Resources have no ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConfigResourcesResponse {
    pub error_code: i16,
    pub config_resources: Vec<ListedConfigResource>,
}

impl ListConfigResourcesResponse {
    pub fn new(error_code: i16, config_resources: Vec<ListedConfigResource>) -> Self {
        Self {
            error_code,
            config_resources,
        }
    }
}

/// ListConfigResources v1 (flexible from v0; KIP-1142, formerly
/// ListClientMetricsResources).
///
/// Official Apache JSON (`apiKey: 74`, request `listeners: ["broker"]`,
/// `validVersions: "0-1"`, `flexibleVersions: "0+"`). Official JSON
/// lists **no** `errorCodes`. Official Java
/// `KafkaApis.handleListConfigResources` answers from the connected
/// broker (`DESCRIBE_CONFIGS` on `CLUSTER`, then
/// `groupConfigManager` / `clientMetricsManager` / `metadataCache`).
/// Official Java `ListConfigResourcesRequest.getErrorResponse` writes
/// the exception onto the top-level `ErrorCode`. Handler-observed
/// codes: `CLUSTER_AUTHORIZATION_FAILED` (31), `UNSUPPORTED_VERSION`
/// (35). `NOT_COORDINATOR` (16) is **not** listed. kafka-protocol
/// 0.18.0 (`ListConfigResourcesRequest` /
/// `ListConfigResourcesResponse`, `VERSIONS` min=0 max=1). This crate
/// targets v1, the version a client encodes (`VERSIONS.max`). Version
/// 0 is the legacy ListClientMetricsResources body (no ResourceTypes
/// / ResourceType) and is not spoken here. Request encode used
/// `features = ["client"]`; response encode used `broker`. Request:
/// compact `ResourceTypes` of INT8, tagged. Response: `ThrottleTimeMs`
/// INT32, top-level `ErrorCode` INT16, compact `ConfigResources` of
/// `{compact ResourceName, ResourceType INT8, tagged}`, tagged.
/// **ErrorCode is top-level**, after throttle — not a first-resource
/// field. Resources have no ErrorCode. Measured independently from
/// kafka-protocol 0.18.0 (`broker` encodes the response) on leftover-
/// empty fixture resource `"r"` type `CLIENT_METRICS` (16): the top-
/// level ErrorCode is the INT16 at **bytes 4–5** — not bytes 5–6
/// (DescribeTopicPartitions / ShareGroupDescribe first-topic / first-
/// group), 7–8 (DeleteGroups after GroupId), 8–9
/// (DescribeShareGroupOffsets first-group after GroupId and Topics),
/// or 12–13 (DescribeProducers first partition). The leftover-empty
/// body is 12 bytes, so those later offsets are not a first ErrorCode
/// here. This offset happens to match DeleteShareGroupOffsets /
/// AlterShareGroupOffsets / ListGroups top-level INT16; it was
/// measured on this API's official top-level field, not copied.
/// Because 16 is not listed, this is broker-only: no FindCoordinator,
/// no controller hop, no partition-leader hop.
pub fn encode_list_config_resources_request(
    buf: &mut BytesMut,
    resource_types: &[i8],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(resource_types.len()))?;
    for ty in resource_types {
        buf.put_i8(*ty);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_list_config_resources_request<B: Buf>(buf: &mut B) -> Result<Vec<i8>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut resource_types = Vec::with_capacity(n);
    for _ in 0..n {
        resource_types.push(buf::get_i8(buf)?);
    }
    buf::skip_tagged_fields(buf)?;
    Ok(resource_types)
}

pub fn encode_list_config_resources_response(
    buf: &mut BytesMut,
    resp: &ListConfigResourcesResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, true, Some(resp.config_resources.len()))?;
    for r in &resp.config_resources {
        buf::put_compact_string(buf, Some(&r.resource_name))?;
        buf.put_i8(r.resource_type);
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_list_config_resources_response<B: Buf>(
    buf: &mut B,
) -> Result<ListConfigResourcesResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut config_resources = Vec::with_capacity(n);
    for _ in 0..n {
        let resource_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let resource_type = buf::get_i8(buf)?;
        buf::skip_tagged_fields(buf)?;
        config_resources.push(ListedConfigResource {
            resource_name,
            resource_type,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ListConfigResourcesResponse {
        error_code,
        config_resources,
    })
}

/// GetTelemetrySubscriptions (api 71) v0 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-subscription
/// field and not a first-metric field. SubscriptionId is an INT32.
/// RequestedMetrics are compact strings with no ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTelemetrySubscriptionsResponse {
    pub error_code: i16,
    pub client_instance_id: [u8; 16],
    pub subscription_id: i32,
    pub accepted_compression_types: Vec<i8>,
    pub push_interval_ms: i32,
    pub telemetry_max_bytes: i32,
    pub delta_temporality: bool,
    pub requested_metrics: Vec<String>,
}

impl GetTelemetrySubscriptionsResponse {
    #[expect(
        clippy::too_many_arguments,
        reason = "constructor mirrors official GetTelemetrySubscriptionsResponse fields"
    )]
    pub fn new(
        error_code: i16,
        client_instance_id: [u8; 16],
        subscription_id: i32,
        accepted_compression_types: Vec<i8>,
        push_interval_ms: i32,
        telemetry_max_bytes: i32,
        delta_temporality: bool,
        requested_metrics: Vec<String>,
    ) -> Self {
        Self {
            error_code,
            client_instance_id,
            subscription_id,
            accepted_compression_types,
            push_interval_ms,
            telemetry_max_bytes,
            delta_temporality,
            requested_metrics,
        }
    }
}

/// GetTelemetrySubscriptions v0 (flexible from v0; KIP-714).
///
/// Official Apache JSON (`apiKey: 71`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Official JSON lists
/// **no** `errorCodes`. Official Java
/// `KafkaApis.handleGetTelemetrySubscriptionsRequest` answers from the
/// connected broker (`clientMetricsManager.processGetTelemetrySubscriptionRequest`).
/// Official Java `GetTelemetrySubscriptionsRequest.getErrorResponse`
/// writes the exception onto the top-level `ErrorCode`. Handler-observed
/// codes: `INVALID_REQUEST` (42) on catch-all, `UNSUPPORTED_VERSION`
/// (35) on the older ZooKeeper path, `THROTTLING_QUOTA_EXCEEDED` (89)
/// when a get arrives before the push interval.
/// `NOT_COORDINATOR` (16) is **not** listed. kafka-protocol 0.18.0
/// (`GetTelemetrySubscriptionsRequest` /
/// `GetTelemetrySubscriptionsResponse`, `VERSIONS` min=0 max=0). This
/// crate targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: `ClientInstanceId` UUID, tagged. Response:
/// `ThrottleTimeMs` INT32, top-level `ErrorCode` INT16,
/// `ClientInstanceId` UUID, `SubscriptionId` INT32, compact
/// `AcceptedCompressionTypes` of INT8, `PushIntervalMs` INT32,
/// `TelemetryMaxBytes` INT32, `DeltaTemporality` BOOLEAN, compact
/// `RequestedMetrics` of compact STRING, tagged.
/// **ErrorCode is top-level**, after throttle — not a first-subscription
/// field and not a first-metric field. Measured independently from
/// kafka-protocol 0.18.0 (`broker` encodes the response) on leftover-
/// empty fixture ClientInstanceId `[0x11; 16]`, SubscriptionId `1`,
/// accepted compression `[1]`, PushIntervalMs `1000`, TelemetryMaxBytes
/// `100`, DeltaTemporality `true`, RequestedMetrics `["m"]`, error
/// `UNSUPPORTED_VERSION` (35): the top-level ErrorCode is the INT16 at
/// **bytes 4–5** — not bytes 5–6 (DescribeTopicPartitions /
/// ShareGroupDescribe first-topic / first-group), 7–8 (DeleteGroups
/// after GroupId), 8–9 (DescribeShareGroupOffsets first-group after
/// GroupId and Topics), 12–13 (DescribeProducers first partition), or
/// 27–28 (DescribeTopicPartitions first-partition). i16=35 hits only
/// at byte 4. This offset happens to match ListConfigResources /
/// DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups
/// top-level INT16; it was measured on this API's official top-level
/// field, not copied. Because 16 is not listed, this is broker-only:
/// no FindCoordinator, no controller hop, no partition-leader hop.
pub fn encode_get_telemetry_subscriptions_request(
    buf: &mut BytesMut,
    client_instance_id: &[u8; 16],
) -> crate::error::Result<()> {
    buf.extend_from_slice(client_instance_id);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_get_telemetry_subscriptions_request<B: Buf>(buf: &mut B) -> Result<[u8; 16]> {
    let client_instance_id = buf::get_uuid(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(client_instance_id)
}

pub fn encode_get_telemetry_subscriptions_response(
    buf: &mut BytesMut,
    resp: &GetTelemetrySubscriptionsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf.extend_from_slice(&resp.client_instance_id);
    buf.put_i32(resp.subscription_id);
    buf::put_array_len(buf, true, Some(resp.accepted_compression_types.len()))?;
    for ty in &resp.accepted_compression_types {
        buf.put_i8(*ty);
    }
    buf.put_i32(resp.push_interval_ms);
    buf.put_i32(resp.telemetry_max_bytes);
    buf.put_u8(u8::from(resp.delta_temporality));
    buf::put_array_len(buf, true, Some(resp.requested_metrics.len()))?;
    for m in &resp.requested_metrics {
        buf::put_compact_string(buf, Some(m))?;
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_get_telemetry_subscriptions_response<B: Buf>(
    buf: &mut B,
) -> Result<GetTelemetrySubscriptionsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let client_instance_id = buf::get_uuid(buf)?;
    let subscription_id = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut accepted_compression_types = Vec::with_capacity(n);
    for _ in 0..n {
        accepted_compression_types.push(buf::get_i8(buf)?);
    }
    let push_interval_ms = buf::get_i32(buf)?;
    let telemetry_max_bytes = buf::get_i32(buf)?;
    let delta_temporality = buf::get_bool(buf)?;
    let mn = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut requested_metrics = Vec::with_capacity(mn);
    for _ in 0..mn {
        requested_metrics.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    buf::skip_tagged_fields(buf)?;
    Ok(GetTelemetrySubscriptionsResponse {
        error_code,
        client_instance_id,
        subscription_id,
        accepted_compression_types,
        push_interval_ms,
        telemetry_max_bytes,
        delta_temporality,
        requested_metrics,
    })
}

/// PushTelemetry (api 72) v0 request body.
///
/// Official Apache JSON (`apiKey: 72`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Metrics are compact
/// BYTES (OTLP MetricsData). There is no per-metric ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTelemetryRequest {
    pub client_instance_id: [u8; 16],
    pub subscription_id: i32,
    pub terminating: bool,
    pub compression_type: i8,
    pub metrics: Vec<u8>,
}

impl PushTelemetryRequest {
    pub fn new(
        client_instance_id: [u8; 16],
        subscription_id: i32,
        terminating: bool,
        compression_type: i8,
        metrics: Vec<u8>,
    ) -> Self {
        Self {
            client_instance_id,
            subscription_id,
            terminating,
            compression_type,
            metrics,
        }
    }
}

/// PushTelemetry (api 72) v0 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-metric
/// field and not a first-payload field. The response has no Metrics
/// array. Official JSON lists only `ThrottleTimeMs` and `ErrorCode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTelemetryResponse {
    pub error_code: i16,
}

impl PushTelemetryResponse {
    pub fn new(error_code: i16) -> Self {
        Self { error_code }
    }
}

/// PushTelemetry v0 (flexible from v0; KIP-714).
///
/// Official Apache JSON (`apiKey: 72`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Official JSON lists
/// **no** `errorCodes`. Official Java
/// `KafkaApis.handlePushTelemetryRequest` answers from the connected
/// broker (`clientMetricsManager.processPushTelemetryRequest`).
/// Official Java `PushTelemetryRequest.getErrorResponse` writes the
/// exception onto the top-level `ErrorCode`. Handler-observed codes:
/// `INVALID_REQUEST` (42) on catch-all / reserved ClientInstanceId /
/// already-terminating, `UNKNOWN_SUBSCRIPTION_ID` (117) on subscription
/// mismatch, `THROTTLING_QUOTA_EXCEEDED` (89) when a push arrives
/// before the interval, `UNSUPPORTED_COMPRESSION_TYPE` (76) on unknown
/// compression, `TELEMETRY_TOO_LARGE` (118) when metrics exceed
/// `client.telemetry.max.bytes`, `INVALID_RECORD` (87) on plugin
/// export failure. `NOT_COORDINATOR` (16) is **not** listed.
/// kafka-protocol 0.18.0 (`PushTelemetryRequest` /
/// `PushTelemetryResponse`, `VERSIONS` min=0 max=0). This crate
/// targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: `ClientInstanceId` UUID, `SubscriptionId` INT32,
/// `Terminating` BOOLEAN, `CompressionType` INT8, compact `Metrics`
/// BYTES, tagged. Response: `ThrottleTimeMs` INT32, top-level
/// `ErrorCode` INT16, tagged.
/// **ErrorCode is top-level**, after throttle — not a first-metric
/// field and not a first-payload field. Measured independently from
/// kafka-protocol 0.18.0 (`broker` encodes the response) on leftover-
/// empty fixture throttle `0`, error `INVALID_REQUEST` (42): the
/// top-level ErrorCode is the INT16 at **bytes 4–5**. The leftover-
/// empty body is 7 bytes, so there is no first-metric / first-payload
/// ErrorCode and no INT16 at bytes 7–8 (DeleteGroups), 8–9
/// (DescribeShareGroupOffsets), 12–13 (DescribeProducers), or 27–28
/// (DescribeTopicPartitions first-partition). i16=42 hits only at
/// byte 4. This offset happens to match GetTelemetrySubscriptions /
/// ListConfigResources / DeleteShareGroupOffsets /
/// AlterShareGroupOffsets / ListGroups top-level INT16; it was
/// measured on this API's official top-level field, not copied.
/// Because 16 is not listed, this is broker-only: no
/// FindCoordinator, no controller hop, no partition-leader hop.
pub fn encode_push_telemetry_request(
    buf: &mut BytesMut,
    req: &PushTelemetryRequest,
) -> crate::error::Result<()> {
    buf.extend_from_slice(&req.client_instance_id);
    buf.put_i32(req.subscription_id);
    buf.put_u8(u8::from(req.terminating));
    buf.put_i8(req.compression_type);
    buf::put_compact_bytes(buf, Some(&req.metrics))?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_push_telemetry_request<B: Buf>(buf: &mut B) -> Result<PushTelemetryRequest> {
    let client_instance_id = buf::get_uuid(buf)?;
    let subscription_id = buf::get_i32(buf)?;
    let terminating = buf::get_bool(buf)?;
    let compression_type = buf::get_i8(buf)?;
    let metrics = buf::get_compact_bytes(buf)?.unwrap_or_default();
    buf::skip_tagged_fields(buf)?;
    Ok(PushTelemetryRequest {
        client_instance_id,
        subscription_id,
        terminating,
        compression_type,
        metrics,
    })
}

pub fn encode_push_telemetry_response(
    buf: &mut BytesMut,
    resp: &PushTelemetryResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_push_telemetry_response<B: Buf>(buf: &mut B) -> Result<PushTelemetryResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(PushTelemetryResponse { error_code })
}

/// One partition in an AssignReplicasToDirs (api 73) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsPartition {
    pub partition_index: i32,
}

impl AssignReplicasToDirsPartition {
    pub fn new(partition_index: i32) -> Self {
        Self { partition_index }
    }
}

/// One topic in an AssignReplicasToDirs (api 73) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsTopic {
    pub topic_id: [u8; 16],
    pub partitions: Vec<AssignReplicasToDirsPartition>,
}

impl AssignReplicasToDirsTopic {
    pub fn new(topic_id: [u8; 16], partitions: Vec<AssignReplicasToDirsPartition>) -> Self {
        Self {
            topic_id,
            partitions,
        }
    }
}

/// One directory in an AssignReplicasToDirs (api 73) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsDirectory {
    pub id: [u8; 16],
    pub topics: Vec<AssignReplicasToDirsTopic>,
}

impl AssignReplicasToDirsDirectory {
    pub fn new(id: [u8; 16], topics: Vec<AssignReplicasToDirsTopic>) -> Self {
        Self { id, topics }
    }
}

/// AssignReplicasToDirs (api 73) v0 request body.
///
/// Official Apache JSON (`apiKey: 73`, request `listeners: ["controller"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Official JSON lists
/// no `errorCodes`. Request has no ErrorCode field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsRequest {
    pub broker_id: i32,
    pub broker_epoch: i64,
    pub directories: Vec<AssignReplicasToDirsDirectory>,
}

impl AssignReplicasToDirsRequest {
    pub fn new(
        broker_id: i32,
        broker_epoch: i64,
        directories: Vec<AssignReplicasToDirsDirectory>,
    ) -> Self {
        Self {
            broker_id,
            broker_epoch,
            directories,
        }
    }
}

/// One partition in an AssignReplicasToDirs (api 73) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsResponsePartition {
    pub partition_index: i32,
    pub error_code: i16,
}

impl AssignReplicasToDirsResponsePartition {
    pub fn new(partition_index: i32, error_code: i16) -> Self {
        Self {
            partition_index,
            error_code,
        }
    }
}

/// One topic in an AssignReplicasToDirs (api 73) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsResponseTopic {
    pub topic_id: [u8; 16],
    pub partitions: Vec<AssignReplicasToDirsResponsePartition>,
}

impl AssignReplicasToDirsResponseTopic {
    pub fn new(topic_id: [u8; 16], partitions: Vec<AssignReplicasToDirsResponsePartition>) -> Self {
        Self {
            topic_id,
            partitions,
        }
    }
}

/// One directory in an AssignReplicasToDirs (api 73) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsResponseDirectory {
    pub id: [u8; 16],
    pub topics: Vec<AssignReplicasToDirsResponseTopic>,
}

impl AssignReplicasToDirsResponseDirectory {
    pub fn new(id: [u8; 16], topics: Vec<AssignReplicasToDirsResponseTopic>) -> Self {
        Self { id, topics }
    }
}

/// AssignReplicasToDirs (api 73) v0 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-directory
/// field and not a first-partition field. Official JSON then lists
/// compact `Directories` with a nested per-partition ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsResponse {
    pub error_code: i16,
    pub directories: Vec<AssignReplicasToDirsResponseDirectory>,
}

impl AssignReplicasToDirsResponse {
    pub fn new(error_code: i16, directories: Vec<AssignReplicasToDirsResponseDirectory>) -> Self {
        Self {
            error_code,
            directories,
        }
    }
}

/// AssignReplicasToDirs v0 (flexible from v0; KIP-858).
///
/// Official Apache JSON (`apiKey: 73`, request `listeners: ["controller"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Official JSON lists
/// **no** `errorCodes`. Official Java
/// `AssignReplicasToDirsRequest.getErrorResponse` writes
/// `Errors.forException(e).code()` onto the top-level `ErrorCode`.
/// Official Java `QuorumController.assignReplicasToDirs` is an
/// `appendWriteEvent`; `ControllerWriteEvent.run` throws
/// `ControllerExceptions.newWrongControllerException` (a
/// `NotControllerException`) when the node is not the active
/// controller, which `getErrorResponse` writes as `NOT_CONTROLLER`
/// (41) on the top-level ErrorCode. Official Java
/// `ReplicationControlManager.handleAssignReplicasToDirs` does not
/// write 41 or `NOT_COORDINATOR` (16); handler-observed per-partition
/// codes are `UNKNOWN_TOPIC_ID`, `UNKNOWN_TOPIC_OR_PARTITION`, and
/// `NOT_LEADER_OR_FOLLOWER` (6) when the broker is not a replica.
/// kafka-protocol 0.18.0 (`AssignReplicasToDirsRequest` /
/// `AssignReplicasToDirsResponse`, `VERSIONS` min=0 max=0). This crate
/// targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: `BrokerId` INT32, `BrokerEpoch` INT64, compact
/// `Directories` of `{Id UUID, Topics compact [{TopicId UUID,
/// Partitions compact [{PartitionIndex INT32, tagged}], tagged}],
/// tagged}`, tagged. Response: `ThrottleTimeMs` INT32, top-level
/// `ErrorCode` INT16, compact `Directories` of `{Id UUID, Topics
/// compact [{TopicId UUID, Partitions compact [{PartitionIndex INT32,
/// ErrorCode INT16, tagged}], tagged}], tagged}`, tagged.
/// **ErrorCode is top-level**, after throttle — not a first-directory
/// field (directory `Id` is a UUID) and not a first-partition field.
/// Measured independently from kafka-protocol 0.18.0 (`broker` encodes
/// the response) on leftover-empty fixture throttle `0`, error
/// `NOT_CONTROLLER` (41), empty `Directories`: the top-level ErrorCode
/// is the INT16 at **bytes 4–5**. The leftover-empty body is 8 bytes
/// (throttle + INT16 + compact empty array + tagged), so there is no
/// first-directory ErrorCode and no first-partition ErrorCode and no
/// INT16 at bytes 7–8 (DeleteGroups), 8–9
/// (DescribeShareGroupOffsets), 12–13 (DescribeProducers), or 27–28
/// (DescribeTopicPartitions first-partition). i16=41 hits only at
/// byte 4. A one-directory / one-topic / one-partition fixture still
/// has 41 only at bytes 4–5; the first-partition ErrorCode is at
/// bytes 45–46. This offset happens to match PushTelemetry /
/// GetTelemetrySubscriptions / ListConfigResources / ListGroups
/// top-level INT16; it was measured on this API's official top-level
/// field, not copied. Because 41 is the hop written by
/// `getErrorResponse` on `NotControllerException` and listeners are
/// controller only, this is a controller hop via Metadata
/// `controller_id`. No FindCoordinator, no `key_type`, no
/// partition-leader hop on 6 (6 is a per-partition handler code and
/// is not listed in the official JSON).
pub fn encode_assign_replicas_to_dirs_request(
    buf: &mut BytesMut,
    req: &AssignReplicasToDirsRequest,
) -> crate::error::Result<()> {
    buf.put_i32(req.broker_id);
    buf.put_i64(req.broker_epoch);
    buf::put_array_len(buf, true, Some(req.directories.len()))?;
    for dir in &req.directories {
        buf.extend_from_slice(&dir.id);
        buf::put_array_len(buf, true, Some(dir.topics.len()))?;
        for topic in &dir.topics {
            buf.extend_from_slice(&topic.topic_id);
            buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
            for part in &topic.partitions {
                buf.put_i32(part.partition_index);
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_assign_replicas_to_dirs_request<B: Buf>(
    buf: &mut B,
) -> Result<AssignReplicasToDirsRequest> {
    let broker_id = buf::get_i32(buf)?;
    let broker_epoch = buf::get_i64(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut directories = Vec::with_capacity(n);
    for _ in 0..n {
        let id = buf::get_uuid(buf)?;
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let topic_id = buf::get_uuid(buf)?;
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                let partition_index = buf::get_i32(buf)?;
                buf::skip_tagged_fields(buf)?;
                partitions.push(AssignReplicasToDirsPartition { partition_index });
            }
            buf::skip_tagged_fields(buf)?;
            topics.push(AssignReplicasToDirsTopic {
                topic_id,
                partitions,
            });
        }
        buf::skip_tagged_fields(buf)?;
        directories.push(AssignReplicasToDirsDirectory { id, topics });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(AssignReplicasToDirsRequest {
        broker_id,
        broker_epoch,
        directories,
    })
}

pub fn encode_assign_replicas_to_dirs_response(
    buf: &mut BytesMut,
    resp: &AssignReplicasToDirsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, true, Some(resp.directories.len()))?;
    for dir in &resp.directories {
        buf.extend_from_slice(&dir.id);
        buf::put_array_len(buf, true, Some(dir.topics.len()))?;
        for topic in &dir.topics {
            buf.extend_from_slice(&topic.topic_id);
            buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
            for part in &topic.partitions {
                buf.put_i32(part.partition_index);
                buf.put_i16(part.error_code);
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_assign_replicas_to_dirs_response<B: Buf>(
    buf: &mut B,
) -> Result<AssignReplicasToDirsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut directories = Vec::with_capacity(n);
    for _ in 0..n {
        let id = buf::get_uuid(buf)?;
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let topic_id = buf::get_uuid(buf)?;
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                let partition_index = buf::get_i32(buf)?;
                let error_code = buf::get_i16(buf)?;
                buf::skip_tagged_fields(buf)?;
                partitions.push(AssignReplicasToDirsResponsePartition {
                    partition_index,
                    error_code,
                });
            }
            buf::skip_tagged_fields(buf)?;
            topics.push(AssignReplicasToDirsResponseTopic {
                topic_id,
                partitions,
            });
        }
        buf::skip_tagged_fields(buf)?;
        directories.push(AssignReplicasToDirsResponseDirectory { id, topics });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(AssignReplicasToDirsResponse {
        error_code,
        directories,
    })
}

/// One topic in an AlterReplicaLogDirs (api 34) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

impl AlterReplicaLogDirsTopic {
    pub fn new(name: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            name: name.into(),
            partitions,
        }
    }
}

/// One directory in an AlterReplicaLogDirs (api 34) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsDirectory {
    pub path: String,
    pub topics: Vec<AlterReplicaLogDirsTopic>,
}

impl AlterReplicaLogDirsDirectory {
    pub fn new(path: impl Into<String>, topics: Vec<AlterReplicaLogDirsTopic>) -> Self {
        Self {
            path: path.into(),
            topics,
        }
    }
}

/// AlterReplicaLogDirs (api 34) v2 request body.
///
/// Official Apache JSON (`apiKey: 34`, request `listeners: ["broker"]`,
/// `validVersions: "1-2"`, `flexibleVersions: "2+"`). Official JSON lists
/// no `errorCodes`. Request has no ErrorCode field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsRequest {
    pub dirs: Vec<AlterReplicaLogDirsDirectory>,
}

impl AlterReplicaLogDirsRequest {
    pub fn new(dirs: Vec<AlterReplicaLogDirsDirectory>) -> Self {
        Self { dirs }
    }
}

/// One partition in an AlterReplicaLogDirs (api 34) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsResponsePartition {
    pub partition_index: i32,
    pub error_code: i16,
}

impl AlterReplicaLogDirsResponsePartition {
    pub fn new(partition_index: i32, error_code: i16) -> Self {
        Self {
            partition_index,
            error_code,
        }
    }
}

/// One topic in an AlterReplicaLogDirs (api 34) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsResponseTopic {
    pub topic_name: String,
    pub partitions: Vec<AlterReplicaLogDirsResponsePartition>,
}

impl AlterReplicaLogDirsResponseTopic {
    pub fn new(
        topic_name: impl Into<String>,
        partitions: Vec<AlterReplicaLogDirsResponsePartition>,
    ) -> Self {
        Self {
            topic_name: topic_name.into(),
            partitions,
        }
    }
}

/// AlterReplicaLogDirs (api 34) v2 response body.
///
/// **ErrorCode is first-partition**, not top-level and not a
/// first-directory field. Official JSON has no top-level ErrorCode;
/// throttle is followed by compact `Results` of `{TopicName,
/// Partitions of {PartitionIndex, ErrorCode}}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsResponse {
    pub results: Vec<AlterReplicaLogDirsResponseTopic>,
}

impl AlterReplicaLogDirsResponse {
    pub fn new(results: Vec<AlterReplicaLogDirsResponseTopic>) -> Self {
        Self { results }
    }
}

/// AlterReplicaLogDirs v2 (flexible from v2; KIP-113).
///
/// Official Apache JSON (`apiKey: 34`, request `listeners: ["broker"]`,
/// `validVersions: "1-2"`, `flexibleVersions: "2+"`). Official JSON lists
/// **no** `errorCodes`. Official Java
/// `AlterReplicaLogDirsRequest.getErrorResponse` writes
/// `Errors.forException(e).code()` onto each **partition** ErrorCode
/// (no top-level field). Official Java
/// `KafkaApis.handleAlterReplicaLogDirsRequest` answers from the
/// connected broker (`replicaManager.alterReplicaLogDirs`); it does
/// not look up a controller or a coordinator. Auth failure uses
/// `getErrorResponse` with `CLUSTER_AUTHORIZATION_FAILED` (31).
/// Official `ReplicaManager.alterReplicaLogDirs` handler-observed
/// codes: `INVALID_TOPIC_EXCEPTION` (17) on a too-long topic name,
/// `KAFKA_STORAGE_ERROR` (56) when the destination dir or partition
/// is offline, `INVALID_REPLICA_ASSIGNMENT` (39) when the dir is
/// cordoned, `LOG_DIR_NOT_FOUND` (57), `REPLICA_NOT_AVAILABLE` (9)
/// when `getPartitionOrException` throws
/// `NotLeaderOrFollowerException` (retained as 9 for compatibility;
/// 6 is **not** written). `NOT_COORDINATOR` (16) is **not** listed.
/// `NOT_CONTROLLER` (41) is **not** listed. kafka-protocol 0.18.0
/// (`AlterReplicaLogDirsRequest` / `AlterReplicaLogDirsResponse`,
/// `VERSIONS` min=1 max=2). This crate targets v2, the version a
/// client encodes (`VERSIONS.max`). Request encode used
/// `features = ["client"]`; response encode used `broker`. Request:
/// compact `Dirs` of `{Path compact STRING, Topics compact [{Name
/// compact STRING, Partitions compact INT32[], tagged}], tagged}`,
/// tagged. Response: `ThrottleTimeMs` INT32, compact `Results` of
/// `{TopicName compact STRING, Partitions compact [{PartitionIndex
/// INT32, ErrorCode INT16, tagged}], tagged}`, tagged.
/// **ErrorCode is first-partition**, after throttle, compact results
/// len, compact topic name, compact partitions len, and
/// PartitionIndex — not a top-level field and not a first-directory
/// field (request directories are paths; the response has no
/// directory array). Measured independently from kafka-protocol
/// 0.18.0 (`broker` encodes the response) on leftover-empty fixture
/// throttle `0`, empty `Results`: the leftover-empty body is **6
/// bytes** (throttle + compact empty array + tagged) and has **no
/// ErrorCode**. On leftover-empty fixture topic `"t"` partition `0`,
/// error `CLUSTER_AUTHORIZATION_FAILED` (31): the first-partition
/// ErrorCode is the INT16 at **bytes 12–13**. i16=31 hits only at
/// byte 12. There is no top-level ErrorCode and no INT16 at bytes
/// 4–5 (AssignReplicasToDirs / PushTelemetry /
/// GetTelemetrySubscriptions / ListConfigResources), 5–6
/// (DescribeTopicPartitions / ShareGroupDescribe), 7–8
/// (DeleteGroups), 8–9 (DescribeShareGroupOffsets), 27–28
/// (DescribeTopicPartitions first-partition), or 45–46
/// (AssignReplicasToDirs first-partition). This offset happens to
/// match DescribeProducers first-partition INT16; it was measured
/// on this API's official first-partition field, not copied.
/// Because 41 is not listed, 16 is not listed, and 6 is converted
/// to 9 as a per-partition handler code (not a client hop), this is
/// broker-only: no FindCoordinator, no `key_type`, no controller
/// hop, no partition-leader hop.
pub fn encode_alter_replica_log_dirs_request(
    buf: &mut BytesMut,
    req: &AlterReplicaLogDirsRequest,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(req.dirs.len()))?;
    for dir in &req.dirs {
        buf::put_compact_string(buf, Some(&dir.path))?;
        buf::put_array_len(buf, true, Some(dir.topics.len()))?;
        for topic in &dir.topics {
            buf::put_compact_string(buf, Some(&topic.name))?;
            buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
            for part in &topic.partitions {
                buf.put_i32(*part);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_alter_replica_log_dirs_request<B: Buf>(
    buf: &mut B,
) -> Result<AlterReplicaLogDirsRequest> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut dirs = Vec::with_capacity(n);
    for _ in 0..n {
        let path = buf::get_compact_string(buf)?.unwrap_or_default();
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let name = buf::get_compact_string(buf)?.unwrap_or_default();
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                partitions.push(buf::get_i32(buf)?);
            }
            buf::skip_tagged_fields(buf)?;
            topics.push(AlterReplicaLogDirsTopic { name, partitions });
        }
        buf::skip_tagged_fields(buf)?;
        dirs.push(AlterReplicaLogDirsDirectory { path, topics });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(AlterReplicaLogDirsRequest { dirs })
}

pub fn encode_alter_replica_log_dirs_response(
    buf: &mut BytesMut,
    resp: &AlterReplicaLogDirsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(resp.results.len()))?;
    for topic in &resp.results {
        buf::put_compact_string(buf, Some(&topic.topic_name))?;
        buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
        for part in &topic.partitions {
            buf.put_i32(part.partition_index);
            buf.put_i16(part.error_code);
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_alter_replica_log_dirs_response<B: Buf>(
    buf: &mut B,
) -> Result<AlterReplicaLogDirsResponse> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(AlterReplicaLogDirsResponsePartition {
                partition_index,
                error_code,
            });
        }
        buf::skip_tagged_fields(buf)?;
        results.push(AlterReplicaLogDirsResponseTopic {
            topic_name,
            partitions,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(AlterReplicaLogDirsResponse { results })
}

/// One topic in a DescribeLogDirs (api 35) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribableLogDirTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

impl DescribableLogDirTopic {
    pub fn new(name: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            name: name.into(),
            partitions,
        }
    }
}

/// DescribeLogDirs (api 35) v4 request body.
///
/// Official Apache JSON (`apiKey: 35`, request `listeners: ["broker"]`,
/// `validVersions: "1-5"` on trunk / `"0-4"` on the 3.9.1 JSON
/// kafka-protocol 0.18.0 was generated against, `flexibleVersions:
/// "2+"`). Official JSON lists no `errorCodes`. Request has no
/// ErrorCode field. `Topics` is nullable: null means all topics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsRequest {
    pub topics: Option<Vec<DescribableLogDirTopic>>,
}

impl DescribeLogDirsRequest {
    pub fn new(topics: Option<Vec<DescribableLogDirTopic>>) -> Self {
        Self { topics }
    }
}

/// One partition in a DescribeLogDirs (api 35) response.
///
/// Official JSON has no partition ErrorCode. Fields are
/// PartitionIndex, PartitionSize, OffsetLag, IsFutureKey.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsPartition {
    pub partition_index: i32,
    pub partition_size: i64,
    pub offset_lag: i64,
    pub is_future_key: bool,
}

impl DescribeLogDirsPartition {
    pub fn new(
        partition_index: i32,
        partition_size: i64,
        offset_lag: i64,
        is_future_key: bool,
    ) -> Self {
        Self {
            partition_index,
            partition_size,
            offset_lag,
            is_future_key,
        }
    }
}

/// One topic in a DescribeLogDirs (api 35) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsTopic {
    pub name: String,
    pub partitions: Vec<DescribeLogDirsPartition>,
}

impl DescribeLogDirsTopic {
    pub fn new(name: impl Into<String>, partitions: Vec<DescribeLogDirsPartition>) -> Self {
        Self {
            name: name.into(),
            partitions,
        }
    }
}

/// One directory in a DescribeLogDirs (api 35) response.
///
/// First-directory ErrorCode is this struct's `error_code`, not a
/// first-partition field. `total_bytes` / `usable_bytes` are v4
/// (official JSON default `-1`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsResult {
    pub error_code: i16,
    pub log_dir: String,
    pub topics: Vec<DescribeLogDirsTopic>,
    pub total_bytes: i64,
    pub usable_bytes: i64,
}

impl DescribeLogDirsResult {
    pub fn new(
        error_code: i16,
        log_dir: impl Into<String>,
        topics: Vec<DescribeLogDirsTopic>,
        total_bytes: i64,
        usable_bytes: i64,
    ) -> Self {
        Self {
            error_code,
            log_dir: log_dir.into(),
            topics,
            total_bytes,
            usable_bytes,
        }
    }
}

/// DescribeLogDirs (api 35) v4 response body.
///
/// **ErrorCode is top-level**, after throttle. Official JSON adds
/// top-level ErrorCode at versions `3+`. Each result also has a
/// first-directory ErrorCode. There is no first-partition ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsResponse {
    pub error_code: i16,
    pub results: Vec<DescribeLogDirsResult>,
}

impl DescribeLogDirsResponse {
    pub fn new(error_code: i16, results: Vec<DescribeLogDirsResult>) -> Self {
        Self {
            error_code,
            results,
        }
    }
}

/// DescribeLogDirs v4 (flexible from v2; KIP-113 / KIP-784 / KIP-827).
///
/// Official Apache JSON (`apiKey: 35`, request `listeners: ["broker"]`,
/// trunk `validVersions: "1-5"`, 3.9.1 `validVersions: "0-4"`,
/// `flexibleVersions: "2+"`). Official JSON lists **no** `errorCodes`.
/// Official Java `KafkaApis.handleDescribeLogDirsRequest` answers from
/// the connected broker (`replicaManager.describeLogDirs`); it does
/// not look up a controller or a coordinator. Auth failure writes
/// `CLUSTER_AUTHORIZATION_FAILED` (31) onto the **top-level**
/// ErrorCode (KIP-784). Official `ReplicaManager.describeLogDirs`
/// writes `KAFKA_STORAGE_ERROR` (56) onto a **first-directory**
/// ErrorCode when that dir is offline, or `Errors.forException(t).code()`
/// for other throwables. `NOT_COORDINATOR` (16) is **not** listed.
/// `NOT_CONTROLLER` (41) is **not** listed. `NOT_LEADER_OR_FOLLOWER`
/// (6) is **not** a client hop. kafka-protocol 0.18.0
/// (`DescribeLogDirsRequest` / `DescribeLogDirsResponse`, `VERSIONS`
/// min=1 max=4). This crate targets v4, the version a client encodes
/// (`VERSIONS.max`). Official trunk lists a later version; that later
/// version stays a named codec gap and is not encoded. Request encode
/// used `features = ["client"]`; response encode used `broker`.
/// Request: compact nullable `Topics` of `{Topic compact STRING,
/// Partitions compact INT32[], tagged}`, tagged. Response:
/// `ThrottleTimeMs` INT32, **top-level `ErrorCode` INT16** (v3+),
/// compact `Results` of `{ErrorCode INT16, LogDir compact STRING,
/// Topics compact [{Name compact STRING, Partitions compact
/// [{PartitionIndex INT32, PartitionSize INT64, OffsetLag INT64,
/// IsFutureKey BOOLEAN, tagged}], tagged}], TotalBytes INT64,
/// UsableBytes INT64, tagged}`, tagged. v4 directory fields are
/// TotalBytes and UsableBytes (official JSON default `-1`).
/// **ErrorCode is top-level**, after throttle — not a first-directory
/// field and not a first-partition field. Measured independently from
/// kafka-protocol 0.18.0 (`broker` encodes the response) on leftover-
/// empty fixture throttle `0`, empty `Results`, error
/// `CLUSTER_AUTHORIZATION_FAILED` (31): the leftover-empty body is
/// **8 bytes** (throttle + top-level INT16 + compact empty array +
/// tagged) and the top-level ErrorCode is the INT16 at **bytes 4–5**.
/// i16=31 hits only at byte 4. On leftover-empty fixture directory
/// `"/d"` topic `"t"` partition `0`, top-level 31 and first-directory
/// 0: the first-directory ErrorCode is the INT16 at **bytes 7–8**.
/// There is no first-partition ErrorCode. Do not assume bytes 4–5
/// from AssignReplicasToDirs / PushTelemetry /
/// GetTelemetrySubscriptions / ListConfigResources: this offset was
/// measured on this API's official top-level field (versions 3+).
/// Not bytes 5–6 (DescribeTopicPartitions / ShareGroupDescribe), 7–8
/// as the hop/auth code (DeleteGroups after GroupId; 7–8 here is
/// first-directory, not the top-level hop field), 8–9
/// (DescribeShareGroupOffsets), 12–13 (AlterReplicaLogDirs /
/// DescribeProducers first-partition), 27–28, or 45–46. Because 41
/// is not listed, 16 is not listed, and 6 is not a client hop, this
/// is broker-only: no FindCoordinator, no `key_type`, no controller
/// hop, no partition-leader hop.
pub fn encode_describe_log_dirs_request(
    buf: &mut BytesMut,
    req: &DescribeLogDirsRequest,
) -> crate::error::Result<()> {
    match &req.topics {
        None => buf::put_array_len(buf, true, None)?,
        Some(topics) => {
            buf::put_array_len(buf, true, Some(topics.len()))?;
            for topic in topics {
                buf::put_compact_string(buf, Some(&topic.name))?;
                buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
                for part in &topic.partitions {
                    buf.put_i32(*part);
                }
                buf::put_empty_tagged_fields(buf);
            }
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_log_dirs_request<B: Buf>(buf: &mut B) -> Result<DescribeLogDirsRequest> {
    let topics = match buf::get_array_len(buf, true)? {
        None => None,
        Some(n) => {
            let mut topics = Vec::with_capacity(n);
            for _ in 0..n {
                let name = buf::get_compact_string(buf)?.unwrap_or_default();
                let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
                let mut partitions = Vec::with_capacity(pn);
                for _ in 0..pn {
                    partitions.push(buf::get_i32(buf)?);
                }
                buf::skip_tagged_fields(buf)?;
                topics.push(DescribableLogDirTopic { name, partitions });
            }
            Some(topics)
        }
    };
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeLogDirsRequest { topics })
}

pub fn encode_describe_log_dirs_response(
    buf: &mut BytesMut,
    resp: &DescribeLogDirsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, true, Some(resp.results.len()))?;
    for dir in &resp.results {
        buf.put_i16(dir.error_code);
        buf::put_compact_string(buf, Some(&dir.log_dir))?;
        buf::put_array_len(buf, true, Some(dir.topics.len()))?;
        for topic in &dir.topics {
            buf::put_compact_string(buf, Some(&topic.name))?;
            buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
            for part in &topic.partitions {
                buf.put_i32(part.partition_index);
                buf.put_i64(part.partition_size);
                buf.put_i64(part.offset_lag);
                buf.put_u8(u8::from(part.is_future_key));
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf.put_i64(dir.total_bytes);
        buf.put_i64(dir.usable_bytes);
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_describe_log_dirs_response<B: Buf>(buf: &mut B) -> Result<DescribeLogDirsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let dir_error = buf::get_i16(buf)?;
        let log_dir = buf::get_compact_string(buf)?.unwrap_or_default();
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let name = buf::get_compact_string(buf)?.unwrap_or_default();
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                let partition_index = buf::get_i32(buf)?;
                let partition_size = buf::get_i64(buf)?;
                let offset_lag = buf::get_i64(buf)?;
                let is_future_key = buf::get_bool(buf)?;
                buf::skip_tagged_fields(buf)?;
                partitions.push(DescribeLogDirsPartition {
                    partition_index,
                    partition_size,
                    offset_lag,
                    is_future_key,
                });
            }
            buf::skip_tagged_fields(buf)?;
            topics.push(DescribeLogDirsTopic { name, partitions });
        }
        let total_bytes = buf::get_i64(buf)?;
        let usable_bytes = buf::get_i64(buf)?;
        buf::skip_tagged_fields(buf)?;
        results.push(DescribeLogDirsResult {
            error_code: dir_error,
            log_dir,
            topics,
            total_bytes,
            usable_bytes,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeLogDirsResponse {
        error_code,
        results,
    })
}

/// One renewer principal in a CreateDelegationToken (api 38) request.
///
/// Official JSON `CreatableRenewers` has PrincipalType and
/// PrincipalName only. There is no per-renewer ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableRenewer {
    pub principal_type: String,
    pub principal_name: String,
}

impl CreatableRenewer {
    pub fn new(principal_type: impl Into<String>, principal_name: impl Into<String>) -> Self {
        Self {
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
        }
    }
}

/// CreateDelegationToken (api 38) v3 request body.
///
/// Official Apache JSON (`apiKey: 38`, request `listeners: ["broker",
/// "controller"]` on trunk / `["zkBroker", "broker", "controller"]` on
/// the 3.9.1 JSON kafka-protocol 0.18.0 was generated against,
/// `validVersions: "1-3"` on trunk / `"0-3"` on 3.9.1,
/// `flexibleVersions: "2+"`). Official JSON lists no `errorCodes`.
/// Request has no ErrorCode field. `OwnerPrincipalType` /
/// `OwnerPrincipalName` are nullable (v3+); null means the token
/// request principal. `MaxLifetimeMs` `-1` uses the server default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDelegationTokenRequest {
    pub owner_principal_type: Option<String>,
    pub owner_principal_name: Option<String>,
    pub renewers: Vec<CreatableRenewer>,
    pub max_lifetime_ms: i64,
}

impl CreateDelegationTokenRequest {
    pub fn new(
        owner_principal_type: Option<String>,
        owner_principal_name: Option<String>,
        renewers: Vec<CreatableRenewer>,
        max_lifetime_ms: i64,
    ) -> Self {
        Self {
            owner_principal_type,
            owner_principal_name,
            renewers,
            max_lifetime_ms,
        }
    }
}

/// CreateDelegationToken (api 38) v3 response body.
///
/// **ErrorCode is top-level**, first field — not after throttle.
/// Official JSON places `ThrottleTimeMs` last. This is a single token,
/// not a token array: there is no first-token ErrorCode and no
/// first-renewer ErrorCode (renewers are request-only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDelegationTokenResponse {
    pub error_code: i16,
    pub principal_type: String,
    pub principal_name: String,
    pub token_requester_principal_type: String,
    pub token_requester_principal_name: String,
    pub issue_timestamp_ms: i64,
    pub expiry_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub token_id: String,
    pub hmac: Vec<u8>,
}

impl CreateDelegationTokenResponse {
    #[expect(
        clippy::too_many_arguments,
        reason = "wire type follows the Kafka spec field-for-field"
    )]
    pub fn new(
        error_code: i16,
        principal_type: impl Into<String>,
        principal_name: impl Into<String>,
        token_requester_principal_type: impl Into<String>,
        token_requester_principal_name: impl Into<String>,
        issue_timestamp_ms: i64,
        expiry_timestamp_ms: i64,
        max_timestamp_ms: i64,
        token_id: impl Into<String>,
        hmac: Vec<u8>,
    ) -> Self {
        Self {
            error_code,
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
            token_requester_principal_type: token_requester_principal_type.into(),
            token_requester_principal_name: token_requester_principal_name.into(),
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id: token_id.into(),
            hmac,
        }
    }
}

/// CreateDelegationToken v3 (flexible from v2; KIP-48 / KIP-373).
///
/// Official Apache JSON (`apiKey: 38`, request `listeners: ["broker",
/// "controller"]` on trunk / `["zkBroker", "broker", "controller"]` on
/// 3.9.1, trunk `validVersions: "1-3"`, 3.9.1 `validVersions: "0-3"`,
/// `flexibleVersions: "2+"`). Official JSON lists **no** `errorCodes`.
/// Official Java `KafkaApis.handleCreateTokenRequest` validates the
/// connection (`allowTokenRequests` →
/// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64)), owner
/// `CREATE_TOKENS` (`DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65)),
/// and renewer principal type (`INVALID_PRINCIPAL_TYPE` (67)), then
/// `forwardToController` — broker-side envelope forwarding, not a
/// client hop. Official Java `CreateDelegationTokenRequest.getErrorResponse`
/// writes `Errors.forException(e).code()` onto the **top-level**
/// ErrorCode. Official Java `KafkaAdminClient.createDelegationToken`
/// uses `LeastLoadedNodeProvider` (any broker). `NOT_COORDINATOR`
/// (16) is **not** listed. `NOT_CONTROLLER` (41) is **not** listed.
/// `NOT_LEADER_OR_FOLLOWER` (6) is **not** a client hop.
/// kafka-protocol 0.18.0 (`CreateDelegationTokenRequest` /
/// `CreateDelegationTokenResponse`, `VERSIONS` min=1 max=3). This
/// crate targets v3, the version a client encodes (`VERSIONS.max`).
/// Official 3.9.1 lists a deprecated v0; that version is not encoded.
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: compact nullable `OwnerPrincipalType`, compact
/// nullable `OwnerPrincipalName` (v3+), compact `Renewers` of
/// `{PrincipalType compact STRING, PrincipalName compact STRING,
/// tagged}`, `MaxLifetimeMs` INT64, tagged. Response: **top-level
/// `ErrorCode` INT16 first**, compact `PrincipalType`, compact
/// `PrincipalName`, compact `TokenRequesterPrincipalType` (v3+),
/// compact `TokenRequesterPrincipalName` (v3+), `IssueTimestampMs`
/// INT64, `ExpiryTimestampMs` INT64, `MaxTimestampMs` INT64, compact
/// `TokenId`, compact `Hmac` BYTES, `ThrottleTimeMs` INT32 last,
/// tagged. **ErrorCode is top-level**, first field — not after
/// throttle, not a first-renewer field, and not a first-token field.
/// Measured independently from kafka-protocol 0.18.0 (`broker`
/// encodes the response) on leftover-empty fixture throttle `0`,
/// empty principals / token / hmac, error
/// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64): the leftover-empty
/// body is **37 bytes** and the top-level ErrorCode is the INT16 at
/// **bytes 0–1**. i16=64 hits only at byte 0. There is no
/// first-renewer ErrorCode and no first-token ErrorCode. Do not
/// assume bytes 4–5 from DescribeLogDirs / AssignReplicasToDirs /
/// PushTelemetry / GetTelemetrySubscriptions / ListConfigResources:
/// this offset was measured on this API's official first field.
/// Not bytes 5–6 (DescribeTopicPartitions / ShareGroupDescribe), 7–8
/// (DeleteGroups after GroupId; DescribeLogDirs first-directory),
/// 8–9 (DescribeShareGroupOffsets), 12–13 (AlterReplicaLogDirs /
/// DescribeProducers first-partition), 27–28, or 45–46. Because 41
/// is not listed, 16 is not listed, and 6 is not a client hop, this
/// is broker-only: no FindCoordinator, no `key_type`, no controller
/// hop, no partition-leader hop. This is not a token store.
pub fn encode_create_delegation_token_request(
    buf: &mut BytesMut,
    req: &CreateDelegationTokenRequest,
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, req.owner_principal_type.as_deref())?;
    buf::put_compact_string(buf, req.owner_principal_name.as_deref())?;
    buf::put_array_len(buf, true, Some(req.renewers.len()))?;
    for renewer in &req.renewers {
        buf::put_compact_string(buf, Some(&renewer.principal_type))?;
        buf::put_compact_string(buf, Some(&renewer.principal_name))?;
        buf::put_empty_tagged_fields(buf);
    }
    buf.put_i64(req.max_lifetime_ms);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_create_delegation_token_request<B: Buf>(
    buf: &mut B,
) -> Result<CreateDelegationTokenRequest> {
    let owner_principal_type = buf::get_compact_string(buf)?;
    let owner_principal_name = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut renewers = Vec::with_capacity(n);
    for _ in 0..n {
        let principal_type = buf::get_compact_string(buf)?.unwrap_or_default();
        let principal_name = buf::get_compact_string(buf)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        renewers.push(CreatableRenewer {
            principal_type,
            principal_name,
        });
    }
    let max_lifetime_ms = buf::get_i64(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(CreateDelegationTokenRequest {
        owner_principal_type,
        owner_principal_name,
        renewers,
        max_lifetime_ms,
    })
}

pub fn encode_create_delegation_token_response(
    buf: &mut BytesMut,
    resp: &CreateDelegationTokenResponse,
) -> crate::error::Result<()> {
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, Some(&resp.principal_type))?;
    buf::put_compact_string(buf, Some(&resp.principal_name))?;
    buf::put_compact_string(buf, Some(&resp.token_requester_principal_type))?;
    buf::put_compact_string(buf, Some(&resp.token_requester_principal_name))?;
    buf.put_i64(resp.issue_timestamp_ms);
    buf.put_i64(resp.expiry_timestamp_ms);
    buf.put_i64(resp.max_timestamp_ms);
    buf::put_compact_string(buf, Some(&resp.token_id))?;
    buf::put_compact_bytes(buf, Some(&resp.hmac))?;
    buf.put_i32(0);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_create_delegation_token_response<B: Buf>(
    buf: &mut B,
) -> Result<CreateDelegationTokenResponse> {
    let error_code = buf::get_i16(buf)?;
    let principal_type = buf::get_compact_string(buf)?.unwrap_or_default();
    let principal_name = buf::get_compact_string(buf)?.unwrap_or_default();
    let token_requester_principal_type = buf::get_compact_string(buf)?.unwrap_or_default();
    let token_requester_principal_name = buf::get_compact_string(buf)?.unwrap_or_default();
    let issue_timestamp_ms = buf::get_i64(buf)?;
    let expiry_timestamp_ms = buf::get_i64(buf)?;
    let max_timestamp_ms = buf::get_i64(buf)?;
    let token_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let hmac = buf::get_compact_bytes(buf)?.unwrap_or_default();
    let _th = buf::get_i32(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(CreateDelegationTokenResponse {
        error_code,
        principal_type,
        principal_name,
        token_requester_principal_type,
        token_requester_principal_name,
        issue_timestamp_ms,
        expiry_timestamp_ms,
        max_timestamp_ms,
        token_id,
        hmac,
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
    fn incremental_alter_configs_not_controller_is_not_at_byte_four() {
        let mut buf = BytesMut::new();
        encode_incremental_alter_configs_response(&mut buf, crate::error::NOT_CONTROLLER, "t")
            .unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "throttle + resource-array length must not look like error 41"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_incremental_alter_configs_response(&mut cur).unwrap(),
            crate::error::NOT_CONTROLLER
        );
        assert!(
            !cur.has_remaining(),
            "IncrementalAlterConfigs v0 NOT_CONTROLLER must be leftover-empty"
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

    #[test]
    fn alter_partition_reassignments_v0_roundtrip_is_leftover_empty() {
        let topics = vec![ReassignableTopic {
            name: "t".into(),
            partitions: vec![
                ReassignablePartition {
                    partition_index: 0,
                    replicas: Some(vec![1, 2]),
                },
                ReassignablePartition {
                    partition_index: 1,
                    replicas: None,
                },
            ],
        }];
        let mut buf = BytesMut::new();
        encode_alter_partition_reassignments_request(&mut buf, 10_000, &topics).unwrap();
        let mut cur = &buf[..];
        let (timeout, got) = decode_alter_partition_reassignments_request(&mut cur).unwrap();
        assert_eq!(timeout, 10_000);
        assert_eq!(got, topics);
        assert!(
            !cur.has_remaining(),
            "AlterPartitionReassignments v0 request must be leftover-empty"
        );

        let resp = AlterPartitionReassignmentsResponse {
            error_code: 0,
            error_message: None,
            results: vec![ReassignmentTopicResult {
                name: "t".into(),
                partitions: vec![
                    ReassignmentPartitionResult {
                        partition_index: 0,
                        error_code: 0,
                        error_message: None,
                    },
                    ReassignmentPartitionResult {
                        partition_index: 1,
                        error_code: 0,
                        error_message: None,
                    },
                ],
            }],
        };
        buf.clear();
        encode_alter_partition_reassignments_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_partition_reassignments_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterPartitionReassignments v0 response must be leftover-empty"
        );
    }

    #[test]
    fn alter_partition_reassignments_not_controller_is_at_byte_four() {
        let resp = AlterPartitionReassignmentsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            results: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_alter_partition_reassignments_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_partition_reassignments_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterPartitionReassignments v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn list_partition_reassignments_v0_roundtrip_is_leftover_empty() {
        let topics = vec![ListReassignmentTopic {
            name: "t".into(),
            partition_indexes: vec![0, 1],
        }];
        let mut buf = BytesMut::new();
        encode_list_partition_reassignments_request(&mut buf, 10_000, Some(&topics)).unwrap();
        let mut cur = &buf[..];
        let (timeout, got) = decode_list_partition_reassignments_request(&mut cur).unwrap();
        assert_eq!(timeout, 10_000);
        assert_eq!(got.as_deref(), Some(topics.as_slice()));
        assert!(
            !cur.has_remaining(),
            "ListPartitionReassignments v0 request must be leftover-empty"
        );

        buf.clear();
        encode_list_partition_reassignments_request(&mut buf, 5_000, None).unwrap();
        let mut cur = &buf[..];
        let (timeout, got) = decode_list_partition_reassignments_request(&mut cur).unwrap();
        assert_eq!(timeout, 5_000);
        assert_eq!(got, None);
        assert!(
            !cur.has_remaining(),
            "ListPartitionReassignments v0 null-topics request must be leftover-empty"
        );

        let resp = ListPartitionReassignmentsResponse {
            error_code: 0,
            error_message: None,
            topics: vec![OngoingTopicReassignment {
                name: "t".into(),
                partitions: vec![OngoingPartitionReassignment {
                    partition_index: 0,
                    replicas: vec![1, 2, 3],
                    adding_replicas: vec![3],
                    removing_replicas: vec![1],
                }],
            }],
        };
        buf.clear();
        encode_list_partition_reassignments_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_partition_reassignments_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListPartitionReassignments v0 response must be leftover-empty"
        );
    }

    #[test]
    fn list_partition_reassignments_not_controller_is_at_byte_four() {
        // Official v0 body: throttle_time_ms INT32, then error_code INT16.
        // That is the same field order as AlterPartitionReassignments v0,
        // verified from the Apache JSON (not copied from Alter).
        let resp = ListPartitionReassignmentsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            topics: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_list_partition_reassignments_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_partition_reassignments_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListPartitionReassignments v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn update_features_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client+broker).
        // Apache JSON: timeout INT32, compact FeatureUpdates
        // {Feature compact string, MaxVersionLevel INT16, AllowDowngrade
        // BOOLEAN, tagged}, tagged. Response: throttle INT32, error INT16,
        // compact-nullable ErrorMessage, compact Results (v0-1), tagged.
        const REQ: &[u8] = &[
            0x00, 0x00, 0x27, 0x10, 0x03, 0x11, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61,
            0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x11, 0x00, 0x00, 0x0e, 0x67,
            0x72, 0x6f, 0x75, 0x70, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x01,
            0x01, 0x00, 0x00,
        ];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x0f, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f, 0x6e,
            0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x01, 0x00,
        ];
        let updates = vec![
            FeatureUpdateKey {
                name: "metadata.version".into(),
                max_version_level: 17,
                allow_downgrade: false,
            },
            FeatureUpdateKey {
                name: "group.version".into(),
                max_version_level: 1,
                allow_downgrade: true,
            },
        ];
        let mut buf = BytesMut::new();
        encode_update_features_request(&mut buf, 10_000, &updates).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = UpdateFeaturesResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            results: Vec::new(),
        };
        buf.clear();
        encode_update_features_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn update_features_v0_roundtrip_is_leftover_empty() {
        let updates = vec![
            FeatureUpdateKey {
                name: "metadata.version".into(),
                max_version_level: 17,
                allow_downgrade: false,
            },
            FeatureUpdateKey {
                name: "group.version".into(),
                max_version_level: 1,
                allow_downgrade: true,
            },
        ];
        let mut buf = BytesMut::new();
        encode_update_features_request(&mut buf, 10_000, &updates).unwrap();
        let mut cur = &buf[..];
        let (timeout, got) = decode_update_features_request(&mut cur).unwrap();
        assert_eq!(timeout, 10_000);
        assert_eq!(got, updates);
        assert!(
            !cur.has_remaining(),
            "UpdateFeatures v0 request must be leftover-empty"
        );

        let resp = UpdateFeaturesResponse {
            error_code: 0,
            error_message: None,
            results: vec![
                UpdatableFeatureResult {
                    name: "metadata.version".into(),
                    error_code: 0,
                    error_message: None,
                },
                UpdatableFeatureResult {
                    name: "group.version".into(),
                    error_code: 0,
                    error_message: None,
                },
            ],
        };
        buf.clear();
        encode_update_features_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_update_features_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "UpdateFeatures v0 response must be leftover-empty"
        );
    }

    #[test]
    fn update_features_not_controller_is_at_byte_four() {
        // Official v0 body: throttle_time_ms INT32, then error_code INT16.
        // Verified from Apache UpdateFeaturesResponse.json and
        // kafka-protocol 0.18.0 (not copied from List/Alter).
        let resp = UpdateFeaturesResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            results: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_update_features_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_update_features_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "UpdateFeatures v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn alter_user_scram_credentials_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 51 v0 is
        // flexible, no timeout, no top-level response error.
        const REQ: &[u8] = &[
            0x02, 0x08, 0x6f, 0x6c, 0x64, 0x75, 0x73, 0x65, 0x72, 0x02, 0x00, 0x02, 0x06, 0x61,
            0x6c, 0x69, 0x63, 0x65, 0x01, 0x00, 0x00, 0x10, 0x00, 0x0b, 0x64, 0x75, 0x6d, 0x6d,
            0x79, 0x2d, 0x73, 0x61, 0x6c, 0x74, 0x0d, 0x64, 0x75, 0x6d, 0x6d, 0x79, 0x2d, 0x73,
            0x61, 0x6c, 0x74, 0x65, 0x64, 0x00, 0x00,
        ];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x29, 0x0f,
            0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72,
            0x00, 0x00,
        ];
        let deletions = vec![ScramCredentialDeletion {
            name: "olduser".into(),
            mechanism: SCRAM_SHA_512,
        }];
        let upsertions = vec![ScramCredentialUpsertion {
            name: "alice".into(),
            mechanism: SCRAM_SHA_256,
            iterations: 4096,
            salt: b"dummy-salt".to_vec(),
            salted_password: b"dummy-salted".to_vec(),
        }];
        let mut buf = BytesMut::new();
        encode_alter_user_scram_credentials_request(&mut buf, &deletions, &upsertions).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![AlterUserScramCredentialsResult {
            user: "alice".into(),
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
        }];
        buf.clear();
        encode_alter_user_scram_credentials_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn alter_user_scram_credentials_v0_roundtrip_is_leftover_empty() {
        let deletions = vec![ScramCredentialDeletion {
            name: "olduser".into(),
            mechanism: SCRAM_SHA_512,
        }];
        let upsertions = vec![ScramCredentialUpsertion {
            name: "alice".into(),
            mechanism: SCRAM_SHA_256,
            iterations: 4096,
            salt: b"dummy-salt".to_vec(),
            salted_password: b"dummy-salted".to_vec(),
        }];
        let mut buf = BytesMut::new();
        encode_alter_user_scram_credentials_request(&mut buf, &deletions, &upsertions).unwrap();
        let mut cur = &buf[..];
        let (got_del, got_up) = decode_alter_user_scram_credentials_request(&mut cur).unwrap();
        assert_eq!(got_del, deletions);
        assert_eq!(got_up, upsertions);
        assert!(
            !cur.has_remaining(),
            "AlterUserScramCredentials v0 request must be leftover-empty"
        );

        let resp = vec![
            AlterUserScramCredentialsResult {
                user: "olduser".into(),
                error_code: 0,
                error_message: None,
            },
            AlterUserScramCredentialsResult {
                user: "alice".into(),
                error_code: 0,
                error_message: None,
            },
        ];
        buf.clear();
        encode_alter_user_scram_credentials_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_user_scram_credentials_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterUserScramCredentials v0 response must be leftover-empty"
        );
    }

    #[test]
    fn alter_user_scram_credentials_not_controller_is_after_user() {
        // Official v0 body: throttle INT32, compact Results[], then each
        // result is compact User then ErrorCode INT16. Verified from Apache
        // AlterUserScramCredentialsResponse.json and kafka-protocol 0.18.0.
        // Not copied from UpdateFeatures (top-level 41 at bytes 4-5).
        let resp = vec![AlterUserScramCredentialsResult {
            user: "alice".into(),
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
        }];
        let mut buf = BytesMut::new();
        encode_alter_user_scram_credentials_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 has no top-level error; bytes 4-5 are compact array/user, not 41"
        );
        let b11 = buf.get(11).copied().unwrap();
        let b12 = buf.get(12).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b11, b12]),
            crate::error::NOT_CONTROLLER,
            "v0 41 is the first result ErrorCode after throttle, results len, and User"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_user_scram_credentials_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterUserScramCredentials v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn alter_client_quotas_v1_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 49
        // validVersions 0-1, flexibleVersions 1+. This crate targets v1.
        // v0 is classic (not copied from AlterUserScramCredentials).
        const REQ: &[u8] = &[
            0x02, 0x02, 0x05, 0x75, 0x73, 0x65, 0x72, 0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00,
            0x02, 0x13, 0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x5f, 0x62, 0x79, 0x74,
            0x65, 0x5f, 0x72, 0x61, 0x74, 0x65, 0x40, 0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x29, 0x0f, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f,
            0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x02, 0x05, 0x75, 0x73, 0x65, 0x72,
            0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00,
        ];
        let entries = vec![ClientQuotaAlteration::new(
            vec![ClientQuotaEntity::new("user", Some("alice".into()))],
            vec![ClientQuotaOp::set("producer_byte_rate", 1024.0)],
        )];
        let mut buf = BytesMut::new();
        encode_alter_client_quotas_request(&mut buf, &entries, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![ClientQuotaAlterationResult {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            entity: vec![ClientQuotaEntity::new("user", Some("alice".into()))],
        }];
        buf.clear();
        encode_alter_client_quotas_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn alter_client_quotas_v1_roundtrip_is_leftover_empty() {
        let entries = vec![
            ClientQuotaAlteration::new(
                vec![ClientQuotaEntity::new("user", Some("alice".into()))],
                vec![ClientQuotaOp::set("producer_byte_rate", 1024.0)],
            ),
            ClientQuotaAlteration::new(
                vec![ClientQuotaEntity::new("user", Some("carol".into()))],
                vec![ClientQuotaOp::remove("producer_byte_rate")],
            ),
        ];
        let mut buf = BytesMut::new();
        encode_alter_client_quotas_request(&mut buf, &entries, true).unwrap();
        let mut cur = &buf[..];
        let (got, validate_only) = decode_alter_client_quotas_request(&mut cur).unwrap();
        assert_eq!(got, entries);
        assert!(validate_only);
        assert!(
            !cur.has_remaining(),
            "AlterClientQuotas v1 request must be leftover-empty"
        );

        let resp = vec![
            ClientQuotaAlterationResult {
                error_code: 0,
                error_message: None,
                entity: vec![ClientQuotaEntity::new("user", Some("alice".into()))],
            },
            ClientQuotaAlterationResult {
                error_code: 0,
                error_message: None,
                entity: vec![ClientQuotaEntity::new("user", Some("carol".into()))],
            },
        ];
        buf.clear();
        encode_alter_client_quotas_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_alter_client_quotas_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "AlterClientQuotas v1 response must be leftover-empty"
        );
    }

    #[test]
    fn alter_client_quotas_not_controller_is_at_byte_five() {
        // Official v1 body: throttle INT32, compact Entries[], then each
        // entry is ErrorCode INT16, ErrorMessage, Entity. Verified from
        // Apache AlterClientQuotasResponse.json and kafka-protocol 0.18.0.
        // Not copied from UpdateFeatures (top-level 41 at bytes 4-5) or
        // AlterUserScramCredentials (41 after compact User at 11-12).
        // ErrorCode is before Entity, not after.
        let resp = vec![ClientQuotaAlterationResult {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            entity: vec![ClientQuotaEntity::new("user", Some("alice".into()))],
        }];
        let mut buf = BytesMut::new();
        encode_alter_client_quotas_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v1 has no top-level error; bytes 4-5 are compact entries len + high byte, not 41"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_CONTROLLER,
            "v1 41 is the first entry ErrorCode after throttle and compact entries len"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_alter_client_quotas_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "AlterClientQuotas v1 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn describe_client_quotas_v1_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 48
        // validVersions 0-1, flexibleVersions 1+, listeners broker only.
        // This crate targets v1. Not copied from AlterClientQuotas
        // (no top-level error; first-entry 41 at bytes 5-6).
        const REQ: &[u8] = &[
            0x02, 0x05, 0x75, 0x73, 0x65, 0x72, 0x00, 0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00,
            0x00, 0x00,
        ];
        const RESP_ERR: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x0f, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f, 0x6e,
            0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x00, 0x00,
        ];
        let components = vec![ClientQuotaFilterComponent::new(
            "user",
            QUOTA_MATCH_EXACT,
            Some("alice".into()),
        )];
        let mut buf = BytesMut::new();
        encode_describe_client_quotas_request(&mut buf, &components, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = DescribeClientQuotasResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            entries: None,
        };
        buf.clear();
        encode_describe_client_quotas_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_ERR);
    }

    #[test]
    fn describe_client_quotas_v1_roundtrip_is_leftover_empty() {
        let components = vec![
            ClientQuotaFilterComponent::new("user", QUOTA_MATCH_EXACT, Some("alice".into())),
            ClientQuotaFilterComponent::new("client-id", QUOTA_MATCH_ANY, None),
        ];
        let mut buf = BytesMut::new();
        encode_describe_client_quotas_request(&mut buf, &components, true).unwrap();
        let mut cur = &buf[..];
        let (got, strict) = decode_describe_client_quotas_request(&mut cur).unwrap();
        assert_eq!(got, components);
        assert!(strict);
        assert!(
            !cur.has_remaining(),
            "DescribeClientQuotas v1 request must be leftover-empty"
        );

        let resp = DescribeClientQuotasResponse {
            error_code: 0,
            error_message: None,
            entries: Some(vec![ClientQuotaEntry::new(
                vec![ClientQuotaEntity::new("user", Some("alice".into()))],
                vec![ClientQuotaValue::new("producer_byte_rate", 1024.0)],
            )]),
        };
        buf.clear();
        encode_describe_client_quotas_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_client_quotas_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeClientQuotas v1 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_client_quotas_error_code_is_at_bytes_4_5() {
        // Official v1 body: throttle INT32, then top-level ErrorCode
        // INT16, ErrorMessage, nullable Entries. Measured independently
        // from Apache DescribeClientQuotasResponse.json and a
        // kafka-protocol 0.18.0 broker encode (`features = ["broker"]`).
        // Do not assume bytes 4-5 from UnregisterBroker /
        // AllocateProducerIds (same offset, different fields after) or
        // AlterClientQuotas (first-entry code at bytes 5-6).
        let resp = DescribeClientQuotasResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            entries: None,
        };
        let mut buf = BytesMut::new();
        encode_describe_client_quotas_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v1 throttle then top-level error must be the INT16 at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_CONTROLLER,
            "v1 ErrorCode is not a first-result field at bytes 5-6"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_client_quotas_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeClientQuotas v1 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn allocate_producer_ids_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 67
        // validVersions 0, flexibleVersions 0+. This crate targets v0.
        // Not copied from AlterClientQuotas / UpdateFeatures / OffsetDelete.
        const REQ: &[u8] = &[
            0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x00,
        ];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_allocate_producer_ids_request(&mut buf, 7, 42).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = AllocateProducerIdsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            producer_id_start: 0,
            producer_id_len: 0,
        };
        buf.clear();
        encode_allocate_producer_ids_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn allocate_producer_ids_v0_roundtrip_is_leftover_empty() {
        let mut buf = BytesMut::new();
        encode_allocate_producer_ids_request(&mut buf, 7, 42).unwrap();
        let mut cur = &buf[..];
        let (broker_id, broker_epoch) = decode_allocate_producer_ids_request(&mut cur).unwrap();
        assert_eq!(broker_id, 7);
        assert_eq!(broker_epoch, 42);
        assert!(
            !cur.has_remaining(),
            "AllocateProducerIds v0 request must be leftover-empty"
        );

        let resp = AllocateProducerIdsResponse {
            error_code: 0,
            producer_id_start: 1000,
            producer_id_len: 1000,
        };
        buf.clear();
        encode_allocate_producer_ids_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_allocate_producer_ids_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AllocateProducerIds v0 response must be leftover-empty"
        );
    }

    #[test]
    fn allocate_producer_ids_not_controller_is_at_byte_four() {
        // Official v0 body: throttle INT32, then top-level ErrorCode INT16,
        // ProducerIdStart INT64, ProducerIdLen INT32, tagged. Measured from
        // Apache AllocateProducerIdsResponse.json and an independent
        // kafka-protocol 0.18.0 broker encode. Not copied from
        // UpdateFeatures (same byte offset, different fields after 41),
        // AlterClientQuotas (41 at bytes 5-6), AlterUserScramCredentials
        // (41 after compact User), or OffsetDelete (error before throttle).
        let resp = AllocateProducerIdsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            producer_id_start: 0,
            producer_id_len: 0,
        };
        let mut buf = BytesMut::new();
        encode_allocate_producer_ids_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_allocate_producer_ids_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AllocateProducerIds v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn describe_transactions_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 65
        // validVersions 0, flexibleVersions 0+. This crate targets v0.
        // Not copied from AllocateProducerIds / AlterClientQuotas /
        // OffsetDelete.
        const REQ: &[u8] = &[0x02, 0x05, 0x74, 0x78, 0x2d, 0x31, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x05, 0x74, 0x78, 0x2d, 0x31, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        ];
        let ids = vec!["tx-1".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_transactions_request(&mut buf, &ids).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![TransactionState {
            error_code: crate::error::NOT_COORDINATOR,
            transactional_id: "tx-1".into(),
            transaction_state: String::new(),
            transaction_timeout_ms: 0,
            transaction_start_time_ms: 0,
            producer_id: 0,
            producer_epoch: 0,
            topics: Vec::new(),
        }];
        buf.clear();
        encode_describe_transactions_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn describe_transactions_v0_roundtrip_is_leftover_empty() {
        let ids = vec!["tx-1".to_string(), "tx-2".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_transactions_request(&mut buf, &ids).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_describe_transactions_request(&mut cur).unwrap(), ids);
        assert!(
            !cur.has_remaining(),
            "DescribeTransactions v0 request must be leftover-empty"
        );

        let resp = vec![TransactionState {
            error_code: 0,
            transactional_id: "tx-1".into(),
            transaction_state: "Ongoing".into(),
            transaction_timeout_ms: 60_000,
            transaction_start_time_ms: 1_700_000_000_000,
            producer_id: 1001,
            producer_epoch: 3,
            topics: vec![TransactionTopic {
                name: "orders".into(),
                partitions: vec![0, 1],
            }],
        }];
        buf.clear();
        encode_describe_transactions_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_transactions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTransactions v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_transactions_not_coordinator_is_at_byte_five() {
        // Official v0 body: throttle INT32, compact TransactionStates[],
        // then each state starts with ErrorCode INT16. Measured from
        // Apache DescribeTransactionsResponse.json and an independent
        // kafka-protocol 0.18.0 broker encode. Not copied from
        // AllocateProducerIds (top-level 41 at bytes 4-5),
        // AlterClientQuotas (41 at bytes 5-6, different fields after),
        // or OffsetDelete (error before throttle).
        let resp = vec![TransactionState {
            error_code: crate::error::NOT_COORDINATOR,
            transactional_id: "tx-1".into(),
            transaction_state: String::new(),
            transaction_timeout_ms: 0,
            transaction_start_time_ms: 0,
            producer_id: 0,
            producer_epoch: 0,
            topics: Vec::new(),
        }];
        let mut buf = BytesMut::new();
        encode_describe_transactions_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 has no top-level error; bytes 4-5 are compact states len + high byte, not 16"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v0 16 is the first result ErrorCode after throttle and compact states len"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_transactions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTransactions v0 NOT_COORDINATOR must be leftover-empty"
        );
    }

    #[test]
    fn list_transactions_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 66
        // validVersions 0-2, flexibleVersions 0+. This crate targets v0.
        // Not copied from DescribeTransactions (no top-level error;
        // 16 at bytes 5-6) or AllocateProducerIds (different fields
        // after the top-level INT16).
        const REQ: &[u8] = &[
            0x02, 0x08, 0x4f, 0x6e, 0x67, 0x6f, 0x69, 0x6e, 0x67, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x03, 0xe9, 0x00,
        ];
        const RESP_16: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x01, 0x01, 0x00];
        let states = vec!["Ongoing".to_string()];
        let pids = vec![1001_i64];
        let mut buf = BytesMut::new();
        encode_list_transactions_request(&mut buf, &states, &pids).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = ListTransactionsResponse {
            error_code: crate::error::NOT_COORDINATOR,
            unknown_state_filters: Vec::new(),
            transaction_states: Vec::new(),
        };
        buf.clear();
        encode_list_transactions_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn list_transactions_v0_roundtrip_is_leftover_empty() {
        let states = vec!["Ongoing".to_string(), "PrepareCommit".to_string()];
        let pids = vec![1001_i64, 1002];
        let mut buf = BytesMut::new();
        encode_list_transactions_request(&mut buf, &states, &pids).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_transactions_request(&mut cur).unwrap(),
            (states, pids)
        );
        assert!(
            !cur.has_remaining(),
            "ListTransactions v0 request must be leftover-empty"
        );

        let resp = ListTransactionsResponse {
            error_code: 0,
            unknown_state_filters: vec!["UnknownState".into()],
            transaction_states: vec![TransactionListing {
                transactional_id: "tx-1".into(),
                producer_id: 1001,
                transaction_state: "Ongoing".into(),
            }],
        };
        buf.clear();
        encode_list_transactions_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_list_transactions_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "ListTransactions v0 response must be leftover-empty"
        );
    }

    #[test]
    fn list_transactions_not_coordinator_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16. Measured from Apache ListTransactionsResponse.json
        // and an independent kafka-protocol 0.18.0 broker encode.
        // Not copied from DescribeTransactions (16 at bytes 5-6, first
        // result after compact TransactionStates length).
        let resp = ListTransactionsResponse {
            error_code: crate::error::NOT_COORDINATOR,
            unknown_state_filters: Vec::new(),
            transaction_states: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_list_transactions_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 throttle then top-level error must be 16 at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v0 16 is not a first-result ErrorCode at bytes 5-6"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_list_transactions_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "ListTransactions v0 NOT_COORDINATOR must be leftover-empty"
        );
    }

    #[test]
    fn describe_user_scram_credentials_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 50
        // validVersions 0, flexibleVersions 0+. This crate targets v0.
        // Not copied from AlterUserScramCredentials (no top-level error;
        // 41 after compact User at 11-12) or ListTransactions (16 at
        // bytes 4-5, different fields after the INT16).
        const REQ: &[u8] = &[0x02, 0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x0f, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f, 0x6e,
            0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x01, 0x00,
        ];
        let users = vec!["alice".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_user_scram_credentials_request(&mut buf, Some(&users)).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = DescribeUserScramCredentialsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            results: Vec::new(),
        };
        buf.clear();
        encode_describe_user_scram_credentials_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn describe_user_scram_credentials_v0_roundtrip_is_leftover_empty() {
        let users = vec!["alice".to_string(), "bob".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_user_scram_credentials_request(&mut buf, Some(&users)).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_user_scram_credentials_request(&mut cur).unwrap(),
            Some(users)
        );
        assert!(
            !cur.has_remaining(),
            "DescribeUserScramCredentials v0 request must be leftover-empty"
        );

        buf.clear();
        encode_describe_user_scram_credentials_request(&mut buf, None).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_user_scram_credentials_request(&mut cur).unwrap(),
            None
        );
        assert!(
            !cur.has_remaining(),
            "DescribeUserScramCredentials v0 null Users request must be leftover-empty"
        );

        let resp = DescribeUserScramCredentialsResponse {
            error_code: 0,
            error_message: None,
            results: vec![DescribeUserScramCredentialsResult {
                user: "alice".into(),
                error_code: 0,
                error_message: None,
                credential_infos: vec![ScramCredentialInfo {
                    mechanism: SCRAM_SHA_256,
                    iterations: 4096,
                }],
            }],
        };
        buf.clear();
        encode_describe_user_scram_credentials_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_user_scram_credentials_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeUserScramCredentials v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_user_scram_credentials_not_controller_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16. Measured from Apache DescribeUserScramCredentialsResponse.json
        // and an independent kafka-protocol 0.18.0 broker encode.
        // Not copied from AlterUserScramCredentials (41 after compact
        // User at 11-12) or ListTransactions (same byte offset, different
        // fields after the INT16).
        let resp = DescribeUserScramCredentialsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            results: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_describe_user_scram_credentials_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let b11 = buf.get(11).copied().unwrap();
        let b12 = buf.get(12).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b11, b12]),
            crate::error::NOT_CONTROLLER,
            "v0 41 is not a first-result ErrorCode after compact User"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_user_scram_credentials_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeUserScramCredentials v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn unregister_broker_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 64
        // validVersions 0, flexibleVersions 0+. This crate targets v0.
        // Not copied from AllocateProducerIds (BrokerId then BrokerEpoch)
        // or DescribeUserScramCredentials (same 41 offset, Results after
        // ErrorMessage).
        const REQ: &[u8] = &[0x00, 0x00, 0x00, 0x07, 0x00];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x0f, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f, 0x6e,
            0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_unregister_broker_request(&mut buf, 7).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = UnregisterBrokerResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
        };
        buf.clear();
        encode_unregister_broker_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn unregister_broker_v0_roundtrip_is_leftover_empty() {
        let mut buf = BytesMut::new();
        encode_unregister_broker_request(&mut buf, 7).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_unregister_broker_request(&mut cur).unwrap(), 7);
        assert!(
            !cur.has_remaining(),
            "UnregisterBroker v0 request must be leftover-empty"
        );

        let resp = UnregisterBrokerResponse {
            error_code: 0,
            error_message: None,
        };
        buf.clear();
        encode_unregister_broker_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_unregister_broker_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "UnregisterBroker v0 response must be leftover-empty"
        );
    }

    #[test]
    fn unregister_broker_not_controller_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16. Measured independently from Apache
        // UnregisterBrokerResponse.json and a kafka-protocol 0.18.0
        // broker encode (`features = ["broker"]`). Not copied from
        // AlterUserScramCredentials (41 after compact User at 11-12),
        // DescribeTransactions (first-result code at bytes 5-6), or
        // DescribeUserScramCredentials (same offset, Results after the
        // INT16 + ErrorMessage).
        let resp = UnregisterBrokerResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
        };
        let mut buf = BytesMut::new();
        encode_unregister_broker_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::NOT_CONTROLLER,
            "v0 41 is not a first-result ErrorCode at bytes 5-6"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_unregister_broker_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "UnregisterBroker v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn describe_producers_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 61
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0. Not copied from DescribeClientQuotas
        // (top-level ErrorCode at bytes 4-5) or DeleteRecords.
        const REQ: &[u8] = &[0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        const RESP_6: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
            0x00, 0x01, 0x00, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_describe_producers_request(&mut buf, "t", &[0]).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = DescribeProducersResponse::new(vec![DescribeProducersTopic::new(
            "t",
            vec![DescribeProducersPartition::new(
                0,
                crate::error::NOT_LEADER_OR_FOLLOWER,
                None,
                vec![],
            )],
        )]);
        buf.clear();
        encode_describe_producers_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_6);
    }

    #[test]
    fn describe_producers_v0_roundtrip_is_leftover_empty() {
        let mut buf = BytesMut::new();
        encode_describe_producers_request(&mut buf, "t", &[0]).unwrap();
        let mut cur = &buf[..];
        let (topic, parts) = decode_describe_producers_request(&mut cur).unwrap();
        assert_eq!(topic, "t");
        assert_eq!(parts, vec![0]);
        assert!(
            !cur.has_remaining(),
            "DescribeProducers v0 request must be leftover-empty"
        );

        let resp = DescribeProducersResponse::new(vec![DescribeProducersTopic::new(
            "t",
            vec![DescribeProducersPartition::new(
                0,
                0,
                None,
                vec![ActiveProducer::new(1000, 1, 7, 1_700_000_000_000, 0, -1)],
            )],
        )]);
        buf.clear();
        encode_describe_producers_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_describe_producers_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DescribeProducers v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_producers_first_partition_error_code_is_at_bytes_12_13() {
        // Official v0 body: throttle INT32, compact Topics of {Name,
        // compact Partitions of {PartitionIndex, ErrorCode, ...}}.
        // Measured independently from Apache DescribeProducersResponse.json
        // and a kafka-protocol 0.18.0 broker encode (`features =
        // ["broker"]`) on leftover-empty fixture topic "t" partition 0.
        // Do not assume bytes 4-5 from DescribeClientQuotas /
        // UnregisterBroker / ListTransactions (top-level ErrorCode).
        let resp = DescribeProducersResponse::new(vec![DescribeProducersTopic::new(
            "t",
            vec![DescribeProducersPartition::new(
                0,
                crate::error::NOT_LEADER_OR_FOLLOWER,
                None,
                vec![],
            )],
        )]);
        let mut buf = BytesMut::new();
        encode_describe_producers_response(&mut buf, &resp).unwrap();
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b12, b13]),
            crate::error::NOT_LEADER_OR_FOLLOWER,
            "v0 first-partition ErrorCode must be the INT16 at bytes 12-13"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_LEADER_OR_FOLLOWER,
            "v0 ErrorCode is not a top-level field at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_describe_producers_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DescribeProducers v0 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn consumer_group_describe_v1_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 69
        // validVersions 0-1, flexibleVersions 0+, listeners broker only.
        // This crate targets v1. Not copied from DescribeClientQuotas
        // (top-level ErrorCode at bytes 4-5) or DescribeProducers
        // (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x00, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00, 0x02, 0x67, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let ids = vec!["g".to_string()];
        let mut buf = BytesMut::new();
        encode_consumer_group_describe_request(&mut buf, &ids, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![DescribedConsumerGroup::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        buf.clear();
        encode_consumer_group_describe_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn consumer_group_describe_v1_roundtrip_is_leftover_empty() {
        let ids = vec!["g".to_string(), "g2".to_string()];
        let mut buf = BytesMut::new();
        encode_consumer_group_describe_request(&mut buf, &ids, true).unwrap();
        let mut cur = &buf[..];
        let (got, include) = decode_consumer_group_describe_request(&mut cur).unwrap();
        assert_eq!(got, ids);
        assert!(include);
        assert!(
            !cur.has_remaining(),
            "ConsumerGroupDescribe v1 request must be leftover-empty"
        );

        let mut member = ConsumerGroupMember::new("m1", 1, "c", "h");
        member.subscribed_topic_names = vec!["t".into()];
        member.assignment = ConsumerGroupAssignment::new(vec![ConsumerGroupTopicPartitions::new(
            [0; 16],
            "t",
            vec![0],
        )]);
        member.target_assignment = member.assignment.clone();
        member.member_type = 1;
        let resp = vec![DescribedConsumerGroup {
            error_code: 0,
            error_message: None,
            group_id: "g".into(),
            group_state: "Stable".into(),
            group_epoch: 1,
            assignment_epoch: 1,
            assignor_name: "uniform".into(),
            members: vec![member],
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }];
        buf.clear();
        encode_consumer_group_describe_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_consumer_group_describe_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ConsumerGroupDescribe v1 response must be leftover-empty"
        );
    }

    #[test]
    fn consumer_group_describe_first_group_error_code_is_at_bytes_5_6() {
        // Official v1 body: throttle INT32, compact Groups of
        // {ErrorCode, ...}. Measured independently from Apache
        // ConsumerGroupDescribeResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture group "g". Do not assume bytes 4-5 from
        // DescribeClientQuotas or bytes 12-13 from DescribeProducers.
        let resp = vec![DescribedConsumerGroup::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        let mut buf = BytesMut::new();
        encode_consumer_group_describe_response(&mut buf, &resp).unwrap();
        let b5 = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v1 first-group ErrorCode must be the INT16 at bytes 5-6"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5b = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5b]),
            crate::error::NOT_COORDINATOR,
            "v1 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::NOT_COORDINATOR,
            "v1 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_consumer_group_describe_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ConsumerGroupDescribe v1 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn describe_groups_v6_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 15
        // validVersions 0-6, flexibleVersions 5+, listeners broker only.
        // This crate targets v6. Not copied from DescribeClientQuotas
        // (top-level ErrorCode at bytes 4-5), ConsumerGroupDescribe
        // (first-group ErrorCode at bytes 5-6), or DescribeProducers
        // (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x00, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00, 0x02, 0x67, 0x01, 0x01, 0x01, 0x01,
            0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let ids = vec!["g".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_groups_request(&mut buf, &ids, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![DescribedGroup::new("g", crate::error::NOT_COORDINATOR)];
        buf.clear();
        encode_describe_groups_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn describe_groups_v6_roundtrip_is_leftover_empty() {
        let ids = vec!["g".to_string(), "g2".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_groups_request(&mut buf, &ids, true).unwrap();
        let mut cur = &buf[..];
        let (got, include) = decode_describe_groups_request(&mut cur).unwrap();
        assert_eq!(got, ids);
        assert!(include);
        assert!(
            !cur.has_remaining(),
            "DescribeGroups v6 request must be leftover-empty"
        );

        let mut member = DescribedGroupMember::new("m1", "c", "h");
        member.group_instance_id = Some("i1".into());
        member.member_metadata = vec![0x01];
        member.member_assignment = vec![0x02];
        let resp = vec![DescribedGroup {
            error_code: 0,
            error_message: None,
            group_id: "g".into(),
            group_state: "Stable".into(),
            protocol_type: "consumer".into(),
            protocol_data: "range".into(),
            members: vec![member],
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }];
        buf.clear();
        encode_describe_groups_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_describe_groups_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DescribeGroups v6 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_groups_first_group_error_code_is_at_bytes_5_6() {
        // Official v6 body: throttle INT32, compact Groups of
        // {ErrorCode, ...}. Measured independently from Apache
        // DescribeGroupsResponse.json and a kafka-protocol 0.18.0
        // broker encode (`features = ["broker"]`) on leftover-empty
        // fixture group "g". Do not assume bytes 4-5 from
        // DescribeClientQuotas, bytes 5-6 from ConsumerGroupDescribe,
        // or bytes 12-13 from DescribeProducers.
        let resp = vec![DescribedGroup::new("g", crate::error::NOT_COORDINATOR)];
        let mut buf = BytesMut::new();
        encode_describe_groups_response(&mut buf, &resp).unwrap();
        let b5 = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v6 first-group ErrorCode must be the INT16 at bytes 5-6"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5b = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5b]),
            crate::error::NOT_COORDINATOR,
            "v6 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::NOT_COORDINATOR,
            "v6 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_describe_groups_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DescribeGroups v6 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn list_groups_v5_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 16
        // validVersions 0-5, flexibleVersions 3+, listeners broker only.
        // This crate targets v5. Not copied from DescribeGroups
        // (first-group ErrorCode at bytes 5-6), DescribeClientQuotas
        // (top-level ErrorCode at bytes 4-5, different fields after),
        // or DescribeProducers (first-partition ErrorCode at bytes
        // 12-13).
        const REQ: &[u8] = &[
            0x02, 0x07, 0x53, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x02, 0x08, 0x63, 0x6c, 0x61, 0x73,
            0x73, 0x69, 0x63, 0x00,
        ];
        const RESP_15: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x02, 0x02, 0x67, 0x01, 0x01, 0x01, 0x00, 0x00,
        ];
        let states = vec!["Stable".to_string()];
        let types = vec!["classic".to_string()];
        let mut buf = BytesMut::new();
        encode_list_groups_request(&mut buf, &states, &types).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = ListGroupsResponse {
            error_code: crate::error::COORDINATOR_NOT_AVAILABLE,
            groups: vec![ListedGroup::new("g")],
        };
        buf.clear();
        encode_list_groups_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_15);
    }

    #[test]
    fn list_groups_v5_roundtrip_is_leftover_empty() {
        let states = vec!["Stable".to_string(), "Empty".to_string()];
        let types = vec!["classic".to_string(), "consumer".to_string()];
        let mut buf = BytesMut::new();
        encode_list_groups_request(&mut buf, &states, &types).unwrap();
        let mut cur = &buf[..];
        let (got_states, got_types) = decode_list_groups_request(&mut cur).unwrap();
        assert_eq!(got_states, states);
        assert_eq!(got_types, types);
        assert!(
            !cur.has_remaining(),
            "ListGroups v5 request must be leftover-empty"
        );

        let resp = ListGroupsResponse {
            error_code: 0,
            groups: vec![ListedGroup {
                group_id: "g".into(),
                protocol_type: "consumer".into(),
                group_state: "Stable".into(),
                group_type: "classic".into(),
            }],
        };
        buf.clear();
        encode_list_groups_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_list_groups_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "ListGroups v5 response must be leftover-empty"
        );
    }

    #[test]
    fn list_groups_error_code_is_at_bytes_4_5() {
        // Official v5 body: throttle INT32, then top-level ErrorCode
        // INT16, then compact Groups. Measured independently from
        // Apache ListGroupsResponse.json and a kafka-protocol 0.18.0
        // broker encode (`features = ["broker"]`) on leftover-empty
        // fixture group "g". Do not assume bytes 5-6 from
        // DescribeGroups / ConsumerGroupDescribe first-group, or
        // bytes 12-13 from DescribeProducers first partition. Official
        // listed errors include COORDINATOR_NOT_AVAILABLE (15), not
        // NOT_COORDINATOR (16).
        let resp = ListGroupsResponse {
            error_code: crate::error::COORDINATOR_NOT_AVAILABLE,
            groups: vec![ListedGroup::new("g")],
        };
        let mut buf = BytesMut::new();
        encode_list_groups_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::COORDINATOR_NOT_AVAILABLE,
            "v5 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::COORDINATOR_NOT_AVAILABLE,
            "v5 ErrorCode is not a first-group field at bytes 5-6"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::COORDINATOR_NOT_AVAILABLE,
            "v5 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_list_groups_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "ListGroups v5 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn delete_groups_v2_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 42
        // validVersions 0-2, flexibleVersions 2+, listeners broker only.
        // This crate targets v2. Not copied from ListGroups (top-level
        // ErrorCode at bytes 4-5), DescribeGroups / ConsumerGroupDescribe
        // (first-group ErrorCode at bytes 5-6), or DescribeProducers
        // (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x67, 0x00, 0x10, 0x00, 0x00,
        ];
        let ids = vec!["g".to_string()];
        let mut buf = BytesMut::new();
        encode_delete_groups_request(&mut buf, &ids).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![DeletableGroupResult::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        buf.clear();
        encode_delete_groups_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn delete_groups_v2_roundtrip_is_leftover_empty() {
        let ids = vec!["g".to_string(), "g2".to_string()];
        let mut buf = BytesMut::new();
        encode_delete_groups_request(&mut buf, &ids).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_delete_groups_request(&mut cur).unwrap(), ids);
        assert!(
            !cur.has_remaining(),
            "DeleteGroups v2 request must be leftover-empty"
        );

        let resp = vec![
            DeletableGroupResult::new("g", 0),
            DeletableGroupResult::new("g2", crate::error::NOT_COORDINATOR),
        ];
        buf.clear();
        encode_delete_groups_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_delete_groups_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DeleteGroups v2 response must be leftover-empty"
        );
    }

    #[test]
    fn delete_groups_first_group_error_code_is_at_bytes_7_8() {
        // Official v2 body: throttle INT32, compact Results of
        // {GroupId, ErrorCode, tagged}. Measured independently from
        // Apache DeleteGroupsResponse.json and a kafka-protocol 0.18.0
        // broker encode (`features = ["broker"]`) on leftover-empty
        // fixture group "g". Do not assume bytes 4-5 from ListGroups /
        // DescribeClientQuotas, bytes 5-6 from DescribeGroups /
        // ConsumerGroupDescribe first-group, or bytes 12-13 from
        // DescribeProducers first partition.
        let resp = vec![DeletableGroupResult::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        let mut buf = BytesMut::new();
        encode_delete_groups_response(&mut buf, &resp).unwrap();
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b7, b8]),
            crate::error::NOT_COORDINATOR,
            "v2 first-group ErrorCode must be the INT16 at bytes 7-8"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v2 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v2 ErrorCode is not a first-group first field at bytes 5-6"
        );
        assert!(
            buf.len() < 14,
            "leftover-empty fixture is shorter than DescribeProducers bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_delete_groups_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DeleteGroups v2 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn share_group_describe_v1_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 77
        // validVersions 1, flexibleVersions 0+, listeners broker only.
        // This crate targets v1 (VERSIONS.max). Not copied from
        // ListGroups (top-level ErrorCode at bytes 4-5), DescribeGroups
        // / ConsumerGroupDescribe (first-group ErrorCode at bytes 5-6
        // on a different member layout), DeleteGroups (after GroupId at
        // bytes 7-8), or DescribeProducers (first-partition ErrorCode
        // at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x00, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00, 0x02, 0x67, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let ids = vec!["g".to_string()];
        let mut buf = BytesMut::new();
        encode_share_group_describe_request(&mut buf, &ids, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![DescribedShareGroup::new("g", crate::error::NOT_COORDINATOR)];
        buf.clear();
        encode_share_group_describe_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn share_group_describe_v1_roundtrip_is_leftover_empty() {
        let ids = vec!["g".to_string(), "g2".to_string()];
        let mut buf = BytesMut::new();
        encode_share_group_describe_request(&mut buf, &ids, true).unwrap();
        let mut cur = &buf[..];
        let (got, include) = decode_share_group_describe_request(&mut cur).unwrap();
        assert_eq!(got, ids);
        assert!(include);
        assert!(
            !cur.has_remaining(),
            "ShareGroupDescribe v1 request must be leftover-empty"
        );

        let mut member = ShareGroupMember::new("m1", 1, "c", "h");
        member.subscribed_topic_names = vec!["t".into()];
        member.assignment =
            ShareGroupAssignment::new(vec![ShareGroupTopicPartitions::new([0; 16], "t", vec![0])]);
        let resp = vec![DescribedShareGroup {
            error_code: 0,
            error_message: None,
            group_id: "g".into(),
            group_state: "Stable".into(),
            group_epoch: 1,
            assignment_epoch: 1,
            assignor_name: "uniform".into(),
            members: vec![member],
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }];
        buf.clear();
        encode_share_group_describe_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_share_group_describe_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ShareGroupDescribe v1 response must be leftover-empty"
        );
    }

    #[test]
    fn share_group_describe_first_group_error_code_is_at_bytes_5_6() {
        // Official v1 body: throttle INT32, compact Groups of
        // {ErrorCode, ...}. Measured independently from Apache
        // ShareGroupDescribeResponse.json and a kafka-protocol 0.18.0
        // broker encode (`features = ["broker"]`) on leftover-empty
        // fixture group "g". Do not assume bytes 4-5 from ListGroups /
        // DescribeClientQuotas, bytes 5-6 from DescribeGroups /
        // ConsumerGroupDescribe, bytes 7-8 from DeleteGroups after
        // GroupId, or bytes 12-13 from DescribeProducers.
        let resp = vec![DescribedShareGroup::new("g", crate::error::NOT_COORDINATOR)];
        let mut buf = BytesMut::new();
        encode_share_group_describe_response(&mut buf, &resp).unwrap();
        let b5 = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v1 first-group ErrorCode must be the INT16 at bytes 5-6"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5b = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5b]),
            crate::error::NOT_COORDINATOR,
            "v1 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::NOT_COORDINATOR,
            "v1 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::NOT_COORDINATOR,
            "v1 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_share_group_describe_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ShareGroupDescribe v1 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn describe_share_group_offsets_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 90
        // validVersions 0-1, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // ListGroups (top-level ErrorCode at bytes 4-5), ShareGroupDescribe
        // / DescribeGroups / ConsumerGroupDescribe (first-group ErrorCode
        // at bytes 5-6), DeleteGroups (after GroupId at bytes 7-8), or
        // DescribeProducers (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x01, 0x00, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x67, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00,
        ];
        let groups = vec![DescribeShareGroupOffsetsGroup::new("g")];
        let mut buf = BytesMut::new();
        encode_describe_share_group_offsets_request(&mut buf, &groups).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![DescribedShareGroupOffsets::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        buf.clear();
        encode_describe_share_group_offsets_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn describe_share_group_offsets_v0_roundtrip_is_leftover_empty() {
        let groups = vec![
            DescribeShareGroupOffsetsGroup {
                group_id: "g".into(),
                topics: Some(vec![DescribeShareGroupOffsetsTopic::new("t", vec![0, 1])]),
            },
            DescribeShareGroupOffsetsGroup {
                group_id: "g2".into(),
                topics: None,
            },
        ];
        let mut buf = BytesMut::new();
        encode_describe_share_group_offsets_request(&mut buf, &groups).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_share_group_offsets_request(&mut cur).unwrap(),
            groups
        );
        assert!(
            !cur.has_remaining(),
            "DescribeShareGroupOffsets v0 request must be leftover-empty"
        );

        let resp = vec![DescribedShareGroupOffsets {
            group_id: "g".into(),
            topics: vec![DescribedShareGroupOffsetsTopic {
                topic_name: "t".into(),
                topic_id: [0; 16],
                partitions: vec![DescribedShareGroupOffsetsPartition {
                    partition_index: 0,
                    start_offset: 7,
                    leader_epoch: 3,
                    error_code: 0,
                    error_message: None,
                }],
            }],
            error_code: 0,
            error_message: None,
        }];
        buf.clear();
        encode_describe_share_group_offsets_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeShareGroupOffsets v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_share_group_offsets_first_group_error_code_is_at_bytes_8_9() {
        // Official v0 body: throttle INT32, compact Groups of
        // {GroupId, Topics, ErrorCode, ...}. Measured independently
        // from Apache DescribeShareGroupOffsetsResponse.json and a
        // kafka-protocol 0.18.0 broker encode (`features = ["broker"]`)
        // on leftover-empty fixture group "g" (empty Topics). Do not
        // assume bytes 4-5 from ListGroups / DescribeClientQuotas,
        // bytes 5-6 from ShareGroupDescribe / DescribeGroups /
        // ConsumerGroupDescribe, bytes 7-8 from DeleteGroups after
        // GroupId, or bytes 12-13 from DescribeProducers.
        let resp = vec![DescribedShareGroupOffsets::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        let mut buf = BytesMut::new();
        encode_describe_share_group_offsets_response(&mut buf, &resp).unwrap();
        let b8 = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b8, b9]),
            crate::error::NOT_COORDINATOR,
            "v0 first-group ErrorCode must be the INT16 at bytes 8-9"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at ShareGroupDescribe first-group bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8b = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8b]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        assert!(
            buf.len() < 14,
            "leftover-empty fixture is shorter than DescribeProducers bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeShareGroupOffsets v0 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn alter_share_group_offsets_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 91
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // ListGroups (top-level ErrorCode at bytes 4-5, different
        // fields after the INT16), ShareGroupDescribe / DescribeGroups
        // / ConsumerGroupDescribe (first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), or DescribeProducers (first-partition
        // ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x67, 0x01, 0x00];
        const RESP_16: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x01, 0x00];
        let mut buf = BytesMut::new();
        encode_alter_share_group_offsets_request(&mut buf, "g", &[]).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = AlteredShareGroupOffsets::new(crate::error::NOT_COORDINATOR);
        buf.clear();
        encode_alter_share_group_offsets_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn alter_share_group_offsets_v0_roundtrip_is_leftover_empty() {
        let topics = vec![
            AlterShareGroupOffsetsTopic::new(
                "t",
                vec![
                    AlterShareGroupOffsetsPartition::new(0, 7),
                    AlterShareGroupOffsetsPartition::new(1, 9),
                ],
            ),
            AlterShareGroupOffsetsTopic::new("t2", vec![]),
        ];
        let mut buf = BytesMut::new();
        encode_alter_share_group_offsets_request(&mut buf, "g", &topics).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_share_group_offsets_request(&mut cur).unwrap(),
            ("g".into(), topics)
        );
        assert!(
            !cur.has_remaining(),
            "AlterShareGroupOffsets v0 request must be leftover-empty"
        );

        let resp = AlteredShareGroupOffsets {
            error_code: 0,
            error_message: None,
            topics: vec![AlteredShareGroupOffsetsTopic {
                topic_name: "t".into(),
                topic_id: [0; 16],
                partitions: vec![AlteredShareGroupOffsetsPartition {
                    partition_index: 0,
                    error_code: 0,
                    error_message: None,
                }],
            }],
        };
        buf.clear();
        encode_alter_share_group_offsets_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterShareGroupOffsets v0 response must be leftover-empty"
        );
    }

    #[test]
    fn alter_share_group_offsets_top_level_error_code_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16, then compact nullable ErrorMessage, then compact
        // Responses. Measured independently from Apache
        // AlterShareGroupOffsetsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture group "g" (empty Responses). Do not assume
        // bytes 4-5 from ListGroups / DescribeClientQuotas, bytes 5-6
        // from ShareGroupDescribe / DescribeGroups /
        // ConsumerGroupDescribe, bytes 7-8 from DeleteGroups after
        // GroupId, bytes 8-9 from DescribeShareGroupOffsets first-
        // group, or bytes 12-13 from DescribeProducers. The hop code
        // is this top-level INT16, not the first-partition ErrorCode
        // (bytes 31-32 when leftover-empty topic "t" partition 0 is
        // present).
        let resp = AlteredShareGroupOffsets::new(crate::error::NOT_COORDINATOR);
        let mut buf = BytesMut::new();
        encode_alter_share_group_offsets_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at ShareGroupDescribe first-group bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        assert!(
            buf.len() < 10,
            "leftover-empty fixture is shorter than DescribeShareGroupOffsets bytes 8-9"
        );
        assert!(
            buf.len() < 14,
            "leftover-empty fixture is shorter than DescribeProducers bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterShareGroupOffsets v0 ErrorCode body must be leftover-empty"
        );

        let with_part = AlteredShareGroupOffsets {
            error_code: crate::error::NOT_COORDINATOR,
            error_message: None,
            topics: vec![AlteredShareGroupOffsetsTopic {
                topic_name: "t".into(),
                topic_id: [0; 16],
                partitions: vec![AlteredShareGroupOffsetsPartition {
                    partition_index: 0,
                    error_code: 0,
                    error_message: None,
                }],
            }],
        };
        buf.clear();
        encode_alter_share_group_offsets_response(&mut buf, &with_part).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 hop ErrorCode stays the top-level INT16 at bytes 4-5"
        );
        let b31 = buf.get(31).copied().unwrap();
        let b32 = buf.get(32).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b31, b32]),
            0,
            "v0 first-partition ErrorCode is the INT16 at bytes 31-32 and is not the hop code"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_share_group_offsets_response(&mut cur).unwrap(),
            with_part
        );
        assert!(
            !cur.has_remaining(),
            "AlterShareGroupOffsets v0 first-partition body must be leftover-empty"
        );
    }

    #[test]
    fn delete_share_group_offsets_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 92
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // AlterShareGroupOffsets / ListGroups (top-level ErrorCode at
        // bytes 4-5, different fields after the INT16),
        // ShareGroupDescribe / DescribeGroups / ConsumerGroupDescribe
        // (first-group ErrorCode at bytes 5-6), DeleteGroups (after
        // GroupId at bytes 7-8), DescribeShareGroupOffsets (first-group
        // after GroupId and Topics at bytes 8-9), or DescribeProducers
        // (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x67, 0x01, 0x00];
        const RESP_16: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x01, 0x00];
        let mut buf = BytesMut::new();
        encode_delete_share_group_offsets_request(&mut buf, "g", &[]).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = DeletedShareGroupOffsets::new(crate::error::NOT_COORDINATOR);
        buf.clear();
        encode_delete_share_group_offsets_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn delete_share_group_offsets_v0_roundtrip_is_leftover_empty() {
        let topics = vec![
            DeleteShareGroupOffsetsTopic::new("t"),
            DeleteShareGroupOffsetsTopic::new("t2"),
        ];
        let mut buf = BytesMut::new();
        encode_delete_share_group_offsets_request(&mut buf, "g", &topics).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_delete_share_group_offsets_request(&mut cur).unwrap(),
            ("g".into(), topics)
        );
        assert!(
            !cur.has_remaining(),
            "DeleteShareGroupOffsets v0 request must be leftover-empty"
        );

        let resp = DeletedShareGroupOffsets {
            error_code: 0,
            error_message: None,
            topics: vec![DeletedShareGroupOffsetsTopic {
                topic_name: "t".into(),
                topic_id: [0; 16],
                error_code: 0,
                error_message: None,
            }],
        };
        buf.clear();
        encode_delete_share_group_offsets_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_delete_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DeleteShareGroupOffsets v0 response must be leftover-empty"
        );
    }

    #[test]
    fn delete_share_group_offsets_top_level_error_code_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16, then compact nullable ErrorMessage, then compact
        // Responses. Measured independently from Apache
        // DeleteShareGroupOffsetsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture group "g" (empty Responses). Do not assume
        // bytes 4-5 from AlterShareGroupOffsets / ListGroups /
        // DescribeClientQuotas, bytes 5-6 from ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, or bytes 12-13 from
        // DescribeProducers. The hop code is this top-level INT16, not
        // the first-topic ErrorCode (bytes 26-27 when leftover-empty
        // topic "t" is present). This API has no first-partition
        // ErrorCode.
        let resp = DeletedShareGroupOffsets::new(crate::error::NOT_COORDINATOR);
        let mut buf = BytesMut::new();
        encode_delete_share_group_offsets_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at ShareGroupDescribe first-group bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        assert!(
            buf.len() < 10,
            "leftover-empty fixture is shorter than DescribeShareGroupOffsets bytes 8-9"
        );
        assert!(
            buf.len() < 14,
            "leftover-empty fixture is shorter than DescribeProducers bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_delete_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DeleteShareGroupOffsets v0 ErrorCode body must be leftover-empty"
        );

        let with_topic = DeletedShareGroupOffsets {
            error_code: crate::error::NOT_COORDINATOR,
            error_message: None,
            topics: vec![DeletedShareGroupOffsetsTopic {
                topic_name: "t".into(),
                topic_id: [0; 16],
                error_code: 0,
                error_message: None,
            }],
        };
        buf.clear();
        encode_delete_share_group_offsets_response(&mut buf, &with_topic).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 hop ErrorCode stays the top-level INT16 at bytes 4-5"
        );
        let b26 = buf.get(26).copied().unwrap();
        let b27 = buf.get(27).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b26, b27]),
            0,
            "v0 first-topic ErrorCode is the INT16 at bytes 26-27 and is not the hop code"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_delete_share_group_offsets_response(&mut cur).unwrap(),
            with_topic
        );
        assert!(
            !cur.has_remaining(),
            "DeleteShareGroupOffsets v0 first-topic body must be leftover-empty"
        );
    }

    #[test]
    fn describe_topic_partitions_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 75
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups
        // (top-level ErrorCode at bytes 4-5), ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe (first-group ErrorCode
        // at bytes 5-6), DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), or DescribeProducers (first-partition
        // ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x74, 0x00, 0x00, 0x00, 0x07, 0xd0, 0xff, 0x00];
        const RESP_29: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x1d, 0x02, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x80,
            0x00, 0x00, 0x00, 0x00, 0xff, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_describe_topic_partitions_request(&mut buf, &["t".into()], 2000, None).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = DescribeTopicPartitionsResponse::new(vec![DescribedTopicPartitions::new(
            "t",
            crate::error::TOPIC_AUTHORIZATION_FAILED,
        )]);
        buf.clear();
        encode_describe_topic_partitions_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_29);
    }

    #[test]
    fn describe_topic_partitions_v0_roundtrip_is_leftover_empty() {
        let topics = vec!["t".into(), "t2".into()];
        let cursor = TopicPartitionCursor::new("t", 3);
        let mut buf = BytesMut::new();
        encode_describe_topic_partitions_request(&mut buf, &topics, 2000, Some(&cursor)).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_topic_partitions_request(&mut cur).unwrap(),
            (topics, 2000, Some(cursor.clone()))
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTopicPartitions v0 request must be leftover-empty"
        );

        let resp = DescribeTopicPartitionsResponse {
            topics: vec![DescribedTopicPartitions {
                error_code: 0,
                name: Some("t".into()),
                topic_id: [0; 16],
                is_internal: false,
                partitions: vec![DescribedTopicPartition {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 0,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    eligible_leader_replicas: None,
                    last_known_elr: None,
                    offline_replicas: Vec::new(),
                }],
                topic_authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
            }],
            next_cursor: Some(cursor),
        };
        buf.clear();
        encode_describe_topic_partitions_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_topic_partitions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTopicPartitions v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_topic_partitions_first_topic_error_code_is_at_bytes_5_6() {
        // Official v0 body: throttle INT32, compact Topics, then each
        // topic starts with ErrorCode INT16. There is no top-level
        // ErrorCode. Measured independently from Apache
        // DescribeTopicPartitionsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture topic "t" (empty Partitions). Do not assume
        // bytes 4-5 from DeleteShareGroupOffsets / AlterShareGroupOffsets
        // / ListGroups, bytes 5-6 from ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, or bytes 12-13 from
        // DescribeProducers. The first ErrorCode is this first-topic
        // INT16. First-partition ErrorCode (bytes 27-28 when leftover-
        // empty partition 0 is present) is not the first ErrorCode.
        let resp = DescribeTopicPartitionsResponse::new(vec![DescribedTopicPartitions::new(
            "t",
            crate::error::TOPIC_AUTHORIZATION_FAILED,
        )]);
        let mut buf = BytesMut::new();
        encode_describe_topic_partitions_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 ErrorCode is not a top-level field at bytes 4-5"
        );
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 first-topic ErrorCode must be the INT16 at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_topic_partitions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTopicPartitions v0 ErrorCode body must be leftover-empty"
        );

        let with_part = DescribeTopicPartitionsResponse::new(vec![DescribedTopicPartitions {
            error_code: crate::error::TOPIC_AUTHORIZATION_FAILED,
            name: Some("t".into()),
            topic_id: [0; 16],
            is_internal: false,
            partitions: vec![DescribedTopicPartition::new(
                crate::error::UNKNOWN_TOPIC_OR_PARTITION,
            )],
            topic_authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }]);
        buf.clear();
        encode_describe_topic_partitions_response(&mut buf, &with_part).unwrap();
        let b5 = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 first ErrorCode stays the first-topic INT16 at bytes 5-6"
        );
        let b27 = buf.get(27).copied().unwrap();
        let b28 = buf.get(28).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b27, b28]),
            crate::error::UNKNOWN_TOPIC_OR_PARTITION,
            "v0 first-partition ErrorCode is the INT16 at bytes 27-28 and is not the first ErrorCode"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_topic_partitions_response(&mut cur).unwrap(),
            with_part
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTopicPartitions v0 first-partition body must be leftover-empty"
        );
    }

    #[test]
    fn list_config_resources_v1_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 74
        // validVersions 0-1, flexibleVersions 0+, listeners broker only.
        // This crate targets v1 (VERSIONS.max). Not copied from
        // DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups
        // (top-level ErrorCode at bytes 4-5, different fields after),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), or DescribeProducers (first-partition
        // ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x10, 0x00];
        const RESP_31: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x1f, 0x02, 0x02, 0x72, 0x10, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_list_config_resources_request(&mut buf, &[RESOURCE_CLIENT_METRICS]).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = ListConfigResourcesResponse::new(
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            vec![ListedConfigResource::new("r", RESOURCE_CLIENT_METRICS)],
        );
        buf.clear();
        encode_list_config_resources_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_31);
    }

    #[test]
    fn list_config_resources_v1_roundtrip_is_leftover_empty() {
        let types = vec![RESOURCE_CLIENT_METRICS, RESOURCE_TOPIC];
        let mut buf = BytesMut::new();
        encode_list_config_resources_request(&mut buf, &types).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_config_resources_request(&mut cur).unwrap(),
            types
        );
        assert!(
            !cur.has_remaining(),
            "ListConfigResources v1 request must be leftover-empty"
        );

        let resp = ListConfigResourcesResponse::new(
            0,
            vec![
                ListedConfigResource::new("r", RESOURCE_CLIENT_METRICS),
                ListedConfigResource::new("t", RESOURCE_TOPIC),
            ],
        );
        buf.clear();
        encode_list_config_resources_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_config_resources_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListConfigResources v1 response must be leftover-empty"
        );
    }

    #[test]
    fn list_config_resources_top_level_error_code_is_at_bytes_4_5() {
        // Official v1 body: throttle INT32, then top-level ErrorCode
        // INT16, then compact ConfigResources. There is no first-
        // resource ErrorCode. Measured independently from Apache
        // ListConfigResourcesResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture resource "r" type CLIENT_METRICS (16). Do not
        // assume bytes 4-5 from DeleteShareGroupOffsets /
        // AlterShareGroupOffsets / ListGroups, bytes 5-6 from
        // DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, or bytes 12-13 from
        // DescribeProducers. The leftover-empty body is 12 bytes, so
        // bytes 12-13 are not present. Official JSON lists no
        // errorCodes; official handler writes
        // CLUSTER_AUTHORIZATION_FAILED (31) via getErrorResponse.
        let resp = ListConfigResourcesResponse::new(
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            vec![ListedConfigResource::new("r", RESOURCE_CLIENT_METRICS)],
        );
        let mut buf = BytesMut::new();
        encode_list_config_resources_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v1 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v1 ErrorCode is not a first-resource field at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v1 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v1 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        assert!(
            buf.get(12).is_none(),
            "v1 leftover-empty body is 12 bytes; DescribeProducers bytes 12-13 are not present"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_config_resources_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListConfigResources v1 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn get_telemetry_subscriptions_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 71
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // ListConfigResources / DeleteShareGroupOffsets /
        // AlterShareGroupOffsets / ListGroups (top-level ErrorCode at
        // bytes 4-5, different fields after), DescribeTopicPartitions /
        // ShareGroupDescribe / DescribeGroups (first-topic / first-
        // group ErrorCode at bytes 5-6), DeleteGroups (after GroupId
        // at bytes 7-8), DescribeShareGroupOffsets (first-group after
        // GroupId and Topics at bytes 8-9), DescribeProducers (first-
        // partition ErrorCode at bytes 12-13), or DescribeTopicPartitions
        // first-partition (bytes 27-28).
        const REQ: &[u8] = &[
            0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
            0x11, 0x11, 0x00,
        ];
        const RESP_35: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x23, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
            0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x00, 0x00, 0x00, 0x01, 0x02, 0x01,
            0x00, 0x00, 0x03, 0xe8, 0x00, 0x00, 0x00, 0x64, 0x01, 0x02, 0x02, 0x6d, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_get_telemetry_subscriptions_request(&mut buf, &[0x11; 16]).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = GetTelemetrySubscriptionsResponse::new(
            crate::error::UNSUPPORTED_VERSION,
            [0x11; 16],
            1,
            vec![1],
            1000,
            100,
            true,
            vec!["m".into()],
        );
        buf.clear();
        encode_get_telemetry_subscriptions_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_35);
    }

    #[test]
    fn get_telemetry_subscriptions_v0_roundtrip_is_leftover_empty() {
        let id = [0x11u8; 16];
        let mut buf = BytesMut::new();
        encode_get_telemetry_subscriptions_request(&mut buf, &id).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_get_telemetry_subscriptions_request(&mut cur).unwrap(),
            id
        );
        assert!(
            !cur.has_remaining(),
            "GetTelemetrySubscriptions v0 request must be leftover-empty"
        );

        let resp = GetTelemetrySubscriptionsResponse::new(
            0,
            id,
            1,
            vec![1],
            1000,
            100,
            true,
            vec!["m".into()],
        );
        buf.clear();
        encode_get_telemetry_subscriptions_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_get_telemetry_subscriptions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "GetTelemetrySubscriptions v0 response must be leftover-empty"
        );
    }

    #[test]
    fn get_telemetry_subscriptions_top_level_error_code_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16, then ClientInstanceId UUID, SubscriptionId INT32,
        // compact AcceptedCompressionTypes, PushIntervalMs,
        // TelemetryMaxBytes, DeltaTemporality, compact RequestedMetrics.
        // There is no first-subscription ErrorCode and no first-metric
        // ErrorCode. Measured independently from Apache
        // GetTelemetrySubscriptionsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture ClientInstanceId [0x11; 16], SubscriptionId 1,
        // accepted compression [1], PushIntervalMs 1000,
        // TelemetryMaxBytes 100, DeltaTemporality true, RequestedMetrics
        // ["m"]. Do not assume bytes 4-5 from ListConfigResources /
        // DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // DescribeProducers, or bytes 27-28 from DescribeTopicPartitions
        // first-partition. Official JSON lists no errorCodes; official
        // handler writes UNSUPPORTED_VERSION (35) via getErrorResponse
        // on the older ZooKeeper path.
        let resp = GetTelemetrySubscriptionsResponse::new(
            crate::error::UNSUPPORTED_VERSION,
            [0x11; 16],
            1,
            vec![1],
            1000,
            100,
            true,
            vec!["m".into()],
        );
        let mut buf = BytesMut::new();
        encode_get_telemetry_subscriptions_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 ErrorCode is not a first-subscription field at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let b27 = buf.get(27).copied().unwrap();
        let b28 = buf.get(28).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b27, b28]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 ErrorCode is not at DescribeTopicPartitions first-partition bytes 27-28"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_get_telemetry_subscriptions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "GetTelemetrySubscriptions v0 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn push_telemetry_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 72
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // GetTelemetrySubscriptions / ListConfigResources /
        // DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups
        // (top-level ErrorCode at bytes 4-5, different fields after),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), DescribeProducers (first-partition
        // ErrorCode at bytes 12-13), or DescribeTopicPartitions
        // first-partition (bytes 27-28).
        const REQ: &[u8] = &[
            0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
            0x11, 0x11, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x02, 0x6d, 0x00,
        ];
        // INVALID_REQUEST (42). Leftover-empty body is 7 bytes.
        const RESP_42: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x00];
        let req = PushTelemetryRequest::new([0x11; 16], 1, false, 0, b"m".to_vec());
        let mut buf = BytesMut::new();
        encode_push_telemetry_request(&mut buf, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = PushTelemetryResponse::new(42);
        buf.clear();
        encode_push_telemetry_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_42);
    }

    #[test]
    fn push_telemetry_v0_roundtrip_is_leftover_empty() {
        let req = PushTelemetryRequest::new([0x11; 16], 1, false, 0, b"m".to_vec());
        let mut buf = BytesMut::new();
        encode_push_telemetry_request(&mut buf, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_push_telemetry_request(&mut cur).unwrap(), req);
        assert!(
            !cur.has_remaining(),
            "PushTelemetry v0 request must be leftover-empty"
        );

        let resp = PushTelemetryResponse::new(0);
        buf.clear();
        encode_push_telemetry_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_push_telemetry_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "PushTelemetry v0 response must be leftover-empty"
        );
    }

    #[test]
    fn push_telemetry_top_level_error_code_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16, then tagged. There is no first-metric ErrorCode and
        // no first-payload ErrorCode. Metrics live on the request.
        // Measured independently from Apache PushTelemetryResponse.json
        // and a kafka-protocol 0.18.0 broker encode
        // (`features = ["broker"]`) on leftover-empty fixture throttle
        // 0, error INVALID_REQUEST (42). Do not assume bytes 4-5 from
        // GetTelemetrySubscriptions / ListConfigResources /
        // DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // DescribeProducers, or bytes 27-28 from DescribeTopicPartitions
        // first-partition. Official JSON lists no errorCodes; official
        // handler writes INVALID_REQUEST (42) via getErrorResponse on
        // catch-all / reserved ClientInstanceId.
        let resp = PushTelemetryResponse::new(42);
        let mut buf = BytesMut::new();
        encode_push_telemetry_response(&mut buf, &resp).unwrap();
        assert_eq!(
            buf.len(),
            7,
            "v0 leftover-empty ErrorCode body is throttle + INT16 + tagged"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            42,
            "v0 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            42,
            "v0 ErrorCode is not a first-metric / first-payload field at bytes 5-6"
        );
        assert!(
            buf.get(7).is_none(),
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        assert!(
            buf.get(8).is_none(),
            "v0 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        assert!(
            buf.get(12).is_none(),
            "v0 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        assert!(
            buf.get(27).is_none(),
            "v0 ErrorCode is not at DescribeTopicPartitions first-partition bytes 27-28"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_push_telemetry_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "PushTelemetry v0 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn assign_replicas_to_dirs_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 73
        // validVersions 0, flexibleVersions 0+, listeners controller
        // only. This crate targets v0 (VERSIONS.max). Not copied from
        // PushTelemetry / GetTelemetrySubscriptions /
        // ListConfigResources / ListGroups (top-level ErrorCode at
        // bytes 4-5, no Directories array after),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), DescribeProducers (first-partition
        // ErrorCode at bytes 12-13), or DescribeTopicPartitions
        // first-partition (bytes 27-28).
        const REQ: &[u8] = &[
            0x00, 0x00, 0x00, 0x07, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x00,
        ];
        // NOT_CONTROLLER (41). Leftover-empty body is 8 bytes.
        const RESP_41: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x01, 0x00];
        let req = AssignReplicasToDirsRequest::new(7, -1, vec![]);
        let mut buf = BytesMut::new();
        encode_assign_replicas_to_dirs_request(&mut buf, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = AssignReplicasToDirsResponse::new(crate::error::NOT_CONTROLLER, vec![]);
        buf.clear();
        encode_assign_replicas_to_dirs_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn assign_replicas_to_dirs_v0_roundtrip_is_leftover_empty() {
        let req = AssignReplicasToDirsRequest::new(
            7,
            -1,
            vec![AssignReplicasToDirsDirectory::new(
                [0x11; 16],
                vec![AssignReplicasToDirsTopic::new(
                    [0x22; 16],
                    vec![AssignReplicasToDirsPartition::new(0)],
                )],
            )],
        );
        let mut buf = BytesMut::new();
        encode_assign_replicas_to_dirs_request(&mut buf, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_assign_replicas_to_dirs_request(&mut cur).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "AssignReplicasToDirs v0 request must be leftover-empty"
        );

        let resp = AssignReplicasToDirsResponse::new(
            0,
            vec![AssignReplicasToDirsResponseDirectory::new(
                [0x11; 16],
                vec![AssignReplicasToDirsResponseTopic::new(
                    [0x22; 16],
                    vec![AssignReplicasToDirsResponsePartition::new(0, 0)],
                )],
            )],
        );
        buf.clear();
        encode_assign_replicas_to_dirs_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_assign_replicas_to_dirs_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AssignReplicasToDirs v0 response must be leftover-empty"
        );
    }

    #[test]
    fn assign_replicas_to_dirs_top_level_error_code_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16, then compact Directories, then tagged. There is no
        // first-directory ErrorCode (directory Id is a UUID) and the
        // leftover-empty fixture has no first-partition ErrorCode.
        // Measured independently from Apache
        // AssignReplicasToDirsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture throttle 0, error NOT_CONTROLLER (41). Do not
        // assume bytes 4-5 from PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // DescribeProducers, or bytes 27-28 from DescribeTopicPartitions
        // first-partition. Official JSON lists no errorCodes; official
        // getErrorResponse writes NOT_CONTROLLER (41) via
        // NotControllerException from ControllerWriteEvent when the
        // node is not the active controller.
        let resp = AssignReplicasToDirsResponse::new(crate::error::NOT_CONTROLLER, vec![]);
        let mut buf = BytesMut::new();
        encode_assign_replicas_to_dirs_response(&mut buf, &resp).unwrap();
        assert_eq!(
            buf.len(),
            8,
            "v0 leftover-empty ErrorCode body is throttle + INT16 + empty dirs + tagged"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::NOT_CONTROLLER,
            "v0 ErrorCode is not a first-directory / first-partition field at bytes 5-6"
        );
        assert!(
            buf.get(8).is_none(),
            "v0 leftover-empty ErrorCode is not at DeleteGroups after-GroupId bytes 7-8 as a 2-byte field past the body"
        );
        assert!(
            buf.get(8).is_none(),
            "v0 leftover-empty ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        assert!(
            buf.get(12).is_none(),
            "v0 leftover-empty ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        assert!(
            buf.get(27).is_none(),
            "v0 leftover-empty ErrorCode is not at DescribeTopicPartitions first-partition bytes 27-28"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_assign_replicas_to_dirs_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AssignReplicasToDirs v0 ErrorCode body must be leftover-empty"
        );

        // One-directory fixture: first-partition ErrorCode is at
        // bytes 45-46, not the hop code.
        let with_part = AssignReplicasToDirsResponse::new(
            crate::error::NOT_CONTROLLER,
            vec![AssignReplicasToDirsResponseDirectory::new(
                [0x11; 16],
                vec![AssignReplicasToDirsResponseTopic::new(
                    [0x22; 16],
                    vec![AssignReplicasToDirsResponsePartition::new(
                        0,
                        crate::error::NOT_LEADER_OR_FOLLOWER,
                    )],
                )],
            )],
        );
        buf.clear();
        encode_assign_replicas_to_dirs_response(&mut buf, &with_part).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 top-level 41 stays at bytes 4-5 when Directories are present"
        );
        let b45 = buf.get(45).copied().unwrap();
        let b46 = buf.get(46).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b45, b46]),
            crate::error::NOT_LEADER_OR_FOLLOWER,
            "v0 first-partition ErrorCode is at bytes 45-46, not a hop"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_assign_replicas_to_dirs_response(&mut cur).unwrap(),
            with_part
        );
        assert!(
            !cur.has_remaining(),
            "AssignReplicasToDirs v0 one-directory body must be leftover-empty"
        );
    }

    #[test]
    fn alter_replica_log_dirs_v2_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 34
        // validVersions 1-2, flexibleVersions 2+, listeners broker only.
        // This crate targets v2 (VERSIONS.max). Not copied from
        // AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups
        // (top-level ErrorCode at bytes 4-5),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), DescribeProducers (first-partition
        // ErrorCode at bytes 12-13), or DescribeTopicPartitions
        // first-partition (bytes 27-28).
        const REQ: &[u8] = &[
            0x02, 0x03, 0x2f, 0x64, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ];
        // CLUSTER_AUTHORIZATION_FAILED (31). One-partition body is 17
        // bytes. Leftover-empty (empty Results) is 6 bytes and has no
        // ErrorCode.
        const RESP_31: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1f,
            0x00, 0x00, 0x00,
        ];
        const RESP_EMPTY: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x01, 0x00];
        let req = AlterReplicaLogDirsRequest::new(vec![AlterReplicaLogDirsDirectory::new(
            "/d",
            vec![AlterReplicaLogDirsTopic::new("t", vec![0])],
        )]);
        let mut buf = BytesMut::new();
        encode_alter_replica_log_dirs_request(&mut buf, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = AlterReplicaLogDirsResponse::new(vec![AlterReplicaLogDirsResponseTopic::new(
            "t",
            vec![AlterReplicaLogDirsResponsePartition::new(
                0,
                crate::error::CLUSTER_AUTHORIZATION_FAILED,
            )],
        )]);
        buf.clear();
        encode_alter_replica_log_dirs_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_31);
        buf.clear();
        encode_alter_replica_log_dirs_response(&mut buf, &AlterReplicaLogDirsResponse::new(vec![]))
            .unwrap();
        assert_eq!(&buf[..], RESP_EMPTY);
    }

    #[test]
    fn alter_replica_log_dirs_v2_roundtrip_is_leftover_empty() {
        let req = AlterReplicaLogDirsRequest::new(vec![AlterReplicaLogDirsDirectory::new(
            "/d",
            vec![AlterReplicaLogDirsTopic::new("t", vec![0])],
        )]);
        let mut buf = BytesMut::new();
        encode_alter_replica_log_dirs_request(&mut buf, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_request(&mut cur).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v2 request must be leftover-empty"
        );

        let resp = AlterReplicaLogDirsResponse::new(vec![AlterReplicaLogDirsResponseTopic::new(
            "t",
            vec![AlterReplicaLogDirsResponsePartition::new(0, 0)],
        )]);
        buf.clear();
        encode_alter_replica_log_dirs_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v2 response must be leftover-empty"
        );

        buf.clear();
        encode_alter_replica_log_dirs_request(&mut buf, &AlterReplicaLogDirsRequest::new(vec![]))
            .unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_request(&mut cur).unwrap(),
            AlterReplicaLogDirsRequest::new(vec![])
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v2 empty request must be leftover-empty"
        );
    }

    #[test]
    fn alter_replica_log_dirs_first_partition_error_code_is_at_bytes_12_13() {
        // Official v2 body: throttle INT32, then compact Results of
        // {TopicName, Partitions of {PartitionIndex INT32, ErrorCode
        // INT16, tagged}}. There is no top-level ErrorCode and no
        // first-directory ErrorCode (the response has no directory
        // array). Leftover-empty empty Results has no ErrorCode.
        // Measured independently from Apache
        // AlterReplicaLogDirsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture throttle 0, topic "t", partition 0, error
        // CLUSTER_AUTHORIZATION_FAILED (31). Do not assume bytes 4-5
        // from AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // DescribeProducers, bytes 27-28 from DescribeTopicPartitions
        // first-partition, or bytes 45-46 from AssignReplicasToDirs
        // first-partition. Official JSON lists no errorCodes; official
        // getErrorResponse writes CLUSTER_AUTHORIZATION_FAILED (31)
        // onto each partition when KafkaApis authorization fails.
        let empty = AlterReplicaLogDirsResponse::new(vec![]);
        let mut buf = BytesMut::new();
        encode_alter_replica_log_dirs_response(&mut buf, &empty).unwrap();
        assert_eq!(
            buf.len(),
            6,
            "v2 leftover-empty empty-Results body is throttle + compact empty + tagged"
        );
        assert!(
            buf.get(4).zip(buf.get(5)).is_some(),
            "v2 leftover-empty body reaches bytes 4-5 as compact empty + tagged, not ErrorCode"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 leftover-empty has no top-level ErrorCode at bytes 4-5"
        );
        assert!(
            buf.get(12).is_none(),
            "v2 leftover-empty empty-Results has no first-partition ErrorCode"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_response(&mut cur).unwrap(),
            empty
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v2 empty-Results body must be leftover-empty"
        );

        let resp = AlterReplicaLogDirsResponse::new(vec![AlterReplicaLogDirsResponseTopic::new(
            "t",
            vec![AlterReplicaLogDirsResponsePartition::new(
                0,
                crate::error::CLUSTER_AUTHORIZATION_FAILED,
            )],
        )]);
        buf.clear();
        encode_alter_replica_log_dirs_response(&mut buf, &resp).unwrap();
        assert_eq!(
            buf.len(),
            17,
            "v2 one-partition ErrorCode body is throttle + results + topic t + partition 0 + INT16 + tagged"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b12, b13]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 first-partition ErrorCode must be the INT16 at bytes 12-13"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 ErrorCode is not a first-topic / first-directory field at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        assert!(
            buf.get(27).is_none(),
            "v2 one-partition body is shorter than DescribeTopicPartitions first-partition bytes 27-28"
        );
        assert!(
            buf.get(45).is_none(),
            "v2 one-partition body is shorter than AssignReplicasToDirs first-partition bytes 45-46"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi]) == crate::error::CLUSTER_AUTHORIZATION_FAILED {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 12, "i16=31 must hit only at byte 12");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=31 hits only at byte 12");
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v2 one-partition body must be leftover-empty"
        );
    }

    #[test]
    fn describe_log_dirs_v4_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 35
        // listeners broker only. This crate targets v4 (VERSIONS.max).
        // Not copied from AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups
        // (top-level ErrorCode at bytes 4-5),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), AlterReplicaLogDirs / DescribeProducers
        // (first-partition ErrorCode at bytes 12-13), or
        // DescribeTopicPartitions first-partition (bytes 27-28).
        const REQ: &[u8] = &[0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        const REQ_EMPTY: &[u8] = &[0x01, 0x00];
        const REQ_NULL: &[u8] = &[0x00, 0x00];
        // CLUSTER_AUTHORIZATION_FAILED (31). Empty-Results body is 8
        // bytes. Top-level ErrorCode is at bytes 4-5.
        const RESP_31: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x1f, 0x01, 0x00];
        const RESP_EMPTY: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00];
        let req =
            DescribeLogDirsRequest::new(Some(vec![DescribableLogDirTopic::new("t", vec![0])]));
        let mut buf = BytesMut::new();
        encode_describe_log_dirs_request(&mut buf, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        buf.clear();
        encode_describe_log_dirs_request(&mut buf, &DescribeLogDirsRequest::new(Some(vec![])))
            .unwrap();
        assert_eq!(&buf[..], REQ_EMPTY);
        buf.clear();
        encode_describe_log_dirs_request(&mut buf, &DescribeLogDirsRequest::new(None)).unwrap();
        assert_eq!(&buf[..], REQ_NULL);
        let resp = DescribeLogDirsResponse::new(crate::error::CLUSTER_AUTHORIZATION_FAILED, vec![]);
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_31);
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, &DescribeLogDirsResponse::new(0, vec![]))
            .unwrap();
        assert_eq!(&buf[..], RESP_EMPTY);
    }

    #[test]
    fn describe_log_dirs_v4_roundtrip_is_leftover_empty() {
        let req =
            DescribeLogDirsRequest::new(Some(vec![DescribableLogDirTopic::new("t", vec![0])]));
        let mut buf = BytesMut::new();
        encode_describe_log_dirs_request(&mut buf, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_describe_log_dirs_request(&mut cur).unwrap(), req);
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v4 request must be leftover-empty"
        );

        let resp = DescribeLogDirsResponse::new(
            0,
            vec![DescribeLogDirsResult::new(
                0,
                "/d",
                vec![DescribeLogDirsTopic::new(
                    "t",
                    vec![DescribeLogDirsPartition::new(0, 0, 0, false)],
                )],
                -1,
                -1,
            )],
        );
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_describe_log_dirs_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v4 response must be leftover-empty"
        );

        buf.clear();
        encode_describe_log_dirs_request(&mut buf, &DescribeLogDirsRequest::new(None)).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_log_dirs_request(&mut cur).unwrap(),
            DescribeLogDirsRequest::new(None)
        );
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v4 null-Topics request must be leftover-empty"
        );
    }

    #[test]
    fn describe_log_dirs_top_level_error_code_is_at_bytes_4_5() {
        // Official v4 body: throttle INT32, then top-level ErrorCode
        // INT16 (versions 3+), then compact Results of {ErrorCode
        // INT16, LogDir, Topics, TotalBytes, UsableBytes, tagged}.
        // Measured independently from Apache DescribeLogDirsResponse.json
        // and a kafka-protocol 0.18.0 broker encode (`features =
        // ["broker"]`) on leftover-empty fixture throttle 0, empty
        // Results, error CLUSTER_AUTHORIZATION_FAILED (31). Do not
        // assume bytes 4-5 from AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // AlterReplicaLogDirs / DescribeProducers first-partition,
        // bytes 27-28 from DescribeTopicPartitions first-partition, or
        // bytes 45-46 from AssignReplicasToDirs first-partition.
        // Official JSON lists no errorCodes; official handler writes
        // CLUSTER_AUTHORIZATION_FAILED (31) onto the top-level field
        // when KafkaApis authorization fails.
        let empty =
            DescribeLogDirsResponse::new(crate::error::CLUSTER_AUTHORIZATION_FAILED, vec![]);
        let mut buf = BytesMut::new();
        encode_describe_log_dirs_response(&mut buf, &empty).unwrap();
        assert_eq!(
            buf.len(),
            8,
            "v4 leftover-empty empty-Results body is throttle + top-level INT16 + compact empty + tagged"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not a first-directory field at bytes 5-6"
        );
        assert!(
            buf.get(8).is_none(),
            "v4 leftover-empty empty-Results has no first-directory ErrorCode at bytes 7-8"
        );
        assert!(
            buf.get(12).is_none(),
            "v4 leftover-empty empty-Results is shorter than AlterReplicaLogDirs first-partition bytes 12-13"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi]) == crate::error::CLUSTER_AUTHORIZATION_FAILED {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 4, "i16=31 must hit only at byte 4");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=31 hits only at byte 4");
        let mut cur = &buf[..];
        assert_eq!(decode_describe_log_dirs_response(&mut cur).unwrap(), empty);
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v4 empty-Results body must be leftover-empty"
        );

        let resp = DescribeLogDirsResponse::new(
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            vec![DescribeLogDirsResult::new(
                0,
                "/d",
                vec![DescribeLogDirsTopic::new(
                    "t",
                    vec![DescribeLogDirsPartition::new(0, 0, 0, false)],
                )],
                -1,
                -1,
            )],
        );
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, &resp).unwrap();
        assert_eq!(
            buf.len(),
            57,
            "v4 one-directory body is throttle + top-level INT16 + results + dir + topic t + partition 0 + TotalBytes + UsableBytes + tagged"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 top-level ErrorCode stays at bytes 4-5 when Results are present"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b7, b8]),
            0,
            "v4 first-directory ErrorCode is at bytes 7-8, not the hop/auth code"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not a first-directory field at bytes 5-6"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not a first-partition field at bytes 12-13"
        );
        assert!(
            buf.get(27).is_some(),
            "v4 one-directory body reaches DescribeTopicPartitions first-partition bytes 27-28 as OffsetLag, not ErrorCode"
        );
        let b27 = buf.get(27).copied().unwrap();
        let b28 = buf.get(28).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b27, b28]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not at DescribeTopicPartitions first-partition bytes 27-28"
        );
        assert!(
            buf.get(45).is_some(),
            "v4 one-directory body reaches AssignReplicasToDirs first-partition bytes 45-46 as UsableBytes, not ErrorCode"
        );
        let b45 = buf.get(45).copied().unwrap();
        let b46 = buf.get(46).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b45, b46]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not at AssignReplicasToDirs first-partition bytes 45-46"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi]) == crate::error::CLUSTER_AUTHORIZATION_FAILED {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 4, "i16=31 must hit only at byte 4");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=31 hits only at byte 4");
        let mut cur = &buf[..];
        assert_eq!(decode_describe_log_dirs_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v4 one-directory body must be leftover-empty"
        );
    }

    #[test]
    fn describe_log_dirs_does_not_speak_v5() {
        // kafka-protocol 0.18.0 VERSIONS.max = 4. This crate
        // negotiates 4 only. Official trunk lists a later version;
        // that later version stays a named codec gap.
        assert_eq!(crate::protocol::api_keys::pick_version(1, 5, 4, 4), Some(4));
        assert_eq!(crate::protocol::api_keys::pick_version(5, 5, 4, 4), None);
        let resp = DescribeLogDirsResponse::new(
            0,
            vec![DescribeLogDirsResult::new(
                0,
                "/d",
                vec![DescribeLogDirsTopic::new(
                    "t",
                    vec![DescribeLogDirsPartition::new(0, 0, 0, false)],
                )],
                -1,
                -1,
            )],
        );
        let mut buf = BytesMut::new();
        encode_describe_log_dirs_response(&mut buf, &resp).unwrap();
        assert_eq!(
            buf.len(),
            57,
            "v4 one-directory leftover-empty has no extra directory field after UsableBytes"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_describe_log_dirs_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "v4 body must be leftover-empty; a later-version directory field would leave leftover"
        );
    }

    #[test]
    fn create_delegation_token_v3_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 38
        // listeners broker + controller. This crate targets v3
        // (VERSIONS.max). Not copied from DescribeLogDirs /
        // AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups
        // (top-level ErrorCode at bytes 4-5),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), AlterReplicaLogDirs / DescribeProducers
        // (first-partition ErrorCode at bytes 12-13), or
        // DescribeTopicPartitions first-partition (bytes 27-28).
        // kafka-protocol Default owner is Some(""); null owner is 0x00.
        const REQ_DEFAULT: &[u8] = &[
            0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const REQ_NULL: &[u8] = &[
            0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00,
        ];
        const REQ_ONE: &[u8] = &[
            0x00, 0x00, 0x02, 0x05, 0x55, 0x73, 0x65, 0x72, 0x02, 0x72, 0x00, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0x00,
        ];
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). Leftover-empty
        // body is 37 bytes. Top-level ErrorCode is at bytes 0-1.
        const RESP_64: &[u8] = &[
            0x00, 0x40, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const RESP_OK: &[u8] = &[
            0x00, 0x00, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let req_default =
            CreateDelegationTokenRequest::new(Some(String::new()), Some(String::new()), vec![], 0);
        let mut buf = BytesMut::new();
        encode_create_delegation_token_request(&mut buf, &req_default).unwrap();
        assert_eq!(&buf[..], REQ_DEFAULT);
        buf.clear();
        encode_create_delegation_token_request(
            &mut buf,
            &CreateDelegationTokenRequest::new(None, None, vec![], -1),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_NULL);
        buf.clear();
        encode_create_delegation_token_request(
            &mut buf,
            &CreateDelegationTokenRequest::new(
                None,
                None,
                vec![CreatableRenewer::new("User", "r")],
                -1,
            ),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_ONE);
        let resp = CreateDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "",
            "",
            "",
            "",
            0,
            0,
            0,
            "",
            vec![],
        );
        buf.clear();
        encode_create_delegation_token_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_64);
        buf.clear();
        encode_create_delegation_token_response(
            &mut buf,
            &CreateDelegationTokenResponse::new(0, "", "", "", "", 0, 0, 0, "", vec![]),
        )
        .unwrap();
        assert_eq!(&buf[..], RESP_OK);
    }

    #[test]
    fn create_delegation_token_v3_roundtrip_is_leftover_empty() {
        let req = CreateDelegationTokenRequest::new(
            Some("User".into()),
            Some("alice".into()),
            vec![CreatableRenewer::new("User", "r")],
            -1,
        );
        let mut buf = BytesMut::new();
        encode_create_delegation_token_request(&mut buf, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_request(&mut cur).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "CreateDelegationToken v3 request must be leftover-empty"
        );

        let resp = CreateDelegationTokenResponse::new(
            0,
            "User",
            "u",
            "User",
            "u",
            0,
            0,
            0,
            "tid",
            vec![0xaa],
        );
        buf.clear();
        encode_create_delegation_token_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "CreateDelegationToken v3 response must be leftover-empty"
        );

        buf.clear();
        encode_create_delegation_token_request(
            &mut buf,
            &CreateDelegationTokenRequest::new(None, None, vec![], -1),
        )
        .unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_request(&mut cur).unwrap(),
            CreateDelegationTokenRequest::new(None, None, vec![], -1)
        );
        assert!(
            !cur.has_remaining(),
            "CreateDelegationToken v3 null-owner request must be leftover-empty"
        );
    }

    #[test]
    fn create_delegation_token_top_level_error_code_is_at_bytes_0_1() {
        // Official v3 body: top-level ErrorCode INT16 first, then
        // compact principals, timestamps, compact TokenId, compact
        // Hmac, ThrottleTimeMs INT32 last. Measured independently
        // from Apache CreateDelegationTokenResponse.json and a
        // kafka-protocol 0.18.0 broker encode (`features =
        // ["broker"]`) on leftover-empty fixture throttle 0, empty
        // principals / token / hmac, error
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). Do not assume
        // bytes 4-5 from DescribeLogDirs / AssignReplicasToDirs /
        // PushTelemetry / GetTelemetrySubscriptions /
        // ListConfigResources / ListGroups, bytes 5-6 from
        // DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // AlterReplicaLogDirs / DescribeProducers first-partition,
        // bytes 27-28 from DescribeTopicPartitions first-partition, or
        // bytes 45-46 from AssignReplicasToDirs first-partition.
        // Official JSON lists no errorCodes; official handler writes
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64) onto the
        // top-level field when allowTokenRequests fails.
        let empty = CreateDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "",
            "",
            "",
            "",
            0,
            0,
            0,
            "",
            vec![],
        );
        let mut buf = BytesMut::new();
        encode_create_delegation_token_response(&mut buf, &empty).unwrap();
        assert_eq!(
            buf.len(),
            37,
            "v3 leftover-empty empty-token body is top-level INT16 + empty principals + timestamps + empty token/hmac + throttle + tagged"
        );
        let b0 = buf.get(0).copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 top-level ErrorCode must be the INT16 at bytes 0-1"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not after throttle at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not a first-renewer / first-topic field at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not at DeleteGroups / first-directory bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not a first-partition field at bytes 12-13"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_response(&mut cur).unwrap(),
            empty
        );
        assert!(
            !cur.has_remaining(),
            "CreateDelegationToken v3 empty-token body must be leftover-empty"
        );

        let resp = CreateDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "User",
            "u",
            "User",
            "u",
            0,
            0,
            0,
            "tid",
            vec![0xaa],
        );
        buf.clear();
        encode_create_delegation_token_response(&mut buf, &resp).unwrap();
        assert_eq!(
            buf.len(),
            51,
            "v3 one-token body is top-level INT16 + User/u principals + timestamps + tid + hmac + throttle + tagged"
        );
        let b0 = buf.get(0).copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 top-level ErrorCode stays at bytes 0-1 when token fields are present"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not after throttle at bytes 4-5"
        );
        assert!(
            buf.get(27).is_some(),
            "v3 one-token body reaches DescribeTopicPartitions first-partition bytes 27-28 as timestamp bytes, not ErrorCode"
        );
        let b27 = buf.get(27).copied().unwrap();
        let b28 = buf.get(28).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b27, b28]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not at DescribeTopicPartitions first-partition bytes 27-28"
        );
        assert!(
            buf.get(45).is_some(),
            "v3 one-token body reaches AssignReplicasToDirs first-partition bytes 45-46 as hmac/throttle, not ErrorCode"
        );
        let b45 = buf.get(45).copied().unwrap();
        let b46 = buf.get(46).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b45, b46]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not at AssignReplicasToDirs first-partition bytes 45-46"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "CreateDelegationToken v3 one-token body must be leftover-empty"
        );
    }

    #[test]
    fn create_delegation_token_does_not_speak_v0() {
        // kafka-protocol 0.18.0 VERSIONS.min = 1, VERSIONS.max = 3.
        // This crate negotiates 3 only. Official 3.9.1 lists deprecated
        // v0; that version is not encoded. Official trunk removed v0.
        assert_eq!(crate::protocol::api_keys::pick_version(1, 3, 3, 3), Some(3));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 3, 3), None);
        assert_eq!(crate::protocol::api_keys::pick_version(1, 2, 3, 3), None);
        let req = CreateDelegationTokenRequest::new(None, None, vec![], -1);
        let mut buf = BytesMut::new();
        encode_create_delegation_token_request(&mut buf, &req).unwrap();
        assert_eq!(
            buf.len(),
            12,
            "v3 leftover-empty null-owner request has no extra field after MaxLifetimeMs"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_request(&mut cur).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "v3 request must be leftover-empty; a later-version field would leave leftover"
        );
    }
}
