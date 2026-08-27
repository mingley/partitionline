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
}
