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
}
