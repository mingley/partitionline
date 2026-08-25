//! ConsumerGroupHeartbeat (KIP-848, api key 68). Flexible v0.

#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitions {
    pub topic_id: [u8; 16],
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupHeartbeatRequest {
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub subscribed_topic_names: Option<Vec<String>>,
    pub topic_partitions: Option<Vec<TopicPartitions>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupHeartbeatResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub member_id: Option<String>,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    pub assignment: Option<Vec<TopicPartitions>>,
}

pub fn encode_consumer_group_heartbeat_request(
    buf: &mut BytesMut,
    req: &ConsumerGroupHeartbeatRequest,
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(&req.group_id))?;
    buf::put_compact_string(buf, Some(&req.member_id))?;
    buf.put_i32(req.member_epoch);
    buf::put_compact_string(buf, None)?; // instance_id
    buf::put_compact_string(buf, None)?; // rack_id
    buf.put_i32(45_000); // rebalance_timeout_ms
    match &req.subscribed_topic_names {
        None => buf::put_array_len(buf, true, None)?,
        Some(names) => {
            buf::put_array_len(buf, true, Some(names.len()))?;
            for n in names {
                buf::put_compact_string(buf, Some(n))?;
            }
        }
    }
    buf::put_compact_string(buf, None)?; // server_assignor
    encode_topic_partitions(buf, req.topic_partitions.as_deref())?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_consumer_group_heartbeat_request<B: Buf>(
    buf: &mut B,
) -> Result<ConsumerGroupHeartbeatRequest> {
    let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let member_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let member_epoch = buf::get_i32(buf)?;
    let _instance = buf::get_compact_string(buf)?;
    let _rack = buf::get_compact_string(buf)?;
    let _rebalance = buf::get_i32(buf)?;
    let subscribed_topic_names = {
        let n = buf::get_array_len(buf, true)?;
        match n {
            None => None,
            Some(n) => {
                let mut names = Vec::with_capacity(n);
                for _ in 0..n {
                    names.push(buf::get_compact_string(buf)?.unwrap_or_default());
                }
                Some(names)
            }
        }
    };
    let _assignor = buf::get_compact_string(buf)?;
    let topic_partitions = decode_topic_partitions(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(ConsumerGroupHeartbeatRequest {
        group_id,
        member_id,
        member_epoch,
        subscribed_topic_names,
        topic_partitions,
    })
}

pub fn encode_consumer_group_heartbeat_response(
    buf: &mut BytesMut,
    resp: &ConsumerGroupHeartbeatResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_compact_string(buf, resp.member_id.as_deref())?;
    buf.put_i32(resp.member_epoch);
    buf.put_i32(resp.heartbeat_interval_ms);
    match &resp.assignment {
        None => buf::put_unsigned_varint(buf, 0),
        Some(parts) => {
            buf::put_unsigned_varint(buf, 1);
            encode_topic_partitions(buf, Some(parts))?;
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_consumer_group_heartbeat_response<B: Buf>(
    buf: &mut B,
) -> Result<ConsumerGroupHeartbeatResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let member_id = buf::get_compact_string(buf)?;
    let member_epoch = buf::get_i32(buf)?;
    let heartbeat_interval_ms = buf::get_i32(buf)?;
    let present = buf::get_unsigned_varint(buf)?;
    let assignment = if present == 0 {
        None
    } else {
        let parts = decode_topic_partitions(buf)?;
        buf::skip_tagged_fields(buf)?;
        parts
    };
    buf::skip_tagged_fields(buf)?;
    Ok(ConsumerGroupHeartbeatResponse {
        error_code,
        error_message,
        member_id,
        member_epoch,
        heartbeat_interval_ms,
        assignment,
    })
}

fn encode_topic_partitions(
    buf: &mut BytesMut,
    parts: Option<&[TopicPartitions]>,
) -> crate::error::Result<()> {
    match parts {
        None => buf::put_array_len(buf, true, None)?,
        Some(parts) => {
            buf::put_array_len(buf, true, Some(parts.len()))?;
            for t in parts {
                buf.extend_from_slice(&t.topic_id);
                buf::put_array_len(buf, true, Some(t.partitions.len()))?;
                for p in &t.partitions {
                    buf.put_i32(*p);
                }
                buf::put_empty_tagged_fields(buf);
            }
        }
    }
    Ok(())
}

fn decode_topic_partitions<B: Buf>(buf: &mut B) -> Result<Option<Vec<TopicPartitions>>> {
    let n = buf::get_array_len(buf, true)?;
    let Some(n) = n else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        buf::skip_tagged_fields(buf)?;
        out.push(TopicPartitions {
            topic_id,
            partitions,
        });
    }
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_group_heartbeat_v0_roundtrip_join() {
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["t".into()]),
            topic_partitions: None,
        };
        let mut buf = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut buf, &req).unwrap();
        assert_eq!(
            decode_consumer_group_heartbeat_request(&mut &buf[..]).unwrap(),
            req
        );

        let topic_id = [7u8; 16];
        let resp = ConsumerGroupHeartbeatResponse {
            error_code: 0,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: Some(vec![TopicPartitions {
                topic_id,
                partitions: vec![0, 1],
            }]),
        };
        buf.clear();
        encode_consumer_group_heartbeat_response(&mut buf, &resp).unwrap();
        assert_eq!(
            decode_consumer_group_heartbeat_response(&mut &buf[..]).unwrap(),
            resp
        );
    }

    #[test]
    fn consumer_group_heartbeat_leave_has_epoch_minus_one() {
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: -1,
            subscribed_topic_names: None,
            topic_partitions: None,
        };
        let mut buf = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut buf, &req).unwrap();
        let decoded = decode_consumer_group_heartbeat_request(&mut &buf[..]).unwrap();
        assert_eq!(decoded.member_epoch, -1);
        assert_eq!(decoded.member_id, "m1");
    }
}
