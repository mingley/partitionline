//! Produce path: kafka-protocol `ProduceRequest` + magic-v2 `RecordBatchEncoder`.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ApiKey, ProduceRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType, NO_PARTITION_LEADER_EPOCH,
    NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};

use crate::client::Client;
use crate::compression::{self, Compression};
use crate::error::{Error, Result};

/// How many in-sync replicas must ack. `-1` is `all`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    /// Fire and forget. Not used in the honest bench table.
    None,
    /// Leader only.
    Leader,
    /// ISR (`acks=-1`).
    All,
}

impl Acks {
    fn as_i16(self) -> i16 {
        match self {
            Self::None => 0,
            Self::Leader => 1,
            Self::All => -1,
        }
    }
}

/// Result of one produce.
#[derive(Debug, Clone)]
pub struct ProduceResult {
    /// Topic.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Log base offset of the batch.
    pub base_offset: i64,
}

/// Thin producer. Batching/linger is next; this still encodes a real magic-v2 batch.
pub struct Producer {
    client: Client,
    acks: Acks,
    compression: Compression,
    timeout_ms: i32,
}

impl Producer {
    /// Wrap a connected client.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            acks: Acks::Leader,
            compression: Compression::None,
            timeout_ms: 30_000,
        }
    }

    /// Set acks.
    pub fn acks(mut self, acks: Acks) -> Self {
        self.acks = acks;
        self
    }

    /// Set record-batch compression (lz4/gzip/snappy via the custom hook).
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Produce one record to `topic`/`partition`.
    pub async fn send(
        &mut self,
        topic: &str,
        partition: i32,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Result<ProduceResult> {
        if self.client.partition_count(topic).is_none() {
            self.client
                .refresh_metadata(Some(&[topic.to_string()]))
                .await?;
        }
        let leader = self.client.leader_id(topic, partition)?;
        let records = encode_record_batch(std::iter::once((key, value)), self.compression)?;
        let req = ProduceRequest::default()
            .with_transactional_id(None)
            .with_acks(self.acks.as_i16())
            .with_timeout_ms(self.timeout_ms)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(topic.to_string())))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(partition)
                    .with_records(Some(records))])]);

        let ver = self.client.negotiated.produce;
        let resp: kafka_protocol::messages::ProduceResponse = self
            .client
            .broker(leader)
            .await?
            .call(ApiKey::Produce, ver, &req)
            .await?;

        let part = resp
            .responses
            .iter()
            .find(|t| t.name.0.as_str() == topic)
            .and_then(|t| t.partition_responses.iter().find(|p| p.index == partition))
            .ok_or_else(|| Error::protocol("produce response missing partition"))?;
        Error::check(part.error_code)?;
        Ok(ProduceResult {
            topic: topic.to_string(),
            partition,
            base_offset: part.base_offset,
        })
    }
}

/// Encode records with kafka-protocol's magic-v2 encoder.
pub fn encode_record_batch(
    records: impl IntoIterator<Item = (Option<Bytes>, Option<Bytes>)>,
    compression: Compression,
) -> Result<Bytes> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let recs: Vec<Record> = records
        .into_iter()
        .enumerate()
        .map(|(i, (key, value))| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            sequence: NO_SEQUENCE,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            timestamp: ts,
            key,
            value,
            headers: indexmap::IndexMap::new(),
        })
        .collect();
    let opts = RecordEncodeOptions {
        version: 2,
        compression: compression.as_wire(),
    };
    let mut buf = BytesMut::new();
    if matches!(compression, Compression::None) {
        RecordBatchEncoder::encode(&mut buf, &recs, &opts).map_err(Error::protocol)?;
    } else {
        RecordBatchEncoder::encode_with_custom_compression(
            &mut buf,
            &recs,
            &opts,
            Some(compression::encode_hook),
        )
        .map_err(Error::protocol)?;
    }
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ProduceRequest;
    use kafka_protocol::protocol::Encodable;
    use kafka_protocol::records::RecordBatchDecoder;

    #[test]
    fn magic_v2_roundtrip_none() {
        let bytes = encode_record_batch(
            [(
                Some(Bytes::from_static(b"k")),
                Some(Bytes::from_static(b"v")),
            )],
            Compression::None,
        )
        .unwrap();
        assert_eq!(bytes[16], 2, "magic byte");
        let set = RecordBatchDecoder::decode(&mut bytes.clone()).unwrap();
        assert_eq!(set.version, 2);
        assert_eq!(set.records.len(), 1);
        assert_eq!(set.records[0].key.as_deref(), Some(&b"k"[..]));
        assert_eq!(set.records[0].value.as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn produce_request_v8_encodes() {
        let rec = encode_record_batch(
            [(None, Some(Bytes::from_static(b"hello")))],
            Compression::None,
        )
        .unwrap();
        let req = ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(1000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str("t")))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(rec))])]);
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 8).unwrap();
        assert!(!buf.is_empty());
    }
}
