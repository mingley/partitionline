//! Fetch path: kafka-protocol `FetchRequest` + magic-v2 `RecordBatchDecoder`.

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{ApiKey, FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Record, RecordBatchDecoder};

use crate::client::Client;
use crate::compression;
use crate::error::{Error, Result};

/// One fetch from a single topic partition.
#[derive(Debug, Clone)]
pub struct Fetched {
    /// Topic.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// High watermark.
    pub high_watermark: i64,
    /// Decoded magic-v2 records (owned; see kafka-protocol issue #42).
    pub records: Vec<Record>,
}

/// Thin fetcher. Classic consumer groups come next.
pub struct Fetcher {
    client: Client,
    max_wait_ms: i32,
    min_bytes: i32,
    partition_max_bytes: i32,
}

impl Fetcher {
    /// Wrap a connected client.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            max_wait_ms: 500,
            min_bytes: 1,
            partition_max_bytes: 1_048_576,
        }
    }

    /// Fetch from `topic`/`partition` starting at `offset`.
    pub async fn fetch(&mut self, topic: &str, partition: i32, offset: i64) -> Result<Fetched> {
        if self.client.partition_count(topic).is_none() {
            self.client
                .refresh_metadata(Some(&[topic.to_string()]))
                .await?;
        }
        let leader = self.client.leader_id(topic, partition)?;
        let req = FetchRequest::default()
            .with_max_wait_ms(self.max_wait_ms)
            .with_min_bytes(self.min_bytes)
            .with_max_bytes(self.partition_max_bytes.saturating_mul(4))
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(partition)
                    .with_fetch_offset(offset)
                    .with_partition_max_bytes(self.partition_max_bytes)])]);

        let ver = self.client.negotiated.fetch;
        let resp: kafka_protocol::messages::FetchResponse = self
            .client
            .broker(leader)
            .await?
            .call(ApiKey::Fetch, ver, &req)
            .await?;
        Error::check(resp.error_code)?;

        let part = resp
            .responses
            .iter()
            .find(|t| t.topic.0.as_str() == topic)
            .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
            .ok_or_else(|| Error::protocol("fetch response missing partition"))?;
        Error::check(part.error_code)?;

        let records = match &part.records {
            Some(raw) if !raw.is_empty() => decode_records(raw.clone())?,
            _ => Vec::new(),
        };
        Ok(Fetched {
            topic: topic.to_string(),
            partition,
            high_watermark: part.high_watermark,
            records,
        })
    }
}

/// Decode one or more magic-v2 batches with the pure-Rust compression hook.
pub fn decode_records(mut raw: Bytes) -> Result<Vec<Record>> {
    let mut out = Vec::new();
    while !raw.is_empty() {
        let set = RecordBatchDecoder::decode_with_custom_compression(
            &mut raw,
            Some(compression::decode_hook),
        )
        .map_err(Error::protocol)?;
        out.extend(set.records);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer::encode_record_batch;
    use crate::Compression;
    use kafka_protocol::protocol::Encodable;

    #[test]
    fn fetch_request_v11_encodes() {
        let req = FetchRequest::default()
            .with_max_wait_ms(100)
            .with_min_bytes(1)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_static_str("t")))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(0)
                    .with_partition_max_bytes(1024)])]);
        let mut buf = bytes::BytesMut::new();
        req.encode(&mut buf, 11).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn decode_records_from_our_batch() {
        let raw = encode_record_batch(
            [(None, Some(Bytes::from_static(b"payload")))],
            Compression::None,
        )
        .unwrap();
        let recs = decode_records(raw).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].value.as_deref(), Some(&b"payload"[..]));
    }
}
