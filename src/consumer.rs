//! Fetch path: kafka-protocol `FetchRequest` + magic-v2 `RecordBatchDecoder`.

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{ApiKey, FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Record, RecordBatchDecoder};
use uuid::Uuid;

use crate::client::{topic_matches, Client};
use crate::compression;
use crate::error::{Error, Result};

/// Consumer replica_id. Followers use a broker id; clients must send -1.
pub const CONSUMER_REPLICA_ID: i32 = -1;

/// `isolation_level = 0` (READ_UNCOMMITTED). Required on Fetch v4+.
pub const ISOLATION_READ_UNCOMMITTED: i8 = 0;

/// `session_id = 0` when not using incremental fetch (v7+/v12+).
pub const FETCH_SESSION_NONE: i32 = 0;

/// `session_epoch = -1` when not using incremental fetch (v7+/v12+).
pub const FETCH_SESSION_EPOCH_INVALID: i32 = -1;

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
        let ver = self.client.negotiated.fetch;
        let topic_id = self.client.require_topic_id(topic, ver)?;
        let req = consumer_fetch_request(ConsumerFetch {
            topic,
            topic_id,
            partition,
            offset,
            max_wait_ms: self.max_wait_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.partition_max_bytes.saturating_mul(4),
            partition_max_bytes: self.partition_max_bytes,
            fetch_version: ver,
        });

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
            .find(|t| topic_matches(ver, topic, topic_id, t.topic.0.as_str(), t.topic_id))
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

/// Fields for a consumer Fetch (not incremental, not a follower).
#[derive(Debug, Clone, Copy)]
pub struct ConsumerFetch<'a> {
    /// Topic name (encoded on v4–v12).
    pub topic: &'a str,
    /// Topic UUID (encoded on v13+).
    pub topic_id: Uuid,
    /// Partition index.
    pub partition: i32,
    /// Fetch offset.
    pub offset: i64,
    /// Max wait.
    pub max_wait_ms: i32,
    /// Min bytes.
    pub min_bytes: i32,
    /// Total max bytes.
    pub max_bytes: i32,
    /// Per-partition max bytes.
    pub partition_max_bytes: i32,
    /// Negotiated Fetch version (v12–v16).
    pub fetch_version: i16,
}

/// Build a consumer FetchRequest with the fields Kafka 4.x requires.
///
/// - `replica_id = -1` (v4–v14; v15+ must leave the default -1 / default ReplicaState)
/// - `isolation_level = 0` (read_uncommitted) for v4+
/// - `session_id = 0`, `session_epoch = -1` when not doing incremental fetch (v7+/v12+)
/// - `topic` name on v12; `topic_id` on v13+
pub fn consumer_fetch_request(f: ConsumerFetch<'_>) -> FetchRequest {
    let mut topic = FetchTopic::default().with_partitions(vec![FetchPartition::default()
        .with_partition(f.partition)
        .with_current_leader_epoch(-1)
        .with_fetch_offset(f.offset)
        .with_last_fetched_epoch(-1)
        .with_log_start_offset(-1)
        .with_partition_max_bytes(f.partition_max_bytes)]);
    if f.fetch_version >= 13 {
        topic = topic.with_topic_id(f.topic_id);
    } else {
        topic = topic.with_topic(TopicName(StrBytes::from_string(f.topic.to_string())));
    }
    FetchRequest::default()
        .with_replica_id(CONSUMER_REPLICA_ID.into())
        .with_max_wait_ms(f.max_wait_ms)
        .with_min_bytes(f.min_bytes)
        .with_max_bytes(f.max_bytes)
        .with_isolation_level(ISOLATION_READ_UNCOMMITTED)
        .with_session_id(FETCH_SESSION_NONE)
        .with_session_epoch(FETCH_SESSION_EPOCH_INVALID)
        .with_topics(vec![topic])
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

    fn sample_fetch(topic_id: Uuid, version: i16) -> FetchRequest {
        consumer_fetch_request(ConsumerFetch {
            topic: "tid-test",
            topic_id,
            partition: 0,
            offset: 0,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 4096,
            partition_max_bytes: 1024,
            fetch_version: version,
        })
    }

    fn prefix_v12_v14(buf: &[u8]) {
        // kafka-protocol FetchRequest encode (docs.rs / generated Encodable):
        // v4–v14: replica_id, max_wait_ms, min_bytes, max_bytes, isolation_level,
        //         session_id (v7+), session_epoch (v7+), topics...
        assert_eq!(&buf[0..4], &(-1i32).to_be_bytes(), "replica_id must be -1");
        assert_eq!(&buf[4..8], &100i32.to_be_bytes(), "max_wait_ms");
        assert_eq!(&buf[8..12], &1i32.to_be_bytes(), "min_bytes");
        assert_eq!(&buf[12..16], &4096i32.to_be_bytes(), "max_bytes");
        assert_eq!(buf[16], 0, "isolation_level read_uncommitted");
        assert_eq!(&buf[17..21], &0i32.to_be_bytes(), "session_id");
        assert_eq!(&buf[21..25], &(-1i32).to_be_bytes(), "session_epoch");
    }

    fn prefix_v15_plus(buf: &[u8]) {
        // v15+: replica_id is not a top-level field (tagged ReplicaState default).
        assert_eq!(
            &buf[0..4],
            &100i32.to_be_bytes(),
            "max_wait_ms first on v15+"
        );
        assert_eq!(&buf[4..8], &1i32.to_be_bytes(), "min_bytes");
        assert_eq!(&buf[8..12], &4096i32.to_be_bytes(), "max_bytes");
        assert_eq!(buf[12], 0, "isolation_level read_uncommitted");
        assert_eq!(&buf[13..17], &0i32.to_be_bytes(), "session_id");
        assert_eq!(&buf[17..21], &(-1i32).to_be_bytes(), "session_epoch");
    }

    #[test]
    fn fetch_v12_to_v16_consumer_fields() {
        let id = Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
        for ver in 12..=16 {
            let req = sample_fetch(id, ver);
            let mut buf = bytes::BytesMut::new();
            req.encode(&mut buf, ver)
                .unwrap_or_else(|e| panic!("Fetch v{ver} encode: {e}"));
            if ver <= 14 {
                prefix_v12_v14(&buf);
            } else {
                prefix_v15_plus(&buf);
            }
            if ver >= 13 {
                assert!(
                    buf.windows(16).any(|w| w == id.as_bytes()),
                    "Fetch v{ver} must encode topic_id"
                );
                assert!(
                    !buf.windows(b"tid-test".len()).any(|w| w == b"tid-test"),
                    "Fetch v{ver} must not encode topic name"
                );
            } else {
                assert!(
                    buf.windows(b"tid-test".len()).any(|w| w == b"tid-test"),
                    "Fetch v12 encodes topic name"
                );
            }
        }
    }

    #[test]
    fn fetch_v15_rejects_nonzero_replica_id() {
        let mut req = sample_fetch(Uuid::nil(), 15);
        req.replica_id = 0.into();
        let mut buf = bytes::BytesMut::new();
        assert!(
            req.encode(&mut buf, 15).is_err(),
            "replica_id is not a v15 field; kafka-protocol bails if it is not -1"
        );
    }

    #[test]
    fn fetch_request_v11_encodes() {
        let req = FetchRequest::default()
            .with_replica_id(CONSUMER_REPLICA_ID.into())
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
        let mut buf12 = bytes::BytesMut::new();
        req.encode(&mut buf12, 12).unwrap();
        assert!(!buf12.is_empty());
    }

    #[test]
    fn decode_records_from_our_batch() {
        let raw = encode_record_batch(
            [(None, Some(Bytes::from_static(b"payload")))],
            Compression::None,
            crate::producer::BatchIdentity::default(),
        )
        .unwrap();
        let recs = decode_records(raw).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].value.as_deref(), Some(&b"payload"[..]));
    }
}
