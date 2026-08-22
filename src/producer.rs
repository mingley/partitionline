//! Produce path: kafka-protocol `ProduceRequest` + magic-v2 `RecordBatchEncoder`.
//!
//! Linger + batch, murmur2/sticky partitioner, Produce v9–v13, InitProducerId.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ApiKey, ProduceRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType, NO_PARTITION_LEADER_EPOCH,
    NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};
use tokio::sync::{mpsc, oneshot};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use uuid::Uuid;

use crate::client::{topic_matches, Client};
use crate::compression::{self, Compression};
use crate::error::{Error, Result};
use crate::partitioner::{hash_partition, Sticky};

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

/// Result of one produce batch (all records in that flush share this base).
#[derive(Debug, Clone)]
pub struct ProduceResult {
    /// Topic.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Log base offset of the batch.
    pub base_offset: i64,
}

/// rust-rdkafka-shaped record. Idiomatic builder, not a C conf string.
#[derive(Debug, Clone)]
pub struct RecordTo {
    /// Topic name.
    pub topic: String,
    /// Explicit partition. `None` uses hash (key) or sticky (null key).
    pub partition: Option<i32>,
    /// Optional key.
    pub key: Option<Bytes>,
    /// Optional value.
    pub value: Option<Bytes>,
}

impl RecordTo {
    /// Start a record for `topic` (maps `FutureRecord::to`).
    pub fn to(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value: None,
        }
    }

    /// Set the key.
    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the payload (rust-rdkafka name).
    pub fn payload(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Pin a partition.
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }
}

/// Idempotent identity stamped onto magic-v2 batches.
#[derive(Debug, Clone, Copy)]
pub struct ProducerId {
    /// Broker-assigned producer id.
    pub id: i64,
    /// Producer epoch.
    pub epoch: i16,
}

/// Encode options for one record batch.
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchIdentity {
    /// When set, records carry pid/epoch/sequence.
    pub producer: Option<ProducerId>,
    /// First sequence in this batch.
    pub base_sequence: i32,
}

/// Builder. Linger 50 ms, compression none, idempotent. Published Lab A used acks=all.
pub struct ProducerBuilder {
    bootstrap: Vec<String>,
    acks: Acks,
    linger: Duration,
    batch_size: usize,
    compression: Compression,
    idempotent: bool,
    timeout_ms: i32,
}

impl ProducerBuilder {
    /// Bootstrap `host:port` list.
    pub fn bootstrap_servers(servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            bootstrap: servers.into_iter().map(Into::into).collect(),
            acks: Acks::Leader,
            linger: Duration::from_millis(50),
            batch_size: 1_048_576,
            compression: Compression::None,
            idempotent: true,
            timeout_ms: 30_000,
        }
    }

    /// Set acks (`1` or `all`).
    pub fn acks(mut self, acks: Acks) -> Self {
        self.acks = acks;
        self
    }

    /// linger.ms.
    pub fn linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    /// Approximate uncompressed batch size in bytes.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Record-batch compression.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Call InitProducerId and stamp sequences (Lab A: true).
    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent = idempotent;
        self
    }

    /// Connect, optionally InitProducerId, start the linger actor.
    pub async fn build(self) -> Result<Producer> {
        let mut client = Client::connect(self.bootstrap.iter()).await?;
        let identity = if self.idempotent {
            let (id, epoch) = client.init_producer_id().await?;
            Some(ProducerId { id, epoch })
        } else {
            None
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let cfg = ActorCfg {
            acks: self.acks,
            linger: self.linger,
            batch_size: self.batch_size,
            compression: self.compression,
            timeout_ms: self.timeout_ms,
            identity,
        };
        tokio::spawn(actor(client, cfg, rx));
        Ok(Producer { tx })
    }
}

struct ActorCfg {
    acks: Acks,
    linger: Duration,
    batch_size: usize,
    compression: Compression,
    timeout_ms: i32,
    identity: Option<ProducerId>,
}

enum Cmd {
    Send {
        rec: RecordTo,
        reply: oneshot::Sender<Result<ProduceResult>>,
    },
    Flush {
        reply: oneshot::Sender<Result<()>>,
    },
}

struct Pending {
    rec: RecordTo,
    reply: oneshot::Sender<Result<ProduceResult>>,
}

struct PartitionBuf {
    first: Instant,
    bytes: usize,
    items: Vec<Pending>,
}

/// Oneshot for a record that has been queued but not yet acked.
///
/// Lab A must enqueue many records (or run N concurrent senders) so linger/batch
/// can fill. Awaiting [`Producer::send`] per record is correctness, not throughput.
pub struct Delivery {
    rx: oneshot::Receiver<Result<ProduceResult>>,
}

impl Delivery {
    /// Wait for this record's batch to be acked.
    pub async fn wait(self) -> Result<ProduceResult> {
        self.await
    }
}

impl Future for Delivery {
    type Output = Result<ProduceResult>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_)) => {
                Poll::Ready(Err(Error::protocol("producer actor dropped reply")))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Handle. `send` maps rust-rdkafka `FutureProducer::send`.
#[derive(Clone)]
pub struct Producer {
    tx: mpsc::UnboundedSender<Cmd>,
}

impl Producer {
    /// Builder (idiomatic; not a C conf map).
    pub fn builder(servers: impl IntoIterator<Item = impl Into<String>>) -> ProducerBuilder {
        ProducerBuilder::bootstrap_servers(servers)
    }

    /// Wrap a connected client with linger=0 (example / tests).
    pub fn new(client: Client) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let cfg = ActorCfg {
            acks: Acks::Leader,
            linger: Duration::ZERO,
            batch_size: 1_048_576,
            compression: Compression::None,
            timeout_ms: 30_000,
            identity: None,
        };
        tokio::spawn(actor(client, cfg, rx));
        Self { tx }
    }

    /// Queue a record without waiting for the Produce ack.
    ///
    /// Linger/batch only fill if the caller keeps enqueueing (or runs concurrent
    /// `send`s). Do not measure one `send().await` per record and call it throughput.
    pub fn enqueue(&self, rec: RecordTo) -> Result<Delivery> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Send { rec, reply })
            .map_err(|_| Error::protocol("producer actor stopped"))?;
        Ok(Delivery { rx })
    }

    /// Enqueue and wait for this record's batch to be acked (correctness).
    pub async fn send(&self, rec: RecordTo) -> Result<ProduceResult> {
        self.enqueue(rec)?.await
    }

    /// Convenience: pin partition (used by the example).
    pub async fn send_to(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Result<ProduceResult> {
        let mut rec = RecordTo::to(topic).partition(partition);
        rec.key = key;
        rec.value = value;
        self.send(rec).await
    }

    /// Flush all buffered batches.
    pub async fn flush(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Flush { reply })
            .map_err(|_| Error::protocol("producer actor stopped"))?;
        rx.await
            .map_err(|_| Error::protocol("producer actor dropped reply"))?
    }
}

async fn actor(mut client: Client, cfg: ActorCfg, mut rx: mpsc::UnboundedReceiver<Cmd>) {
    let mut bufs: HashMap<(String, i32), PartitionBuf> = HashMap::new();
    let mut sticky: HashMap<String, Sticky> = HashMap::new();
    let mut sequences: HashMap<(String, i32), i32> = HashMap::new();
    let mut sticky_at: HashMap<String, Instant> = HashMap::new();

    loop {
        let linger_deadline = bufs.values().map(|b| b.first + cfg.linger).min();
        let cmd = if let Some(at) = linger_deadline {
            let sleep = at.saturating_duration_since(Instant::now());
            tokio::select! {
                c = rx.recv() => c,
                _ = tokio::time::sleep(sleep) => {
                    let now = Instant::now();
                    let due: Vec<_> = bufs
                        .iter()
                        .filter(|(_, b)| now >= b.first + cfg.linger)
                        .map(|(k, _)| k.clone())
                        .collect();
                    flush_many(&mut client, &cfg, &mut bufs, &mut sequences, due).await;
                    continue;
                }
            }
        } else {
            rx.recv().await
        };

        let Some(cmd) = cmd else { break };
        match cmd {
            Cmd::Send { rec, reply } => {
                enqueue(
                    &mut client,
                    &cfg,
                    &mut bufs,
                    &mut sticky,
                    &mut sticky_at,
                    rec,
                    reply,
                )
                .await;
                let full: Vec<_> = bufs
                    .iter()
                    .filter(|(_, b)| b.bytes >= cfg.batch_size)
                    .map(|(k, _)| k.clone())
                    .collect();
                flush_many(&mut client, &cfg, &mut bufs, &mut sequences, full).await;
            }
            Cmd::Flush { reply } => {
                let keys: Vec<_> = bufs.keys().cloned().collect();
                flush_many(&mut client, &cfg, &mut bufs, &mut sequences, keys).await;
                let _ = reply.send(Ok(()));
            }
        }
    }
}

async fn enqueue(
    client: &mut Client,
    cfg: &ActorCfg,
    bufs: &mut HashMap<(String, i32), PartitionBuf>,
    sticky: &mut HashMap<String, Sticky>,
    sticky_at: &mut HashMap<String, Instant>,
    rec: RecordTo,
    reply: oneshot::Sender<Result<ProduceResult>>,
) {
    if client.partition_count(&rec.topic).is_none() {
        if let Err(e) = client.refresh_metadata(Some(&[rec.topic.clone()])).await {
            let _ = reply.send(Err(e));
            return;
        }
    }
    let Some(n) = client.partition_count(&rec.topic) else {
        let _ = reply.send(Err(Error::UnknownPartition {
            topic: rec.topic.clone(),
            partition: rec.partition.unwrap_or(0),
        }));
        return;
    };
    let partition = match rec.partition {
        Some(p) => p,
        None => match &rec.key {
            Some(k) => hash_partition(k, n),
            None => {
                let rotate = sticky_at
                    .get(&rec.topic)
                    .map(|t| t.elapsed() >= cfg.linger.max(Duration::from_millis(10)))
                    .unwrap_or(true);
                if rotate {
                    sticky_at.insert(rec.topic.clone(), Instant::now());
                }
                sticky.entry(rec.topic.clone()).or_default().pick(n, rotate)
            }
        },
    };
    let nbytes = rec.key.as_ref().map(|b| b.len()).unwrap_or(0)
        + rec.value.as_ref().map(|b| b.len()).unwrap_or(0);
    let e = bufs
        .entry((rec.topic.clone(), partition))
        .or_insert_with(|| PartitionBuf {
            first: Instant::now(),
            bytes: 0,
            items: Vec::new(),
        });
    e.bytes += nbytes;
    e.items.push(Pending { rec, reply });
}

fn fail_items(items: Vec<Pending>, err: Error) {
    let msg = err.to_string();
    let mut items = items.into_iter();
    if let Some(first) = items.next() {
        let _ = first.reply.send(Err(err));
    }
    for item in items {
        let _ = item.reply.send(Err(Error::protocol(msg.clone())));
    }
}

fn succeed_items(items: Vec<Pending>, result: ProduceResult) {
    for item in items {
        let _ = item.reply.send(Ok(result.clone()));
    }
}

struct PreparedProduce {
    key: (String, i32),
    topic: String,
    topic_id: Uuid,
    partition: i32,
    ver: i16,
    n: i32,
    base_sequence: i32,
    broker: crate::broker::Broker,
    req: ProduceRequest,
}

/// Flush every ready partition without waiting for one Produce before starting
/// the next. Different leaders (and the same pipelined socket) go in flight
/// together. Sequences are peeked here and incremented only after ack.
async fn flush_many(
    client: &mut Client,
    cfg: &ActorCfg,
    bufs: &mut HashMap<(String, i32), PartitionBuf>,
    sequences: &mut HashMap<(String, i32), i32>,
    keys: Vec<(String, i32)>,
) {
    let mut prepared = Vec::new();
    for key in keys {
        let Some(buf) = bufs.remove(&key) else {
            continue;
        };
        match prepare_produce(client, cfg, sequences, &key, &buf.items).await {
            Ok(p) => prepared.push((buf.items, p)),
            Err(e) => fail_items(buf.items, e),
        }
    }

    let mut joins = Vec::with_capacity(prepared.len());
    for (items, p) in prepared {
        joins.push(async move {
            let result = send_produce_retrying(&p).await;
            (items, p, result)
        });
    }

    // Poll every partition Produce concurrently (dynamic count, no extra crate).
    let outcomes = join_all(joins).await;
    for (items, p, result) in outcomes {
        match result {
            Ok(r) => {
                if cfg.identity.is_some() {
                    sequences.insert(p.key, p.base_sequence.wrapping_add(p.n));
                }
                succeed_items(items, r);
            }
            Err(e) => fail_items(items, e),
        }
    }
}

/// Peek the next sequence. Do **not** increment until the batch is acked.
/// A retry of these records reuses the same pid/epoch/base_sequence.
async fn prepare_produce(
    client: &mut Client,
    cfg: &ActorCfg,
    sequences: &HashMap<(String, i32), i32>,
    key: &(String, i32),
    items: &[Pending],
) -> Result<PreparedProduce> {
    if items.is_empty() {
        return Err(Error::protocol("empty produce batch"));
    }
    let (topic, partition) = key;
    let leader = client.leader_id(topic, *partition)?;
    let ver = client.negotiated.produce;
    let topic_id = client.require_topic_id(topic, ver)?;
    let base_sequence = sequences.get(key).copied().unwrap_or(0);
    let n = items.len() as i32;

    let pairs = items
        .iter()
        .map(|p| (p.rec.key.clone(), p.rec.value.clone()));
    let records = encode_record_batch(
        pairs,
        cfg.compression,
        BatchIdentity {
            producer: cfg.identity,
            base_sequence,
        },
    )?;
    let req = ProduceRequest::default()
        .with_transactional_id(None)
        .with_acks(cfg.acks.as_i16())
        .with_timeout_ms(cfg.timeout_ms)
        .with_topic_data(vec![topic_produce_data(
            topic, topic_id, *partition, records,
        )]);

    let broker = client.broker(leader).await?;
    Ok(PreparedProduce {
        key: key.clone(),
        topic: topic.clone(),
        topic_id,
        partition: *partition,
        ver,
        n,
        base_sequence,
        broker,
        req,
    })
}

fn topic_produce_data(
    topic: &str,
    topic_id: Uuid,
    partition: i32,
    records: Bytes,
) -> TopicProduceData {
    TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
        .with_topic_id(topic_id)
        .with_partition_data(vec![PartitionProduceData::default()
            .with_index(partition)
            .with_records(Some(records))])
}

async fn send_produce_retrying(p: &PreparedProduce) -> Result<ProduceResult> {
    let mut last = Error::protocol("produce retry exhausted");
    for attempt in 0..3 {
        match p.broker.call(ApiKey::Produce, p.ver, &p.req).await {
            Ok(resp) => return match_produce(&resp, p),
            Err(e) => {
                last = e;
                if attempt + 1 < 3 {
                    continue;
                }
            }
        }
    }
    Err(last)
}

fn match_produce(
    resp: &kafka_protocol::messages::ProduceResponse,
    p: &PreparedProduce,
) -> Result<ProduceResult> {
    let part = resp
        .responses
        .iter()
        .find(|t| topic_matches(p.ver, &p.topic, p.topic_id, t.name.0.as_str(), t.topic_id))
        .and_then(|t| {
            t.partition_responses
                .iter()
                .find(|r| r.index == p.partition)
        })
        .ok_or_else(|| Error::protocol("produce response missing partition"))?;
    Error::check(part.error_code)?;
    Ok(ProduceResult {
        topic: p.topic.clone(),
        partition: p.partition,
        base_offset: part.base_offset,
    })
}

/// Poll a dynamic set of futures without adding a `futures` crate.
async fn join_all<F, T>(futs: Vec<F>) -> Vec<T>
where
    F: Future<Output = T>,
{
    let mut futs: Vec<Pin<Box<F>>> = futs.into_iter().map(Box::pin).collect();
    let mut out: Vec<Option<T>> = (0..futs.len()).map(|_| None).collect();
    loop {
        if out.iter().all(|s| s.is_some()) {
            return out.into_iter().map(Option::unwrap).collect();
        }
        std::future::poll_fn(|cx| {
            let mut pending = false;
            for (i, f) in futs.iter_mut().enumerate() {
                if out[i].is_some() {
                    continue;
                }
                match f.as_mut().poll(cx) {
                    Poll::Ready(v) => out[i] = Some(v),
                    Poll::Pending => pending = true,
                }
            }
            if pending {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    }
}

/// Encode records with kafka-protocol's magic-v2 encoder.
pub fn encode_record_batch(
    records: impl IntoIterator<Item = (Option<Bytes>, Option<Bytes>)>,
    compression: Compression,
    identity: BatchIdentity,
) -> Result<Bytes> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let (producer_id, producer_epoch) = match identity.producer {
        Some(p) => (p.id, p.epoch),
        None => (NO_PRODUCER_ID, NO_PRODUCER_EPOCH),
    };
    let recs: Vec<Record> = records
        .into_iter()
        .enumerate()
        .map(|(i, (key, value))| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id,
            producer_epoch,
            sequence: if identity.producer.is_some() {
                identity.base_sequence.wrapping_add(i as i32)
            } else {
                NO_SEQUENCE
            },
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
            BatchIdentity::default(),
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
    fn produce_request_v9_encodes() {
        let rec = encode_record_batch(
            [(None, Some(Bytes::from_static(b"hello")))],
            Compression::None,
            BatchIdentity::default(),
        )
        .unwrap();
        let req = ProduceRequest::default()
            .with_transactional_id(None)
            .with_acks(1)
            .with_timeout_ms(1000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str("t")))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(rec))])]);
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 9).unwrap();
        assert!(!buf.is_empty());
        let mut buf13 = BytesMut::new();
        req.encode(&mut buf13, 13).unwrap();
        assert!(!buf13.is_empty());
    }

    #[test]
    fn produce_v13_encodes_topic_id_not_name() {
        let id = Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
        let rec = encode_record_batch(
            [(None, Some(Bytes::from_static(b"hello")))],
            Compression::None,
            BatchIdentity::default(),
        )
        .unwrap();
        let req = ProduceRequest::default()
            .with_transactional_id(None)
            .with_acks(1)
            .with_timeout_ms(1000)
            .with_topic_data(vec![topic_produce_data("tid-test", id, 0, rec)]);
        let mut v13 = BytesMut::new();
        req.encode(&mut v13, 13).unwrap();
        assert!(
            v13.windows(16).any(|w| w == id.as_bytes()),
            "Produce v13 encodes topic_id"
        );
        assert!(
            !v13.windows(b"tid-test".len()).any(|w| w == b"tid-test"),
            "Produce v13 must not encode the topic name"
        );
        let mut v12 = BytesMut::new();
        req.encode(&mut v12, 12).unwrap();
        assert!(
            v12.windows(b"tid-test".len()).any(|w| w == b"tid-test"),
            "Produce v12 encodes the topic name"
        );
    }

    #[test]
    fn produce_request_v8_still_encodes() {
        let rec = encode_record_batch(
            [(None, Some(Bytes::from_static(b"hello")))],
            Compression::None,
            BatchIdentity::default(),
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
