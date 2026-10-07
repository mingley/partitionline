//! Observe caller backing destruction while an accepted record still owns its budget.
mod common;

use bytes::Bytes;
use partitionline::{Error, Header, ProduceRecord, Producer, ProducerConfig, StickyPartitioner};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

struct ObservedAllocation {
    data: Vec<u8>,
    drops: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for ObservedAllocation {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
impl Drop for ObservedAllocation {
    fn drop(&mut self) {
        let _previous = self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
fn backed(drops: &Arc<AtomicUsize>, byte: u8) -> Bytes {
    Bytes::from_owner(ObservedAllocation {
        data: vec![byte; 4 * 1024 * 1024],
        drops: Arc::clone(drops),
    })
    .slice(1024..1088)
}
fn record(drops: &Arc<AtomicUsize>) -> ProduceRecord {
    let mut header_key = String::with_capacity(4 * 1024 * 1024);
    header_key.push('h');
    let mut record = ProduceRecord::to("t")
        .partition(0)
        .key(backed(drops, b'k'))
        .value(backed(drops, b'v'));
    record.headers = vec![Header {
        key: header_key,
        value: Some(backed(drops, b'h')),
    }];
    record
}
async fn producer(mock: &common::Mock, sticky: bool) -> Result<Producer, Error> {
    let mut config = ProducerConfig::bootstrap([mock.addr.clone()])
        .linger(Duration::ZERO)
        .buffer_memory(1024);
    if sticky {
        config = config.partitioner(StickyPartitioner::new());
    }
    let producer = Producer::new(config).await?;
    let metadata = producer
        .send(ProduceRecord::to("t").partition(0).value(Bytes::new()))
        .await?;
    assert_eq!(metadata.partition, 0);
    producer.flush().await?;
    mock.set_produce_response_delay(Duration::from_millis(300));
    Ok(producer)
}

#[tokio::test]
async fn ordinary_try_send_releases_large_shared_backing_before_ack() -> Result<(), Error> {
    let mock = common::Mock::start().await;
    let producer = producer(&mock, false).await?;
    let drops = Arc::new(AtomicUsize::new(0));
    producer.try_send(record(&drops))?;
    let early = drops.load(Ordering::SeqCst);
    let held = producer.metrics().bytes_buffered;
    producer.flush().await?;
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await?;
    assert_eq!(held, 193);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    assert_eq!(
        early, 3,
        "accepted payload retained three 4MiB caller allocations"
    );
    Ok(())
}

#[tokio::test]
async fn sticky_try_send_releases_large_shared_backing_before_ack() -> Result<(), Error> {
    let mock = common::Mock::start().await;
    let producer = producer(&mock, true).await?;
    let drops = Arc::new(AtomicUsize::new(0));
    producer.try_send(record(&drops))?;
    let early = drops.load(Ordering::SeqCst);
    let held = producer.metrics().bytes_buffered;
    producer.flush().await?;
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await?;
    assert_eq!(held, 193);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    assert_eq!(early, 3, "sticky admission retained the caller allocations");
    Ok(())
}

#[tokio::test]
async fn retained_caller_alias_does_not_keep_producer_backing_after_alias_drop() -> Result<(), Error>
{
    let mock = common::Mock::start().await;
    let producer = producer(&mock, false).await?;
    let drops = Arc::new(AtomicUsize::new(0));
    let value = backed(&drops, b'v');
    let alias = value.clone();
    producer.try_send(ProduceRecord::to("t").partition(0).value(value))?;
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(alias);
    let early = drops.load(Ordering::SeqCst);
    let held = producer.metrics().bytes_buffered;
    producer.flush().await?;
    producer.close().await?;
    assert_eq!(held, 64);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        early, 1,
        "the queued record kept the alias's large backing alive"
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_an_accepted_send_keeps_only_compacted_payload_and_one_reservation(
) -> Result<(), Error> {
    let mock = common::Mock::start().await;
    let producer = producer(&mock, false).await?;
    let drops = Arc::new(AtomicUsize::new(0));
    let input = record(&drops);
    let task_producer = producer.clone();
    let task = tokio::spawn(async move { task_producer.send(input).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while producer.metrics().records_queued < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| Error::Timeout)?;
    let early = drops.load(Ordering::SeqCst);
    let held = producer.metrics().bytes_buffered;
    task.abort();
    assert!(task.await.is_err_and(|error| error.is_cancelled()));
    assert_eq!(producer.metrics().bytes_buffered, held);
    producer.flush().await?;
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await?;
    assert_eq!(held, 193);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    assert_eq!(
        early, 3,
        "accepted async admission retained the original backing"
    );
    Ok(())
}

#[tokio::test]
async fn rejected_record_releases_caller_backing_without_reserving() -> Result<(), Error> {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).buffer_memory(32)).await?;
    let drops = Arc::new(AtomicUsize::new(0));
    assert!(matches!(
        producer.try_send(record(&drops)),
        Err(Error::RecordTooLarge { .. })
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(producer.metrics().records_queued, 0);
    producer.close().await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_channel_admission_releases_only_the_unenqueued_reservation() -> Result<(), Error>
{
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(25) // Existing channel policy: max(100000 / connections, 4096).
            .max_in_flight(1)
            .batch_records(1)
            .linger(Duration::ZERO)
            .buffer_memory(0)
            .delivery_timeout(Duration::from_secs(15)),
    )
    .await?;
    let warm = producer
        .send(ProduceRecord::to("t").partition(0).value("warm"))
        .await?;
    assert_eq!(warm.offset, 0);
    producer.flush().await?;
    mock.set_produce_response_delay(Duration::from_secs(3));
    let task_producer = producer.clone();
    let inputs = (0..5000).map(|_| ProduceRecord::to("t").partition(0).value("x"));
    let task = tokio::spawn(async move { task_producer.send_all(inputs).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let metrics = producer.metrics();
            if metrics.records_queued >= 4097 && metrics.bytes_buffered == metrics.records_queued {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| Error::Timeout)?;
    let accepted = producer.metrics().records_queued - 1;
    assert!(accepted < 5000, "input never blocked on the channel");
    assert_eq!(producer.metrics().bytes_buffered, accepted + 1);
    task.abort();
    assert!(task.await.is_err_and(|error| error.is_cancelled()));
    let after_cancel = producer.metrics().bytes_buffered;
    mock.set_produce_response_delay(Duration::ZERO);
    tokio::time::timeout(Duration::from_secs(10), producer.flush())
        .await
        .map_err(|_| Error::Timeout)??;
    let after_flush = producer.metrics().bytes_buffered;
    producer.close().await?;
    assert_eq!(
        after_cancel, accepted,
        "cancelled channel admission leaked its reservation"
    );
    assert_eq!(after_flush, 0);
    Ok(())
}

#[tokio::test]
async fn retry_keeps_compacted_payload_and_releases_one_reservation() -> Result<(), Error> {
    let mock = common::Mock::start().await;
    let producer = producer(&mock, false).await?;
    mock.set_produce_error_times(partitionline::error::NOT_LEADER_OR_FOLLOWER, 1);
    let drops = Arc::new(AtomicUsize::new(0));
    producer.try_send(record(&drops))?;
    let early = drops.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while producer.retries_in_flight() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| Error::Timeout)?;
    let held_retry = producer.metrics().bytes_buffered;
    producer.flush().await?;
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await?;
    assert_eq!(early, 3);
    assert_eq!(held_retry, 193);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn terminal_broker_error_releases_compacted_payload_and_budget() -> Result<(), Error> {
    let mock = common::Mock::start().await;
    let producer = producer(&mock, false).await?;
    mock.set_produce_error_times(partitionline::error::TOPIC_AUTHORIZATION_FAILED, 1);
    let drops = Arc::new(AtomicUsize::new(0));
    producer.try_send(record(&drops))?;
    let early = drops.load(Ordering::SeqCst);
    let error = producer.flush().await.expect_err("terminal broker error");
    assert_eq!(
        error.broker_code(),
        Some(partitionline::error::TOPIC_AUTHORIZATION_FAILED)
    );
    assert_eq!(producer.metrics().bytes_buffered, 0);
    let _result = producer.close().await;
    assert_eq!(early, 3);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
#[ignore = "requires an owned native Kafka topic and independent Java/C readback"]
async fn native_retained_backing_peer() -> Result<(), Error> {
    use partitionline::Compression;
    let env = |name| std::env::var(name).map_err(|_| Error::protocol(format!("required {name}")));
    let bootstrap = env("PL_BACKING_BOOTSTRAP")?;
    let topic = env("PL_BACKING_TOPIC")?;
    let codec = Compression::from_name(&env("PL_BACKING_CODEC")?)?;
    let timestamp: i64 = env("PL_BACKING_TIMESTAMP")?
        .parse()
        .map_err(|_| Error::protocol("invalid native timestamp"))?;
    fn entropy(size: usize) -> Vec<u8> {
        let mut out = Vec::new();
        let mut state = 0x5eed0001u64;
        while out.len() < size {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            out.extend_from_slice(&state.to_le_bytes());
        }
        out.truncate(size);
        out
    }
    fn owned(data: &[u8], drops: &Arc<AtomicUsize>) -> Bytes {
        let mut allocation = vec![0; 2 * 1024 * 1024];
        allocation[512..512 + data.len()].copy_from_slice(data);
        Bytes::from_owner(ObservedAllocation {
            data: allocation,
            drops: Arc::clone(drops),
        })
        .slice(512..512 + data.len())
    }
    fn roomy_key(name: &str) -> String {
        let mut key = String::with_capacity(1024 * 1024);
        key.push_str(name);
        key
    }
    let producer = Producer::new(
        ProducerConfig::bootstrap([bootstrap])
            .compression(codec)
            .linger(Duration::from_millis(300)),
    )
    .await?;
    let drops = Arc::new(AtomicUsize::new(0));
    let mut constructed = 0usize;
    for id in 0..9 {
        let delta = i64::try_from(id).map_err(|_| Error::protocol("native ID overflow"))?;
        let mut record = ProduceRecord::to(topic.clone()).partition(0);
        record.key = match id {
            1 => Some(owned(b"", &drops)),
            2 => Some(owned(b"key", &drops)),
            5 => Some(owned(b"headers", &drops)),
            _ => None,
        };
        if record.key.is_some() {
            constructed += 1;
        }
        let value = match id {
            0 => None,
            1 => Some(Vec::new()),
            2 => Some("世界".as_bytes().to_vec()),
            3 => Some(entropy(65536)),
            4 => Some(vec![b'x'; 200000]),
            5 => Some(vec![0, 1, 2]),
            _ => Some(entropy(131071 + id - 6)),
        };
        record.value = value.as_ref().map(|data| owned(data, &drops));
        if record.value.is_some() {
            constructed += 1;
        }
        record.headers = vec![
            Header {
                key: roomy_key("id"),
                value: Some(owned(&delta.to_be_bytes(), &drops)),
            },
            Header {
                key: roomy_key("a"),
                value: Some(owned(&[0, 255], &drops)),
            },
            Header {
                key: roomy_key("nullable"),
                value: None,
            },
        ];
        constructed += 2;
        record.timestamp = Some(timestamp + delta);
        let before = producer.metrics().records_queued;
        let task_producer = producer.clone();
        let task = tokio::spawn(async move { task_producer.send(record).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while producer.metrics().records_queued == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| Error::Timeout)?;
        assert_eq!(
            drops.load(Ordering::SeqCst),
            constructed,
            "large backing survived admission"
        );
        assert!(
            producer.metrics().bytes_buffered > 0,
            "observation occurred after ACK"
        );
        let metadata = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::protocol("native send task failed"))??;
        assert_eq!(metadata.offset, delta);
    }
    producer.flush().await?;
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await?;
    assert_eq!(drops.load(Ordering::SeqCst), constructed);
    Ok(())
}
