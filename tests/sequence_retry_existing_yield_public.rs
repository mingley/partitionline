//! Public mock history held at an existing producer retry yield.
#![cfg(sequence_retry_yield_witness)]

mod common;

use partitionline::producer::sequence_retry_yield_witness as gate;
use partitionline::{
    Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata,
};
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

const COUNT: usize = 8193;
const LIMIT: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
struct Counts {
    acks: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
}
impl ProducerInterceptor for Counts {
    fn on_ack(&self, _metadata: &RecordMetadata) {
        let _ = self.acks.fetch_add(1, Ordering::SeqCst);
    }

    fn on_error(&self, _error: &Error) {
        let _ = self.errors.fetch_add(1, Ordering::SeqCst);
    }
}

struct ReleaseGate;
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        gate::release();
    }
}

async fn history(flush_while_held: bool) {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let counts = Counts::default();
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .batch_records(COUNT)
            .batch_bytes(2 * 1024 * 1024)
            .max_request_size(2 * 1024 * 1024)
            .buffer_memory(0)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(20))
            .delivery_timeout(Duration::from_secs(120))
            .max_block(Duration::from_secs(20))
            .interceptor(counts.clone()),
    )
    .await
    .expect("producer setup");
    let warm = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("t").partition(0).value(&b"warm"[..])),
    )
    .await
    .expect("warm deadline")
    .expect("warm result");
    assert_eq!(warm.offset, 0);
    assert_eq!(counts.acks.load(Ordering::SeqCst), 1);

    mock.set_produce_drop_response_times(1);
    gate::arm();
    let _release = ReleaseGate;
    let mut first =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"first"[..])));
    assert!(poll_fn(|cx| Poll::Ready(first.as_mut().poll(cx))).await.is_pending());
    for index in 1..COUNT - 1 {
        let value = u64::try_from(index).unwrap().to_le_bytes();
        producer
            .try_send(ProduceRecord::to("t").partition(0).value(value.to_vec()))
            .expect("middle record admitted");
    }
    let mut last =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"last"[..])));
    assert!(poll_fn(|cx| Poll::Ready(last.as_mut().poll(cx))).await.is_pending());
    assert_eq!(producer.metrics().records_queued, u64::try_from(COUNT + 1).unwrap());

    tokio::time::timeout(LIMIT, gate::reached())
        .await
        .expect("existing 8192-record retry yield not reached");
    tokio::time::timeout(LIMIT, gate::transferred(COUNT - 1))
        .await
        .expect("first 8192 retry handoffs");
    let (first_transfer, last_transfer) = gate::snapshots();
    assert_eq!(first_transfer, Some((Some(1), COUNT, Some(1), Some(0))));
    assert_eq!(last_transfer, Some((Some(1), COUNT, Some(8192), Some(0))));
    let seqs = mock.produce_sequences();
    assert_eq!(seqs.len(), 2, "only warm and original writes before flush");
    assert_eq!((seqs[0].2, seqs[0].3), (0, 1));
    assert_eq!((seqs[1].2, seqs[1].3), (1, 8193));
    assert_eq!((seqs[0].0, seqs[0].1), (seqs[1].0, seqs[1].1));
    let log = mock.log_values("t", 0);
    assert_eq!(log.len(), COUNT + 1);
    assert_eq!(log.first(), Some(&(0, Some(b"warm".to_vec()))));
    assert_eq!(log.get(1), Some(&(1, Some(b"first".to_vec()))));
    for index in 1..COUNT - 1 {
        let value = u64::try_from(index).unwrap().to_le_bytes();
        assert_eq!(
            log.get(index + 1),
            Some(&(i64::try_from(index + 1).unwrap(), Some(value.to_vec())))
        );
    }
    assert_eq!(log.last(), Some(&(8193, Some(b"last".to_vec()))));

    if !flush_while_held {
        gate::release();
        let (one, end) = tokio::time::timeout(LIMIT, async {
            tokio::join!(first.as_mut(), last.as_mut())
        })
        .await
        .expect("complete retry deadline");
        assert_eq!(one.expect("first duplicate receipt").offset, 1);
        assert_eq!(end.expect("last duplicate receipt").offset, 8193);
        let seqs = mock.produce_sequences();
        assert_eq!(seqs.len(), 3);
        assert_eq!((seqs[2].2, seqs[2].3), (1, 8193));
        assert_eq!((seqs[2].0, seqs[2].1), (seqs[1].0, seqs[1].1));
        assert_eq!(mock.log_values("t", 0), log);
        assert_eq!(counts.acks.load(Ordering::SeqCst), COUNT + 1);
        assert_eq!(counts.errors.load(Ordering::SeqCst), 0);
        println!("YIELD_RELEASED original=1:8193 retry=1:8193 receipts=1,8193 callbacks=8193 log=once");
    } else {
        let flushing = {
            let p = producer.clone();
            tokio::spawn(async move { p.flush_timeout(Duration::from_secs(20)).await })
        };
        tokio::time::timeout(LIMIT, async {
            while mock.produce_sequences().len() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("partial retry while existing yield held");
        let seqs = mock.produce_sequences();
        assert_eq!(seqs.len(), 3);
        assert_eq!((seqs[2].2, seqs[2].3), (1, 8192));
        assert_eq!((seqs[2].0, seqs[2].1), (seqs[1].0, seqs[1].1));
        assert_eq!(mock.log_values("t", 0), log);
        let first_result = tokio::time::timeout(LIMIT, first.as_mut())
            .await
            .expect("first receipt deadline");
        assert_eq!(first_result.as_ref().err().and_then(|e| e.broker_code()), Some(45));
        let flush_result = tokio::time::timeout(LIMIT, flushing)
            .await
            .expect("flush deadline")
            .expect("flush task");
        assert_eq!(flush_result.as_ref().err().and_then(|e| e.broker_code()), Some(45));
        assert_eq!(counts.acks.load(Ordering::SeqCst), 1);
        assert_eq!(counts.errors.load(Ordering::SeqCst), COUNT - 1);
        println!("YIELD_HELD original=1:8193 retry=1:8192 receipt=error45 flush=error45 callbacks=8192 log=once");

        gate::release();
        tokio::time::timeout(LIMIT, gate::transferred(COUNT))
            .await
            .expect("last retry handoff");
        let last_flush = producer.flush_timeout(Duration::from_secs(20)).await;
        let last_result = tokio::time::timeout(LIMIT, last.as_mut())
            .await
            .expect("last receipt deadline");
        assert_eq!(last_result.as_ref().err().and_then(|e| e.broker_code()), Some(45));
        assert_eq!(last_flush.as_ref().err().and_then(|e| e.broker_code()), Some(45));
        let seqs = mock.produce_sequences();
        assert_eq!(seqs.len(), 4);
        assert_eq!((seqs[3].2, seqs[3].3), (8193, 1));
        assert_eq!((seqs[3].0, seqs[3].1), (seqs[1].0, seqs[1].1));
        assert_eq!(mock.log_values("t", 0), log);
        assert_eq!(counts.acks.load(Ordering::SeqCst), 1);
        assert_eq!(counts.errors.load(Ordering::SeqCst), COUNT);
        println!("YIELD_LAST retry=8193:1 receipt=error45 flush=error45 callbacks=8193 log=once");
    }
    drop(first);
    drop(last);
    let _close = producer.close_timeout(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_flush_at_existing_retry_yield() {
    history(false).await;
    history(true).await;
}
