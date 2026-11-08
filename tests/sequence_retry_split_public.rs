//! Public mock history for a partial retry handoff; qualification only.
#![cfg(sequence_retry_split_witness)]

mod common;

use partitionline::producer::sequence_retry_split_witness as gate;
use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::future::{poll_fn, Future};
use std::task::Poll;
use std::time::Duration;

const LIMIT: Duration = Duration::from_secs(20);

struct ReleaseGate;
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        gate::release();
    }
}

async fn history(flush_while_held: bool) {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .batch_records(2)
            .batch_bytes(1024 * 1024)
            .max_request_size(1024 * 1024)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(10))
            .delivery_timeout(Duration::from_secs(60))
            .max_block(Duration::from_secs(10)),
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

    mock.set_produce_drop_response_times(1);
    gate::arm();
    let _release = ReleaseGate;
    let mut first =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"one"[..])));
    let mut second =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"two"[..])));
    assert!(poll_fn(|cx| Poll::Ready(first.as_mut().poll(cx))).await.is_pending());
    assert!(poll_fn(|cx| Poll::Ready(second.as_mut().poll(cx))).await.is_pending());
    assert_eq!(producer.metrics().records_queued, 3);

    tokio::time::timeout(LIMIT, gate::reached())
        .await
        .expect("first retry handoff gate");
    assert_eq!(gate::snapshot(), Some((Some(1), 2, Some(1), Some(0))));
    let seqs = mock.produce_sequences();
    assert_eq!(seqs.len(), 2, "only warm and original writes before gate");
    assert_eq!((seqs[0].2, seqs[0].3), (0, 1));
    assert_eq!((seqs[1].2, seqs[1].3), (1, 2));
    assert_eq!((seqs[0].0, seqs[0].1), (seqs[1].0, seqs[1].1));
    let original = vec![
        (0, Some(b"warm".to_vec())),
        (1, Some(b"one".to_vec())),
        (2, Some(b"two".to_vec())),
    ];
    assert_eq!(mock.log_values("t", 0), original);

    if !flush_while_held {
        gate::release();
        let (one, two) = tokio::time::timeout(LIMIT, async {
            tokio::join!(first.as_mut(), second.as_mut())
        })
        .await
        .expect("complete retry deadline");
        assert_eq!(one.expect("first duplicate receipt").offset, 1);
        assert_eq!(two.expect("second duplicate receipt").offset, 2);
        let seqs = mock.produce_sequences();
        assert_eq!(seqs.len(), 3);
        assert_eq!((seqs[2].2, seqs[2].3), (1, 2));
        assert_eq!((seqs[2].0, seqs[2].1), (seqs[1].0, seqs[1].1));
        assert_eq!(mock.log_values("t", 0), original);
        println!("SPLIT_RELEASED original=1:2 retry=1:2 receipts=1,2 log=warm,one,two");
    } else {
        let flushing = {
            let p = producer.clone();
            tokio::spawn(async move { p.flush_timeout(Duration::from_secs(10)).await })
        };
        tokio::time::timeout(LIMIT, async {
            while mock.produce_sequences().len() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first partial retry while gate held");
        let seqs = mock.produce_sequences();
        assert_eq!(seqs.len(), 3);
        assert_eq!((seqs[2].2, seqs[2].3), (1, 1));
        assert_eq!((seqs[2].0, seqs[2].1), (seqs[1].0, seqs[1].1));
        assert_eq!(mock.log_values("t", 0), original);
        let first_result = tokio::time::timeout(LIMIT, first.as_mut())
            .await
            .expect("first receipt deadline");
        assert_eq!(first_result.as_ref().err().and_then(|e| e.broker_code()), Some(45));
        let flush_result = tokio::time::timeout(LIMIT, flushing)
            .await
            .expect("flush completion deadline")
            .expect("flush task");
        assert_eq!(flush_result.as_ref().err().and_then(|e| e.broker_code()), Some(45));
        assert_eq!(gate::snapshot(), Some((Some(1), 2, Some(1), Some(0))));
        println!("SPLIT_HELD original=1:2 first_retry=1:1 receipt=error45 flush=error45 log=warm,one,two");

        gate::release();
        tokio::time::timeout(LIMIT, gate::transferred(2))
            .await
            .expect("second retry handoff");
        let second_flush = producer.flush_timeout(Duration::from_secs(10)).await;
        let second_result = tokio::time::timeout(LIMIT, second.as_mut())
            .await
            .expect("second receipt deadline");
        assert_eq!(second_result.as_ref().err().and_then(|e| e.broker_code()), Some(45));
        assert_eq!(second_flush.as_ref().err().and_then(|e| e.broker_code()), Some(45));
        let seqs = mock.produce_sequences();
        assert_eq!(seqs.len(), 4);
        assert_eq!((seqs[3].2, seqs[3].3), (2, 1));
        assert_eq!((seqs[3].0, seqs[3].1), (seqs[1].0, seqs[1].1));
        assert_eq!(mock.log_values("t", 0), original);
        println!("SPLIT_SECOND retry=2:1 receipt=error45 flush=error45 log=warm,one,two");
    }
    drop(first);
    drop(second);
    let _close = producer.close_timeout(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_flush_can_split_a_retry_batch() {
    history(false).await;
    history(true).await;
}
