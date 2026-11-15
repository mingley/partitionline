//! Public control for a retry that is already admitted when close seals admission.
//! The mock appends the first request, delays, then drops its response.

mod common;

use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::future::{poll_fn, Future};
use std::task::Poll;
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn orderly_close_allows_an_admitted_retry_to_finish() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let producer = Producer::new_bounded(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(3))
            .delivery_timeout(Duration::from_secs(10)),
        16,
    )
    .await
    .expect("producer");
    let warm = producer
        .send(ProduceRecord::to("t").partition(0).value(&b"warm"[..]))
        .await
        .expect("warmup");
    assert_eq!(warm.offset, 0);

    mock.set_produce_drop_response_times(1);
    mock.set_produce_response_delay(Duration::from_millis(400));
    let sender = {
        let producer = producer.clone();
        tokio::spawn(async move {
            producer
                .send(ProduceRecord::to("t").partition(0).value(&b"target"[..]))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while mock.log_len("t", 0) < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("original append");
    let closing_producer = producer.clone();
    let mut close = Box::pin(closing_producer.close_timeout(Duration::from_secs(4)));
    let first = poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx))).await;
    assert!(first.is_pending(), "close must wait for the delayed response");

    let (sent, closed) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(sender, close.as_mut())
    })
    .await
    .expect("receipt and close deadline");
    let sent = sent.expect("sender task");
    println!(
        "ORDERLY_CLOSE_RETRY send={sent:?} close={closed:?} sequences={:?} log_len={}",
        mock.produce_sequences(),
        mock.log_len("t", 0)
    );
    let metadata = sent.expect("duplicate retry receipt");
    assert_eq!(metadata.offset, 1);
    closed.expect("close should finish the admitted retry");
    let sequences = mock.produce_sequences();
    assert_eq!(sequences.len(), 3);
    assert_eq!(sequences.get(1), sequences.get(2), "retry changed batch identity");
    assert_eq!(mock.log_len("t", 0), 2, "retry duplicated the target");
    assert_eq!(producer.metrics().bytes_buffered, 0);
}
