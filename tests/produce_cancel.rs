//! KL-02 slice: produce cancellation / shutdown outcome contract (mock).
//!
//! Dropping a `send` future must not be read as "never written". Buffered work
//! can still reach the broker; the caller's outcome is ambiguous until
//! `flush`/`close` settles delivery.

mod common;

use partitionline::error;
use partitionline::{Error, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn send_completes_with_record_metadata() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.topic, "t");
    assert_eq!(md.partition, 0);
    assert!(!mock.produce_nodes().is_empty());
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn send_fails_when_broker_returns_produce_error() {
    let mock = common::Mock::start().await;
    mock.set_produce_error(error::INVALID_RECORD);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .delivery_timeout(Duration::from_secs(2))
            .max_block(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    let err = producer
        .send(ProduceRecord::to("t").value(&b"fail"[..]))
        .await
        .expect_err("broker produce error must fail the send future");
    assert!(
        !matches!(err, Error::Closed),
        "failed delivery is not Closed; got {err:?}"
    );
    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "failed records must release buffer_memory"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn dropping_send_future_while_buffered_is_ambiguous_but_still_delivers() {
    let mock = common::Mock::start().await;
    // Long linger keeps the record buffered so we can drop before the wire send.
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::from_secs(30))
            .delivery_timeout(Duration::from_secs(10)),
    )
    .await
    .unwrap();

    let mut send_fut = Box::pin(producer.send(ProduceRecord::to("t").value(&b"ambig"[..])));
    timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                biased;
                res = &mut send_fut => {
                    panic!("send completed before linger expiry: {res:?}");
                }
                _ = sleep(Duration::from_millis(10)) => {
                    if producer.metrics().bytes_buffered > 0 {
                        break;
                    }
                }
            }
        }
    })
    .await
    .expect("record should enter buffer_memory before linger fires");

    // Drop the caller future: outcome is ambiguous; worker must keep the record.
    drop(send_fut);

    producer.flush().await.unwrap();
    assert!(
        !mock.produce_nodes().is_empty(),
        "dropping send must not imply the record was never written"
    );
    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "flush must release buffer after delivery"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn send_after_close_returns_closed() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let other = producer.clone();
    producer.close().await.unwrap();
    let err = other
        .send(ProduceRecord::to("t").value(&b"late"[..]))
        .await
        .expect_err("send after close must fail");
    assert!(
        matches!(err, Error::Closed),
        "expected Closed after shutdown, got {err:?}"
    );
    let err = other.try_send(ProduceRecord::to("t").value(&b"late2"[..]));
    assert!(
        matches!(err, Err(Error::Closed) | Err(Error::QueueFull)),
        "try_send after close must not silently enqueue; got {err:?}"
    );
}

#[tokio::test]
async fn stalled_broker_close_timeout_terminates_boundedly_and_completes_inflight() {
    let mock = common::Mock::start().await;
    // Broker stalls on produce for 10s.
    mock.set_produce_delay(Duration::from_secs(10));
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(10))
            .delivery_timeout(Duration::from_secs(10)),
    )
    .await
    .unwrap();

    let send_producer = producer.clone();
    let send_handle = tokio::spawn(async move {
        send_producer
            .send(ProduceRecord::to("t").value(&b"stalled"[..]))
            .await
    });

    // Wait until record is transmitted to broker and in flight
    timeout(Duration::from_secs(2), async {
        while mock.produce_request_nodes().is_empty() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("record must reach broker socket and be in flight");

    // close_timeout with 100ms
    let start = std::time::Instant::now();
    let p_close = producer.clone();
    let res = p_close.close_timeout(Duration::from_millis(100)).await;
    let elapsed = start.elapsed();

    assert!(
        matches!(res, Err(Error::Timeout)),
        "expected Timeout from stalled close_timeout, got {res:?}"
    );
    assert!(
        elapsed < Duration::from_millis(800),
        "stalled close_timeout must terminate boundedly, elapsed: {elapsed:?}"
    );

    // The in-flight send future must terminate with Error::Timeout (ambiguous delivery, NOT Closed).
    let send_res = timeout(Duration::from_secs(1), send_handle)
        .await
        .expect("send future must resolve boundedly")
        .unwrap();
    assert!(
        matches!(send_res, Err(Error::Timeout)),
        "in-flight record must fail with Timeout (ambiguous delivery), got {send_res:?}"
    );

    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "buffer permits must be released exactly once after stalled shutdown"
    );

    // Clones observe Closed
    let err = producer
        .send(ProduceRecord::to("t").value(&b"new"[..]))
        .await
        .expect_err("new sends after closed producer must fail");
    assert!(
        matches!(err, Error::Closed),
        "expected Closed for send after close_timeout, got {err:?}"
    );
}

#[tokio::test]
async fn retry_queue_shutdown_boundedly_completes_and_releases_permits() {
    let mock = common::Mock::start().await;
    mock.set_produce_error(error::REQUEST_TIMED_OUT);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .retry_backoff(Duration::from_millis(50))
            .delivery_timeout(Duration::from_secs(10)),
    )
    .await
    .unwrap();

    let send_producer = producer.clone();
    let send_handle = tokio::spawn(async move {
        send_producer
            .send(ProduceRecord::to("t").value(&b"retry-me"[..]))
            .await
    });

    // Wait until record was transmitted and moved to retry queue
    timeout(Duration::from_secs(2), async {
        while mock.produce_request_nodes().is_empty() || producer.retries_in_flight() == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("record must be transmitted and in retry queue");

    let start = std::time::Instant::now();
    let p_close = producer.clone();
    let res = p_close.close_timeout(Duration::from_millis(100)).await;
    let elapsed = start.elapsed();

    assert!(
        matches!(res, Err(Error::Timeout)),
        "expected Timeout on retrying close_timeout, got {res:?}"
    );
    assert!(
        elapsed < Duration::from_millis(800),
        "retry shutdown must terminate boundedly, elapsed: {elapsed:?}"
    );

    let send_res = timeout(Duration::from_secs(1), send_handle)
        .await
        .expect("send future must resolve boundedly")
        .unwrap();
    assert!(
        matches!(send_res, Err(Error::Timeout)),
        "transmitted retrying record must report ambiguous Timeout, got {send_res:?}"
    );

    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "buffer permit must be released exactly once on retry shutdown"
    );
}

#[tokio::test]
async fn last_handle_drop_releases_permits_and_aborts_tasks() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .delivery_timeout(Duration::from_secs(10)),
    )
    .await
    .unwrap();

    // Warm up worker connection
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"warm"[..]))
        .await
        .unwrap();
    let tasks = producer.test_worker_tasks();
    assert!(!tasks.is_empty(), "worker task must be running");

    // Stalled broker for subsequent produce
    mock.set_produce_delay(Duration::from_secs(10));
    producer
        .try_send(ProduceRecord::to("t").value(&b"dropped"[..]))
        .unwrap();
    assert!(producer.metrics().bytes_buffered > 0);

    // Drop the producer handle - this is the last handle
    drop(producer);

    // All worker tasks must abort and complete boundedly (not hanging for 10s)
    let start = std::time::Instant::now();
    for t in tasks {
        timeout(Duration::from_millis(800), async {
            while !t.is_finished() {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("worker task must terminate boundedly after last handle drop");
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(800),
        "last handle drop must abort stalled tasks boundedly, elapsed: {elapsed:?}"
    );
}

#[tokio::test]
async fn concurrent_sends_during_close_do_not_hang_and_observe_closed() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::from_millis(10))
            .delivery_timeout(Duration::from_secs(2)),
    )
    .await
    .unwrap();

    let p_close = producer.clone();
    let mut send_handles = Vec::new();
    for i in 0..10 {
        let p = producer.clone();
        send_handles.push(tokio::spawn(async move {
            let val = format!("val-{i}");
            p.send(ProduceRecord::to("t").value(val.into_bytes())).await
        }));
    }

    let close_handle =
        tokio::spawn(async move { p_close.close_timeout(Duration::from_millis(200)).await });

    let (close_res, _) = tokio::join!(close_handle, async {
        for h in send_handles {
            let res = h.await.expect("task must not panic");
            match res {
                Ok(md) => {
                    assert_eq!(md.topic, "t");
                }
                Err(e) => {
                    assert!(
                        matches!(e, Error::Closed | Error::Timeout),
                        "expected Closed or Timeout, got {e:?}"
                    );
                }
            }
        }
    });
    let _ = close_res.expect("close task must not panic");

    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "all permits must be released after concurrent sends and close"
    );

    // Any send after close must observe Closed
    let err = producer
        .send(ProduceRecord::to("t").value(&b"late"[..]))
        .await
        .expect_err("send after close must fail");
    assert!(
        matches!(err, Error::Closed),
        "expected Closed after shutdown, got {err:?}"
    );
}

#[tokio::test]
async fn durable_closed_flag_across_clones() {
    let mock = common::Mock::start().await;
    let p1 = Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
        .await
        .unwrap();
    let p2 = p1.clone();
    let p3 = p1.clone();

    p1.close().await.unwrap();

    let err2 = p2
        .send(ProduceRecord::to("t").value(&b"p2"[..]))
        .await
        .expect_err("p2 send must fail with Closed");
    assert!(
        matches!(err2, Error::Closed),
        "expected Closed on p2, got {err2:?}"
    );

    let err2_try = p2.try_send(ProduceRecord::to("t").value(&b"p2-try"[..]));
    assert!(
        matches!(err2_try, Err(Error::Closed)),
        "expected Closed on p2 try_send, got {err2_try:?}"
    );

    let err3 = p3
        .send_all([ProduceRecord::to("t").value(&b"p3"[..])])
        .await
        .expect_err("p3 send_all must fail with Closed");
    assert!(
        matches!(err3, Error::Closed),
        "expected Closed on p3, got {err3:?}"
    );

    // Repeated close on clone is idempotent Ok(())
    p3.close().await.unwrap();
}
