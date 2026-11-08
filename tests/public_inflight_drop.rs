//! Controlled public close deadline and in-flight callback witness.
#![cfg(inflight_drop_witness)]

mod common;

use partitionline::producer::inflight_drop_witness as marker;
use partitionline::{
    Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata,
};
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct Callbacks {
    acks: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
    entered: std::sync::mpsc::Sender<()>,
    release: Arc<parking_lot::Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
}
impl ProducerInterceptor for Callbacks {
    fn on_ack(&self, _metadata: &RecordMetadata) {
        let _previous = self.acks.fetch_add(1, Ordering::SeqCst);
    }
    fn on_error(&self, _error: &Error) {
        let _previous = self.errors.fetch_add(1, Ordering::SeqCst);
        let _sent = self.entered.send(());
        let release = self.release.lock().take();
        if let Some(release) = release {
            let _released = release.recv();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn public_close_deadline_with_inflight_interceptor() {
    let held = std::env::var("PL_HOLD_CALLBACK").as_deref() == Ok("1");
    let mock = common::Mock::start().await;
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let callbacks = Callbacks {
        acks: Arc::new(AtomicUsize::new(0)),
        errors: Arc::new(AtomicUsize::new(0)),
        entered: entered_tx,
        release: Arc::new(parking_lot::Mutex::new(Some(release_rx))),
    };
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(false)
            .connections(1)
            .linger(Duration::ZERO)
            .delivery_timeout(Duration::from_secs(30))
            .request_timeout(Duration::from_secs(10))
            .interceptor(callbacks.clone()),
    )
    .await
    .expect("producer setup");
    let warm = producer
        .send(ProduceRecord::to("t").partition(0).value(&b"warm"[..]))
        .await
        .expect("warmup");
    assert_eq!(warm.offset, 0);
    assert_eq!(callbacks.acks.load(Ordering::SeqCst), 1);

    mock.set_produce_response_delay(Duration::from_secs(10));
    marker::arm();
    let mut target =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"target"[..])));
    assert!(poll_fn(|cx| Poll::Ready(target.as_mut().poll(cx))).await.is_pending());
    tokio::time::timeout(Duration::from_secs(10), marker::reached())
        .await
        .expect("worker did not hold in-flight response guard");
    tokio::time::timeout(Duration::from_secs(10), async {
        while mock.log_len("t", 0) < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("mock did not append target before withholding response");
    assert_eq!(producer.metrics().bytes_buffered, 6);
    assert_eq!(callbacks.errors.load(Ordering::SeqCst), 0);

    let ticked = Arc::new(AtomicBool::new(false));
    let ticker_flag = Arc::clone(&ticked);
    let ticker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        ticker_flag.store(true, Ordering::SeqCst);
    });
    tokio::task::yield_now().await;
    let observer_flag = Arc::clone(&ticked);
    let watchdog = std::thread::spawn(move || {
        if entered_rx.recv_timeout(Duration::from_secs(10)).is_err() {
            return None;
        }
        if held {
            std::thread::sleep(Duration::from_millis(600));
        }
        let before_release = observer_flag.load(Ordering::SeqCst);
        let _sent = release_tx.send(());
        Some(before_release)
    });
    let start = Instant::now();
    let close = producer.clone().close_timeout(Duration::from_millis(150)).await;
    let elapsed = start.elapsed();
    assert!(matches!(close, Err(Error::Timeout)), "close result: {close:?}");
    let tick_before_release = watchdog.join().expect("watchdog thread").expect("callback entered");
    if held {
        assert!(!tick_before_release, "timer ran while current-thread callback was held");
        assert!(elapsed >= Duration::from_millis(600), "close returned before release: {elapsed:?}");
    }
    let delivery = tokio::time::timeout(Duration::from_secs(5), target.as_mut())
        .await
        .expect("target receipt deadline");
    assert!(matches!(delivery, Err(Error::Timeout)), "target result: {delivery:?}");
    assert_eq!(callbacks.acks.load(Ordering::SeqCst), 1);
    assert_eq!(callbacks.errors.load(Ordering::SeqCst), 1);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(
        mock.log_values("t", 0),
        vec![(0, Some(b"warm".to_vec())), (1, Some(b"target".to_vec()))]
    );
    ticker.abort();
    println!(
        "INFLIGHT_DROP_WITNESS held={held} close_ms={} tick_before_release={tick_before_release} receipt=timeout callbacks=1 bytes=0 target_appended=1",
        elapsed.as_millis()
    );
}
