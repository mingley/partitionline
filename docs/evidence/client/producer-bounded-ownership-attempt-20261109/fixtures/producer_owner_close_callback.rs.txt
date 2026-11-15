//! Desired close/callback ordering control for the experimental bounded mode.
//! The callback intentionally blocks on a condition variable for a short,
//! bounded interval. The broker is the in-repository mock.

mod common;

use parking_lot::{Condvar, Mutex};
use partitionline::producer::PreSendFault;
use partitionline::{Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct State {
    armed: AtomicBool,
    entered: AtomicBool,
    active: AtomicBool,
    overlapped: AtomicBool,
    close_calls: AtomicUsize,
    timed_out: AtomicBool,
    open: Mutex<bool>,
    changed: Condvar,
}

impl State {
    fn release(&self) {
        *self.open.lock() = true;
        let _notified = self.changed.notify_all();
    }
}

struct Interceptor(Arc<State>);

impl ProducerInterceptor for Interceptor {
    fn on_error(&self, _error: &Error) {
        if !self.0.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        self.0.active.store(true, Ordering::SeqCst);
        self.0.entered.store(true, Ordering::SeqCst);
        let mut open = self.0.open.lock();
        while !*open {
            if self
                .0
                .changed
                .wait_for(&mut open, Duration::from_secs(5))
                .timed_out()
            {
                self.0.timed_out.store(true, Ordering::SeqCst);
                break;
            }
        }
        self.0.active.store(false, Ordering::SeqCst);
    }

    fn close(&self) {
        if self.0.active.load(Ordering::SeqCst) {
            self.0.overlapped.store(true, Ordering::SeqCst);
        }
        let _previous = self.0.close_calls.fetch_add(1, Ordering::SeqCst);
    }
}

struct ReleaseOnDrop(Arc<State>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timed_close_does_not_close_interceptor_during_callback() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let state = Arc::new(State::default());
    let _release = ReleaseOnDrop(Arc::clone(&state));
    let producer = Producer::new_bounded(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(5))
            .delivery_timeout(Duration::from_secs(10))
            .interceptor(Interceptor(Arc::clone(&state))),
        16,
    )
    .await
    .expect("producer");
    let _warm = producer
        .send(ProduceRecord::to("t").partition(0).value(&b"warm"[..]))
        .await
        .expect("warmup");
    state.armed.store(true, Ordering::SeqCst);
    producer.inject_pre_send_fault(PreSendFault::Encode);
    let sender = {
        let producer = producer.clone();
        tokio::spawn(async move {
            producer
                .send(ProduceRecord::to("t").partition(0).value(&b"fail"[..]))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !state.entered.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("error callback entry");
    let closer = {
        let producer = producer.clone();
        tokio::spawn(async move { producer.close_timeout(Duration::from_millis(150)).await })
    };
    tokio::time::sleep(Duration::from_millis(400)).await;
    let close_calls_while_held = state.close_calls.load(Ordering::SeqCst);
    let overlap = state.overlapped.load(Ordering::SeqCst);
    state.release();
    let sender_result = tokio::time::timeout(Duration::from_secs(5), sender)
        .await
        .expect("sender deadline")
        .expect("sender task");
    let close_result = tokio::time::timeout(Duration::from_secs(5), closer)
        .await
        .expect("close deadline")
        .expect("close task");
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.close_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deferred interceptor close");
    println!(
        "CALLBACK_CLOSE overlap={overlap} closes_while_held={close_calls_while_held} sender={sender_result:?} close={close_result:?} eventual_closes={}",
        state.close_calls.load(Ordering::SeqCst)
    );
    assert!(!state.timed_out.load(Ordering::SeqCst));
    assert!(matches!(close_result, Err(Error::Timeout)));
    assert_eq!(state.close_calls.load(Ordering::SeqCst), 1);
    assert!(!overlap, "interceptor close overlapped its error callback");
}
