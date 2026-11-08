use super::*;

#[tokio::test(flavor = "current_thread")]
async fn completed_failure_batch() {
    const RECORDS: usize = 16_385;
    const WARMUPS: usize = 2;
    const SAMPLES: usize = 8;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&addr, "failure-cleanup-bench", Duration::from_secs(1))
        .await
        .unwrap();
    let (_peer, _) = listener.accept().await.unwrap();
    let shared = test_shared(ProducerConfig::default(), meta);
    let topic: Arc<str> = Arc::from("failure-benchmark");
    for sample in 0..(WARMUPS + SAMPLES) {
        let mut batch = Vec::with_capacity(RECORDS);
        let mut receivers = Vec::with_capacity(RECORDS);
        for _ in 0..RECORDS {
            let now = Instant::now();
            let mut rec = ProduceRecord::to("failure-benchmark").partition(0);
            rec.topic = Arc::clone(&topic);
            rec.value = Some(Bytes::from_static(b"x"));
            let (tx, rx) = oneshot::channel();
            batch.push(Pending {
                sticky_record: None,
                sticky: None,
                rec,
                tx: Some(tx),
                seq: None,
                batch_base_seq: None,
                batch_len: 1,
                epoch_gen: 0,
                deadline: now,
                queued_at: now,
                retry: 0,
                skip_meta_refresh: false,
                retry_after: now,
            });
            receivers.push(rx);
        }
        let allocation = batch.as_ptr();
        assert!(shared.try_reserve_buffer(RECORDS as u64));
        let started = Instant::now();
        let empty = fail_kept_pendings(&shared, batch, Error::Timeout).await;
        let elapsed = started.elapsed();
        assert!(empty.is_empty());
        assert!(empty.capacity() >= RECORDS);
        assert_eq!(empty.as_ptr(), allocation);
        assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(
            shared.m_errors.load(Ordering::SeqCst),
            ((sample + 1) * RECORDS) as u64
        );
        for receiver in receivers {
            assert!(matches!(receiver.await, Ok(Err(Error::Timeout))));
        }
        if sample >= WARMUPS {
            println!(
                "PL_FAILURE_BENCH sample={} records={} elapsed_ns={}",
                sample - WARMUPS,
                RECORDS,
                elapsed.as_nanos()
            );
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unpolled_fingerprint() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&addr, "failure-fingerprint", Duration::from_secs(1))
        .await
        .unwrap();
    let (_peer, _) = listener.accept().await.unwrap();
    let shared = test_shared(ProducerConfig::default(), meta);
    let now = Instant::now();
    let mut rec = ProduceRecord::to("failure-fingerprint").partition(0);
    rec.value = Some(Bytes::from_static(b"x"));
    let (tx, mut rx) = oneshot::channel();
    let pending = Pending {
        sticky_record: None,
        sticky: None,
        rec,
        tx: Some(tx),
        seq: None,
        batch_base_seq: None,
        batch_len: 1,
        epoch_gen: 0,
        deadline: now,
        queued_at: now,
        retry: 0,
        skip_meta_refresh: false,
        retry_after: now,
    };
    assert!(shared.try_reserve_buffer(1));
    drop(fail_kept_pendings(&shared, vec![pending], Error::Timeout));
    let expected = std::env::var("FINGERPRINT_EXPECTED").unwrap();
    match expected.as_str() {
        "baseline" => {
            assert!(matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed)));
            assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 1);
            shared.release_buffer(1);
        }
        "candidate" => {
            assert!(matches!(rx.try_recv(), Ok(Err(Error::Timeout))));
            assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
        }
        _ => panic!("unknown benchmark arm"),
    }
    println!("PL_FINGERPRINT arm={expected} verified=true");
}

#[tokio::test(flavor = "current_thread")]
async fn scheduling_fingerprint() {
    const RECORDS: usize = 16_385;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&addr, "failure-scheduling-fingerprint", Duration::from_secs(1))
        .await
        .unwrap();
    let (_peer, _) = listener.accept().await.unwrap();
    let shared = test_shared(ProducerConfig::default(), meta);
    let topic: Arc<str> = Arc::from("failure-scheduling-fingerprint");
    let mut batch = Vec::with_capacity(RECORDS);
    let mut receivers = Vec::with_capacity(RECORDS);
    for _ in 0..RECORDS {
        let now = Instant::now();
        let mut rec = ProduceRecord::to("failure-scheduling-fingerprint").partition(0);
        rec.topic = Arc::clone(&topic);
        rec.value = Some(Bytes::from_static(b"x"));
        let (tx, rx) = oneshot::channel();
        batch.push(Pending {
            sticky_record: None,
            sticky: None,
            rec,
            tx: Some(tx),
            seq: None,
            batch_base_seq: None,
            batch_len: 1,
            epoch_gen: 0,
            deadline: now,
            queued_at: now,
            retry: 0,
            skip_meta_refresh: false,
            retry_after: now,
        });
        receivers.push(rx);
    }
    assert!(shared.try_reserve_buffer(RECORDS as u64));
    let mut cleanup = Box::pin(fail_kept_pendings(&shared, batch, Error::Timeout));
    let mut observed = vec![false; RECORDS];
    let mut completed = 0;
    let expected = [0, 0, 0, 0, 0, 0, 1, 8_193, RECORDS];
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    for expected_completed in expected {
        assert!(matches!(
            cleanup.as_mut().poll(&mut context),
            std::task::Poll::Pending
        ));
        for (index, receiver) in receivers.iter_mut().enumerate() {
            if observed[index] {
                continue;
            }
            match receiver.try_recv() {
                Ok(Err(Error::Timeout)) => {
                    observed[index] = true;
                    completed += 1;
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
                other => panic!("unexpected outcome: {other:?}"),
            }
        }
        assert_eq!(completed, expected_completed);
    }
    let std::task::Poll::Ready(empty) = cleanup.as_mut().poll(&mut context) else {
        panic!("cleanup did not complete after the expected yields");
    };
    assert!(empty.is_empty());
    assert!(observed.into_iter().all(|value| value));
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(shared.m_errors.load(Ordering::SeqCst), RECORDS as u64);
    println!("PL_SCHEDULING_FINGERPRINT pending=0,0,0,0,0,0,1,8193,16385 ready=10 verified=true");
}

struct DeadlineCallback {
    entered: std::sync::mpsc::Sender<bool>,
    latch: Arc<(parking_lot::Mutex<bool>, parking_lot::Condvar)>,
    calls: Arc<AtomicUsize>,
}

impl crate::interceptor::ProducerInterceptor for DeadlineCallback {
    fn on_error(&self, _: &Error) {
        let _ = self.calls.fetch_add(1, Ordering::SeqCst);
        let _sent = self.entered.send(true);
        let (released, changed) = &*self.latch;
        let mut released = released.lock();
        while !*released {
            changed.wait(&mut released);
        }
    }
}

struct ReleaseDeadlineLatch(Arc<(parking_lot::Mutex<bool>, parking_lot::Condvar)>);
impl Drop for ReleaseDeadlineLatch {
    fn drop(&mut self) {
        let (released, changed) = &*self.0;
        *released.lock() = true;
        let _woken = changed.notify_all();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn current_thread_cancel_deadline_probe() {
    let expected = std::env::var("FINGERPRINT_EXPECTED").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&addr, "failure-deadline-probe", Duration::from_secs(1))
        .await
        .unwrap();
    let (_peer, _) = listener.accept().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let latch = Arc::new((parking_lot::Mutex::new(false), parking_lot::Condvar::new()));
    let release_on_drop = ReleaseDeadlineLatch(Arc::clone(&latch));
    let (signal_tx, signal_rx) = std::sync::mpsc::channel();
    let cfg = ProducerConfig::default().interceptor(DeadlineCallback {
        entered: signal_tx.clone(),
        latch: Arc::clone(&latch),
        calls: Arc::clone(&calls),
    });
    let shared = test_shared(cfg, meta);
    let (finished_tx, finished_rx) = oneshot::channel();
    let release_thread = std::thread::spawn(move || {
        // Always release even on a timeout or panic before the callback enters.
        let release_at_exit = ReleaseDeadlineLatch(latch);
        let held = if matches!(signal_rx.recv_timeout(Duration::from_secs(3)), Ok(true)) {
            let (_delay_tx, delay_rx) = std::sync::mpsc::channel::<()>();
            matches!(
                delay_rx.recv_timeout(Duration::from_millis(600)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            )
        } else {
            false
        };
        drop(release_at_exit);
        let _notified = finished_tx.send(held);
    });
    let now = Instant::now();
    let mut rec = ProduceRecord::to("failure-deadline-probe").partition(0);
    rec.value = Some(Bytes::from_static(b"x"));
    let (tx, mut rx) = oneshot::channel();
    let pending = Pending {
        sticky_record: None,
        sticky: None,
        rec,
        tx: Some(tx),
        seq: None,
        batch_base_seq: None,
        batch_len: 1,
        epoch_gen: 0,
        deadline: now,
        queued_at: now,
        retry: 0,
        skip_meta_refresh: false,
        retry_after: now,
    };
    assert!(shared.try_reserve_buffer(1));
    let timer_fired = Arc::new(AtomicBool::new(false));
    let timer_flag = Arc::clone(&timer_fired);
    let (armed_tx, armed_rx) = oneshot::channel();
    let timer = tokio::spawn(async move {
        let deadline = tokio::time::sleep(Duration::from_millis(200));
        tokio::pin!(deadline);
        let mut armed = Some(armed_tx);
        std::future::poll_fn(|context| {
            let state = deadline.as_mut().poll(context);
            if let Some(tx) = armed.take() {
                assert!(state.is_pending(), "deadline elapsed before registration");
                let _armed = tx.send(());
            }
            state
        })
        .await;
        timer_flag.store(true, Ordering::SeqCst);
    });
    armed_rx.await.unwrap();
    let cleanup = fail_kept_pendings(&shared, vec![pending], Error::Timeout);
    let started = Instant::now();
    drop(cleanup);
    let elapsed = started.elapsed();
    let timer_pending_after_drop = !timer_fired.load(Ordering::SeqCst);
    match expected.as_str() {
        "baseline" => {
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed)));
            assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 1);
            shared.release_buffer(1);
            let _sent = signal_tx.send(false);
        }
        "candidate" => {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(elapsed >= Duration::from_millis(400), "{elapsed:?}");
            assert!(timer_pending_after_drop, "timer ran while its runtime thread was blocked");
            assert!(matches!(rx.try_recv(), Ok(Err(Error::Timeout))));
            assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
        }
        _ => panic!("unknown benchmark arm"),
    }
    timer.await.unwrap();
    assert!(timer_fired.load(Ordering::SeqCst));
    let held = tokio::time::timeout(Duration::from_secs(3), finished_rx)
        .await
        .expect("release thread did not finish")
        .expect("release thread panicked");
    assert_eq!(held, expected == "candidate");
    tokio::time::timeout(Duration::from_secs(3), async {
        while !release_thread.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("release thread did not exit");
    release_thread.join().expect("release thread panicked");
    drop(release_on_drop);
    println!(
        "PL_DEADLINE arm={expected} elapsed_ms={} timer_pending_after_drop={timer_pending_after_drop} callback_calls={}",
        elapsed.as_millis(),
        calls.load(Ordering::SeqCst)
    );
}
