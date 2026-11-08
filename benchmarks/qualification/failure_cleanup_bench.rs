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
    timed_out: Arc<AtomicBool>,
}

impl crate::interceptor::ProducerInterceptor for DeadlineCallback {
    fn on_error(&self, _: &Error) {
        let _ = self.calls.fetch_add(1, Ordering::SeqCst);
        let (released, changed) = &*self.latch;
        let mut released = released.lock();
        // The release thread must acquire this lock after entry, proving that
        // this callback reached Condvar::wait before its hold interval starts.
        let _sent = self.entered.send(true);
        let deadline = Instant::now() + Duration::from_secs(3);
        while !*released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || changed.wait_for(&mut released, remaining).timed_out() {
                if !*released {
                    self.timed_out.store(true, Ordering::SeqCst);
                    break;
                }
            }
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
    let callback_timed_out = Arc::new(AtomicBool::new(false));
    let latch = Arc::new((parking_lot::Mutex::new(false), parking_lot::Condvar::new()));
    let release_on_drop = ReleaseDeadlineLatch(Arc::clone(&latch));
    let (signal_tx, signal_rx) = std::sync::mpsc::channel();
    let cfg = ProducerConfig::default().interceptor(DeadlineCallback {
        entered: signal_tx.clone(),
        latch: Arc::clone(&latch),
        calls: Arc::clone(&calls),
        timed_out: Arc::clone(&callback_timed_out),
    });
    let shared = test_shared(cfg, meta);
    let (finished_tx, finished_rx) = oneshot::channel();
    let release_thread = std::thread::spawn(move || {
        // Always release even on a timeout or panic before the callback enters.
        let release_at_exit = ReleaseDeadlineLatch(latch);
        let times = if matches!(signal_rx.recv_timeout(Duration::from_secs(3)), Ok(true)) {
            let (released, changed) = &*release_at_exit.0;
            // Acquiring this lock proves that the callback has entered wait.
            let guard = released.lock();
            let waited_at = Instant::now();
            drop(guard);
            // Do not hold the latch lock: the callback's own timeout must work
            // even if this release thread stalls.
            let (_delay_tx, delay_rx) = std::sync::mpsc::channel::<()>();
            let held = matches!(
                delay_rx.recv_timeout(Duration::from_millis(600)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            );
            let mut guard = released.lock();
            let released_at = Instant::now();
            *guard = true;
            let _woken = changed.notify_all();
            drop(guard);
            Some((waited_at, released_at, held))
        } else {
            None
        };
        drop(release_at_exit);
        let _notified = finished_tx.send(times);
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
    let absolute_deadline = Instant::now() + Duration::from_millis(200);
    let (armed_tx, armed_rx) = oneshot::channel();
    let timer = tokio::spawn(async move {
        let deadline = tokio::time::sleep_until(tokio::time::Instant::from_std(absolute_deadline));
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
    tokio::time::timeout(Duration::from_secs(3), armed_rx)
        .await
        .expect("timer did not arm")
        .expect("timer setup task panicked");
    let cleanup = fail_kept_pendings(&shared, vec![pending], Error::Timeout);
    let started = Instant::now();
    drop(cleanup);
    let drop_ended = Instant::now();
    let elapsed = drop_ended.duration_since(started);
    let timer_pending_after_drop = !timer_fired.load(Ordering::SeqCst);
    match expected.as_str() {
        "baseline" => {
            assert!(started < absolute_deadline && drop_ended < absolute_deadline);
            assert!(timer_pending_after_drop);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed)));
            assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 1);
            shared.release_buffer(1);
            let _sent = signal_tx.send(false);
        }
        "candidate" => {
            assert!(started < absolute_deadline);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(elapsed >= Duration::from_millis(400), "{elapsed:?}");
            assert!(timer_pending_after_drop, "timer ran while its runtime thread was blocked");
            assert!(matches!(rx.try_recv(), Ok(Err(Error::Timeout))));
            assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
        }
        _ => panic!("unknown benchmark arm"),
    }
    tokio::time::timeout(Duration::from_secs(3), timer)
        .await
        .expect("timer task did not finish")
        .expect("timer task panicked");
    assert!(timer_fired.load(Ordering::SeqCst));
    let times = tokio::time::timeout(Duration::from_secs(3), finished_rx)
        .await
        .expect("release thread did not finish")
        .expect("release thread panicked");
    if expected == "candidate" {
        let (waited_at, released_at, held) = times.expect("callback wait was not observed");
        assert!(held && !callback_timed_out.load(Ordering::SeqCst));
        assert!(
            started <= waited_at
                && waited_at < absolute_deadline
                && absolute_deadline < released_at
                && released_at <= drop_ended
        );
        println!(
            "PL_DEADLINE_INTERVAL wait_us={} deadline_us={} release_us={} drop_end_us={}",
            waited_at.duration_since(started).as_micros(),
            absolute_deadline.duration_since(started).as_micros(),
            released_at.duration_since(started).as_micros(),
            drop_ended.duration_since(started).as_micros()
        );
    } else {
        assert!(times.is_none());
        assert!(!callback_timed_out.load(Ordering::SeqCst));
    }
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
        "PL_DEADLINE arm={expected} elapsed_ms={} timer_pending_after_drop={timer_pending_after_drop} callback_wait_confirmed={} callback_calls={}",
        elapsed.as_millis(),
        expected == "candidate",
        calls.load(Ordering::SeqCst)
    );
}
