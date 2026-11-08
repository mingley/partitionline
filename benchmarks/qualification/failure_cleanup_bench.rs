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
