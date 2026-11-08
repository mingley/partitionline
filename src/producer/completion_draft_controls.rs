//! Narrow controls for the experimental completion token.
//!
//! These controls do not exercise broker I/O, callback cleanup, transactions,
//! or shutdown. The associated production draft is not a completed repair.

use super::*;

fn pending(domain: &Arc<CompletionDomain>) -> Pending {
    let now = Instant::now();
    Pending {
        completion: None,
        completion_reservation: Some(domain.try_reserve().unwrap().unwrap()),
        sticky_record: None,
        sticky: None,
        rec: ProduceRecord::to("t").partition(0).value("value"),
        tx: None,
        seq: None,
        batch_base_seq: None,
        batch_len: 1,
        epoch_gen: 0,
        deadline: now + Duration::from_secs(30),
        queued_at: now,
        retry: 0,
        skip_meta_refresh: false,
        retry_after: now,
    }
}

fn worker() -> (WorkerHandle, mpsc::Receiver<Pending>) {
    let (data, receiver) = mpsc::channel(1);
    let (ctrl, _control_receiver) = mpsc::channel(1);
    let admission = Arc::new(WorkerAdmission::new());
    let task = Arc::new(tokio::spawn(std::future::pending::<()>()));
    (
        WorkerHandle::new(Arc::new(()), admission, data, ctrl, task),
        receiver,
    )
}

#[tokio::test]
async fn worker_admission_holds_completion_until_terminal_publication() {
    let domain = CompletionDomain::new();
    let before = domain.cutoff();
    let (worker, mut receiver) = worker();
    assert!(worker.try_send(pending(&domain)).is_ok());
    let after = domain.cutoff();
    assert_eq!(domain.status(before), CompletionStatus::Complete);
    assert_eq!(domain.status(after), CompletionStatus::Pending);
    let pending = receiver.recv().await.unwrap();
    pending.complete_completion();
    assert_eq!(domain.status(after), CompletionStatus::Complete);
}

#[tokio::test]
async fn retry_handoff_keeps_the_original_completion_ticket() {
    let domain = CompletionDomain::new();
    let (first, mut first_rx) = worker();
    let (second, mut second_rx) = worker();
    assert!(first.try_send(pending(&domain)).is_ok());
    let cutoff = domain.cutoff();
    let pending = first_rx.recv().await.unwrap();
    first.admission.close();
    assert!(second.try_send(pending).is_ok());
    assert_eq!(domain.cutoff(), cutoff);
    assert_eq!(domain.status(cutoff), CompletionStatus::Pending);
    let retried = second_rx.recv().await.unwrap();
    retried.complete_completion();
    assert_eq!(domain.status(cutoff), CompletionStatus::Complete);
}

#[tokio::test]
async fn dropped_worker_owned_record_marks_completion_lost() {
    let domain = CompletionDomain::new();
    let (worker, mut receiver) = worker();
    assert!(worker.try_send(pending(&domain)).is_ok());
    let cutoff = domain.cutoff();
    drop(receiver.recv().await.unwrap());
    assert_eq!(domain.status(cutoff), CompletionStatus::Lost);
}


#[tokio::test(flavor = "current_thread")]
async fn ticket_waits_until_sticky_permit_drop_and_its_wake_return() {
    use std::sync::{mpsc as std_mpsc, Mutex as StdMutex};
    use std::task::{Context, Wake, Waker};

    struct BlockingWake {
        entered: std_mpsc::SyncSender<()>,
        release: StdMutex<std_mpsc::Receiver<()>>,
    }

    impl Wake for BlockingWake {
        fn wake(self: Arc<Self>) {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
    }

    let domain = CompletionDomain::new();
    let (worker, mut receiver) = worker();
    assert!(worker.try_send(pending(&domain)).is_ok());
    let cutoff = domain.cutoff();
    let mut pending = receiver.recv().await.unwrap();

    let budget = Arc::new(StickyRecordBudget::new(1));
    pending.sticky_record = Some(budget.acquire().unwrap());
    let (entered_tx, entered_rx) = std_mpsc::sync_channel(1);
    let (release_tx, release_rx) = std_mpsc::channel();
    let waker = Waker::from(Arc::new(BlockingWake {
        entered: entered_tx,
        release: StdMutex::new(release_rx),
    }));
    let mut notified = Box::pin(budget.nudge.notified());
    let mut context = Context::from_waker(&waker);
    assert!(notified.as_mut().poll(&mut context).is_pending());

    let owner = std::thread::spawn(move || pending.complete_completion());
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(domain.status(cutoff), CompletionStatus::Pending);
    release_tx.send(()).unwrap();
    owner.join().unwrap();
    assert_eq!(domain.status(cutoff), CompletionStatus::Complete);
}

#[tokio::test]
async fn lost_ticket_blocks_flush_abort_drain_and_successful_close() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&addr, "completion-draft", Duration::from_secs(2))
        .await
        .unwrap();
    let (_peer, _) = listener.accept().await.unwrap();
    let shared = test_shared(ProducerConfig::default(), meta);
    let mut reservation = shared.completion.try_reserve().unwrap().unwrap();
    let ticket = reservation.commit().unwrap();
    drop(ticket);
    let producer = Producer {
        inner: Arc::new(Inner { shared }),
    };

    assert!(matches!(
        producer.flush_timeout(Duration::from_secs(2)).await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        producer.drain_before_abort().await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        producer.close_timeout(Duration::from_secs(2)).await,
        Err(Error::Closed)
    ));
}


#[tokio::test]
async fn sequence_exhaustion_is_not_a_worker_retirement() {
    let domain = CompletionDomain::new();
    let pending = pending(&domain);
    domain.exhaust_sequence_for_test();
    let (worker, mut receiver) = worker();
    let result = worker.try_send(pending);
    assert!(matches!(
        result.as_ref(),
        Err(mpsc::error::TrySendError::Closed(returned)) if returned.completion_unavailable()
    ));
    assert!(worker.admission.is_accepting());
    assert!(receiver.try_recv().is_err());
}


#[tokio::test]
async fn close_seals_admission_before_waiting_for_accepted_ticket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&addr, "completion-close", Duration::from_secs(2))
        .await
        .unwrap();
    let (_peer, _) = listener.accept().await.unwrap();
    let shared = test_shared(ProducerConfig::default(), meta);
    let mut reservation = shared.completion.try_reserve().unwrap().unwrap();
    let ticket = reservation.commit().unwrap();
    let producer = Producer {
        inner: Arc::new(Inner {
            shared: Arc::clone(&shared),
        }),
    };
    let other = producer.clone();
    let closing = tokio::spawn(producer.close_timeout(Duration::from_secs(2)));
    tokio::time::timeout(Duration::from_secs(1), async {
        while !shared.completion.is_sealed() {
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    assert!(matches!(
        other.try_send(ProduceRecord::to("t").value("later")),
        Err(Error::Closed)
    ));
    assert!(shared.completion.try_reserve().is_err());
    ticket.complete();
    assert!(closing.await.unwrap().is_ok());
}

#[tokio::test]
async fn abort_drain_does_not_ignore_worker_error_with_pending_ticket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&addr, "completion-abort", Duration::from_secs(2))
        .await
        .unwrap();
    let (_peer, _) = listener.accept().await.unwrap();
    let shared = test_shared(ProducerConfig::default(), meta);
    let mut reservation = shared.completion.try_reserve().unwrap().unwrap();
    let ticket = reservation.commit().unwrap();
    let (data, _data_rx) = mpsc::channel(1);
    let (control, mut control_rx) = mpsc::channel(1);
    let handle = WorkerHandle::new(
        Arc::new(()),
        Arc::new(WorkerAdmission::new()),
        data,
        control,
        Arc::new(tokio::spawn(std::future::pending::<()>())),
    );
    let _previous = shared.nodes.lock().insert(1, vec![Some(handle)]);
    let response = tokio::spawn(async move {
        if let Some(Ctrl::Flush(reply)) = control_rx.recv().await {
            drop(reply.send(Err(Error::protocol("injected worker failure"))));
        }
    });
    let producer = Producer {
        inner: Arc::new(Inner {
            shared: Arc::clone(&shared),
        }),
    };
    assert!(producer.drain_before_abort().await.is_err());
    response.await.unwrap();
    drop(ticket);
}
