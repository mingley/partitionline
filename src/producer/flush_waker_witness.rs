//! Test-only witness for an unwind during the producer's channel handoff.
use super::*;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::task::{Context, Wake, Waker};

struct CountWake {
    called: Arc<AtomicBool>,
}

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.called.store(true, Ordering::SeqCst);
    }
}

struct PanicWake {
    admission: Arc<WorkerAdmission>,
    called: Arc<AtomicBool>,
    locked: Arc<AtomicBool>,
}

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.called.store(true, Ordering::SeqCst);
        self.locked.store(
            self.admission.commit.try_lock().is_none(),
            Ordering::SeqCst,
        );
        panic!("injected receiver-waker panic");
    }
}

async fn fixture() -> (Arc<Shared>, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&address, "flush-waker-test", Duration::from_secs(2))
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    (test_shared(ProducerConfig::default(), meta), peer)
}

fn pending() -> Pending {
    let now = Instant::now();
    Pending {
        sticky_record: None,
        sticky: None,
        rec: ProduceRecord::to("flush-waker").partition(0).value(Bytes::from_static(b"x")),
        tx: None,
        seq: None,
        batch_base_seq: None,
        batch_len: 1,
        epoch_gen: 0,
        deadline: now + Duration::from_secs(5),
        queued_at: now,
        retry: 0,
        skip_meta_refresh: false,
        retry_after: now,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn normal_handoff_keeps_one_reservation_until_terminal() {
    let (shared, _peer) = fixture().await;
    assert!(shared.try_reserve_buffer(1));
    let admission = WorkerAdmission::new();
    let (data_tx, mut data_rx) = mpsc::channel(1);
    let permit = WorkerPermit {
        admission: &admission,
        permit: data_tx.reserve().await.unwrap(),
    };
    let called = Arc::new(AtomicBool::new(false));
    let waker = Waker::from(Arc::new(CountWake {
        called: Arc::clone(&called),
    }));
    let mut cx = Context::from_waker(&waker);
    let mut recv = Box::pin(data_rx.recv());
    assert!(recv.as_mut().poll(&mut cx).is_pending());
    {
        let mut reservation = BufferReservation {
            shared: &shared,
            bytes: 1,
            active: true,
        };
        assert!(permit.send(pending()).is_ok());
        reservation.active = false;
    }
    assert!(called.load(Ordering::SeqCst));
    drop(recv);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 1);
    fail_pendings(&shared, vec![data_rx.try_recv().unwrap()], Error::Timeout);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    println!("NORMAL_HANDOFF_RESERVED_THEN_RELEASED_ONCE");
}

#[tokio::test(flavor = "current_thread")]
async fn injected_panicking_receiver_waker_enqueues_before_reservation_unwinds() {
    let (shared, _peer) = fixture().await;
    assert!(shared.try_reserve_buffer(1));
    let admission = Arc::new(WorkerAdmission::new());
    let (data_tx, mut data_rx) = mpsc::channel(1);
    let permit = WorkerPermit {
        admission: &admission,
        permit: data_tx.reserve().await.unwrap(),
    };
    let called = Arc::new(AtomicBool::new(false));
    let locked = Arc::new(AtomicBool::new(false));
    let waker = Waker::from(Arc::new(PanicWake {
        admission: Arc::clone(&admission),
        called: Arc::clone(&called),
        locked: Arc::clone(&locked),
    }));
    let mut cx = Context::from_waker(&waker);
    let mut recv = Box::pin(data_rx.recv());
    assert!(recv.as_mut().poll(&mut cx).is_pending());

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let mut reservation = BufferReservation {
            shared: &shared,
            bytes: 1,
            active: true,
        };
        assert!(permit.send(pending()).is_ok());
        reservation.active = false;
    }));
    assert!(panic.is_err());
    assert!(called.load(Ordering::SeqCst));
    assert!(locked.load(Ordering::SeqCst));
    drop(recv);

    let queued = data_rx.try_recv().expect("record was pushed before wake panicked");
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    fail_pendings(&shared, vec![queued], Error::Timeout);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), u64::MAX);
    shared.buffered_bytes.store(0, Ordering::SeqCst);
    println!("PANICKING_WAKE_QUEUED_WHILE_COMMIT_LOCKED_AND_DOUBLE_RELEASED");
}
