use super::*;
use std::sync::mpsc;
use std::thread;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    AbortBeforeCause,
    AbortClaimed,
    AbortEntry,
    ClearBeforeCause,
    FatalEntry,
    ClearEntry,
    ReaderBeforeCause,
    TicketBeforeClaim,
    TicketAfterClaim,
}

struct Hook {
    target: usize,
    pause: Point,
    pause_used: AtomicBool,
    watch: Point,
    paused: mpsc::Sender<()>,
    attempted: mpsc::Sender<()>,
    release: parking_lot::Mutex<mpsc::Receiver<()>>,
}

static ACTIVE: parking_lot::Mutex<Option<Arc<Hook>>> = parking_lot::Mutex::new(None);
static SERIAL: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

fn active(shared: &Shared) -> Option<Arc<Hook>> {
    let target = std::ptr::from_ref(shared) as usize;
    ACTIVE
        .lock()
        .as_ref()
        .filter(|hook| hook.target == target)
        .cloned()
}

pub(super) fn mark(shared: &Shared, point: Point) {
    if let Some(hook) = active(shared).filter(|hook| hook.watch == point) {
        hook.attempted.send(()).unwrap();
    }
}

pub(super) fn pause(shared: &Shared, point: Point) {
    if let Some(hook) = active(shared)
        .filter(|hook| hook.pause == point && !hook.pause_used.swap(true, Ordering::SeqCst))
    {
        hook.paused.send(()).unwrap();
        hook.release
            .lock()
            .recv_timeout(Duration::from_secs(15))
            .unwrap();
    }
}

async fn fixture() -> (Arc<Shared>, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&address, "txn-state", Duration::from_secs(2))
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let shared = test_shared(
        ProducerConfig::default().transactional_id("txn-state"),
        meta,
    );
    (shared, peer)
}

#[tokio::test(flavor = "current_thread")]
async fn fatal_response_supersedes_an_abortable_cause() {
    use crate::protocol::txn::{
        decode_add_partitions_to_txn_response, encode_add_partitions_to_txn_topics_response,
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    };

    let (shared, _peer) = fixture().await;
    shared.set_txn_abortable(Error::broker(
        error::TOPIC_AUTHORIZATION_FAILED,
        "previous abortable error",
    ));
    assert_eq!(
        shared.fail_on_txn_error().unwrap_err().broker_code(),
        Some(error::TOPIC_AUTHORIZATION_FAILED)
    );
    assert!(shared.fail_on_txn_fatal().is_ok());

    let mut wire = BytesMut::new();
    let result = AddPartitionsToTxnTopicResult {
        topic: "a".to_owned(),
        partitions: vec![AddPartitionsToTxnPartitionResult::error(
            0,
            error::PRODUCER_FENCED,
        )],
    };
    encode_add_partitions_to_txn_topics_response(&mut wire, 0, &[result]).unwrap();
    let mut input = wire.as_ref();
    let code = decode_add_partitions_to_txn_response(&mut input, 0).unwrap();
    let disposition = classify_add_partitions_error(code, true);
    assert_eq!(disposition, TxnErrorDisposition::Fatal);
    shared.apply_txn_disposition(disposition, Error::broker(code, "AddPartitionsToTxn"));
    let cause = shared.fail_on_txn_fatal().unwrap_err().broker_code();
    println!("PL_TXN_FATAL_CAUSE observed={cause:?}");
    assert_eq!(cause, Some(error::PRODUCER_FENCED), "PL_TXN_FATAL_CAUSE");

    shared.set_txn_fatal(Error::broker(error::INVALID_TXN_STATE, "later fatal error"));
    shared.clear_txn_abortable();
    assert_eq!(
        shared.fail_on_txn_fatal().unwrap_err().broker_code(),
        Some(error::PRODUCER_FENCED)
    );
}

#[derive(Clone, Copy)]
enum Race {
    AbortThenFatal,
    AbortThenClear,
    ClearThenFatal,
}

async fn run_race(race: Race) {
    let (shared, _peer) = fixture().await;
    if matches!(race, Race::ClearThenFatal) {
        shared.set_txn_abortable(Error::broker(
            error::TOPIC_AUTHORIZATION_FAILED,
            "previous abortable error",
        ));
    }
    let _serial = SERIAL.lock();
    let (pause, watch, marker) = match race {
        Race::AbortThenFatal => (
            Point::AbortBeforeCause,
            Point::FatalEntry,
            "PL_TXN_ABORT_FATAL_RACE",
        ),
        Race::AbortThenClear => (
            Point::AbortBeforeCause,
            Point::ClearEntry,
            "PL_TXN_ABORT_CLEAR_RACE",
        ),
        Race::ClearThenFatal => (
            Point::ClearBeforeCause,
            Point::FatalEntry,
            "PL_TXN_CLEAR_FATAL_RACE",
        ),
    };
    let (paused_tx, paused_rx) = mpsc::channel();
    let (attempted_tx, attempted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let hook = Arc::new(Hook {
        target: Arc::as_ptr(&shared) as usize,
        pause,
        pause_used: AtomicBool::new(false),
        watch,
        paused: paused_tx,
        attempted: attempted_tx,
        release: parking_lot::Mutex::new(release_rx),
    });
    {
        let mut active = ACTIVE.lock();
        assert!(active.is_none());
        *active = Some(hook);
    }
    let first_shared = Arc::clone(&shared);
    let first = thread::spawn(move || match race {
        Race::AbortThenFatal | Race::AbortThenClear => first_shared.set_txn_abortable(
            Error::broker(error::TOPIC_AUTHORIZATION_FAILED, "abortable error"),
        ),
        Race::ClearThenFatal => first_shared.clear_txn_abortable(),
    });
    paused_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let second_shared = Arc::clone(&shared);
    let (done_tx, done_rx) = mpsc::channel();
    let second = thread::spawn(move || {
        match race {
            Race::AbortThenFatal | Race::ClearThenFatal => {
                second_shared.set_txn_fatal(Error::broker(error::PRODUCER_FENCED, "fatal error"))
            }
            Race::AbortThenClear => second_shared.clear_txn_abortable(),
        }
        done_tx.send(()).unwrap();
    });
    attempted_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let completed_early = done_rx.recv_timeout(Duration::from_secs(2)).is_ok();
    release_tx.send(()).unwrap();
    first.join().unwrap();
    second.join().unwrap();
    *ACTIVE.lock() = None;

    let (fatal, abortable) = shared.txn_error_test_snapshot();
    let cause = shared
        .txn_error
        .lock()
        .as_ref()
        .and_then(Error::broker_code);
    println!(
        "{marker} early={completed_early} fatal={fatal} abortable={abortable} cause={cause:?}"
    );
    assert!(
        !completed_early,
        "{marker}: transition escaped the state lock"
    );
    match race {
        Race::AbortThenFatal | Race::ClearThenFatal => {
            assert!(fatal);
            assert_eq!(cause, Some(error::PRODUCER_FENCED));
            assert_eq!(
                shared.fail_on_txn_fatal().unwrap_err().broker_code(),
                Some(error::PRODUCER_FENCED)
            );
        }
        Race::AbortThenClear => {
            assert!(!fatal);
            assert!(!abortable);
            assert!(cause.is_none());
            assert!(shared.fail_on_txn_error().is_ok());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn abortable_publication_cannot_overtake_fatal() {
    run_race(Race::AbortThenFatal).await;
}

#[tokio::test(flavor = "current_thread")]
async fn abortable_publication_cannot_overtake_clear() {
    run_race(Race::AbortThenClear).await;
}

#[tokio::test(flavor = "current_thread")]
async fn clear_cannot_erase_a_fatal_cause() {
    run_race(Race::ClearThenFatal).await;
}

fn watch_fatal(shared: &Arc<Shared>) -> mpsc::Receiver<()> {
    let (paused_tx, _paused_rx) = mpsc::channel();
    let (attempted_tx, attempted_rx) = mpsc::channel();
    let (_release_tx, release_rx) = mpsc::channel();
    let hook = Arc::new(Hook {
        target: Arc::as_ptr(shared) as usize,
        pause: Point::AbortBeforeCause,
        pause_used: AtomicBool::new(false),
        watch: Point::FatalEntry,
        paused: paused_tx,
        attempted: attempted_tx,
        release: parking_lot::Mutex::new(release_rx),
    });
    let mut active = ACTIVE.lock();
    assert!(active.is_none());
    *active = Some(hook);
    attempted_rx
}

fn wait_for_fatal_publication(shared: &Shared) -> bool {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !shared.txn_error_test_snapshot().0 && Instant::now() < deadline {
        thread::yield_now();
    }
    shared.txn_error_test_snapshot().0
}

#[tokio::test(flavor = "current_thread")]
async fn fatal_publishes_before_waiting_for_cause_lock() {
    let (shared, _peer) = fixture().await;
    let _serial = SERIAL.lock();
    let entered = watch_fatal(&shared);
    let held = shared.txn_error.lock();
    let setter_shared = Arc::clone(&shared);
    let setter = thread::spawn(move || {
        setter_shared.set_txn_fatal(Error::broker(error::PRODUCER_FENCED, "fatal error"));
    });
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let published = wait_for_fatal_publication(&shared);
    let (results_tx, results_rx) = mpsc::channel();
    let fatal_shared = Arc::clone(&shared);
    let fatal_tx = results_tx.clone();
    let fatal_reader = thread::spawn(move || {
        fatal_tx
            .send(
                fatal_shared
                    .fail_on_txn_fatal()
                    .err()
                    .map(|error| error.to_string()),
            )
            .unwrap();
    });
    let any_shared = Arc::clone(&shared);
    let any_reader = thread::spawn(move || {
        results_tx
            .send(
                any_shared
                    .fail_on_txn_error()
                    .err()
                    .map(|error| error.to_string()),
            )
            .unwrap();
    });
    let mut early = Vec::new();
    for _ in 0..2 {
        if let Ok(message) = results_rx.recv_timeout(Duration::from_millis(500)) {
            early.push(message);
        }
    }
    drop(held);
    setter.join().unwrap();
    fatal_reader.join().unwrap();
    any_reader.join().unwrap();
    *ACTIVE.lock() = None;
    println!("PL_TXN_FATAL_PUBLICATION published={published} early={early:?}");
    assert!(published, "PL_TXN_FATAL_PUBLICATION");
    assert_eq!(early.len(), 2, "PL_TXN_FATAL_READERS_BLOCKED");
    let expected_message = Some("protocol: transactional producer in fatal error state");
    assert!(
        early
            .iter()
            .all(|message| message.as_deref() == expected_message),
        "PL_TXN_FATAL_READER_ALLOWED"
    );
    assert_eq!(
        shared.fail_on_txn_fatal().unwrap_err().broker_code(),
        Some(error::PRODUCER_FENCED)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn first_fatal_wins_during_clear_and_competing_fatal() {
    let (shared, _peer) = fixture().await;
    shared.set_txn_abortable(Error::broker(
        error::TOPIC_AUTHORIZATION_FAILED,
        "earlier abortable",
    ));
    let _serial = SERIAL.lock();
    let entered = watch_fatal(&shared);
    let held = shared.txn_error.lock();
    let first_shared = Arc::clone(&shared);
    let first = thread::spawn(move || {
        first_shared.set_txn_fatal(Error::broker(error::PRODUCER_FENCED, "first fatal"));
    });
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let published = wait_for_fatal_publication(&shared);
    let (second_done_tx, second_done_rx) = mpsc::channel();
    let second_shared = Arc::clone(&shared);
    let second = thread::spawn(move || {
        second_shared.set_txn_fatal(Error::broker(error::INVALID_TXN_STATE, "second fatal"));
        second_done_tx.send(()).unwrap();
    });
    let second_completed = second_done_rx.recv_timeout(Duration::from_secs(2)).is_ok();
    let (reader_tx, reader_rx) = mpsc::channel();
    let reader_shared = Arc::clone(&shared);
    let reader = thread::spawn(move || {
        let result = reader_shared.fail_on_txn_error();
        let ok = result.is_ok();
        let code = result.err().and_then(|error| error.broker_code());
        reader_tx.send((ok, code)).unwrap();
    });
    let pending_read = reader_rx.recv_timeout(Duration::from_millis(500)).ok();
    let clear_shared = Arc::clone(&shared);
    let clear = thread::spawn(move || clear_shared.clear_txn_abortable());
    drop(held);
    first.join().unwrap();
    second.join().unwrap();
    reader.join().unwrap();
    clear.join().unwrap();
    *ACTIVE.lock() = None;
    let cause = shared.fail_on_txn_fatal().unwrap_err().broker_code();
    println!("PL_TXN_FATAL_WINNER published={published} second={second_completed} cause={cause:?}");
    assert!(published, "PL_TXN_FATAL_WINNER");
    assert!(second_completed, "PL_TXN_FATAL_WINNER");
    let pending_safe = pending_read
        .is_none_or(|(ok, code)| !ok && code != Some(error::TOPIC_AUTHORIZATION_FAILED));
    assert!(pending_safe, "PL_TXN_PENDING_READER");
    assert_eq!(cause, Some(error::PRODUCER_FENCED), "PL_TXN_FATAL_WINNER");
    assert_eq!(
        shared.fail_on_txn_error().unwrap_err().broker_code(),
        Some(error::PRODUCER_FENCED)
    );
}

fn install_control(
    shared: &Arc<Shared>,
    pause: Point,
    watch: Point,
) -> (mpsc::Receiver<()>, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (paused_tx, paused_rx) = mpsc::channel();
    let (attempted_tx, attempted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let hook = Arc::new(Hook {
        target: Arc::as_ptr(shared) as usize,
        pause,
        pause_used: AtomicBool::new(false),
        watch,
        paused: paused_tx,
        attempted: attempted_tx,
        release: parking_lot::Mutex::new(release_rx),
    });
    let mut active = ACTIVE.lock();
    assert!(active.is_none());
    *active = Some(hook);
    (paused_rx, attempted_rx, release_tx)
}

fn wait_for_abortable_publication(shared: &Shared) -> bool {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !shared.txn_error_test_snapshot().1 && Instant::now() < deadline {
        thread::yield_now();
    }
    shared.txn_error_test_snapshot().1
}

#[tokio::test(flavor = "current_thread")]
async fn abortable_publishes_before_waiting_for_cause_lock() {
    let (shared, _peer) = fixture().await;
    let _serial = SERIAL.lock();
    let (_paused, attempted, _release) =
        install_control(&shared, Point::ClearBeforeCause, Point::AbortEntry);
    let held = shared.txn_error.lock();
    let setter_shared = Arc::clone(&shared);
    let setter = thread::spawn(move || {
        setter_shared.set_txn_abortable(Error::broker(
            error::TOPIC_AUTHORIZATION_FAILED,
            "abortable error",
        ));
    });
    attempted.recv_timeout(Duration::from_secs(5)).unwrap();
    let published = wait_for_abortable_publication(&shared);
    let (result_tx, result_rx) = mpsc::channel();
    let reader_shared = Arc::clone(&shared);
    let reader = thread::spawn(move || {
        result_tx
            .send(reader_shared.fail_on_txn_error().is_ok())
            .unwrap();
    });
    let early = result_rx.recv_timeout(Duration::from_secs(1)).ok();
    let fatal_only_allowed = shared.fail_on_txn_fatal().is_ok();
    drop(held);
    setter.join().unwrap();
    reader.join().unwrap();
    *ACTIVE.lock() = None;
    println!("PL_TXN_ABORT_PUBLICATION published={published} early={early:?}");
    assert!(published, "PL_TXN_ABORT_PUBLICATION");
    assert_eq!(early, Some(false), "PL_TXN_ABORT_READER");
    assert!(fatal_only_allowed);
    assert_eq!(
        shared.fail_on_txn_error().unwrap_err().broker_code(),
        Some(error::TOPIC_AUTHORIZATION_FAILED)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cleared_stalled_abort_cannot_overwrite_new_abort() {
    let (shared, _peer) = fixture().await;
    let _serial = SERIAL.lock();
    let (paused, _attempted, release) =
        install_control(&shared, Point::AbortClaimed, Point::ClearEntry);
    let old_shared = Arc::clone(&shared);
    let old = thread::spawn(move || {
        old_shared.set_txn_abortable(Error::broker(
            error::TOPIC_AUTHORIZATION_FAILED,
            "old abortable",
        ));
    });
    paused.recv_timeout(Duration::from_secs(5)).unwrap();
    shared.clear_txn_abortable();
    assert!(shared.fail_on_txn_error().is_ok());
    let new_shared = Arc::clone(&shared);
    let new = thread::spawn(move || {
        new_shared.set_txn_abortable(Error::broker(error::TRANSACTION_ABORTABLE, "new abortable"));
    });
    new.join().unwrap();
    let before = shared.fail_on_txn_error().unwrap_err().broker_code();
    release.send(()).unwrap();
    old.join().unwrap();
    *ACTIVE.lock() = None;
    let after = shared.fail_on_txn_error().unwrap_err().broker_code();
    println!("PL_TXN_STALE_ABORT before={before:?} after={after:?}");
    assert_eq!(before, Some(error::TRANSACTION_ABORTABLE));
    assert_eq!(
        after,
        Some(error::TRANSACTION_ABORTABLE),
        "PL_TXN_STALE_ABORT"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reader_rechecks_after_fatal_is_published() {
    let (shared, _peer) = fixture().await;
    shared.set_txn_abortable(Error::broker(
        error::TOPIC_AUTHORIZATION_FAILED,
        "stale abortable",
    ));
    let _serial = SERIAL.lock();
    let (paused, attempted, release) =
        install_control(&shared, Point::ReaderBeforeCause, Point::FatalEntry);
    let held = shared.txn_error.lock();
    let reader_shared = Arc::clone(&shared);
    let reader = thread::spawn(move || reader_shared.fail_on_txn_error());
    paused.recv_timeout(Duration::from_secs(5)).unwrap();
    release.send(()).unwrap();
    let fatal_shared = Arc::clone(&shared);
    let fatal = thread::spawn(move || {
        fatal_shared.set_txn_fatal(Error::broker(error::PRODUCER_FENCED, "fatal error"));
    });
    attempted.recv_timeout(Duration::from_secs(5)).unwrap();
    let published = wait_for_fatal_publication(&shared);
    drop(held);
    let result = reader.join().unwrap();
    fatal.join().unwrap();
    *ACTIVE.lock() = None;
    let ok = result.is_ok();
    let code = result.err().and_then(|error| error.broker_code());
    println!("PL_TXN_RECHECK_FATAL published={published} ok={ok} reader_code={code:?}");
    assert!(published, "PL_TXN_RECHECK_FATAL");
    assert!(!ok, "PL_TXN_RECHECK_FATAL");
    assert_ne!(
        code,
        Some(error::TOPIC_AUTHORIZATION_FAILED),
        "PL_TXN_RECHECK_FATAL"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn abort_ticket_exhaustion_fails_closed() {
    let (shared, _peer) = fixture().await;
    shared.txn_error_test_force_ticket_exhaustion();
    shared.set_txn_abortable(Error::broker(
        error::TRANSACTION_ABORTABLE,
        "abortable error",
    ));
    let fatal = shared.fail_on_txn_fatal();
    println!("PL_TXN_TICKET_EXHAUSTION fatal={fatal:?}");
    assert!(fatal.is_err(), "PL_TXN_TICKET_EXHAUSTION");
}

#[tokio::test(flavor = "current_thread")]
async fn last_abort_ticket_clears_then_exhaustion_fences() {
    let (shared, _peer) = fixture().await;
    shared.txn_error_test_force_ticket_before_exhaustion();
    shared.set_txn_abortable(Error::broker(
        error::TOPIC_AUTHORIZATION_FAILED,
        "last valid abortable",
    ));
    assert_eq!(
        shared.fail_on_txn_error().unwrap_err().broker_code(),
        Some(error::TOPIC_AUTHORIZATION_FAILED)
    );
    shared.clear_txn_abortable();
    assert!(shared.fail_on_txn_error().is_ok());
    shared.set_txn_abortable(Error::broker(
        error::TRANSACTION_ABORTABLE,
        "overflowing abortable",
    ));
    let fatal = shared.fail_on_txn_fatal().unwrap_err().to_string();
    println!("PL_TXN_LAST_TICKET fatal={fatal}");
    assert!(
        fatal.contains("transactional error ticket exhausted"),
        "PL_TXN_LAST_TICKET"
    );
    shared.set_txn_fatal(Error::broker(error::PRODUCER_FENCED, "later fatal"));
    assert_eq!(shared.fail_on_txn_fatal().unwrap_err().to_string(), fatal);
}

#[tokio::test(flavor = "current_thread")]
async fn real_fatal_wins_before_exhaustion_is_published() {
    let (shared, _peer) = fixture().await;
    shared.txn_error_test_force_ticket_exhaustion();
    let _serial = SERIAL.lock();
    let (paused, _attempted, release) =
        install_control(&shared, Point::TicketBeforeClaim, Point::FatalEntry);
    let overflow_shared = Arc::clone(&shared);
    let overflow = thread::spawn(move || {
        overflow_shared.set_txn_abortable(Error::broker(
            error::TRANSACTION_ABORTABLE,
            "overflowing abortable",
        ));
    });
    paused.recv_timeout(Duration::from_secs(5)).unwrap();
    shared.set_txn_fatal(Error::broker(error::PRODUCER_FENCED, "real fatal"));
    release.send(()).unwrap();
    overflow.join().unwrap();
    *ACTIVE.lock() = None;
    let cause = shared.fail_on_txn_fatal().unwrap_err().broker_code();
    println!("PL_TXN_REAL_FATAL_BEFORE_EXHAUSTION cause={cause:?}");
    assert_eq!(
        cause,
        Some(error::PRODUCER_FENCED),
        "PL_TXN_REAL_FATAL_BEFORE_EXHAUSTION"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn overflow_pending_fences_and_wins_over_later_fatal() {
    let (shared, _peer) = fixture().await;
    shared.txn_error_test_force_ticket_exhaustion();
    let _serial = SERIAL.lock();
    let (paused, _attempted, release) =
        install_control(&shared, Point::TicketAfterClaim, Point::FatalEntry);
    let overflow_shared = Arc::clone(&shared);
    let overflow = thread::spawn(move || {
        overflow_shared.set_txn_abortable(Error::broker(
            error::TRANSACTION_ABORTABLE,
            "overflowing abortable",
        ));
    });
    paused.recv_timeout(Duration::from_secs(5)).unwrap();
    let pending = shared.fail_on_txn_error().unwrap_err().to_string();
    let pending_fatal = shared.fail_on_txn_fatal().unwrap_err().to_string();
    shared.set_txn_fatal(Error::broker(error::PRODUCER_FENCED, "later fatal"));
    release.send(()).unwrap();
    overflow.join().unwrap();
    *ACTIVE.lock() = None;
    let cause = shared.fail_on_txn_fatal().unwrap_err().to_string();
    println!("PL_TXN_OVERFLOW_PENDING pending={pending} final={cause}");
    assert_eq!(
        pending,
        "protocol: transactional producer in fatal error state"
    );
    assert_eq!(pending_fatal, pending);
    assert!(
        cause.contains("transactional error ticket exhausted"),
        "PL_TXN_OVERFLOW_PENDING"
    );
}
