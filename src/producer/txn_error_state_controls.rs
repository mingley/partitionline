use super::*;
use std::sync::{mpsc, Mutex as StdMutex};
use std::thread;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    AbortBeforeCause,
    ClearBeforeCause,
    FatalEntry,
    ClearEntry,
}

struct Hook {
    target: usize,
    pause: Point,
    watch: Point,
    paused: mpsc::Sender<()>,
    attempted: mpsc::Sender<()>,
    release: StdMutex<mpsc::Receiver<()>>,
}

static ACTIVE: StdMutex<Option<Arc<Hook>>> = StdMutex::new(None);
static SERIAL: StdMutex<()> = StdMutex::new(());

fn active(shared: &Shared) -> Option<Arc<Hook>> {
    let target = std::ptr::from_ref(shared) as usize;
    ACTIVE
        .lock()
        .unwrap()
        .as_ref()
        .filter(|hook| hook.target == target)
        .cloned()
}

pub(super) fn mark(shared: &Shared, point: Point) {
    if let Some(hook) = active(shared) {
        if hook.watch == point {
            hook.attempted.send(()).unwrap();
        }
    }
}

pub(super) fn pause(shared: &Shared, point: Point) {
    if let Some(hook) = active(shared) {
        if hook.pause == point {
            hook.paused.send(()).unwrap();
            hook.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(15))
                .unwrap();
        }
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
    let _serial = SERIAL.lock().unwrap();
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
        watch,
        paused: paused_tx,
        attempted: attempted_tx,
        release: StdMutex::new(release_rx),
    });
    {
        let mut active = ACTIVE.lock().unwrap();
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
    *ACTIVE.lock().unwrap() = None;

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
