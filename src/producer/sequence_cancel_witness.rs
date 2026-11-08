//! Isolated cancellation witness for reserved idempotent sequences.
use super::*;
use std::future::Future;
use std::task::{Context, Wake, Waker};

const RECORDS: usize = 8_193;

struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}

fn batch() -> (Vec<(Arc<str>, i32, Vec<Pending>)>, Vec<oneshot::Receiver<Result<RecordMetadata>>>) {
    let topic: Arc<str> = Arc::from("sequence-cancel");
    let mut pending = Vec::with_capacity(RECORDS);
    let mut receipts = Vec::with_capacity(RECORDS);
    let now = Instant::now();
    for _ in 0..RECORDS {
        let (tx, rx) = oneshot::channel();
        pending.push(Pending {
            sticky_record: None,
            sticky: None,
            rec: ProduceRecord::to(Arc::clone(&topic)).partition(0),
            tx: Some(tx),
            seq: None,
            batch_base_seq: None,
            batch_len: 1,
            epoch_gen: 0,
            deadline: now + Duration::from_secs(60),
            queued_at: now,
            retry: 0,
            skip_meta_refresh: false,
            retry_after: now,
        });
        receipts.push(rx);
    }
    (vec![(topic, 0, pending)], receipts)
}

fn next_sequence(seqs: &parking_lot::Mutex<SequenceMap>) -> Option<i32> {
    seqs.lock()
        .get("sequence-cancel")
        .and_then(|parts| parts.get(&0))
        .copied()
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_after_range_reservation_drops_pending_receipts() {
    let seqs = Arc::new(parking_lot::Mutex::new(SequenceMap::new()));
    let (mut groups, mut receipts) = batch();
    let future_seqs = Arc::clone(&seqs);
    let mut future = Box::pin(async move {
        assign_sequences(&mut groups, 5, &future_seqs, 0).await;
        std::future::pending::<()>().await;
    });
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    let mut reserved = false;
    for _ in 0..8 {
        assert!(future.as_mut().poll(&mut cx).is_pending());
        if next_sequence(&seqs) == Some(RECORDS as i32) {
            reserved = true;
            break;
        }
    }
    assert!(reserved, "the real helper did not yield after reserving its range");
    assert!(receipts.iter_mut().all(|rx| matches!(
        rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)
    )));
    drop(future);
    assert!(receipts.iter_mut().all(|rx| matches!(
        rx.try_recv(), Err(oneshot::error::TryRecvError::Closed)
    )));
    assert_eq!(next_sequence(&seqs), Some(RECORDS as i32));
    let next_base = {
        let mut map = seqs.lock();
        sequence_in(map.get_mut("sequence-cancel").unwrap(), 0, 1)
    };
    assert_eq!(next_base, RECORDS as i32);
    println!("CANCEL_CLOSED_8193_RECEIPTS_AND_LEFT_NEXT_SEQUENCE_8193");
}

#[tokio::test(flavor = "current_thread")]
async fn completed_assignment_keeps_receipts_and_assigns_the_full_range() {
    let seqs = parking_lot::Mutex::new(SequenceMap::new());
    let (mut groups, mut receipts) = batch();
    assign_sequences(&mut groups, 5, &seqs, 0).await;
    assert_eq!(next_sequence(&seqs), Some(RECORDS as i32));
    assert_eq!(groups[0].2[0].seq, Some(0));
    assert_eq!(groups[0].2[RECORDS - 1].seq, Some((RECORDS - 1) as i32));
    assert!(receipts.iter_mut().all(|rx| matches!(
        rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)
    )));
    println!("NORMAL_ASSIGNMENT_RETAINED_8193_RECEIPTS_AND_FULL_RANGE");
}
