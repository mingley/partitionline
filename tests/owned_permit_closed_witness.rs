//! Witness reserved channel sends after receiver shutdown.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct Tracked(Arc<AtomicUsize>);

impl Drop for Tracked {
    fn drop(&mut self) {
        let _ = self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reserved_owned_permit_can_send_after_receiver_close() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let permit = sender.reserve_owned().await.expect("reserve");
    receiver.close();
    let sender = permit.send(Tracked(Arc::clone(&dropped)));
    assert!(sender.is_closed());
    let value = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("receive deadline")
        .expect("closed receiver still drains reserved send");
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(value);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(receiver.recv().await.is_none());
    println!("OWNED_PERMIT_CLOSED_RECEIVER=drained");
}

#[tokio::test(flavor = "current_thread")]
async fn reserved_owned_permit_send_has_no_live_receiver_after_drop() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let permit = sender.reserve_owned().await.expect("reserve");
    drop(receiver);
    // send returns a Sender, rather than Result<T>, even after receiver Drop.
    let sender = permit.send(Tracked(Arc::clone(&dropped)));
    assert!(sender.is_closed());
    drop(sender);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    println!("OWNED_PERMIT_DROPPED_RECEIVER=payload_dropped");
}
