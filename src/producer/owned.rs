//! Experimental admission-rooted record ownership for the bounded producer.
//!
//! This branch is an incomplete integration: batch identity, retry fencing and
//! identity/write serialization still need to be implemented before use.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use tokio::sync::{OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

const PREPARED: u8 = 0;
const ADMITTED: u8 = 1;
const ORPHANED: u8 = 2;
const COMPLETING: u8 = 3;
const DONE: u8 = 4;

struct Slot {
    id: u64,
    payload: parking_lot::Mutex<Option<Arc<Mutex<PendingData>>>>,
    stage: AtomicU8,
    ticket: AtomicU64,
    sticky_retired: AtomicBool,
    _permit: OwnedSemaphorePermit,
}

#[derive(Default)]
struct State {
    next_slot: u64,
    next_ticket: u64,
    slots: HashMap<u64, Arc<Slot>>,
}

pub(super) struct Owner {
    state: parking_lot::Mutex<State>,
    pub(super) permits: Arc<Semaphore>,
    changed: Notify,
    closed: Arc<AtomicBool>,
    sealed: AtomicBool,
    close_requested: AtomicBool,
    close_finished: AtomicBool,
}

impl Owner {
    pub(super) fn new(limit: usize, closed: Arc<AtomicBool>) -> Self {
        Self {
            state: parking_lot::Mutex::new(State::default()),
            permits: Arc::new(Semaphore::new(limit)),
            changed: Notify::new(),
            closed,
            sealed: AtomicBool::new(false),
            close_requested: AtomicBool::new(false),
            close_finished: AtomicBool::new(false),
        }
    }

    pub(super) fn prepare(
        self: &Arc<Self>,
        pending: Pending,
        permit: OwnedSemaphorePermit,
    ) -> Result<Pending> {
        let data = match pending {
            Pending {
                data: PendingStorage::Inline(data),
                release: None,
            } => data,
            _ => return Err(Error::protocol("pending record already owned")),
        };
        let payload = Arc::new(Mutex::new(data));
        let guard = Arc::clone(&payload)
            .try_lock_owned()
            .map_err(|_| Error::protocol("new record slot is locked"))?;
        let mut state = self.state.lock();
        if self.sealed.load(Ordering::SeqCst) || self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        let id = match state.next_slot.checked_add(1) {
            Some(id) => id,
            None => {
                self.sealed.store(true, Ordering::SeqCst);
                self.closed.store(true, Ordering::SeqCst);
                self.permits.close();
                self.changed.notify_waiters();
                return Err(Error::protocol("producer record slot counter exhausted"));
            }
        };
        state.next_slot = id;
        let slot = Arc::new(Slot {
            id,
            payload: parking_lot::Mutex::new(Some(payload)),
            stage: AtomicU8::new(PREPARED),
            ticket: AtomicU64::new(0),
            sticky_retired: AtomicBool::new(false),
            _permit: permit,
        });
        let _old = state.slots.insert(id, Arc::clone(&slot));
        drop(state);
        Ok(Pending {
            data: PendingStorage::Rooted(guard),
            release: Some(Release {
                owner: Arc::clone(self),
                slot,
            }),
        })
    }

    fn admit(&self, slot: &Slot) -> Result<()> {
        let mut state = self.state.lock();
        if self.sealed.load(Ordering::SeqCst) || self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        if slot.stage.load(Ordering::SeqCst) == PREPARED {
            let ticket = match state.next_ticket.checked_add(1) {
                Some(ticket) => ticket,
                None => {
                    self.sealed.store(true, Ordering::SeqCst);
                    self.closed.store(true, Ordering::SeqCst);
                    self.permits.close();
                    self.changed.notify_waiters();
                    return Err(Error::protocol("producer admission counter exhausted"));
                }
            };
            state.next_ticket = ticket;
            slot.ticket.store(ticket, Ordering::SeqCst);
            slot.stage.store(ADMITTED, Ordering::SeqCst);
        }
        Ok(())
    }

    pub(super) fn seal(&self) {
        let _state = self.state.lock();
        self.sealed.store(true, Ordering::SeqCst);
        self.permits.close();
        self.changed.notify_waiters();
    }

    pub(super) fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::SeqCst)
    }

    pub(super) fn request_interceptor_close(&self) {
        self.close_requested.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    pub(super) async fn wait_interceptor_close(&self, deadline: Instant) -> Result<()> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            let _registered = notified.as_mut().enable();
            if self.close_finished.load(Ordering::SeqCst) {
                return Ok(());
            }
            tokio::time::timeout_at(deadline.into(), notified)
                .await
                .map_err(|_| Error::Timeout)?;
        }
    }

    pub(super) fn cutoff(&self) -> u64 {
        self.state.lock().next_ticket
    }

    fn complete_through(&self, cutoff: u64) -> bool {
        self.state.lock().slots.values().all(|slot| {
            let ticket = slot.ticket.load(Ordering::SeqCst);
            ticket == 0 || ticket > cutoff
        })
    }

    pub(super) async fn wait_through(&self, cutoff: u64, deadline: Instant) -> Result<()> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            let _registered = notified.as_mut().enable();
            if self.complete_through(cutoff) {
                return Ok(());
            }
            tokio::time::timeout_at(deadline.into(), notified)
                .await
                .map_err(|_| Error::Timeout)?;
        }
    }

    fn remove(&self, slot: &Slot) {
        slot.stage.store(DONE, Ordering::SeqCst);
        let _removed = self.state.lock().slots.remove(&slot.id);
        self.changed.notify_waiters();
    }

    fn claim_orphan(self: &Arc<Self>) -> Option<Pending> {
        let slot = self
            .state
            .lock()
            .slots
            .values()
            .find(|slot| slot.stage.load(Ordering::SeqCst) == ORPHANED)
            .cloned()?;
        if slot
            .stage
            .compare_exchange(ORPHANED, ADMITTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        let payload = slot.payload.lock().as_ref().cloned()?;
        let guard = match payload.try_lock_owned() {
            Ok(guard) => guard,
            Err(_) => {
                slot.stage.store(ORPHANED, Ordering::SeqCst);
                self.changed.notify_one();
                return None;
            }
        };
        Some(Pending {
            data: PendingStorage::Rooted(guard),
            release: Some(Release {
                owner: Arc::clone(self),
                slot,
            }),
        })
    }
}

pub(super) struct Release {
    owner: Arc<Owner>,
    slot: Arc<Slot>,
}

impl Release {
    pub(super) fn admit(&self) -> Result<()> {
        self.owner.admit(&self.slot)
    }

    pub(super) fn sticky_first_retirement(&self) -> bool {
        !self.slot.sticky_retired.swap(true, Ordering::SeqCst)
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        match self.slot.stage.load(Ordering::SeqCst) {
            PREPARED | DONE => self.owner.remove(&self.slot),
            ADMITTED => {
                self.slot.stage.store(ORPHANED, Ordering::SeqCst);
                // This is conservative: do not accept additional sends after
                // a claimed record loses its worker. Write fencing remains open.
                self.owner.closed.store(true, Ordering::SeqCst);
                self.owner.seal();
            }
            _ => {}
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Return a non-rooted pending record without allocating."
)]
fn take_rooted(
    pending: Pending,
) -> std::result::Result<(OwnedMutexGuard<PendingData>, Release), Pending> {
    match pending {
        Pending {
            data: PendingStorage::Rooted(guard),
            release: Some(release),
        } => Ok((guard, release)),
        pending => Err(pending),
    }
}

/// Invoke a callback with no record or owner lock held. A panic is observed
/// only as an attempted callback in this experiment; it is never re-invoked.
pub(super) fn complete(
    shared: &Shared,
    pending: Pending,
    result: Result<RecordMetadata>,
) {
    match take_rooted(pending) {
        Ok((mut guard, release)) => {
            let owner = Arc::clone(&release.owner);
            let slot = Arc::clone(&release.slot);
            slot.stage.store(COMPLETING, Ordering::SeqCst);
            let bytes = rec_bytes(&guard.rec);
            let tx = guard.tx.take();
            drop(guard);
            drop(release);
            let _callback = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match &result {
                    Ok(metadata) => shared.interceptors.on_ack(metadata),
                    Err(error) => shared.interceptors.on_error(error),
                }
            }));
            if let Some(tx) = tx {
                drop(tx.send(result));
            }
            let payload = slot.payload.lock().take();
            let _destroyed =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)));
            shared.release_buffer(bytes);
            owner.remove(&slot);
        }
        Err(mut pending) => {
            match &result {
                Ok(metadata) => shared.interceptors.on_ack(metadata),
                Err(error) => shared.interceptors.on_error(error),
            }
            if let Some(tx) = pending.tx.take() {
                drop(tx.send(result));
            }
        }
    }
}

/// Retire a successful record when neither a receipt nor an interceptor needs
/// its metadata. No user code runs while the slot is locked.
pub(super) fn complete_without_consumer(shared: &Shared, pending: Pending) {
    if let Ok((guard, release)) = take_rooted(pending) {
        let owner = Arc::clone(&release.owner);
        let slot = Arc::clone(&release.slot);
        slot.stage.store(COMPLETING, Ordering::SeqCst);
        let bytes = rec_bytes(&guard.rec);
        drop(guard);
        drop(release);
        let payload = slot.payload.lock().take();
        let _destroyed =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)));
        shared.release_buffer(bytes);
        owner.remove(&slot);
    }
}

pub(super) async fn supervise(shared: Arc<Shared>, owner: Arc<Owner>) {
    loop {
        let notified = owner.changed.notified();
        tokio::pin!(notified);
        let _registered = notified.as_mut().enable();
        if let Some(pending) = owner.claim_orphan() {
            fail_pendings(&shared, vec![pending], Error::Closed);
            continue;
        }
        let ready_to_close = {
            let state = owner.state.lock();
            owner.sealed.load(Ordering::SeqCst)
                && owner.close_requested.load(Ordering::SeqCst)
                && state.slots.is_empty()
        };
        if ready_to_close {
            let _closed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                shared.interceptors.close();
            }));
            owner.close_finished.store(true, Ordering::SeqCst);
            owner.changed.notify_waiters();
            break;
        }
        notified.await;
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Default)]
    struct DropGate {
        entered: AtomicBool,
        timed_out: AtomicBool,
        open: parking_lot::Mutex<bool>,
        changed: parking_lot::Condvar,
    }

    impl DropGate {
        fn release(&self) {
            *self.open.lock() = true;
            let _notified = self.changed.notify_all();
        }
    }

    struct ReleaseOnDrop(Arc<DropGate>);

    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    struct BlockingBytes {
        bytes: Vec<u8>,
        gate: Arc<DropGate>,
    }

    impl AsRef<[u8]> for BlockingBytes {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for BlockingBytes {
        fn drop(&mut self) {
            self.gate.entered.store(true, Ordering::SeqCst);
            let mut open = self.gate.open.lock();
            while !*open {
                if self
                    .gate
                    .changed
                    .wait_for(&mut open, Duration::from_secs(5))
                    .timed_out()
                {
                    self.gate.timed_out.store(true, Ordering::SeqCst);
                    break;
                }
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cutoff_waits_for_payload_destruction_outside_owner_lock() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let conn = BrokerConn::connect(&address, "owner-drop", Duration::from_secs(1))
            .await
            .unwrap();
        let (_peer, _) = listener.accept().await.unwrap();
        let shared = test_shared(ProducerConfig::default(), conn);
        shared.buffered_bytes.store(1, Ordering::SeqCst);
        let owner = Arc::new(Owner::new(1, Arc::clone(&shared.closed)));
        let gate = Arc::new(DropGate::default());
        let _release = ReleaseOnDrop(Arc::clone(&gate));
        let now = Instant::now();
        let data = PendingData {
            sticky_record: None,
            sticky: None,
            rec: ProduceRecord::to("t")
                .partition(0)
                .value(Bytes::from_owner(BlockingBytes {
                    bytes: vec![1],
                    gate: Arc::clone(&gate),
                })),
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
        };
        let permit = Arc::clone(&owner.permits).try_acquire_owned().unwrap();
        let pending = owner.prepare(Pending::inline(data), permit).unwrap();
        pending.admit().unwrap();
        let cutoff = owner.cutoff();
        assert_eq!(cutoff, 1);
        let worker_shared = Arc::clone(&shared);
        let worker = std::thread::spawn(move || {
            complete_without_consumer(&worker_shared, pending);
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !gate.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(owner.cutoff(), cutoff);
        assert!(Arc::clone(&owner.permits).try_acquire_owned().is_err());
        assert!(tokio::time::timeout(
            Duration::from_millis(100),
            owner.wait_through(cutoff, Instant::now() + Duration::from_secs(2)),
        )
        .await
        .is_err());
        gate.release();
        worker.join().unwrap();
        owner
            .wait_through(cutoff, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert!(!gate.timed_out.load(Ordering::SeqCst));
        assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
        assert!(Arc::clone(&owner.permits).try_acquire_owned().is_ok());
    }
}
