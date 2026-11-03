//! Isolated accounting sketch: one generation, no producer integration.
//! Uncompiled and untested. It does not implement flush snapshots, resources,
//! interceptors, retries, shutdown, a wakeup, or a production error type.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const SEALED: usize = (usize::MAX / 2) + 1;
const COUNT: usize = SEALED - 1;

struct Epoch {
    state: AtomicUsize,
    first_error: Mutex<Option<&'static str>>,
}

struct Ticket {
    epoch: Arc<Epoch>,
    finished: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum Snapshot {
    Open,
    Pending,
    Complete(Option<&'static str>),
}

impl Epoch {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicUsize::new(0),
            first_error: Mutex::new(None),
        })
    }

    // The caller must own an infallible channel permit before registering.
    // A failed registration is not an accepted record.
    fn register(epoch: &Arc<Self>) -> Option<Ticket> {
        let mut state = epoch.state.load(Ordering::Acquire);
        loop {
            if state & SEALED != 0 || state & COUNT == COUNT {
                return None;
            }
            match epoch.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(Ticket {
                        epoch: Arc::clone(epoch),
                        finished: false,
                    })
                }
                Err(observed) => state = observed,
            }
        }
    }

    fn seal(&self) {
        let _previous = self.state.fetch_or(SEALED, Ordering::AcqRel);
    }

    fn note_error(&self, error: &'static str) {
        let mut first = self.first_error.lock().unwrap_or_else(|p| p.into_inner());
        if first.is_none() {
            *first = Some(error);
        }
    }

    // The production design must register Notify before the state check.
    fn snapshot(&self) -> Snapshot {
        let state = self.state.load(Ordering::Acquire);
        if state & SEALED == 0 {
            Snapshot::Open
        } else if state & COUNT != 0 {
            Snapshot::Pending
        } else {
            Snapshot::Complete(*self.first_error.lock().unwrap_or_else(|p| p.into_inner()))
        }
    }
}

impl Ticket {
    // In a real producer, this must follow all resource disposition,
    // interceptor handling, and per-record delivery-result publication.
    fn finish(mut self, error: Option<&'static str>) {
        if let Some(error) = error {
            self.epoch.note_error(error);
        }
        let _previous = self.epoch.state.fetch_sub(1, Ordering::AcqRel);
        self.finished = true;
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if !self.finished {
            self.epoch.note_error("owner dropped unexpectedly");
            let _previous = self.epoch.state.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_byte_record_still_blocks_the_sealed_generation() {
        let epoch = Epoch::new();
        let _bytes = 0usize;
        let ticket = Epoch::register(&epoch).unwrap();
        epoch.seal();
        assert_eq!(epoch.snapshot(), Snapshot::Pending);
        ticket.finish(None);
        assert_eq!(epoch.snapshot(), Snapshot::Complete(None));
    }

    #[test]
    fn records_finishing_out_of_order_do_not_open_the_barrier() {
        let epoch = Epoch::new();
        let first = Epoch::register(&epoch).unwrap();
        let second = Epoch::register(&epoch).unwrap();
        epoch.seal();
        second.finish(None);
        assert_eq!(epoch.snapshot(), Snapshot::Pending);
        first.finish(Some("delivery timeout"));
        assert_eq!(
            epoch.snapshot(),
            Snapshot::Complete(Some("delivery timeout"))
        );
    }

    #[test]
    fn sealing_rejects_new_registration_and_drop_publishes_failure() {
        let epoch = Epoch::new();
        let accepted = Epoch::register(&epoch).unwrap();
        epoch.seal();
        assert!(Epoch::register(&epoch).is_none());
        drop(accepted);
        assert_eq!(
            epoch.snapshot(),
            Snapshot::Complete(Some("owner dropped unexpectedly"))
        );
    }

    #[test]
    fn admission_and_seal_have_an_atomic_order() {
        use std::sync::mpsc;
        use std::thread;

        let epoch = Epoch::new();
        let (registered, seen) = mpsc::sync_channel(0);
        let (release, released) = mpsc::sync_channel(0);
        thread::scope(|scope| {
            let epoch_for_thread = Arc::clone(&epoch);
            let task = scope.spawn(move || {
                let ticket = Epoch::register(&epoch_for_thread).unwrap();
                registered.send(()).unwrap();
                released.recv().unwrap();
                ticket.finish(None);
            });
            seen.recv().unwrap();
            epoch.seal();
            assert_eq!(epoch.snapshot(), Snapshot::Pending);
            release.send(()).unwrap();
            task.join().unwrap();
        });
        assert_eq!(epoch.snapshot(), Snapshot::Complete(None));
    }
}
