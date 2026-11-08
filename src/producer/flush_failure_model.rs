//! Isolated draft: bounded, coalescing worker flush-error epochs.
//!
//! This is not a complete producer fix. It does not retain errors from retired
//! workers or repair accepted-record ownership and flush completion horizons.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::Error;

pub(super) struct FailureEpoch {
    error: Mutex<Option<Arc<Error>>>,
}

struct State {
    current: Arc<FailureEpoch>,
    sealed: bool,
    next: Option<Arc<Error>>,
    overflow: bool,
}

pub(super) struct WorkerFailures {
    state: Mutex<State>,
    coalesced: Arc<Error>,
}

impl WorkerFailures {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                current: Arc::new(FailureEpoch {
                    error: Mutex::new(None),
                }),
                sealed: false,
                next: None,
                overflow: false,
            }),
            coalesced: Arc::new(Error::protocol("additional worker errors were coalesced")),
        })
    }

    /// A flush captures its view before queuing its control request.
    pub(super) fn register(&self) -> Arc<FailureEpoch> {
        Arc::clone(&self.state.lock().current)
    }

    /// The first error is retained; after it has been sent, the first later
    /// error is retained separately and further errors set a summary flag.
    pub(super) fn record(&self, error: Error) {
        let event = Arc::new(error);
        {
            let mut state = self.state.lock();
            if state.sealed {
                if state.next.is_none() {
                    state.next = Some(Arc::clone(&event));
                } else {
                    state.overflow = true;
                }
            } else {
                let is_summary = {
                    let mut current = state.current.error.lock();
                    if current.is_none() {
                        *current = Some(Arc::clone(&event));
                        false
                    } else {
                        current
                            .as_ref()
                            .is_some_and(|stored| Arc::ptr_eq(stored, &self.coalesced))
                    }
                };
                // A pending summary stands for already-coalesced errors. Keep
                // the first newer exact error separately rather than hiding it.
                if is_summary {
                    if state.next.is_none() {
                        state.next = Some(Arc::clone(&event));
                    } else {
                        state.overflow = true;
                    }
                }
            }
        }
        // Any discarded error is destroyed after the state locks are released.
        drop(event);
    }

    /// Return this waiter's snapshot. An empty epoch stays open.
    pub(super) fn snapshot(&self, epoch: &Arc<FailureEpoch>) -> Option<Arc<Error>> {
        let mut state = self.state.lock();
        let event = epoch.error.lock().as_ref().map(Arc::clone);
        if event.is_some() && Arc::ptr_eq(&state.current, epoch) {
            state.sealed = true;
        }
        event
    }

    /// Called only at a public return that actually reports this error.
    pub(super) fn acknowledge(&self, epoch: &Arc<FailureEpoch>, event: &Arc<Error>) {
        let old = {
            let mut state = self.state.lock();
            if !state.sealed || !Arc::ptr_eq(&state.current, epoch) {
                return;
            }
            let matched = epoch
                .error
                .lock()
                .as_ref()
                .is_some_and(|stored| Arc::ptr_eq(stored, event));
            if !matched {
                return;
            }
            let next = state.next.take();
            let replacement = Arc::new(FailureEpoch {
                error: Mutex::new(next),
            });
            if state.overflow {
                state.next = Some(Arc::clone(&self.coalesced));
                state.overflow = false;
            }
            state.sealed = false;
            std::mem::replace(&mut state.current, replacement)
        };
        // The last reference to an old error may invoke user-defined Drop.
        drop(old);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    fn event(label: &str) -> Error {
        Error::protocol(label)
    }

    fn label(error: &Arc<Error>) -> &str {
        match error.as_ref() {
            Error::Protocol(message) => message,
            other => panic!("unexpected category: {other:?}"),
        }
    }

    fn snapshot(failures: &WorkerFailures, epoch: &Arc<FailureEpoch>, expected: &str) -> Arc<Error> {
        let result = failures.snapshot(epoch).expect("expected failure");
        assert_eq!(label(&result), expected);
        result
    }

    #[test]
    fn empty_control_does_not_hide_later_failure_from_an_already_registered_waiter() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        let second = failures.register();
        assert!(failures.snapshot(&first).is_none());
        failures.record(event("A"));
        let a = snapshot(&failures, &second, "A");
        failures.acknowledge(&second, &a);
        assert!(failures.snapshot(&failures.register()).is_none());
    }

    #[test]
    fn canceled_first_waiter_preserves_first_next_and_overflow() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        failures.record(event("A"));
        let a = snapshot(&failures, &first, "A");
        failures.record(event("B"));
        failures.record(event("C"));
        // First response is sent but unpolled; dropping it is not an ack.
        drop(a);
        drop(first);
        let survivor = failures.register();
        let a = snapshot(&failures, &survivor, "A");
        failures.acknowledge(&survivor, &a);
        let next = failures.register();
        let b = snapshot(&failures, &next, "B");
        failures.acknowledge(&next, &b);
        let summary = failures.register();
        let marker = snapshot(&failures, &summary, "additional worker errors were coalesced");
        failures.acknowledge(&summary, &marker);
        assert!(failures.snapshot(&failures.register()).is_none());
    }

    #[test]
    fn reverse_old_acknowledgments_cannot_clear_new_errors() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        let second = failures.register();
        failures.record(event("A"));
        let a1 = snapshot(&failures, &first, "A");
        let a2 = snapshot(&failures, &second, "A");
        failures.record(event("B"));
        failures.acknowledge(&second, &a2);
        let third = failures.register();
        let b = snapshot(&failures, &third, "B");
        failures.record(event("C"));
        failures.acknowledge(&first, &a1);
        failures.acknowledge(&third, &b);
        let fourth = failures.register();
        let c = snapshot(&failures, &fourth, "C");
        failures.acknowledge(&first, &a1);
        assert_eq!(label(&snapshot(&failures, &fourth, "C")), "C");
        failures.acknowledge(&fourth, &c);
    }

    #[test]
    fn overflow_summary_is_not_starved_by_new_errors() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        failures.record(event("A"));
        let a = snapshot(&failures, &first, "A");
        failures.record(event("B"));
        failures.record(event("C"));
        failures.acknowledge(&first, &a);
        let second = failures.register();
        let b = snapshot(&failures, &second, "B");
        failures.record(event("D"));
        failures.acknowledge(&second, &b);
        let third = failures.register();
        let summary = snapshot(&failures, &third, "additional worker errors were coalesced");
        failures.acknowledge(&third, &summary);
    }

    #[test]
    fn concurrent_record_and_ack_preserve_the_later_error() {
        for _ in 0..256 {
            let failures = WorkerFailures::new();
            let first = failures.register();
            failures.record(event("A"));
            let a = snapshot(&failures, &first, "A");
            let barrier = Arc::new(Barrier::new(3));
            std::thread::scope(|scope| {
                let left = Arc::clone(&failures);
                let left_barrier = Arc::clone(&barrier);
                let left_thread = scope.spawn(move || {
                    let _arrival = left_barrier.wait();
                    left.record(event("B"));
                });
                let right = Arc::clone(&failures);
                let right_barrier = Arc::clone(&barrier);
                let right_thread = scope.spawn(move || {
                    let _arrival = right_barrier.wait();
                    right.acknowledge(&first, &a);
                });
                let _arrival = barrier.wait();
                left_thread.join().expect("record thread");
                right_thread.join().expect("ack thread");
            });
            let next = failures.register();
            let b = snapshot(&failures, &next, "B");
            failures.acknowledge(&next, &b);
        }
    }

    #[test]
    fn new_error_behind_unsealed_summary_is_not_silently_discarded() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        failures.record(event("A"));
        let a = snapshot(&failures, &first, "A");
        failures.record(event("B"));
        failures.record(event("C"));
        failures.acknowledge(&first, &a);
        let second = failures.register();
        let b = snapshot(&failures, &second, "B");
        failures.acknowledge(&second, &b);
        failures.record(event("D"));
        let summary = failures.register();
        let marker = snapshot(&failures, &summary, "additional worker errors were coalesced");
        failures.acknowledge(&summary, &marker);
        let fourth = failures.register();
        let d = snapshot(&failures, &fourth, "D");
        failures.acknowledge(&fourth, &d);
    }

    #[test]
    fn record_before_ack_promotes_later_error() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        failures.record(event("A"));
        let a = snapshot(&failures, &first, "A");
        failures.record(event("B"));
        failures.acknowledge(&first, &a);
        let next = failures.register();
        let b = snapshot(&failures, &next, "B");
        failures.acknowledge(&next, &b);
    }

    #[test]
    fn ack_before_record_places_later_error_in_new_epoch() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        failures.record(event("A"));
        let a = snapshot(&failures, &first, "A");
        failures.acknowledge(&first, &a);
        failures.record(event("B"));
        let next = failures.register();
        let b = snapshot(&failures, &next, "B");
        failures.acknowledge(&next, &b);
    }

    #[test]
    fn marker_then_exact_error_then_new_overflow_remain_visible() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        failures.record(event("A"));
        let a = snapshot(&failures, &first, "A");
        failures.record(event("B"));
        failures.record(event("C"));
        failures.acknowledge(&first, &a);
        let second = failures.register();
        let b = snapshot(&failures, &second, "B");
        failures.acknowledge(&second, &b);
        let third = failures.register();
        let summary = snapshot(&failures, &third, "additional worker errors were coalesced");
        failures.record(event("D"));
        failures.record(event("E"));
        failures.acknowledge(&third, &summary);
        let fourth = failures.register();
        let d = snapshot(&failures, &fourth, "D");
        failures.acknowledge(&fourth, &d);
        let fifth = failures.register();
        let summary2 = snapshot(&failures, &fifth, "additional worker errors were coalesced");
        failures.acknowledge(&fifth, &summary2);
    }


    #[test]
    fn late_summary_ack_cannot_clear_later_epoch_with_same_marker_arc() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        failures.record(event("A"));
        let a = snapshot(&failures, &first, "A");
        failures.record(event("B"));
        failures.record(event("C"));
        failures.acknowledge(&first, &a);
        let second = failures.register();
        let b = snapshot(&failures, &second, "B");
        failures.acknowledge(&second, &b);
        let old_summary = failures.register();
        let old_marker = snapshot(&failures, &old_summary, "additional worker errors were coalesced");
        failures.record(event("D"));
        failures.record(event("E"));
        failures.acknowledge(&old_summary, &old_marker);
        let fourth = failures.register();
        let d = snapshot(&failures, &fourth, "D");
        failures.acknowledge(&fourth, &d);
        let new_summary = failures.register();
        let new_marker = snapshot(&failures, &new_summary, "additional worker errors were coalesced");
        assert!(Arc::ptr_eq(&old_marker, &new_marker));
        failures.acknowledge(&old_summary, &old_marker);
        assert!(Arc::ptr_eq(
            &snapshot(&failures, &new_summary, "additional worker errors were coalesced"),
            &new_marker
        ));
        failures.acknowledge(&new_summary, &new_marker);
    }

    #[derive(Debug)]
    struct BlockingDrop {
        entered: std::sync::mpsc::Sender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl std::fmt::Display for BlockingDrop {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("private blocking drop")
        }
    }
    impl std::error::Error for BlockingDrop {}
    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            let _sent = self.entered.send(());
            let _release = self.release.lock().recv();
        }
    }

    #[test]
    fn private_suppressed_error_destructor_can_block_record_thread_without_holding_lock() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        failures.record(event("A"));
        let a = snapshot(&failures, &first, "A");
        failures.record(event("B"));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let payload = BlockingDrop {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        };
        struct ReleaseOnDrop(Option<std::sync::mpsc::Sender<()>>);
        impl ReleaseOnDrop {
            fn release(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _sent = sender.send(());
                }
            }
        }
        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                self.release();
            }
        }
        let mut release = ReleaseOnDrop(Some(release_tx));
        let worker = Arc::clone(&failures);
        let record = std::thread::spawn(move || {
            worker.record(Error::Io(std::io::Error::other(payload)));
        });
        entered_rx.recv_timeout(std::time::Duration::from_secs(3)).expect("drop entered");
        let (observed_tx, observed_rx) = std::sync::mpsc::channel();
        let observer = Arc::clone(&failures);
        let observer_epoch = Arc::clone(&first);
        let read = std::thread::spawn(move || {
            let value = observer.snapshot(&observer_epoch).map(|error| label(&error).to_string());
            let _sent = observed_tx.send(value);
        });
        let before_release = observed_rx.recv_timeout(std::time::Duration::from_millis(500));
        release.release();
        record.join().expect("record thread");
        read.join().expect("snapshot thread");
        assert_eq!(before_release.expect("snapshot held state lock"), Some("A".to_string()));
        failures.acknowledge(&first, &a);
        let next = failures.register();
        let b = snapshot(&failures, &next, "B");
        failures.acknowledge(&next, &b);
        let summary = failures.register();
        let marker = snapshot(&failures, &summary, "additional worker errors were coalesced");
        failures.acknowledge(&summary, &marker);
    }

    #[derive(Debug)]
    struct PanicOnDrop;
    impl std::fmt::Display for PanicOnDrop {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("private panic drop")
        }
    }
    impl std::error::Error for PanicOnDrop {}
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("PRIVATE_ERROR_DROP_PANIC");
        }
    }

    #[test]
    fn private_suppressed_error_destructor_can_unwind_record_after_unlock() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        failures.record(event("A"));
        let a = snapshot(&failures, &first, "A");
        failures.record(event("B"));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            failures.record(Error::Io(std::io::Error::other(PanicOnDrop)));
        }));
        let panic_payload = outcome.expect_err("private destructor must panic");
        let message = panic_payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic_payload.downcast_ref::<String>().map(String::as_str));
        assert_eq!(message, Some("PRIVATE_ERROR_DROP_PANIC"));
        assert_eq!(label(&snapshot(&failures, &first, "A")), "A");
        failures.acknowledge(&first, &a);
        let next = failures.register();
        let b = snapshot(&failures, &next, "B");
        failures.acknowledge(&next, &b);
        let summary = failures.register();
        let marker = snapshot(&failures, &summary, "additional worker errors were coalesced");
        failures.acknowledge(&summary, &marker);
    }

    #[test]
    #[ignore = "negative control: the draft hides a timeout behind a generic summary"]
    fn overflow_must_not_hide_worker_timeout() {
        let failures = WorkerFailures::new();
        let first = failures.register();
        failures.record(event("A"));
        let a = snapshot(&failures, &first, "A");
        failures.record(event("B"));
        failures.record(Error::Timeout);
        failures.acknowledge(&first, &a);
        let second = failures.register();
        let b = snapshot(&failures, &second, "B");
        failures.acknowledge(&second, &b);
        let summary = failures.register();
        let result = failures.snapshot(&summary).expect("overflow must be reported");
        assert!(
            matches!(result.as_ref(), Error::Timeout),
            "OVERFLOW_HIDES_TIMEOUT: {result:?}"
        );
    }

}
