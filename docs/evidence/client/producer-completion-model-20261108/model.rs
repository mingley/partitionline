//! Isolated completion-domain model; this is not wired into Producer.
//!
//! The caller manually polls Flush. There is no executor notification or
//! automatic cleanup. All payload, callback and broker effects are modeled by
//! explicit state transitions. See README.md for the unimplemented obligations.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Lease {
    sequence: u64,
    incarnation: u64,
    worker: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Publishing(u64),
    Queued(u64),
    InFlight(u64),
    Retry,
    Finalizing { released: bool, result: bool, callback: bool },
}

#[derive(Debug)]
struct Slot {
    incarnation: u64,
    phase: Phase,
}

#[derive(Debug, Eq, PartialEq)]
enum DomainError {
    Closed,
    Full,
    WorkerRetired,
    InvalidWorker,
    Exhausted,
    Stale,
    Busy,
    Incomplete,
}

#[derive(Debug, Eq, PartialEq)]
enum FlushError {
    Deadline,
    Recorded(&'static str),
}

struct State {
    capacity: usize,
    last: u64,
    prefix: u64,
    slots: BTreeMap<u64, Slot>,
    intervals: BTreeMap<u64, u64>,
    retired: BTreeSet<u64>,
    closed: bool,
    recorded_error: Option<&'static str>,
}

struct Domain {
    state: Mutex<State>,
}

impl Domain {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                capacity,
                last: 0,
                prefix: 0,
                slots: BTreeMap::new(),
                intervals: BTreeMap::new(),
                retired: BTreeSet::new(),
                closed: false,
                recorded_error: None,
            }),
        })
    }

    // This lock represents the admission/cutoff linearization point. The
    // prototype has no actual channel permit, payload, or sticky policy.
    fn admit(&self, worker: u64) -> Result<Lease, DomainError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(DomainError::Closed);
        }
        if worker >= 64 {
            return Err(DomainError::InvalidWorker);
        }
        if state.retired.contains(&worker) {
            return Err(DomainError::WorkerRetired);
        }
        if state.slots.len() == state.capacity {
            return Err(DomainError::Full);
        }
        let Some(sequence) = state.last.checked_add(1) else {
            state.closed = true;
            return Err(DomainError::Exhausted);
        };
        state.last = sequence;
        state.slots.insert(sequence, Slot { incarnation: 0, phase: Phase::Publishing(worker) });
        Ok(Lease { sequence, incarnation: 0, worker })
    }

    // Represents successful return from channel publication; dequeue may have
    // won while the publisher was waking the receiver.
    fn published(&self, lease: Lease) -> Result<(), DomainError> {
        let mut state = self.state.lock().unwrap();
        let slot = state.slots.get_mut(&lease.sequence).ok_or(DomainError::Stale)?;
        if slot.incarnation != lease.incarnation {
            return Err(DomainError::Stale);
        }
        match slot.phase {
            Phase::Publishing(worker) if worker == lease.worker => {
                slot.phase = Phase::Queued(worker);
                Ok(())
            }
            Phase::InFlight(worker) if worker == lease.worker => Ok(()),
            _ => Err(DomainError::Stale),
        }
    }

    fn claim(&self, lease: Lease) -> Result<(), DomainError> {
        let mut state = self.state.lock().unwrap();
        if state.retired.contains(&lease.worker) {
            return Err(DomainError::WorkerRetired);
        }
        let slot = state.slots.get_mut(&lease.sequence).ok_or(DomainError::Stale)?;
        if slot.incarnation != lease.incarnation {
            return Err(DomainError::Stale);
        }
        match slot.phase {
            Phase::Publishing(worker) | Phase::Queued(worker) if worker == lease.worker => {
                slot.phase = Phase::InFlight(worker);
                Ok(())
            }
            _ => Err(DomainError::Stale),
        }
    }

    fn retire_worker(&self, worker: u64) {
        assert!(worker < 64, "this model only admits 64 distinct worker IDs");
        self.state.lock().unwrap().retired.insert(worker);
    }

    // This arbitration invalidates a possibly enqueued descriptor. An
    // in-flight lease cannot be recovered by this pre-write operation.
    fn recover_unclaimed(&self, lease: Lease) -> Result<(), DomainError> {
        let mut state = self.state.lock().unwrap();
        let slot = state.slots.get_mut(&lease.sequence).ok_or(DomainError::Stale)?;
        if slot.incarnation != lease.incarnation {
            return Err(DomainError::Stale);
        }
        match slot.phase {
            Phase::Publishing(worker) | Phase::Queued(worker) if worker == lease.worker => {
                let Some(incarnation) = slot.incarnation.checked_add(1) else {
                    state.closed = true;
                    return Err(DomainError::Exhausted);
                };
                slot.incarnation = incarnation;
                slot.phase = Phase::Retry;
                Ok(())
            }
            Phase::InFlight(worker) if worker == lease.worker => Err(DomainError::Busy),
            _ => Err(DomainError::Stale),
        }
    }

    // The caller must have separately established that the old in-flight owner
    // can no longer produce side effects. Kafka batch ambiguity is not modeled.
    fn recover_after_owner_stopped(&self, lease: Lease) -> Result<(), DomainError> {
        let mut state = self.state.lock().unwrap();
        let slot = state.slots.get_mut(&lease.sequence).ok_or(DomainError::Stale)?;
        if slot.incarnation != lease.incarnation || slot.phase != Phase::InFlight(lease.worker) {
            return Err(DomainError::Stale);
        }
        let Some(incarnation) = slot.incarnation.checked_add(1) else {
            state.closed = true;
            return Err(DomainError::Exhausted);
        };
        slot.incarnation = incarnation;
        slot.phase = Phase::Retry;
        Ok(())
    }

    fn retry(&self, sequence: u64, worker: u64) -> Result<Lease, DomainError> {
        let mut state = self.state.lock().unwrap();
        if worker >= 64 {
            return Err(DomainError::InvalidWorker);
        }
        if state.retired.contains(&worker) {
            return Err(DomainError::WorkerRetired);
        }
        let slot = state.slots.get_mut(&sequence).ok_or(DomainError::Stale)?;
        if slot.phase != Phase::Retry {
            return Err(DomainError::Busy);
        }
        slot.phase = Phase::Publishing(worker);
        Ok(Lease { sequence, incarnation: slot.incarnation, worker })
    }

    fn begin_finalization(&self, lease: Lease) -> Result<(), DomainError> {
        let mut state = self.state.lock().unwrap();
        let slot = state.slots.get_mut(&lease.sequence).ok_or(DomainError::Stale)?;
        if slot.incarnation != lease.incarnation || slot.phase != Phase::InFlight(lease.worker) {
            return Err(DomainError::Stale);
        }
        slot.phase = Phase::Finalizing { released: false, result: false, callback: false };
        Ok(())
    }

    // These flags are witness inputs, not implementations of the effects.
    fn mark_finalization(&self, lease: Lease, released: bool, result: bool, callback: bool) -> Result<(), DomainError> {
        let mut state = self.state.lock().unwrap();
        let slot = state.slots.get_mut(&lease.sequence).ok_or(DomainError::Stale)?;
        if slot.incarnation != lease.incarnation {
            return Err(DomainError::Stale);
        }
        let Phase::Finalizing { released: r, result: s, callback: c } = &mut slot.phase else {
            return Err(DomainError::Stale);
        };
        *r |= released;
        *s |= result;
        *c |= callback;
        Ok(())
    }

    fn finish(&self, lease: Lease) -> Result<(), DomainError> {
        let mut state = self.state.lock().unwrap();
        let slot = state.slots.get(&lease.sequence).ok_or(DomainError::Stale)?;
        if slot.incarnation != lease.incarnation {
            return Err(DomainError::Stale);
        }
        if slot.phase != (Phase::Finalizing { released: true, result: true, callback: true }) {
            return Err(DomainError::Incomplete);
        }
        state.slots.remove(&lease.sequence);
        state.insert_complete(lease.sequence);
        Ok(())
    }

    fn record_worker_error(&self, message: &'static str) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.recorded_error.is_some() {
            return false;
        }
        state.recorded_error = Some(message);
        true
    }

    fn flush(self: &Arc<Self>, deadline: Instant) -> Flush {
        let cutoff = self.state.lock().unwrap().last;
        Flush { domain: Arc::clone(self), cutoff, deadline, done: false }
    }

    fn close_and_cutoff(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        state.last
    }
}

impl State {
    fn insert_complete(&mut self, sequence: u64) {
        let mut start = sequence;
        let mut end = sequence;
        if let Some((&left, &right)) = self.intervals.range(..=sequence).next_back() {
            assert!(right < sequence, "duplicate completion");
            if right.checked_add(1) == Some(sequence) {
                start = left;
                self.intervals.remove(&left);
            }
        }
        if let Some((&left, &right)) = self.intervals.range(sequence..).next() {
            if end.checked_add(1) == Some(left) {
                end = right;
                self.intervals.remove(&left);
            }
        }
        if self.prefix.checked_add(1) == Some(start) {
            self.prefix = end;
        } else {
            self.intervals.insert(start, end);
        }
    }
}

struct Flush {
    domain: Arc<Domain>,
    cutoff: u64,
    deadline: Instant,
    done: bool,
}

impl Future for Flush {
    type Output = Result<(), FlushError>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "a completed model future was polled again");
        let answer = {
            let mut state = self.domain.state.lock().unwrap();
            if Instant::now() >= self.deadline {
                Some(Err(FlushError::Deadline))
            } else if state.prefix < self.cutoff {
                None
            } else {
                Some(match state.recorded_error.take() {
                    Some(error) => Err(FlushError::Recorded(error)),
                    None => Ok(()),
                })
            }
        };
        match answer {
            Some(answer) => {
                self.done = true;
                Poll::Ready(answer)
            }
            None => Poll::Pending,
        }
    }
}

fn exercise(domain: &Domain) {
    let lease = domain.admit(1).unwrap();
    domain.published(lease).unwrap();
    domain.claim(lease).unwrap();
    domain.begin_finalization(lease).unwrap();
    domain.mark_finalization(lease, true, true, true).unwrap();
    domain.finish(lease).unwrap();
}

fn main() {
    let iterations = 100_000u32;
    let mut samples = Vec::new();
    for _ in 0..9 {
        let domain = Domain::new(32);
        let start = Instant::now();
        for _ in 0..iterations {
            exercise(std::hint::black_box(&domain));
        }
        let nanos = start.elapsed().as_nanos() / u128::from(iterations);
        samples.push(nanos);
    }
    samples.sort_unstable();
    println!("model admitted-to-finish ns/record sorted={samples:?}");
    println!("model median_ns={} iterations_per_sample={iterations}", samples[4]);
    println!("model limitations: no broker, callback, wake, payload, retries, allocator baseline, or production comparison");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, Mutex};
    use std::task::{Wake, Waker};
    use std::thread;
    use std::time::Duration;

    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }

    fn poll_flush(flush: &mut Flush) -> Poll<Result<(), FlushError>> {
        let waker = Waker::from(Arc::new(Noop));
        Pin::new(flush).poll(&mut Context::from_waker(&waker))
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(30)
    }

    fn begin(domain: &Domain, worker: u64) -> Lease {
        let lease = domain.admit(worker).unwrap();
        domain.published(lease).unwrap();
        domain.claim(lease).unwrap();
        lease
    }

    fn finish(domain: &Domain, lease: Lease) {
        domain.begin_finalization(lease).unwrap();
        domain.mark_finalization(lease, true, true, true).unwrap();
        domain.finish(lease).unwrap();
    }

    #[test]
    fn overlapping_flushes_wait_for_prior_records_and_ignore_later_records() {
        let domain = Domain::new(4);
        let first = begin(&domain, 1);
        let mut a = domain.flush(deadline());
        let mut b = domain.flush(deadline());
        assert!(poll_flush(&mut a).is_pending());
        assert!(poll_flush(&mut b).is_pending());
        drop(a);
        let later = begin(&domain, 1);
        finish(&domain, first);
        assert_eq!(poll_flush(&mut b), Poll::Ready(Ok(())));
        assert_eq!(domain.state.lock().unwrap().slots.len(), 1);
        finish(&domain, later);
    }

    #[test]
    fn terminal_publication_is_required_before_horizon_advances() {
        let domain = Domain::new(1);
        let lease = begin(&domain, 1);
        let mut flush = domain.flush(deadline());
        domain.begin_finalization(lease).unwrap();
        domain.mark_finalization(lease, true, false, true).unwrap();
        assert_eq!(domain.finish(lease), Err(DomainError::Incomplete));
        assert!(poll_flush(&mut flush).is_pending());
        domain.mark_finalization(lease, false, true, false).unwrap();
        domain.finish(lease).unwrap();
        assert_eq!(poll_flush(&mut flush), Poll::Ready(Ok(())));
    }

    #[test]
    fn unclaimed_publication_recovery_invalidates_a_queued_descriptor() {
        let domain = Domain::new(1);
        let old = domain.admit(1).unwrap();
        domain.published(old).unwrap();
        domain.recover_unclaimed(old).unwrap();
        assert_eq!(domain.claim(old), Err(DomainError::Stale));
        domain.retire_worker(1);
        assert_eq!(domain.claim(old), Err(DomainError::WorkerRetired));
        let next = domain.retry(old.sequence, 2).unwrap();
        assert_ne!(old.incarnation, next.incarnation);
        domain.claim(next).unwrap();
        domain.published(next).unwrap();
        assert_eq!(domain.recover_unclaimed(next), Err(DomainError::Busy));
        finish(&domain, next);
        assert_eq!(domain.finish(next), Err(DomainError::Stale));
    }

    #[test]
    fn claimed_owner_requires_explicit_stop_before_retry() {
        let domain = Domain::new(1);
        let old = begin(&domain, 1);
        domain.retire_worker(1);
        assert_eq!(domain.recover_unclaimed(old), Err(DomainError::Busy));
        domain.recover_after_owner_stopped(old).unwrap();
        let next = domain.retry(old.sequence, 2).unwrap();
        domain.claim(next).unwrap();
        finish(&domain, next);
    }

    #[test]
    fn concurrent_receiver_claim_and_recovery_have_one_winner() {
        for _ in 0..250 {
            let domain = Domain::new(1);
            let lease = domain.admit(1).unwrap();
            let barrier = Arc::new(Barrier::new(3));
            let left_domain = Arc::clone(&domain);
            let left_barrier = Arc::clone(&barrier);
            let left = thread::spawn(move || {
                left_barrier.wait();
                left_domain.claim(lease)
            });
            let right_domain = Arc::clone(&domain);
            let right_barrier = Arc::clone(&barrier);
            let right = thread::spawn(move || {
                right_barrier.wait();
                right_domain.recover_unclaimed(lease)
            });
            barrier.wait();
            let claimed = left.join().unwrap();
            let recovered = right.join().unwrap();
            assert_ne!(claimed.is_ok(), recovered.is_ok());
            let winner = if claimed.is_ok() {
                lease
            } else {
                let next = domain.retry(lease.sequence, 2).unwrap();
                domain.claim(next).unwrap();
                next
            };
            finish(&domain, winner);
        }
    }

    #[test]
    fn canceled_or_expired_flush_does_not_claim_recorded_error() {
        let domain = Domain::new(2);
        let lease = begin(&domain, 1);
        assert!(domain.record_worker_error("recorded worker failure"));
        assert!(!domain.record_worker_error("coalesced failure"));
        let mut canceled = domain.flush(deadline());
        assert!(poll_flush(&mut canceled).is_pending());
        drop(canceled);
        let mut expired = domain.flush(Instant::now());
        assert_eq!(poll_flush(&mut expired), Poll::Ready(Err(FlushError::Deadline)));
        finish(&domain, lease);
        let mut claimant = domain.flush(deadline());
        let mut overlap = domain.flush(deadline());
        assert_eq!(poll_flush(&mut claimant), Poll::Ready(Err(FlushError::Recorded("recorded worker failure"))));
        assert_eq!(poll_flush(&mut overlap), Poll::Ready(Ok(())));
    }


    #[test]
    fn recorded_delivery_timeout_is_not_the_flush_deadline() {
        let domain = Domain::new(1);
        let lease = begin(&domain, 1);
        assert!(domain.record_worker_error("delivery timeout"));
        finish(&domain, lease);
        let mut flush = domain.flush(deadline());
        assert_eq!(poll_flush(&mut flush), Poll::Ready(Err(FlushError::Recorded("delivery timeout"))));
    }

    #[test]
    fn close_cutoff_capacity_and_exhaustion_fail_closed() {
        let domain = Domain::new(1);
        assert_eq!(domain.admit(64), Err(DomainError::InvalidWorker));
        let lease = begin(&domain, 1);
        assert_eq!(domain.admit(1), Err(DomainError::Full));
        assert_eq!(domain.close_and_cutoff(), lease.sequence);
        assert_eq!(domain.admit(1), Err(DomainError::Closed));
        finish(&domain, lease);
        let domain = Domain::new(1);
        domain.state.lock().unwrap().last = u64::MAX;
        assert_eq!(domain.admit(1), Err(DomainError::Exhausted));
        assert_eq!(domain.admit(1), Err(DomainError::Closed));
    }

    #[test]
    fn interval_horizon_matches_reference_under_random_out_of_order_completion() {
        let domain = Domain::new(16);
        let mut active: Vec<Lease> = Vec::new();
        let mut snapshots: Vec<(u64, Flush)> = Vec::new();
        let mut random = 0x5eed_1234_9abc_def0u64;
        for _ in 0..50_000 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            match random % 4 {
                0 if active.len() < 16 => active.push(begin(&domain, 1)),
                1 if !active.is_empty() => {
                    let index = (random as usize) % active.len();
                    let lease = active.swap_remove(index);
                    finish(&domain, lease);
                }
                2 if snapshots.len() < 16 => {
                    let flush = domain.flush(deadline());
                    snapshots.push((flush.cutoff, flush));
                }
                _ if !snapshots.is_empty() => {
                    let index = (random as usize) % snapshots.len();
                    let (cutoff, mut flush) = snapshots.swap_remove(index);
                    let expected_ready = active.iter().all(|lease| lease.sequence > cutoff);
                    assert_eq!(poll_flush(&mut flush).is_ready(), expected_ready);
                }
                _ => {}
            }
            let state = domain.state.lock().unwrap();
            assert!(state.intervals.len() <= state.slots.len());
            assert!(state.slots.len() <= state.capacity);
        }
        for lease in active {
            finish(&domain, lease);
        }
        let state = domain.state.lock().unwrap();
        assert_eq!(state.prefix, state.last);
        assert!(state.intervals.is_empty());
    }

    #[test]
    fn incarnation_exhaustion_keeps_slot_pending_and_closes_admission() {
        let domain = Domain::new(1);
        let mut lease = domain.admit(1).unwrap();
        {
            let mut state = domain.state.lock().unwrap();
            state.slots.get_mut(&lease.sequence).unwrap().incarnation = u64::MAX;
        }
        lease.incarnation = u64::MAX;
        assert_eq!(domain.recover_unclaimed(lease), Err(DomainError::Exhausted));
        assert_eq!(domain.admit(2), Err(DomainError::Closed));
        let mut flush = domain.flush(deadline());
        assert!(poll_flush(&mut flush).is_pending());
    }
}
