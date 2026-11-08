//! Experimental fail-closed completion tracking.
//!
//! This draft does not own record payloads, callbacks, receipts, or byte
//! reservations. It must not be merged as a production completion barrier.

use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::Mutex;

const RECORD_LIMIT: usize = 100_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CompletionStatus {
    Pending,
    Complete,
    Lost,
}

#[derive(Default)]
struct State {
    used: usize,
    last: u64,
    pending: BTreeSet<u64>,
    lost: Option<u64>,
    exhausted: bool,
}

pub(super) struct CompletionDomain {
    limit: usize,
    state: Mutex<State>,
}

pub(super) struct CompletionReservation {
    domain: Option<Arc<CompletionDomain>>,
}

pub(super) struct CompletionTicket {
    domain: Arc<CompletionDomain>,
    sequence: Option<u64>,
}

impl CompletionDomain {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            limit: RECORD_LIMIT,
            state: Mutex::new(State::default()),
        })
    }

    pub(super) fn try_reserve(self: &Arc<Self>) -> Result<Option<CompletionReservation>, ()> {
        let mut state = self.state.lock();
        if state.exhausted {
            return Err(());
        }
        if state.used >= self.limit {
            return Ok(None);
        }
        state.used += 1;
        Ok(Some(CompletionReservation {
            domain: Some(Arc::clone(self)),
        }))
    }

    #[cfg(test)]
    pub(super) fn exhaust_sequence_for_test(&self) {
        self.state.lock().last = u64::MAX;
    }

    pub(super) fn cutoff(&self) -> u64 {
        self.state.lock().last
    }

    pub(super) fn status(&self, cutoff: u64) -> CompletionStatus {
        let state = self.state.lock();
        if state.lost.is_some_and(|lost| lost <= cutoff) {
            CompletionStatus::Lost
        } else if state.pending.first().is_some_and(|first| *first <= cutoff) {
            CompletionStatus::Pending
        } else {
            CompletionStatus::Complete
        }
    }

    fn finish(&self, sequence: u64, lost: bool) {
        let mut state = self.state.lock();
        if state.pending.remove(&sequence) {
            state.used -= 1;
            if lost {
                state.lost = Some(state.lost.map_or(sequence, |old| old.min(sequence)));
            }
        }
    }
}

impl CompletionReservation {
    pub(super) fn exhausted(&self) -> bool {
        self.domain
            .as_ref()
            .is_some_and(|domain| domain.state.lock().exhausted)
    }

    pub(super) fn commit(&mut self) -> Option<CompletionTicket> {
        let domain = Arc::clone(self.domain.as_ref()?);
        let mut state = domain.state.lock();
        let Some(sequence) = state.last.checked_add(1) else {
            state.exhausted = true;
            return None;
        };
        state.last = sequence;
        let _ = state.pending.insert(sequence);
        drop(state);
        self.domain = None;
        Some(CompletionTicket {
            domain,
            sequence: Some(sequence),
        })
    }
}

impl Drop for CompletionReservation {
    fn drop(&mut self) {
        if let Some(domain) = self.domain.take() {
            domain.state.lock().used -= 1;
        }
    }
}

impl CompletionTicket {
    pub(super) fn complete(mut self) {
        if let Some(sequence) = self.sequence.take() {
            self.domain.finish(sequence, false);
        }
    }
}

impl Drop for CompletionTicket {
    fn drop(&mut self) {
        if let Some(sequence) = self.sequence.take() {
            self.domain.finish(sequence, true);
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn admit(domain: &Arc<CompletionDomain>) -> CompletionTicket {
        domain.try_reserve().unwrap().unwrap().commit().unwrap()
    }

    #[test]
    fn overlapping_cutoffs_cover_old_records_but_not_later_records() {
        let domain = CompletionDomain::new();
        let old = admit(&domain);
        let first = domain.cutoff();
        let second = domain.cutoff();
        let later = admit(&domain);
        assert_eq!(domain.status(first), CompletionStatus::Pending);
        assert_eq!(domain.status(second), CompletionStatus::Pending);
        old.complete();
        assert_eq!(domain.status(first), CompletionStatus::Complete);
        assert_eq!(domain.status(second), CompletionStatus::Complete);
        assert_eq!(domain.status(domain.cutoff()), CompletionStatus::Pending);
        later.complete();
    }

    #[test]
    fn dropped_ticket_fails_closed_without_poisoning_earlier_cutoff() {
        let domain = CompletionDomain::new();
        let before = domain.cutoff();
        let ticket = admit(&domain);
        let after = domain.cutoff();
        drop(ticket);
        assert_eq!(domain.status(before), CompletionStatus::Complete);
        assert_eq!(domain.status(after), CompletionStatus::Lost);
    }

    #[test]
    fn canceled_uncommitted_reservation_is_not_an_admitted_record() {
        let domain = CompletionDomain::new();
        let reservation = domain.try_reserve().unwrap().unwrap();
        drop(reservation);
        assert_eq!(domain.cutoff(), 0);
        assert_eq!(domain.status(0), CompletionStatus::Complete);
        assert_eq!(domain.state.lock().used, 0);
    }

    #[test]
    fn reservations_bound_zero_byte_records() {
        let domain = Arc::new(CompletionDomain {
            limit: 1,
            state: Mutex::new(State::default()),
        });
        let reservation = domain.try_reserve().unwrap().unwrap();
        assert!(domain.try_reserve().unwrap().is_none());
        drop(reservation);
        assert!(domain.try_reserve().unwrap().is_some());
    }

    #[test]
    fn sequence_exhaustion_rejects_new_records_without_wrapping() {
        let domain = CompletionDomain::new();
        domain.state.lock().last = u64::MAX - 1;
        let last = admit(&domain);
        assert_eq!(domain.cutoff(), u64::MAX);
        last.complete();
        let mut reservation = domain.try_reserve().unwrap().unwrap();
        assert!(reservation.commit().is_none());
        drop(reservation);
        assert!(domain.try_reserve().is_err());
    }
}
