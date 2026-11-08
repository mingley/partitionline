//! Isolated sequence-ledger model. Not connected to the producer.
use std::collections::VecDeque;

const MAX: i32 = i32::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    InvalidCount,
    Full,
    Exhausted,
    Fenced,
    Stale,
    NotHead,
    NotTail,
    WrongState,
    Busy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Ticket {
    key: u64,
    id: u64,
    generation: u64,
    base: i32,
    count: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Reserved,
    Writing,
    Ambiguous,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Entry {
    ticket: Ticket,
    phase: Phase,
}

/// Bounded one-partition model. Its caller supplies exclusive ownership and
/// keeps every accepted batch rooted across cancellation.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Ledger {
    key: u64,
    generation: u64,
    next_ticket: u64,
    next_sequence: i32,
    bound: usize,
    fenced: bool,
    unsettled: Option<Ticket>,
    entries: VecDeque<Entry>,
}

impl Ledger {
    fn new(bound: usize, next_sequence: i32) -> Self {
        Self::for_key(0, bound, next_sequence)
    }

    fn for_key(key: u64, bound: usize, next_sequence: i32) -> Self {
        assert!(bound > 0 && next_sequence >= 0);
        Self {
            key,
            generation: 0,
            next_ticket: 1,
            next_sequence,
            bound,
            fenced: false,
            unsettled: None,
            entries: VecDeque::with_capacity(bound),
        }
    }

    fn reserve(&mut self, count: i32) -> Result<Ticket, Failure> {
        if self.fenced {
            return Err(Failure::Fenced);
        }
        if self.unsettled.is_some() {
            return Err(Failure::Busy);
        }
        if count <= 0 {
            return Err(Failure::InvalidCount);
        }
        if self.entries.len() == self.bound {
            return Err(Failure::Full);
        }
        let next_ticket = self.next_ticket.checked_add(1).ok_or(Failure::Exhausted)?;
        let ticket = Ticket {
            key: self.key,
            id: self.next_ticket,
            generation: self.generation,
            base: self.next_sequence,
            count,
        };
        let next_sequence =
            ((u64::try_from(ticket.base).unwrap() + u64::try_from(count).unwrap())
                % (u64::try_from(MAX).unwrap() + 1)) as i32;
        self.next_ticket = next_ticket;
        self.next_sequence = next_sequence;
        self.entries.push_back(Entry {
            ticket,
            phase: Phase::Reserved,
        });
        Ok(ticket)
    }

    fn begin(&mut self, ticket: Ticket) -> Result<Ticket, Failure> {
        if self.fenced {
            return Err(Failure::Fenced);
        }
        if self.unsettled.is_some_and(|unsettled| unsettled != ticket) {
            return Err(Failure::Busy);
        }
        let head = self.entries.front_mut().ok_or(Failure::Stale)?;
        if head.ticket != ticket {
            return if self.entries.iter().any(|entry| entry.ticket == ticket) {
                Err(Failure::NotHead)
            } else {
                Err(Failure::Stale)
            };
        }
        if head.phase == Phase::Writing {
            return Err(Failure::WrongState);
        }
        head.phase = Phase::Writing;
        Ok(ticket)
    }

    fn ambiguous(&mut self, ticket: Ticket) -> Result<(), Failure> {
        let head = self.entries.front_mut().ok_or(Failure::Stale)?;
        if head.ticket != ticket {
            return Err(Failure::Stale);
        }
        if head.phase != Phase::Writing {
            return Err(Failure::WrongState);
        }
        head.phase = Phase::Ambiguous;
        self.unsettled = Some(ticket);
        Ok(())
    }

    fn acknowledge(&mut self, ticket: Ticket) -> Result<(), Failure> {
        let head = self.entries.front().ok_or(Failure::Stale)?;
        if head.ticket != ticket {
            return Err(Failure::Stale);
        }
        if head.phase != Phase::Writing {
            return Err(Failure::WrongState);
        }
        let _ = self.entries.pop_front();
        Ok(())
    }

    fn rollback(&mut self, ticket: Ticket) -> Result<(), Failure> {
        let tail = self.entries.back().ok_or(Failure::Stale)?;
        if tail.ticket != ticket {
            return if self.entries.iter().any(|entry| entry.ticket == ticket) {
                Err(Failure::NotTail)
            } else {
                Err(Failure::Stale)
            };
        }
        if tail.phase != Phase::Reserved {
            return Err(Failure::WrongState);
        }
        let _ = self.entries.pop_back();
        self.next_sequence = ticket.base;
        Ok(())
    }

    fn fence(&mut self, ticket: Ticket) -> Result<(), Failure> {
        if !self.entries.iter().any(|entry| entry.ticket == ticket) {
            return Err(Failure::Stale);
        }
        self.fenced = true;
        Ok(())
    }

    /// Requires external proof that no outstanding write claim, completion,
    /// or accepted batch still belongs to this ledger. An ambiguous attempt
    /// leaves this model permanently non-quiescent; no settlement API is modeled.
    fn new_identity(&mut self) -> Result<(), Failure> {
        if !self.entries.is_empty() || self.unsettled.is_some() {
            return Err(Failure::Busy);
        }
        let generation = self.generation.checked_add(1).ok_or(Failure::Exhausted)?;
        self.generation = generation;
        self.next_sequence = 0;
        self.fenced = false;
        Ok(())
    }
}

#[test]
fn wrap_matches_a_widened_oracle() {
    for base in [0, 1, MAX - 2, MAX - 1, MAX] {
        for count in [1, 2, 3, MAX] {
            let mut ledger = Ledger::new(5, base);
            let ticket = ledger.reserve(count).unwrap();
            assert_eq!(ticket.base, base);
            let expected = ((base as u64 + count as u64) % (1u64 << 31)) as i32;
            assert_eq!(ledger.next_sequence, expected);
        }
    }
}

#[test]
fn a_reused_wire_base_does_not_reuse_a_ticket() {
    let mut ledger = Ledger::new(5, MAX);
    let old = ledger.reserve(1).unwrap();
    assert_eq!(ledger.next_sequence, 0);
    ledger.rollback(old).unwrap();
    let current = ledger.reserve(1).unwrap();
    assert_eq!(old.base, current.base);
    assert_ne!(old.id, current.id);
    let before = ledger.clone();
    assert_eq!(ledger.rollback(old), Err(Failure::Stale));
    assert_eq!(ledger.begin(old), Err(Failure::Stale));
    assert_eq!(ledger, before);
    assert_eq!(ledger.begin(current), Ok(current));
}

#[test]
fn only_the_head_can_write_and_an_ambiguous_write_keeps_the_same_range() {
    let mut ledger = Ledger::new(5, MAX - 1);
    let first = ledger.reserve(3).unwrap();
    let second = ledger.reserve(1).unwrap();
    assert_eq!((first.base, second.base, ledger.next_sequence), (MAX - 1, 1, 2));
    assert_eq!(ledger.begin(second), Err(Failure::NotHead));
    assert_eq!(ledger.begin(first), Ok(first));
    assert_eq!(ledger.rollback(first), Err(Failure::NotTail));
    ledger.ambiguous(first).unwrap();
    assert_eq!(ledger.begin(second), Err(Failure::Busy));
    assert_eq!(ledger.begin(first), Ok(first));
    ledger.acknowledge(first).unwrap();
    // A retry acknowledgment alone does not prove the first attempt can no
    // longer arrive, so later writes stay blocked in this conservative model.
    assert_eq!(ledger.begin(second), Err(Failure::Busy));
    assert_eq!(ledger.reserve(1), Err(Failure::Busy));
    assert_eq!(ledger.new_identity(), Err(Failure::Busy));
    assert_eq!(ledger.next_sequence, 2);
}

#[test]
fn rollback_requires_the_current_unwritten_tail() {
    let mut ledger = Ledger::new(5, MAX);
    let a = ledger.reserve(1).unwrap();
    let b = ledger.reserve(2).unwrap();
    assert_eq!(ledger.rollback(a), Err(Failure::NotTail));
    ledger.begin(a).unwrap();
    ledger.rollback(b).unwrap();
    let before = ledger.clone();
    assert_eq!(ledger.rollback(a), Err(Failure::WrongState));
    assert_eq!(ledger, before);
    ledger.ambiguous(a).unwrap();
    assert_eq!(ledger.rollback(a), Err(Failure::WrongState));
    ledger.begin(a).unwrap();
    ledger.acknowledge(a).unwrap();
    assert_eq!(ledger.reserve(1), Err(Failure::Busy));
    assert_eq!(ledger.new_identity(), Err(Failure::Busy));
}

#[test]
fn ambiguous_attempt_stays_fenced_after_a_retry_is_acknowledged() {
    let mut ledger = Ledger::new(5, 7);
    let old = ledger.reserve(1).unwrap();
    ledger.begin(old).unwrap();
    ledger.ambiguous(old).unwrap();
    ledger.begin(old).unwrap();
    ledger.acknowledge(old).unwrap();
    assert!(ledger.entries.is_empty());
    assert_eq!(ledger.reserve(1), Err(Failure::Busy));
    assert_eq!(ledger.new_identity(), Err(Failure::Busy));
}

#[test]
fn identity_changes_need_quiescence_and_never_reuse_tickets() {
    let mut ledger = Ledger::new(5, 7);
    let old = ledger.reserve(1).unwrap();
    ledger.begin(old).unwrap();
    assert_eq!(ledger.new_identity(), Err(Failure::Busy));
    ledger.acknowledge(old).unwrap();
    ledger.new_identity().unwrap();
    let current = ledger.reserve(1).unwrap();
    assert_eq!((current.generation, current.base), (1, 0));
    assert_ne!(old.id, current.id);
    let before = ledger.clone();
    assert_eq!(ledger.begin(old), Err(Failure::Stale));
    assert_eq!(ledger, before);
}

#[test]
fn bounds_and_fencing_fail_without_advancing_state() {
    let mut ledger = Ledger::new(1, MAX);
    for count in [0, -1] {
        let before = ledger.clone();
        assert_eq!(ledger.reserve(count), Err(Failure::InvalidCount));
        assert_eq!(ledger, before);
    }
    let token = ledger.reserve(1).unwrap();
    let before = ledger.clone();
    assert_eq!(ledger.reserve(1), Err(Failure::Full));
    assert_eq!(ledger, before);
    ledger.fence(token).unwrap();
    let fenced = ledger.clone();
    assert_eq!(ledger.reserve(1), Err(Failure::Fenced));
    assert_eq!(ledger.begin(token), Err(Failure::Fenced));
    assert_eq!(ledger, fenced);
    ledger.rollback(token).unwrap();
    ledger.new_identity().unwrap();
    ledger.next_ticket = u64::MAX;
    let before = ledger.clone();
    assert_eq!(ledger.reserve(1), Err(Failure::Exhausted));
    assert_eq!(ledger, before);
    ledger.generation = u64::MAX;
    let before = ledger.clone();
    assert_eq!(ledger.new_identity(), Err(Failure::Exhausted));
    assert_eq!(ledger, before);
}

#[test]
fn a_ticket_from_another_partition_is_never_accepted() {
    let mut a = Ledger::for_key(7, 5, MAX);
    let mut b = Ledger::for_key(8, 5, MAX);
    let first = a.reserve(1).unwrap();
    let second = b.reserve(1).unwrap();
    assert_eq!((first.id, first.generation, first.base, first.count),
               (second.id, second.generation, second.base, second.count));
    let before = b.clone();
    assert_eq!(b.begin(first), Err(Failure::Stale));
    assert_eq!(b.rollback(first), Err(Failure::Stale));
    assert_eq!(b, before);
    b.begin(second).unwrap();
}

#[test]
fn concurrent_reservations_serialize_before_the_write_gate() {
    use std::sync::{Arc, Barrier, Mutex};
    let ledger = Arc::new(Mutex::new(Ledger::new(5, MAX)));
    let start = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let ledger = Arc::clone(&ledger);
        let start = Arc::clone(&start);
        threads.push(std::thread::spawn(move || {
            start.wait();
            ledger.lock().unwrap().reserve(1).unwrap()
        }));
    }
    start.wait();
    let mut tickets = threads.into_iter().map(|t| t.join().unwrap()).collect::<Vec<_>>();
    tickets.sort_by_key(|t| t.id);
    assert_eq!((tickets[0].base, tickets[1].base), (MAX, 0));
    let mut ledger = ledger.lock().unwrap();
    assert_eq!(ledger.begin(tickets[1]), Err(Failure::NotHead));
    ledger.begin(tickets[0]).unwrap();
    ledger.acknowledge(tickets[0]).unwrap();
    ledger.begin(tickets[1]).unwrap();
    ledger.acknowledge(tickets[1]).unwrap();
}
