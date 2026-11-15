#![forbid(unsafe_code)]

//! Standalone model. This code is not connected to the producer or its transport.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, MutexGuard};
#[allow(clippy::disallowed_types, reason = "Synchronous model tests lock poisoning; no async or await")]
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Partition {
    pub topic_incarnation: u128,
    pub index: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Identity {
    pub producer_id: i64,
    pub epoch: i16,
    pub generation: u64,
}

pub type BatchSnapshot = Vec<(Partition, Arc<[u8]>)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Closed,
    WrongState,
    NotFound,
    Conflict,
    Invalid,
    Capacity,
    Exhausted,
    Poisoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Attempt {
    id: u64,
    session: u64,
    correlation: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    Ready,
    Claimed { retry: bool },
    Offered(Attempt),
    Ambiguous(Attempt),
    AmbiguousRetired(Attempt),
    RetryReady,
    Completed { receipt_recorded: bool },
}

struct Frame {
    identity: Identity,
    // These bytes stand for immutable, already sequenced record batches,
    // not a full request frame with a changeable correlation ID.
    batches: BTreeMap<Partition, Arc<[u8]>>,
    bytes: usize,
    last_session: Option<u64>,
    stage: Stage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Open,
    Draining(u64),
    RpcOffered(u64),
    RpcUnknown(u64),
    Fatal { rpc_unknown: bool },
}

struct State {
    phase: Phase,
    identity: Identity,
    frames: BTreeMap<u64, Frame>,
    busy: BTreeMap<Partition, u64>,
    inflight: BTreeSet<(u64, i32)>,
    bytes: usize,
    max_frames: usize,
    max_parts_per_frame: usize,
    max_bytes: usize,
    next_frame: u64,
    next_attempt: u64,
    next_transition: u64,
    // Ticket and whether its guard is still live. Dropping the guard alone
    // is not proof that all network actors have been joined.
    rpc_active: Option<(u64, bool)>,
}

#[derive(Clone)]
#[allow(clippy::disallowed_types, reason = "Synchronous model tests lock poisoning; no async or await")]
pub struct Journal(Arc<Mutex<State>>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeadlineStatus {
    DefinitelyUnsent,
    BrokerOutcomeUnknown,
    DefinitiveAwaitingRetirement,
}

impl Journal {
    #[allow(clippy::disallowed_types, reason = "Synchronous model tests lock poisoning; no async or await")]
    pub fn new(identity: Identity, max_frames: usize, max_parts_per_frame: usize, max_bytes: usize) -> Result<Self, Error> {
        if identity.producer_id < 0 || identity.epoch < 0 {
            return Err(Error::Invalid);
        }
        Ok(Self(Arc::new(Mutex::new(State {
            phase: Phase::Open,
            identity,
            frames: BTreeMap::new(),
            busy: BTreeMap::new(),
            inflight: BTreeSet::new(),
            bytes: 0,
            max_frames,
            max_parts_per_frame,
            max_bytes,
            next_frame: 1,
            next_attempt: 1,
            next_transition: 1,
            rpc_active: None,
        }))))
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>, Error> {
        self.0.lock().map_err(|_| Error::Poisoned)
    }

    pub fn reserve(&self, batches: BatchSnapshot) -> Result<u64, Error> {
        let mut map = BTreeMap::new();
        let mut bytes = 0usize;
        for (partition, batch) in batches {
            if partition.topic_incarnation == 0 || partition.index < 0 || batch.is_empty() {
                return Err(Error::Invalid);
            }
            bytes = bytes.checked_add(batch.len()).ok_or(Error::Capacity)?;
            if map.insert(partition, batch).is_some() {
                return Err(Error::Invalid);
            }
        }
        if map.is_empty() {
            return Err(Error::Invalid);
        }
        let mut state = self.lock()?;
        if state.phase != Phase::Open {
            return Err(Error::Closed);
        }
        let total = state.bytes.checked_add(bytes).ok_or(Error::Capacity)?;
        if state.frames.len() >= state.max_frames
            || map.len() > state.max_parts_per_frame
            || total > state.max_bytes
        {
            return Err(Error::Capacity);
        }
        if map.keys().any(|partition| state.busy.contains_key(partition)) {
            return Err(Error::Conflict);
        }
        let id = state.next_frame;
        state.next_frame = id.checked_add(1).ok_or(Error::Exhausted)?;
        let identity = state.identity;
        for partition in map.keys() {
            state.busy.insert(*partition, id);
        }
        state.frames.insert(id, Frame { identity, batches: map, bytes, last_session: None, stage: Stage::Ready });
        state.bytes = total;
        Ok(id)
    }

    pub fn claim(&self, id: u64) -> Result<Claim, Error> {
        let mut state = self.lock()?;
        if !matches!(state.phase, Phase::Open | Phase::Draining(_)) {
            return Err(Error::Closed);
        }
        let frame = state.frames.get_mut(&id).ok_or(Error::NotFound)?;
        let retry = match frame.stage {
            Stage::Ready => false,
            Stage::RetryReady => true,
            _ => return Err(Error::WrongState),
        };
        frame.stage = Stage::Claimed { retry };
        Ok(Claim { journal: self.clone(), id, retry, active: true })
    }

    pub fn drain(&self) -> Result<u64, Error> {
        let mut state = self.lock()?;
        if state.phase != Phase::Open {
            return Err(Error::Closed);
        }
        let ticket = state.next_transition;
        state.next_transition = ticket.checked_add(1).ok_or(Error::Exhausted)?;
        state.phase = Phase::Draining(ticket);
        Ok(ticket)
    }

    pub fn cancel_drain(&self, ticket: u64) -> Result<(), Error> {
        let mut state = self.lock()?;
        if state.phase != Phase::Draining(ticket) {
            return Err(Error::WrongState);
        }
        state.phase = Phase::Open;
        Ok(())
    }

    pub fn begin_identity_rpc(&self, ticket: u64) -> Result<IdentityRpc, Error> {
        let mut state = self.lock()?;
        if state.phase != Phase::Draining(ticket) || !state.frames.is_empty() {
            return Err(Error::WrongState);
        }
        state.phase = Phase::RpcOffered(ticket);
        state.rpc_active = Some((ticket, true));
        Ok(IdentityRpc { journal: self.clone(), ticket, active: true })
    }

    /// Attests that every old request writer and response dispatcher for this
    /// exact attempt is retired and joined. This does NOT prove non-append.
    pub fn attest_attempt_retired(&self, id: u64, attempt_id: u64, session: u64, correlation: i32) -> Result<(), Error> {
        let mut state = self.lock()?;
        let fatal = match state.phase {
            Phase::Open | Phase::Draining(_) => false,
            Phase::Fatal { .. } => true,
            _ => return Err(Error::Closed),
        };
        let attempt = Attempt { id: attempt_id, session, correlation };
        if state.frames.get(&id).ok_or(Error::NotFound)?.stage != Stage::Ambiguous(attempt) {
            return Err(Error::WrongState);
        }
        if !state.inflight.remove(&(session, correlation)) {
            return Err(Error::WrongState);
        }
        state.frames.get_mut(&id).ok_or(Error::NotFound)?.stage =
            if fatal { Stage::AmbiguousRetired(attempt) } else { Stage::RetryReady };
        Ok(())
    }

    /// Attests that all actors for an unknown identity RPC stopped and joined.
    /// This permits model-level shutdown quiescence; it does not reopen the gate.
    pub fn attest_identity_rpc_retired(&self, ticket: u64) -> Result<(), Error> {
        let mut state = self.lock()?;
        let eligible = matches!(state.phase, Phase::RpcUnknown(id) if id == ticket)
            || matches!(state.phase, Phase::Fatal { rpc_unknown: true });
        if state.rpc_active != Some((ticket, false)) || !eligible {
            return Err(Error::WrongState);
        }
        state.rpc_active = None;
        Ok(())
    }

    /// This is only a receipt bookkeeping attestation, not a callback.
    pub fn record_receipt(&self, id: u64) -> Result<(), Error> {
        let mut state = self.lock()?;
        let frame = state.frames.get_mut(&id).ok_or(Error::NotFound)?;
        if frame.stage != (Stage::Completed { receipt_recorded: false }) {
            return Err(Error::WrongState);
        }
        frame.stage = Stage::Completed { receipt_recorded: true };
        Ok(())
    }

    pub fn retire_completed(&self, id: u64) -> Result<(), Error> {
        let removed = {
            let mut state = self.lock()?;
            let frame = state.frames.get(&id).ok_or(Error::NotFound)?;
            if frame.stage != (Stage::Completed { receipt_recorded: true }) {
                return Err(Error::WrongState);
            }
            let frame = state.frames.remove(&id).ok_or(Error::NotFound)?;
            for partition in frame.batches.keys() {
                state.busy.remove(partition);
            }
            state.bytes -= frame.bytes;
            frame
        };
        // No payload destructor runs under the state lock.
        drop(removed);
        Ok(())
    }

    /// Fatal starts a fence; already offered socket tasks still need joining.
    pub fn fatal(&self) -> Result<(), Error> {
        let mut state = self.lock()?;
        let rpc_unknown = match state.phase {
            Phase::RpcOffered(_) | Phase::RpcUnknown(_) => true,
            Phase::Fatal { rpc_unknown } => rpc_unknown,
            Phase::Open | Phase::Draining(_) => false,
        };
        state.phase = Phase::Fatal { rpc_unknown };
        Ok(())
    }

    /// Caller must separately wait for all actual transport actors to stop.
    pub fn fatal_model_quiescent(&self) -> Result<bool, Error> {
        let state = self.lock()?;
        if !matches!(state.phase, Phase::Fatal { .. }) {
            return Err(Error::WrongState);
        }
        Ok(state.rpc_active.is_none() && state.inflight.is_empty() && state.frames.values().all(|frame| !matches!(frame.stage, Stage::Claimed { .. } | Stage::Offered(_))))
    }

    /// A fatal shutdown deadline reports uncertainty; it does not retire anything.
    /// Actual transport tasks must also be joined before shutdown is quiescent.
    pub fn deadline_status(&self, id: u64) -> Result<DeadlineStatus, Error> {
        let state = self.lock()?;
        if !matches!(state.phase, Phase::Fatal { .. }) {
            return Err(Error::WrongState);
        }
        let frame = state.frames.get(&id).ok_or(Error::NotFound)?;
        Ok(match frame.stage {
            Stage::Ready | Stage::Claimed { retry: false } => DeadlineStatus::DefinitelyUnsent,
            Stage::Claimed { retry: true } | Stage::Offered(_) | Stage::Ambiguous(_) | Stage::AmbiguousRetired(_) | Stage::RetryReady => DeadlineStatus::BrokerOutcomeUnknown,
            Stage::Completed { .. } => DeadlineStatus::DefinitiveAwaitingRetirement,
        })
    }
}

pub struct Claim {
    journal: Journal,
    id: u64,
    retry: bool,
    active: bool,
}

impl Claim {
    pub fn snapshot(&self) -> Result<(Identity, BatchSnapshot), Error> {
        let state = self.journal.lock()?;
        let frame = state.frames.get(&self.id).ok_or(Error::NotFound)?;
        if frame.stage != (Stage::Claimed { retry: self.retry }) {
            return Err(Error::WrongState);
        }
        Ok((frame.identity, frame.batches.iter().map(|(p, b)| (*p, b.clone())).collect()))
    }

    /// Must happen before the first possible socket poll.
    pub fn begin_offer(mut self, session: u64, correlation: i32) -> Result<Offer, Error> {
        let mut state = self.journal.lock()?;
        if !matches!(state.phase, Phase::Open | Phase::Draining(_)) {
            return Err(Error::Closed);
        }
        let frame = state.frames.get(&self.id).ok_or(Error::NotFound)?;
        if frame.stage != (Stage::Claimed { retry: self.retry }) {
            return Err(Error::WrongState);
        }
        if session == 0 || frame.last_session.is_some_and(|previous| session <= previous) {
            return Err(Error::Invalid);
        }
        if state.inflight.contains(&(session, correlation)) {
            return Err(Error::Conflict);
        }
        let attempt = Attempt { id: state.next_attempt, session, correlation };
        state.next_attempt = attempt.id.checked_add(1).ok_or(Error::Exhausted)?;
        let frame = state.frames.get_mut(&self.id).ok_or(Error::NotFound)?;
        frame.last_session = Some(session);
        frame.stage = Stage::Offered(attempt);
        state.inflight.insert((session, correlation));
        self.active = false;
        drop(state);
        Ok(Offer { journal: self.journal.clone(), id: self.id, attempt, active: true })
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.journal.lock()
            && let Some(frame) = state.frames.get_mut(&self.id)
            && frame.stage == (Stage::Claimed { retry: self.retry })
        {
            frame.stage = if self.retry { Stage::RetryReady } else { Stage::Ready };
        }
    }
}

pub struct Offer {
    journal: Journal,
    id: u64,
    attempt: Attempt,
    active: bool,
}

impl Offer {
    pub fn attempt_id(&self) -> u64 {
        self.attempt.id
    }

    /// The caller must validate Kafka framing and every partition result.
    pub fn complete(mut self, identity: Identity, session: u64, correlation: i32, parts: &[Partition]) -> Result<(), Error> {
        let mut state = self.journal.lock()?;
        let frame = state.frames.get(&self.id).ok_or(Error::NotFound)?;
        let actual: BTreeSet<_> = parts.iter().copied().collect();
        if frame.stage != Stage::Offered(self.attempt)
            || frame.identity != identity
            || self.attempt.session != session
            || self.attempt.correlation != correlation
            || actual.len() != parts.len()
            || !frame.batches.keys().copied().eq(actual.iter().copied())
        {
            return Err(Error::Invalid);
        }
        if !state.inflight.remove(&(session, correlation)) {
            return Err(Error::WrongState);
        }
        state.frames.get_mut(&self.id).ok_or(Error::NotFound)?.stage = Stage::Completed { receipt_recorded: false };
        self.active = false;
        Ok(())
    }
}

impl Drop for Offer {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.journal.lock()
            && let Some(frame) = state.frames.get_mut(&self.id)
            && frame.stage == Stage::Offered(self.attempt)
        {
            frame.stage = Stage::Ambiguous(self.attempt);
        }
    }
}

pub struct IdentityRpc {
    journal: Journal,
    ticket: u64,
    active: bool,
}

impl IdentityRpc {
    /// Called only after definitive RPC success and after all its transport
    /// actors are retired and joined. The model cannot verify this attestation.
    pub fn publish(mut self, producer_id: i64, epoch: i16) -> Result<Identity, Error> {
        let mut state = self.journal.lock()?;
        if state.phase != Phase::RpcOffered(self.ticket) {
            return Err(Error::WrongState);
        }
        if producer_id < 0 || epoch < 0 {
            return Err(Error::Invalid);
        }
        let generation = state.identity.generation.checked_add(1).ok_or(Error::Exhausted)?;
        let identity = Identity { producer_id, epoch, generation };
        state.identity = identity;
        state.rpc_active = None;
        state.phase = Phase::Open;
        self.active = false;
        Ok(identity)
    }
}

impl Drop for IdentityRpc {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.journal.lock() {
            match state.phase {
                Phase::RpcOffered(ticket) if ticket == self.ticket => {
                    state.rpc_active = Some((self.ticket, false));
                    state.phase = Phase::RpcUnknown(self.ticket);
                }
                Phase::Fatal { .. } => state.rpc_active = Some((self.ticket, false)),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    fn identity() -> Identity {
        Identity { producer_id: 8, epoch: 2, generation: 1 }
    }

    fn part(index: i32) -> Partition {
        Partition { topic_incarnation: 42, index }
    }

    fn bytes(value: &'static [u8]) -> Arc<[u8]> {
        Arc::from(value)
    }

    fn journal() -> Journal {
        Journal::new(identity(), 8, 8, 1024).unwrap()
    }

    fn complete_and_retire(journal: &Journal, frame: u64, offer: Offer, parts: &[Partition]) {
        offer.complete(identity(), 11, 22, parts).unwrap();
        assert!(matches!(journal.retire_completed(frame), Err(Error::WrongState)));
        journal.record_receipt(frame).unwrap();
        journal.retire_completed(frame).unwrap();
    }

    #[test]
    fn ambiguous_attempt_keeps_exact_batch_and_blocks_identity_until_receipt() {
        let journal = journal();
        let original = bytes(b"sequenced batch");
        let frame = journal.reserve(vec![(part(0), original.clone())]).unwrap();
        let first = journal.claim(frame).unwrap();
        let (first_identity, first_batch) = first.snapshot().unwrap();
        let offer = first.begin_offer(11, 22).unwrap();
        let attempt = offer.attempt_id();
        let ticket = journal.drain().unwrap();
        drop(offer);
        assert!(matches!(journal.lock().unwrap().frames[&frame].stage, Stage::Ambiguous(_)));
        assert!(matches!(journal.begin_identity_rpc(ticket), Err(Error::WrongState)));
        assert_eq!(journal.attest_attempt_retired(frame, attempt, 12, 22), Err(Error::WrongState));
        assert_eq!(journal.attest_attempt_retired(frame, attempt + 1, 11, 22), Err(Error::WrongState));
        journal.attest_attempt_retired(frame, attempt, 11, 22).unwrap();
        let same_session = journal.claim(frame).unwrap();
        assert!(matches!(same_session.begin_offer(11, 23), Err(Error::Invalid)));
        let retry = journal.claim(frame).unwrap();
        let (retry_identity, retry_batch) = retry.snapshot().unwrap();
        assert_eq!(retry_identity, first_identity);
        assert!(Arc::ptr_eq(&original, &retry_batch[0].1));
        assert!(Arc::ptr_eq(&first_batch[0].1, &retry_batch[0].1));
        let offer = retry.begin_offer(12, 23).unwrap();
        offer.complete(identity(), 12, 23, &[part(0)]).unwrap();
        assert_eq!(journal.lock().unwrap().frames[&frame].stage, Stage::Completed { receipt_recorded: false });
        assert!(matches!(journal.begin_identity_rpc(ticket), Err(Error::WrongState)));
        journal.record_receipt(frame).unwrap();
        journal.retire_completed(frame).unwrap();
        let new_identity = journal.begin_identity_rpc(ticket).unwrap().publish(9, 0).unwrap();
        assert_eq!(new_identity, Identity { producer_id: 9, epoch: 0, generation: 2 });
    }

    #[test]
    fn draining_rejects_new_frames_but_allows_old_frames_to_finish() {
        let journal = journal();
        let frame = journal.reserve(vec![(part(0), bytes(b"one"))]).unwrap();
        let ticket = journal.drain().unwrap();
        assert_eq!(journal.reserve(vec![(part(1), bytes(b"two"))]), Err(Error::Closed));
        let claim = journal.claim(frame).unwrap();
        let offer = claim.begin_offer(11, 22).unwrap();
        assert!(matches!(journal.begin_identity_rpc(ticket), Err(Error::WrongState)));
        complete_and_retire(&journal, frame, offer, &[part(0)]);
        let new_identity = journal.begin_identity_rpc(ticket).unwrap().publish(12, 0).unwrap();
        let next = journal.reserve(vec![(part(1), bytes(b"next"))]).unwrap();
        assert_eq!(journal.claim(next).unwrap().snapshot().unwrap().0, new_identity);
        assert!(matches!(journal.claim(frame), Err(Error::NotFound)));
    }

    #[test]
    fn fatal_fences_claims_but_does_not_erase_offers() {
        let journal = journal();
        let a = journal.reserve(vec![(part(0), bytes(b"a"))]).unwrap();
        let b = journal.reserve(vec![(part(1), bytes(b"b"))]).unwrap();
        let a_claim = journal.claim(a).unwrap();
        let b_offer = journal.claim(b).unwrap().begin_offer(11, 22).unwrap();
        journal.fatal().unwrap();
        assert_eq!(journal.fatal_model_quiescent(), Ok(false));
        assert!(matches!(a_claim.begin_offer(11, 23), Err(Error::Closed)));
        assert_eq!(journal.deadline_status(a), Ok(DeadlineStatus::DefinitelyUnsent));
        assert!(matches!(journal.claim(a), Err(Error::Closed)));
        drop(b_offer);
        assert_eq!(journal.deadline_status(b), Ok(DeadlineStatus::BrokerOutcomeUnknown));
        assert_eq!(journal.fatal_model_quiescent(), Ok(false));
        assert!(matches!(journal.claim(b), Err(Error::Closed)));
        journal.attest_attempt_retired(b, 1, 11, 22).unwrap();
        assert_eq!(journal.fatal_model_quiescent(), Ok(true));
        assert_eq!(journal.deadline_status(b), Ok(DeadlineStatus::BrokerOutcomeUnknown));
    }

    #[test]
    fn multi_partition_reservation_is_atomic_and_serializes_each_partition() {
        let journal = journal();
        let a = journal.reserve(vec![(part(0), bytes(b"a")), (part(1), bytes(b"b"))]).unwrap();
        assert_eq!(journal.reserve(vec![(part(2), bytes(b"c")), (part(1), bytes(b"d"))]), Err(Error::Conflict));
        let c = journal.reserve(vec![(part(2), bytes(b"c"))]).unwrap();
        assert_eq!(journal.reserve(vec![(part(3), bytes(b"x")), (part(3), bytes(b"y"))]), Err(Error::Invalid));
        assert_eq!(journal.reserve(vec![(part(0), bytes(b"x"))]), Err(Error::Conflict));
        let offer = journal.claim(a).unwrap().begin_offer(11, 22).unwrap();
        complete_and_retire(&journal, a, offer, &[part(1), part(0)]);
        assert!(journal.reserve(vec![(part(0), bytes(b"next"))]).is_ok());
        assert_eq!(journal.lock().unwrap().frames[&c].stage, Stage::Ready);
    }

    #[test]
    fn unknown_identity_rpc_never_reopens_and_fatal_keeps_unknown_flag() {
        let journal = journal();
        let first = journal.drain().unwrap();
        journal.cancel_drain(first).unwrap();
        let second = journal.drain().unwrap();
        assert_eq!(journal.cancel_drain(first), Err(Error::WrongState));
        let rpc = journal.begin_identity_rpc(second).unwrap();
        assert_eq!(journal.cancel_drain(second), Err(Error::WrongState));
        drop(rpc);
        assert!(matches!(journal.reserve(vec![(part(0), bytes(b"a"))]), Err(Error::Closed)));
        assert!(matches!(journal.drain(), Err(Error::Closed)));
        journal.fatal().unwrap();
        assert_eq!(journal.lock().unwrap().phase, Phase::Fatal { rpc_unknown: true });
        assert_eq!(journal.fatal_model_quiescent(), Ok(false));
        journal.attest_identity_rpc_retired(second).unwrap();
        assert_eq!(journal.fatal_model_quiescent(), Ok(true));

        let other = self::journal();
        let ticket = other.drain().unwrap();
        let rpc = other.begin_identity_rpc(ticket).unwrap();
        other.fatal().unwrap();
        assert_eq!(other.fatal_model_quiescent(), Ok(false));
        drop(rpc);
        assert_eq!(other.fatal_model_quiescent(), Ok(false));
        assert_eq!(other.attest_identity_rpc_retired(ticket + 1), Err(Error::WrongState));
        other.attest_identity_rpc_retired(ticket).unwrap();
        assert_eq!(other.fatal_model_quiescent(), Ok(true));
        assert_eq!(other.lock().unwrap().phase, Phase::Fatal { rpc_unknown: true });
    }

    #[test]
    fn capacity_and_counter_exhaustion_never_partially_reserve_or_wrap() {
        let journal = Journal::new(identity(), 1, 2, 3).unwrap();
        let id = journal.reserve(vec![(part(0), bytes(b"abc"))]).unwrap();
        assert_eq!(journal.reserve(vec![(part(1), bytes(b"x"))]), Err(Error::Capacity));
        let state = journal.lock().unwrap();
        assert_eq!(state.bytes, 3);
        assert_eq!(state.frames.len(), 1);
        assert!(!state.busy.contains_key(&part(1)));
        drop(state);
        let offer = journal.claim(id).unwrap().begin_offer(11, 22).unwrap();
        complete_and_retire(&journal, id, offer, &[part(0)]);
        journal.lock().unwrap().next_frame = u64::MAX;
        assert_eq!(journal.reserve(vec![(part(1), bytes(b"x"))]), Err(Error::Exhausted));
        let state = journal.lock().unwrap();
        assert!(state.frames.is_empty());
        assert!(state.busy.is_empty());
        assert_eq!(state.bytes, 0);
        drop(state);
        journal.lock().unwrap().next_transition = u64::MAX;
        assert_eq!(journal.drain(), Err(Error::Exhausted));
        assert_eq!(journal.lock().unwrap().phase, Phase::Open);
    }

    #[test]
    fn exhausted_attempt_and_generation_do_not_release_or_reopen() {
        let journal = journal();
        let frame = journal.reserve(vec![(part(0), bytes(b"a"))]).unwrap();
        journal.lock().unwrap().next_attempt = u64::MAX;
        assert!(matches!(journal.claim(frame).unwrap().begin_offer(11, 22), Err(Error::Exhausted)));
        assert_eq!(journal.lock().unwrap().frames[&frame].stage, Stage::Ready);
        assert_eq!(journal.reserve(vec![(part(0), bytes(b"b"))]), Err(Error::Conflict));

        let overflow = Journal::new(Identity { generation: u64::MAX, ..identity() }, 1, 1, 10).unwrap();
        let ticket = overflow.drain().unwrap();
        let rpc = overflow.begin_identity_rpc(ticket).unwrap();
        assert_eq!(rpc.publish(9, 0), Err(Error::Exhausted));
        assert_eq!(overflow.lock().unwrap().phase, Phase::RpcUnknown(ticket));
        assert!(matches!(overflow.reserve(vec![(part(0), bytes(b"a"))]), Err(Error::Closed)));
    }

    #[test]
    fn incomplete_or_wrong_response_cannot_settle_the_frame() {
        let journal = journal();
        let id = journal.reserve(vec![(part(0), bytes(b"a")), (part(1), bytes(b"b"))]).unwrap();
        let offer = journal.claim(id).unwrap().begin_offer(11, 22).unwrap();
        assert_eq!(offer.complete(identity(), 11, 22, &[part(0)]), Err(Error::Invalid));
        assert!(matches!(journal.lock().unwrap().frames[&id].stage, Stage::Ambiguous(_)));
        assert_eq!(journal.retire_completed(id), Err(Error::WrongState));
        assert_eq!(journal.reserve(vec![(part(0), bytes(b"x"))]), Err(Error::Conflict));
    }

    #[test]
    fn session_correlation_stays_reserved_until_response_or_retirement() {
        let journal = journal();
        let first = journal.reserve(vec![(part(0), bytes(b"first"))]).unwrap();
        let second = journal.reserve(vec![(part(1), bytes(b"second"))]).unwrap();
        let offer = journal.claim(first).unwrap().begin_offer(11, 22).unwrap();
        let attempt = offer.attempt_id();
        assert!(matches!(journal.claim(second).unwrap().begin_offer(11, 22), Err(Error::Conflict)));
        drop(offer);
        assert!(matches!(journal.claim(second).unwrap().begin_offer(11, 22), Err(Error::Conflict)));
        journal.attest_attempt_retired(first, attempt, 11, 22).unwrap();
        let second_offer = journal.claim(second).unwrap().begin_offer(12, 22).unwrap();
        complete_and_retire_different_session(&journal, second, second_offer, &[part(1)]);
        assert!(matches!(journal.lock().unwrap().frames[&first].stage, Stage::RetryReady));
    }

    fn complete_and_retire_different_session(journal: &Journal, frame: u64, offer: Offer, parts: &[Partition]) {
        offer.complete(identity(), 12, 22, parts).unwrap();
        journal.record_receipt(frame).unwrap();
        journal.retire_completed(frame).unwrap();
    }

    #[test]
    fn permanent_unknown_survives_deadline_and_blocks_reuse() {
        let journal = journal();
        let frame = journal.reserve(vec![(part(0), bytes(b"a"))]).unwrap();
        let offer = journal.claim(frame).unwrap().begin_offer(11, 22).unwrap();
        let attempt = offer.attempt_id();
        drop(offer);
        let ticket = journal.drain().unwrap();
        assert!(matches!(journal.begin_identity_rpc(ticket), Err(Error::WrongState)));
        journal.fatal().unwrap();
        assert_eq!(journal.fatal_model_quiescent(), Ok(false));
        journal.attest_attempt_retired(frame, attempt, 11, 22).unwrap();
        assert_eq!(journal.fatal_model_quiescent(), Ok(true));
        for _ in 0..3 {
            assert_eq!(journal.deadline_status(frame), Ok(DeadlineStatus::BrokerOutcomeUnknown));
        }
        assert!(matches!(journal.begin_identity_rpc(ticket), Err(Error::WrongState)));
    }

    #[test]
    fn poisoned_state_never_reopens_for_admission_or_identity() {
        let journal = journal();
        let other = journal.clone();
        let worker = thread::spawn(move || {
            let _guard = other.lock().unwrap();
            panic!("deliberate model lock poison");
        });
        assert!(worker.join().is_err());
        assert_eq!(journal.reserve(vec![(part(0), bytes(b"x"))]), Err(Error::Poisoned));
        assert_eq!(journal.drain(), Err(Error::Poisoned));
        assert_eq!(journal.fatal(), Err(Error::Poisoned));
    }

    #[test]
    fn fatal_linearizes_before_a_later_offer_attempt() {
        let journal = journal();
        let frame = journal.reserve(vec![(part(0), bytes(b"a"))]).unwrap();
        let claim = journal.claim(frame).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = barrier.clone();
        let worker = thread::spawn(move || {
            worker_barrier.wait();
            matches!(claim.begin_offer(11, 22), Err(Error::Closed))
        });
        journal.fatal().unwrap();
        barrier.wait();
        assert!(worker.join().unwrap());
        assert_eq!(journal.deadline_status(frame), Ok(DeadlineStatus::DefinitelyUnsent));
    }
}
