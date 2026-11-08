//! Isolated producer ownership substrate. Not wired into the Kafka producer.
//!
//! This prototype roots payload records before worker handoff and journals one
//! immutable batch per partition. It intentionally fences unknown writes. It
//! does not implement receipts, callbacks, retries after an unknown write,
//! transaction fencing, Kafka encoding, or a complete memory bound.
//!
//! The supervisor must keep the root alive until all records and workers are
//! resolved. Calling `confirm_stopped_after_join` requires the actual worker
//! owning that claim to have terminated. A joined worker alone does not prove
//! that an earlier write cannot still reach the broker.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const SEQUENCE_MODULUS: u64 = 1_u64 << 31;
const PHASE_BITS: u32 = 3;
const PHASE_MASK: u64 = (1_u64 << PHASE_BITS) - 1;
const MAX_CLAIM_GENERATION: u64 = u64::MAX >> PHASE_BITS;
const READY: u64 = 0;
const CLAIMED: u64 = 1;
const OFFERED: u64 = 2;
const RETIRING: u64 = 3;
const UNKNOWN: u64 = 4;
const SETTLED: u64 = 5;

fn state(generation: u64, phase: u64) -> u64 {
    (generation << PHASE_BITS) | phase
}

fn generation(value: u64) -> u64 {
    value >> PHASE_BITS
}

fn phase(value: u64) -> u64 {
    value & PHASE_MASK
}

/// The caller is responsible for the meaning and lifetime of the incarnation.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct Partition {
    pub(crate) topic: Arc<str>,
    pub(crate) incarnation: Option<u128>,
    pub(crate) partition: i32,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct RecordKey(u64);

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct BatchKey(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AttemptKey {
    batch: BatchKey,
    claim_generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Barrier {
    cutoff: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Identity {
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) generation: u64,
}

/// Zero means unlimited in each dimension, preserving legacy zero semantics.
/// Byte accounting here covers logical payload bytes only.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    records: Option<usize>,
    logical_bytes: Option<usize>,
    partitions: Option<usize>,
}

impl Limits {
    pub(crate) fn new(records: usize, logical_bytes: usize, partitions: usize) -> Self {
        Self {
            records: (records != 0).then_some(records),
            logical_bytes: (logical_bytes != 0).then_some(logical_bytes),
            partitions: (partitions != 0).then_some(partitions),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerError {
    Closed,
    Full,
    TooLarge,
    InvalidLimit,
    Empty,
    Busy,
    Stale,
    Expired,
    Exhausted,
    Poisoned,
}

/// A rejected admission returns the caller's payload.
#[derive(Debug)]
pub(crate) struct AdmitFailure {
    pub(crate) kind: OwnerError,
    pub(crate) payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BatchPhase {
    Ready,
    Claimed,
    Offered,
    Retiring,
    Unknown,
    Settled,
}

fn phase_name(value: u64) -> Option<BatchPhase> {
    match phase(value) {
        READY => Some(BatchPhase::Ready),
        CLAIMED => Some(BatchPhase::Claimed),
        OFFERED => Some(BatchPhase::Offered),
        RETIRING => Some(BatchPhase::Retiring),
        UNKNOWN => Some(BatchPhase::Unknown),
        SETTLED => Some(BatchPhase::Settled),
        _ => None,
    }
}

struct Record {
    payload: Vec<u8>,
    deadline: Instant,
    timestamp: Option<i64>,
    batch: Option<BatchKey>,
}

#[derive(Default)]
struct PartitionState {
    queue: VecDeque<RecordKey>,
    active: Option<BatchKey>,
    next_sequence: u32,
}

struct Batch {
    key: BatchKey,
    partition: Partition,
    identity: Identity,
    members: Box<[RecordKey]>,
    base_sequence: u32,
    earliest_deadline: Instant,
    /// Claim generation and phase are changed together.
    state: AtomicU64,
}

struct Core {
    limits: Limits,
    identity: Identity,
    next_record: u64,
    next_batch: u64,
    records: HashMap<RecordKey, Record>,
    partitions: HashMap<Partition, PartitionState>,
    batches: HashMap<BatchKey, Arc<Batch>>,
    bytes: usize,
    admission_closed: bool,
    claims_stopped: bool,
    close_barrier: Option<Barrier>,
}

#[derive(Clone)]
pub(crate) struct Owner {
    core: Arc<Mutex<Core>>,
}

#[derive(Debug)]
pub(crate) struct BatchSnapshot {
    pub(crate) key: BatchKey,
    pub(crate) partition: Partition,
    pub(crate) identity: Identity,
    pub(crate) members: Vec<RecordKey>,
    pub(crate) base_sequence: u32,
    pub(crate) phase: BatchPhase,
}

impl BatchSnapshot {
    pub(crate) fn sequence_at(&self, index: usize) -> Option<u32> {
        if index >= self.members.len() {
            return None;
        }
        let offset = u64::try_from(index).ok()?;
        u32::try_from((u64::from(self.base_sequence) + offset) % SEQUENCE_MODULUS).ok()
    }
}

impl Owner {
    pub(crate) fn new(identity: Identity, limits: Limits) -> Self {
        Self {
            core: Arc::new(Mutex::new(Core {
                limits,
                identity,
                next_record: 1,
                next_batch: 1,
                records: HashMap::new(),
                partitions: HashMap::new(),
                batches: HashMap::new(),
                bytes: 0,
                admission_closed: false,
                claims_stopped: false,
                close_barrier: None,
            })),
        }
    }

    pub(crate) fn try_admit(
        &self,
        partition: Partition,
        payload: Vec<u8>,
        deadline: Instant,
        timestamp: Option<i64>,
    ) -> Result<RecordKey, AdmitFailure> {
        let mut core = match self.core.lock() {
            Ok(core) => core,
            Err(_) => return Err(AdmitFailure { kind: OwnerError::Poisoned, payload }),
        };
        let fail = if core.admission_closed {
            Some(OwnerError::Closed)
        } else if core.limits.records.is_some_and(|limit| core.records.len() >= limit) {
            Some(OwnerError::Full)
        } else if core.limits.logical_bytes.is_some_and(|limit| payload.len() > limit) {
            Some(OwnerError::TooLarge)
        } else if core.limits.logical_bytes.is_some_and(|limit| {
            core.bytes.checked_add(payload.len()).is_none_or(|next| next > limit)
        }) || core.bytes.checked_add(payload.len()).is_none()
            || (!core.partitions.contains_key(&partition)
                && core.limits.partitions.is_some_and(|limit| core.partitions.len() >= limit))
        {
            Some(OwnerError::Full)
        } else {
            None
        };
        if let Some(kind) = fail {
            return Err(AdmitFailure { kind, payload });
        }
        let Some(next) = core.next_record.checked_add(1) else {
            return Err(AdmitFailure { kind: OwnerError::Exhausted, payload });
        };
        let key = RecordKey(core.next_record);
        core.next_record = next;
        core.bytes += payload.len();
        drop(core.records.insert(key, Record { payload, deadline, timestamp, batch: None }));
        core.partitions.entry(partition).or_default().queue.push_back(key);
        Ok(key)
    }

    /// Atomically roots a whole batch and its sequence reservation. The byte
    /// target is logical payload only; it is not a Kafka request-size check.
    pub(crate) fn seal_batch(
        &self,
        partition: &Partition,
        record_limit: usize,
        byte_limit: usize,
        now: Instant,
        timestamp: i64,
    ) -> Result<BatchKey, OwnerError> {
        // As with admission limits, zero means no additional limit.
        let record_limit = if record_limit == 0 { usize::MAX } else { record_limit };
        let byte_limit = if byte_limit == 0 { usize::MAX } else { byte_limit };
        let mut core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        if core.claims_stopped {
            return Err(OwnerError::Closed);
        }
        let partition_state = core.partitions.get(partition).ok_or(OwnerError::Empty)?;
        if partition_state.active.is_some() {
            return Err(OwnerError::Busy);
        }
        let mut members = Vec::new();
        let mut bytes = 0_usize;
        let mut earliest_deadline: Option<Instant> = None;
        for key in partition_state.queue.iter().take(record_limit) {
            let record = core.records.get(key).ok_or(OwnerError::Stale)?;
            if record.deadline <= now {
                return Err(OwnerError::Expired);
            }
            let next = bytes.checked_add(record.payload.len()).ok_or(OwnerError::Exhausted)?;
            if next > byte_limit {
                if members.is_empty() {
                    return Err(OwnerError::TooLarge);
                }
                break;
            }
            members.push(*key);
            earliest_deadline = Some(earliest_deadline.map_or(record.deadline, |prior| prior.min(record.deadline)));
            bytes = next;
        }
        if members.is_empty() {
            return Err(OwnerError::Empty);
        }
        let count = u64::try_from(members.len()).map_err(|_| OwnerError::Exhausted)?;
        if count >= SEQUENCE_MODULUS {
            return Err(OwnerError::TooLarge);
        }
        let Some(next_batch) = core.next_batch.checked_add(1) else {
            return Err(OwnerError::Exhausted);
        };
        let key = BatchKey(core.next_batch);
        let base = partition_state.next_sequence;
        let next_sequence = u32::try_from((u64::from(base) + count) % SEQUENCE_MODULUS)
            .map_err(|_| OwnerError::Exhausted)?;
        let identity = core.identity;
        let batch = Arc::new(Batch {
            key,
            partition: partition.clone(),
            identity,
            members: members.into_boxed_slice(),
            base_sequence: base,
            earliest_deadline: earliest_deadline.ok_or(OwnerError::Empty)?,
            state: AtomicU64::new(state(0, READY)),
        });
        // No yield or user callback occurs between sequence advance and rooting.
        for record_key in batch.members.iter() {
            let record = core.records.get_mut(record_key).ok_or(OwnerError::Stale)?;
            record.batch = Some(key);
            if record.timestamp.is_none() {
                record.timestamp = Some(timestamp);
            }
        }
        let partition_state = core.partitions.get_mut(partition).ok_or(OwnerError::Stale)?;
        for record_key in batch.members.iter() {
            if partition_state.queue.pop_front() != Some(*record_key) {
                return Err(OwnerError::Stale);
            }
        }
        partition_state.active = Some(key);
        partition_state.next_sequence = next_sequence;
        core.next_batch = next_batch;
        drop(core.batches.insert(key, batch));
        Ok(key)
    }

    pub(crate) fn snapshot(&self, key: BatchKey) -> Result<BatchSnapshot, OwnerError> {
        let core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        let batch = core.batches.get(&key).ok_or(OwnerError::Stale)?;
        let current = batch.state.load(Ordering::Acquire);
        Ok(BatchSnapshot {
            key: batch.key,
            partition: batch.partition.clone(),
            identity: batch.identity,
            members: batch.members.to_vec(),
            base_sequence: batch.base_sequence,
            phase: phase_name(current).ok_or(OwnerError::Stale)?,
        })
    }

    /// Calls a non-yielding closure under the root lock. This is diagnostic
    /// substrate, not a suitable production encode or callback interface.
    pub(crate) fn with_record<R>(
        &self,
        key: RecordKey,
        f: impl FnOnce(&[u8], Instant, Option<i64>) -> R,
    ) -> Result<R, OwnerError> {
        let core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        let record = core.records.get(&key).ok_or(OwnerError::Stale)?;
        Ok(f(&record.payload, record.deadline, record.timestamp))
    }

    pub(crate) fn claim(&self, key: BatchKey) -> Result<BatchClaim, OwnerError> {
        let core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        if core.claims_stopped {
            return Err(OwnerError::Closed);
        }
        let batch = Arc::clone(core.batches.get(&key).ok_or(OwnerError::Stale)?);
        let current = batch.state.load(Ordering::Acquire);
        if phase(current) != READY {
            return Err(OwnerError::Busy);
        }
        let Some(next_generation) = generation(current).checked_add(1) else {
            return Err(OwnerError::Exhausted);
        };
        if next_generation > MAX_CLAIM_GENERATION {
            return Err(OwnerError::Exhausted);
        }
        let _previous = batch
            .state
            .compare_exchange(
                current,
                state(next_generation, CLAIMED),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| OwnerError::Busy)?;
        Ok(BatchClaim { batch, generation: next_generation })
    }

    /// The caller must have awaited the actual worker's termination. This is
    /// not proof that an earlier socket write did not reach Kafka.
    pub(crate) fn confirm_stopped_after_join(
        &self,
        key: BatchKey,
        claim_generation: u64,
    ) -> Result<BatchPhase, OwnerError> {
        let core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        let batch = core.batches.get(&key).ok_or(OwnerError::Stale)?;
        let current = batch.state.load(Ordering::Acquire);
        if generation(current) != claim_generation {
            return Err(OwnerError::Stale);
        }
        match phase(current) {
            RETIRING => {
                let _previous = batch
                    .state
                    .compare_exchange(
                        current,
                        state(claim_generation, READY),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .map_err(|_| OwnerError::Busy)?;
                Ok(BatchPhase::Ready)
            }
            UNKNOWN => Ok(BatchPhase::Unknown),
            SETTLED => Ok(BatchPhase::Settled),
            _ => Err(OwnerError::Busy),
        }
    }

    pub(crate) fn flush_barrier(&self) -> Result<Barrier, OwnerError> {
        let core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        Ok(Barrier { cutoff: core.next_record - 1 })
    }

    pub(crate) fn begin_close(&self) -> Result<Barrier, OwnerError> {
        let mut core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        if let Some(barrier) = core.close_barrier {
            return Ok(barrier);
        }
        core.admission_closed = true;
        let barrier = Barrier { cutoff: core.next_record - 1 };
        core.close_barrier = Some(barrier);
        Ok(barrier)
    }

    /// Tests ownership only. It does not report errors or wait for callbacks.
    pub(crate) fn drained(&self, barrier: Barrier) -> Result<bool, OwnerError> {
        let core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        Ok(!core.records.keys().any(|key| key.0 <= barrier.cutoff))
    }

    pub(crate) fn counts(&self) -> Result<(usize, usize), OwnerError> {
        let core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        Ok((core.records.len(), core.bytes))
    }

    /// Stop new claims after a modeled close timeout. An already offered write
    /// may still begin or finish at the socket and remains unknown.
    pub(crate) fn stop_after_timeout(&self) -> Result<(), OwnerError> {
        let mut core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        if !core.admission_closed {
            return Err(OwnerError::Busy);
        }
        core.claims_stopped = true;
        for batch in core.batches.values() {
            let mut current = batch.state.load(Ordering::Acquire);
            loop {
                let replacement = match phase(current) {
                    CLAIMED => state(generation(current), RETIRING),
                    OFFERED => state(generation(current), UNKNOWN),
                    _ => break,
                };
                match batch.state.compare_exchange_weak(
                    current,
                    replacement,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(observed) => current = observed,
                }
            }
        }
        Ok(())
    }

    /// Simulates reclamation after an external completion publisher has
    /// finished. The prototype neither runs nor proves those callbacks.
    pub(crate) fn retire_after_publication(&self, key: BatchKey) -> Result<(), OwnerError> {
        let mut core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        let batch = Arc::clone(core.batches.get(&key).ok_or(OwnerError::Stale)?);
        if phase(batch.state.load(Ordering::Acquire)) != SETTLED {
            return Err(OwnerError::Busy);
        }
        if core.partitions.get(&batch.partition).and_then(|part| part.active) != Some(key) {
            return Err(OwnerError::Stale);
        }
        if batch.members.iter().any(|record_key| {
            core.records.get(record_key).is_none_or(|record| record.batch != Some(key))
        }) {
            return Err(OwnerError::Stale);
        }
        for record_key in batch.members.iter() {
            let record = core.records.remove(record_key).ok_or(OwnerError::Stale)?;
            core.bytes = core.bytes.checked_sub(record.payload.len()).ok_or(OwnerError::Stale)?;
        }
        let partition = core.partitions.get_mut(&batch.partition).ok_or(OwnerError::Stale)?;
        partition.active = None;
        drop(core.batches.remove(&key));
        Ok(())
    }

    /// Test-only probe for whether a claim keeps the payload root alive.
    #[cfg(test)]
    pub(crate) fn root_lifetime_probe(&self) -> impl Fn() -> bool + 'static {
        let root = Arc::downgrade(&self.core);
        move || root.upgrade().is_some()
    }

    #[cfg(test)]
    pub(crate) fn set_next_sequence_for_test(
        &self,
        partition: &Partition,
        value: u32,
    ) -> Result<(), OwnerError> {
        if u64::from(value) >= SEQUENCE_MODULUS {
            return Err(OwnerError::InvalidLimit);
        }
        let mut core = self.core.lock().map_err(|_| OwnerError::Poisoned)?;
        let part = core.partitions.get_mut(partition).ok_or(OwnerError::Empty)?;
        if part.active.is_some() {
            return Err(OwnerError::Busy);
        }
        part.next_sequence = value;
        Ok(())
    }
}

/// Holds only a batch descriptor. Dropping it never removes rooted payload
/// records or invokes user callbacks, but the owner must outlive the claim.
pub(crate) struct BatchClaim {
    batch: Arc<Batch>,
    generation: u64,
}

impl BatchClaim {
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Records uncertainty before the caller crosses an I/O boundary.
    pub(crate) fn offer(&self) -> Result<AttemptKey, OwnerError> {
        self.offer_at(Instant::now())
    }

    pub(crate) fn offer_at(&self, now: Instant) -> Result<AttemptKey, OwnerError> {
        if now >= self.batch.earliest_deadline {
            return Err(OwnerError::Expired);
        }
        let _previous = self.batch
            .state
            .compare_exchange(
                state(self.generation, CLAIMED),
                state(self.generation, OFFERED),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| OwnerError::Stale)?;
        Ok(AttemptKey { batch: self.batch.key, claim_generation: self.generation })
    }

    /// The caller supplies a response for this exact attempt; this prototype
    /// does not perform Kafka correlation or prove broker fencing.
    pub(crate) fn acknowledge(&self, attempt: AttemptKey) -> Result<(), OwnerError> {
        self.acknowledge_at(attempt, Instant::now())
    }

    pub(crate) fn acknowledge_at(
        &self,
        attempt: AttemptKey,
        now: Instant,
    ) -> Result<(), OwnerError> {
        if attempt.batch != self.batch.key || attempt.claim_generation != self.generation {
            return Err(OwnerError::Stale);
        }
        if now >= self.batch.earliest_deadline {
            let _previous = self.batch
                .state
                .compare_exchange(
                    state(self.generation, OFFERED),
                    state(self.generation, UNKNOWN),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .map_err(|_| OwnerError::Stale)?;
            return Err(OwnerError::Expired);
        }
        let _previous = self.batch
            .state
            .compare_exchange(
                state(self.generation, OFFERED),
                state(self.generation, SETTLED),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| OwnerError::Stale)?;
        Ok(())
    }
}

impl Drop for BatchClaim {
    fn drop(&mut self) {
        let mut current = self.batch.state.load(Ordering::Acquire);
        loop {
            if generation(current) != self.generation {
                return;
            }
            let replacement = match phase(current) {
                CLAIMED => state(self.generation, RETIRING),
                OFFERED => state(self.generation, UNKNOWN),
                _ => return,
            };
            match self.batch.state.compare_exchange_weak(
                current,
                replacement,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}
