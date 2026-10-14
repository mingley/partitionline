//! Share-fetch locks, partial acknowledgements, and expiry.
//!
//! A record can be accepted only while it is locked for the acknowledging
//! member and the lock deadline is still ahead. Release, reject, gap, expiry,
//! and an acknowledgement of an expired lock return it without accepting it.
//! Delivery stops after the configured limit. Every lock has a deadline, so
//! restart and leader loss cannot leave a permanent lock or turn an
//! undelivered record into an acknowledgement.
//!
//! This is not the ShareFetch or ShareAcknowledge RPC.

use crate::journal::{self, Journal};
use std::collections::BTreeMap;
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLSHD001";

/// Share acknowledge type `GAP`.
pub const ACK_GAP: i8 = 0;
/// Share acknowledge type `ACCEPT`.
pub const ACK_ACCEPT: i8 = 1;
/// Share acknowledge type `RELEASE`.
pub const ACK_RELEASE: i8 = 2;
/// Share acknowledge type `REJECT`.
pub const ACK_REJECT: i8 = 3;
/// Share acknowledge type `RENEW`.
pub const ACK_RENEW: i8 = 4;
/// Per-offset success.
pub const ACK_OK: i16 = 0;
/// Kafka `INVALID_REQUEST`, used when the offset is not locked for this member.
pub const INVALID_REQUEST: i16 = 42;

/// One acknowledgement in a partial batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    /// Record offset.
    pub offset: i64,
    /// [`ACK_ACCEPT`], [`ACK_RELEASE`], [`ACK_REJECT`], [`ACK_RENEW`], or [`ACK_GAP`].
    pub kind: i8,
}

/// Per-offset acknowledgement result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckResult {
    /// Record offset.
    pub offset: i64,
    /// [`ACK_OK`] or [`INVALID_REQUEST`].
    pub error_code: i16,
}

/// Why a delivery operation was refused.
#[derive(Debug)]
pub enum Error {
    /// The member, offset, or deadline is not usable.
    InvalidRequest,
    /// A failed append left this log unable to confirm further writes.
    Poisoned,
    /// The limits cannot store the log.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Delivery bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_records: usize,
    max_deliveries: u32,
    lock_ms: i64,
    journal: journal::Limits,
}

impl Limits {
    /// Configure a positive record cap, delivery limit, and lock timeout.
    pub fn new(
        max_records: usize,
        max_deliveries: u32,
        lock_ms: i64,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=1_000_000).contains(&max_records)
            || !(1..=1_000).contains(&max_deliveries)
            || !(1..=86_400_000).contains(&lock_ms)
            || journal.max_entry_bytes() < 1024
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_records,
            max_deliveries,
            lock_ms,
            journal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Lock {
    member: String,
    deadline_ms: i64,
    leader_epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    Available,
    Locked(Lock),
    Accepted,
    Rejected,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    deliveries: u32,
    phase: Phase,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct State {
    records: BTreeMap<i64, Record>,
}

/// Durable share-delivery log.
pub struct Delivery {
    limits: Limits,
    state: State,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Delivery {
    /// Open or recover the delivery log.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut delivery = Self {
            limits,
            state: State {
                records: BTreeMap::new(),
            },
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut cursor = delivery.journal.base_offset();
        loop {
            let batch =
                delivery
                    .journal
                    .fetch(cursor, 32, delivery.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                delivery.state = decode(&entry.payload)?;
                delivery.stored = delivery.stored.saturating_add(1);
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(delivery)
    }

    /// Add one available record. Offsets must be new and non-negative.
    pub fn append(&mut self, offset: i64) -> Result<(), Error> {
        self.ready()?;
        if offset < 0 || self.state.records.contains_key(&offset) {
            return Err(Error::InvalidRequest);
        }
        if self.state.records.len() >= self.limits.max_records {
            return Err(Error::InvalidLimits);
        }
        let mut next = self.state.clone();
        next.records.insert(
            offset,
            Record {
                deliveries: 0,
                phase: Phase::Available,
            },
        );
        self.commit(next)
    }

    /// Lock available records for `member` under `leader_epoch`.
    ///
    /// A lock whose deadline has already passed is available again. Records at
    /// the delivery limit are archived instead of locked. Records already
    /// accepted, rejected, or locked by someone else are skipped.
    pub fn acquire(
        &mut self,
        member: &str,
        leader_epoch: i64,
        now_ms: i64,
        offsets: &[i64],
    ) -> Result<Vec<i64>, Error> {
        self.ready()?;
        if !valid_member(member) || leader_epoch < 0 || now_ms < 0 {
            return Err(Error::InvalidRequest);
        }
        let deadline = now_ms
            .checked_add(self.limits.lock_ms)
            .ok_or(Error::InvalidRequest)?;
        let mut next = self.state.clone();
        let mut locked = Vec::new();
        for offset in offsets {
            let Some(record) = next.records.get_mut(offset) else {
                continue;
            };
            let reusable = match &record.phase {
                Phase::Available => true,
                Phase::Locked(lock) => now_ms >= lock.deadline_ms,
                Phase::Accepted | Phase::Rejected | Phase::Archived => false,
            };
            if !reusable {
                continue;
            }
            if record.deliveries >= self.limits.max_deliveries {
                record.phase = Phase::Archived;
                continue;
            }
            record.deliveries = record.deliveries.saturating_add(1);
            record.phase = Phase::Locked(Lock {
                member: member.to_owned(),
                deadline_ms: deadline,
                leader_epoch,
            });
            locked.push(*offset);
        }
        if locked.is_empty() && next == self.state {
            return Ok(locked);
        }
        self.commit(next)?;
        Ok(locked)
    }

    /// Apply a partial acknowledgement batch.
    ///
    /// An offset that is not locked for `member` is reported as
    /// [`INVALID_REQUEST`] and is not accepted. A release, a gap, or an
    /// acknowledgement of an expired lock returns the record only while
    /// another delivery is allowed. At the delivery limit those paths archive
    /// it. Successful entries in the same batch are still stored.
    pub fn acknowledge(
        &mut self,
        member: &str,
        now_ms: i64,
        acks: &[Ack],
    ) -> Result<Vec<AckResult>, Error> {
        self.ready()?;
        if !valid_member(member) || now_ms < 0 {
            return Err(Error::InvalidRequest);
        }
        let deadline = now_ms
            .checked_add(self.limits.lock_ms)
            .ok_or(Error::InvalidRequest)?;
        let mut next = self.state.clone();
        let mut results = Vec::new();
        let mut changed = false;
        for ack in acks {
            let Some(record) = next.records.get_mut(&ack.offset) else {
                results.push(AckResult {
                    offset: ack.offset,
                    error_code: INVALID_REQUEST,
                });
                continue;
            };
            let (locked_member, leader_epoch, deadline_ms) = match &record.phase {
                Phase::Locked(lock) => (lock.member.clone(), lock.leader_epoch, lock.deadline_ms),
                _ => {
                    results.push(AckResult {
                        offset: ack.offset,
                        error_code: INVALID_REQUEST,
                    });
                    continue;
                }
            };
            if now_ms >= deadline_ms {
                // The lock is already dead. At the delivery limit this is the
                // same archive `expire` would perform; a live member must not
                // accept the record on the way out.
                release_lock(record, self.limits.max_deliveries);
                changed = true;
                results.push(AckResult {
                    offset: ack.offset,
                    error_code: INVALID_REQUEST,
                });
                continue;
            }
            if locked_member != member {
                results.push(AckResult {
                    offset: ack.offset,
                    error_code: INVALID_REQUEST,
                });
                continue;
            }
            match ack.kind {
                ACK_ACCEPT => record.phase = Phase::Accepted,
                ACK_RELEASE | ACK_GAP => release_lock(record, self.limits.max_deliveries),
                ACK_REJECT => record.phase = Phase::Rejected,
                ACK_RENEW => {
                    record.phase = Phase::Locked(Lock {
                        member: locked_member,
                        deadline_ms: deadline,
                        leader_epoch,
                    });
                }
                _ => {
                    results.push(AckResult {
                        offset: ack.offset,
                        error_code: INVALID_REQUEST,
                    });
                    continue;
                }
            }
            changed = true;
            results.push(AckResult {
                offset: ack.offset,
                error_code: ACK_OK,
            });
        }
        if changed {
            self.commit(next)?;
        }
        Ok(results)
    }

    /// Release locks whose deadline has passed. Accepted records stay accepted.
    /// A lock that already used its delivery limit is archived.
    pub fn expire(&mut self, now_ms: i64) -> Result<usize, Error> {
        self.ready()?;
        if now_ms < 0 {
            return Err(Error::InvalidRequest);
        }
        let mut next = self.state.clone();
        let mut released = 0usize;
        for record in next.records.values_mut() {
            if let Phase::Locked(lock) = &record.phase {
                if now_ms >= lock.deadline_ms {
                    release_lock(record, self.limits.max_deliveries);
                    released = released.saturating_add(1);
                }
            }
        }
        if released > 0 {
            self.commit(next)?;
        }
        Ok(released)
    }

    /// Release locks taken under `leader_epoch` without accepting them.
    pub fn lose_leader(&mut self, leader_epoch: i64) -> Result<usize, Error> {
        self.ready()?;
        if leader_epoch < 0 {
            return Err(Error::InvalidRequest);
        }
        let mut next = self.state.clone();
        let mut released = 0usize;
        for record in next.records.values_mut() {
            if let Phase::Locked(lock) = &record.phase {
                if lock.leader_epoch == leader_epoch {
                    release_lock(record, self.limits.max_deliveries);
                    released = released.saturating_add(1);
                }
            }
        }
        if released > 0 {
            self.commit(next)?;
        }
        Ok(released)
    }

    /// Offsets that were accepted.
    pub fn accepted(&self) -> Vec<i64> {
        self.offsets_in(|phase| matches!(phase, Phase::Accepted))
    }

    /// Offsets that reached the delivery limit.
    pub fn archived(&self) -> Vec<i64> {
        self.offsets_in(|phase| matches!(phase, Phase::Archived))
    }

    /// Offsets that are currently locked.
    pub fn locked(&self) -> Vec<i64> {
        self.offsets_in(|phase| matches!(phase, Phase::Locked(_)))
    }

    fn offsets_in(&self, pred: impl Fn(&Phase) -> bool) -> Vec<i64> {
        self.state
            .records
            .iter()
            .filter(|(_, record)| pred(&record.phase))
            .map(|(offset, _)| *offset)
            .collect()
    }

    fn commit(&mut self, state: State) -> Result<(), Error> {
        let payload = encode(&state)?;
        if payload.len() > self.limits.journal.max_entry_bytes() {
            return Err(Error::InvalidLimits);
        }
        self.persist(&payload)?;
        self.state = state;
        Ok(())
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist(&mut self, payload: &[u8]) -> Result<(), Error> {
        if self.stored >= self.limits.max_records.saturating_mul(8) {
            return Err(Error::InvalidLimits);
        }
        match self.journal.append(1, payload) {
            Ok(_) => {
                self.stored = self.stored.saturating_add(1);
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(error.into())
            }
        }
    }
}

fn valid_member(member: &str) -> bool {
    !member.is_empty() && member.len() <= 249 && !member.chars().any(char::is_control)
}

fn encode(state: &State) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    let count = u32::try_from(state.records.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (offset, record) in &state.records {
        bytes.extend_from_slice(&offset.to_be_bytes());
        bytes.extend_from_slice(&record.deliveries.to_be_bytes());
        match &record.phase {
            Phase::Available => bytes.push(1),
            Phase::Accepted => bytes.push(2),
            Phase::Rejected => bytes.push(3),
            Phase::Archived => bytes.push(4),
            Phase::Locked(lock) => {
                bytes.push(5);
                put_str(&mut bytes, &lock.member)?;
                bytes.extend_from_slice(&lock.deadline_ms.to_be_bytes());
                bytes.extend_from_slice(&lock.leader_epoch.to_be_bytes());
            }
        }
    }
    Ok(bytes)
}

fn decode(mut bytes: &[u8]) -> Result<State, Error> {
    if bytes.len() < RECORD_MAGIC.len() || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    bytes = &bytes[RECORD_MAGIC.len()..];
    let count = take_u32(&mut bytes)?;
    let mut records = BTreeMap::new();
    for _ in 0..count {
        let offset = take_i64(&mut bytes)?;
        let deliveries = take_u32(&mut bytes)?;
        let phase = match take_u8(&mut bytes)? {
            1 => Phase::Available,
            2 => Phase::Accepted,
            3 => Phase::Rejected,
            4 => Phase::Archived,
            5 => Phase::Locked(Lock {
                member: take_str(&mut bytes)?,
                deadline_ms: take_i64(&mut bytes)?,
                leader_epoch: take_i64(&mut bytes)?,
            }),
            _ => return Err(Error::InvalidRequest),
        };
        records.insert(offset, Record { deliveries, phase });
    }
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok(State { records })
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    let len = u16::try_from(value.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn release_lock(record: &mut Record, max_deliveries: u32) {
    record.phase = if record.deliveries >= max_deliveries {
        Phase::Archived
    } else {
        Phase::Available
    };
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if input.len() < len {
        return Err(Error::InvalidRequest);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn take_str(input: &mut &[u8]) -> Result<String, Error> {
    let len = usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    ));
    String::from_utf8(take(input, len)?.to_vec()).map_err(|_| Error::InvalidRequest)
}

fn take_i64(input: &mut &[u8]) -> Result<i64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(i64::from_be_bytes(value))
}

fn take_u32(input: &mut &[u8]) -> Result<u32, Error> {
    let mut value = [0u8; 4];
    value.copy_from_slice(take(input, 4)?);
    Ok(u32::from_be_bytes(value))
}

fn take_u8(input: &mut &[u8]) -> Result<u8, Error> {
    Ok(take(input, 1)?[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(8192, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(32, 2, 1_000, journal).unwrap_or_else(|_| panic!("delivery limits"))
    }

    fn open(name: &str) -> (Delivery, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-share-del-{name}-{nanos}"));
        let delivery = Delivery::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (delivery, path)
    }

    #[test]
    fn partial_ack_expiry_and_leader_loss_do_not_accept_undelivered_records() {
        let (mut delivery, path) = open("locks");
        for offset in [1, 2, 4] {
            delivery
                .append(offset)
                .unwrap_or_else(|error| panic!("{error:?}"));
        }
        assert_eq!(
            delivery
                .acquire("alice", 1, 0, &[1, 2])
                .unwrap_or_else(|error| panic!("{error:?}")),
            vec![1, 2]
        );
        let acked = delivery
            .acknowledge(
                "alice",
                0,
                &[
                    Ack {
                        offset: 1,
                        kind: ACK_ACCEPT,
                    },
                    Ack {
                        offset: 9,
                        kind: ACK_ACCEPT,
                    },
                    Ack {
                        offset: 2,
                        kind: ACK_RELEASE,
                    },
                ],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(acked[0].error_code, ACK_OK);
        assert_eq!(acked[1].error_code, INVALID_REQUEST);
        assert_eq!(acked[2].error_code, ACK_OK);
        assert_eq!(delivery.accepted(), vec![1]);
        assert!(delivery.locked().is_empty());
        assert_eq!(
            delivery
                .acquire("alice", 1, 0, &[2])
                .unwrap_or_else(|error| panic!("{error:?}")),
            vec![2]
        );
        assert_eq!(
            delivery
                .lose_leader(1)
                .unwrap_or_else(|error| panic!("{error:?}")),
            1
        );
        assert!(!delivery.accepted().contains(&2));
        assert!(delivery.locked().is_empty());
        assert_eq!(
            delivery
                .acquire("alice", 2, 0, &[4])
                .unwrap_or_else(|error| panic!("{error:?}")),
            vec![4]
        );
        let renewed = delivery
            .acknowledge(
                "alice",
                500,
                &[Ack {
                    offset: 4,
                    kind: ACK_RENEW,
                }],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(renewed[0].error_code, ACK_OK);
        assert_eq!(
            delivery
                .expire(1_000)
                .unwrap_or_else(|error| panic!("{error:?}")),
            0
        );
        assert_eq!(delivery.locked(), vec![4]);
        drop(delivery);
        let mut delivery =
            Delivery::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(delivery.accepted(), vec![1]);
        assert_eq!(delivery.locked(), vec![4]);
        assert!(!delivery.accepted().contains(&9));
        let expired = delivery
            .acknowledge(
                "alice",
                1_500,
                &[Ack {
                    offset: 4,
                    kind: ACK_ACCEPT,
                }],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(expired[0].error_code, INVALID_REQUEST);
        assert!(!delivery.accepted().contains(&4));
        assert!(delivery.locked().is_empty());
        assert_eq!(
            delivery
                .expire(1_500)
                .unwrap_or_else(|error| panic!("{error:?}")),
            0
        );
        assert_eq!(delivery.accepted(), vec![1]);
        assert!(delivery
            .acquire("alice", 2, 2_000, &[2])
            .unwrap_or_else(|error| panic!("{error:?}"))
            .is_empty());
        assert!(!delivery.accepted().contains(&2));
        assert!(delivery.locked().is_empty());
    }

    #[test]
    fn acquire_takes_a_lock_whose_deadline_has_passed() {
        let (mut delivery, _) = open("reclaim");
        delivery
            .append(7)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            delivery
                .acquire("alice", 1, 0, &[7])
                .unwrap_or_else(|error| panic!("{error:?}")),
            vec![7]
        );
        assert!(delivery
            .acquire("bob", 1, 999, &[7])
            .unwrap_or_else(|error| panic!("{error:?}"))
            .is_empty());
        assert_eq!(
            delivery
                .acquire("bob", 1, 1_000, &[7])
                .unwrap_or_else(|error| panic!("{error:?}")),
            vec![7]
        );
        let late = delivery
            .acknowledge(
                "alice",
                1_000,
                &[Ack {
                    offset: 7,
                    kind: ACK_ACCEPT,
                }],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(late[0].error_code, INVALID_REQUEST);
        assert!(delivery.accepted().is_empty());
        let accepted = delivery
            .acknowledge(
                "bob",
                1_000,
                &[Ack {
                    offset: 7,
                    kind: ACK_ACCEPT,
                }],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(accepted[0].error_code, ACK_OK);
        assert_eq!(delivery.accepted(), vec![7]);
    }

    #[test]
    fn expiry_at_the_delivery_limit_archives_the_record() {
        let (mut delivery, _) = open("archive");
        delivery
            .append(8)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            delivery
                .acquire("alice", 1, 0, &[8])
                .unwrap_or_else(|error| panic!("{error:?}")),
            vec![8]
        );
        delivery
            .acknowledge(
                "alice",
                0,
                &[Ack {
                    offset: 8,
                    kind: ACK_RELEASE,
                }],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            delivery
                .acquire("alice", 1, 0, &[8])
                .unwrap_or_else(|error| panic!("{error:?}")),
            vec![8]
        );
        assert_eq!(
            delivery
                .expire(1_000)
                .unwrap_or_else(|error| panic!("{error:?}")),
            1
        );
        assert_eq!(delivery.archived(), vec![8]);
        assert!(delivery.locked().is_empty());
        assert!(delivery
            .acquire("bob", 1, 1_000, &[8])
            .unwrap_or_else(|error| panic!("{error:?}"))
            .is_empty());
        assert!(delivery.accepted().is_empty());
    }

    #[test]
    fn release_gap_and_expired_ack_at_the_delivery_limit_archive_the_record() {
        let (mut delivery, path) = open("limit-release");
        for offset in [3, 4, 5] {
            delivery
                .append(offset)
                .unwrap_or_else(|error| panic!("{error:?}"));
            assert_eq!(
                delivery
                    .acquire("alice", 1, 0, &[offset])
                    .unwrap_or_else(|error| panic!("{error:?}")),
                vec![offset]
            );
            delivery
                .acknowledge(
                    "alice",
                    0,
                    &[Ack {
                        offset,
                        kind: ACK_RELEASE,
                    }],
                )
                .unwrap_or_else(|error| panic!("{error:?}"));
            assert_eq!(
                delivery
                    .acquire("alice", 1, 0, &[offset])
                    .unwrap_or_else(|error| panic!("{error:?}")),
                vec![offset]
            );
        }
        let released = delivery
            .acknowledge(
                "alice",
                0,
                &[Ack {
                    offset: 3,
                    kind: ACK_RELEASE,
                }],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(released[0].error_code, ACK_OK);
        let gap = delivery
            .acknowledge(
                "alice",
                0,
                &[Ack {
                    offset: 4,
                    kind: ACK_GAP,
                }],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(gap[0].error_code, ACK_OK);
        let expired = delivery
            .acknowledge(
                "alice",
                1_000,
                &[Ack {
                    offset: 5,
                    kind: ACK_ACCEPT,
                }],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(expired[0].error_code, INVALID_REQUEST);
        assert_eq!(delivery.archived(), vec![3, 4, 5]);
        assert!(delivery.locked().is_empty());
        assert!(delivery.accepted().is_empty());
        for offset in [3, 4, 5] {
            assert!(delivery
                .acquire("bob", 1, 1_000, &[offset])
                .unwrap_or_else(|error| panic!("{error:?}"))
                .is_empty());
        }
        drop(delivery);
        let delivery = Delivery::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(delivery.archived(), vec![3, 4, 5]);
        assert!(delivery.accepted().is_empty());
    }
}
