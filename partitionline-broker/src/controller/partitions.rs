//! Durable controller partition placement, leader epochs, and ISR changes.
//!
//! A placement names the replica set, the leader, and the in-sync replicas.
//! Electing a replica that is not in the ISR is refused unless the caller sets
//! unclean leader election. That path shrinks the ISR to the new leader and
//! bumps the leader epoch, because the previous in-sync history was abandoned.
//! Reassignments may add or remove replicas, but they cannot remove the leader
//! or leave the leader outside the ISR.
//!
//! Every confirmed change is one journal record. Restart replays the controller
//! epoch and the latest placement. A call that presents an older controller
//! epoch does not mutate state. This is not an AlterPartition or
//! AlterPartitionReassignments RPC, and it does not replicate partition bytes.

use crate::catalog;
use crate::journal::{self, Journal};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLPASN01";
const KIND_CONTROLLER: u8 = 1;
const KIND_PLACEMENT: u8 = 2;
const MAX_REPLICAS: usize = 128;

/// Kafka `STALE_CONTROLLER_EPOCH`.
pub const STALE_CONTROLLER_EPOCH: i16 = 11;
/// Kafka `UNKNOWN_TOPIC_OR_PARTITION`.
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// Kafka `INVALID_REPLICA_ASSIGNMENT`.
pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
/// Kafka `FENCED_LEADER_EPOCH`.
pub const FENCED_LEADER_EPOCH: i16 = 74;
/// Kafka `INELIGIBLE_REPLICA`.
pub const INELIGIBLE_REPLICA: i16 = 107;

/// Confirmed replica placement for one topic partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// Topic name.
    pub topic: String,
    /// Nonnegative partition index.
    pub partition: i32,
    /// Ordered replica broker ids.
    pub replicas: Vec<i32>,
    /// Current leader. Always a member of `replicas` and `isr`.
    pub leader: i32,
    /// In-sync replica ids. The leader is always included.
    pub isr: Vec<i32>,
    /// Leader epoch. Starts at 0 and increases when the leader changes or an
    /// unclean election discards the previous ISR.
    pub leader_epoch: i32,
}

/// Initial placement. The leader epoch starts at 0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Create {
    /// Topic name, using the catalog's Kafka name rules.
    pub topic: String,
    /// Nonnegative partition index.
    pub partition: i32,
    /// Nonempty unique nonnegative replica ids.
    pub replicas: Vec<i32>,
    /// Leader, which must belong to `replicas` and `isr`.
    pub leader: i32,
    /// Initial in-sync set. Must include the leader and no other brokers.
    pub isr: Vec<i32>,
}

/// Leader election against the current placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Election {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Candidate leader. Must already be a replica.
    pub leader: i32,
    /// Epoch the caller observed. A mismatch fences the call.
    pub expected_leader_epoch: i32,
    /// Permit electing a replica that is not currently in the ISR.
    pub unclean: bool,
}

/// Replica-set change that keeps the current leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reassignment {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Replacement replica set. Must still contain the leader.
    pub replicas: Vec<i32>,
    /// Epoch the caller observed.
    pub expected_leader_epoch: i32,
}

/// Why a placement change was refused or could not be stored.
#[derive(Debug)]
pub enum Error {
    /// The controller epoch is older than the recovered epoch.
    StaleController,
    /// The partition has no placement.
    UnknownPartition,
    /// The replica set, leader, or ISR is not a valid assignment.
    InvalidAssignment,
    /// The leader epoch is not the current one.
    FencedLeader,
    /// The candidate is a replica but not in the ISR, and unclean election is off.
    IneligibleReplica,
    /// Another confirmed record would exceed the budget.
    Budget,
    /// A failed append left this handle unable to confirm further changes.
    Poisoned,
    /// The supplied limits cannot store a maximum placement.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a caller-visible refusal. Storage and budget failures have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::StaleController => STALE_CONTROLLER_EPOCH,
            Self::UnknownPartition => UNKNOWN_TOPIC_OR_PARTITION,
            Self::InvalidAssignment | Self::InvalidLimits => INVALID_REPLICA_ASSIGNMENT,
            Self::FencedLeader => FENCED_LEADER_EPOCH,
            Self::IneligibleReplica => INELIGIBLE_REPLICA,
            Self::Budget | Self::Poisoned | Self::Storage(_) => return None,
        })
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Placement bounds and the journal that stores them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_partitions: usize,
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure a positive partition cap up to 100,000 and a record cap up to
    /// one million. The journal must accept one maximum placement record.
    pub fn new(
        max_partitions: usize,
        max_records: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=100_000).contains(&max_partitions)
            || !(1..=1_000_000).contains(&max_records)
            || journal.max_entry_bytes() < max_record_bytes()
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_partitions,
            max_records,
            journal,
        })
    }
}

/// Durable placement table for one controller lineage.
pub struct Table {
    limits: Limits,
    controller_epoch: i64,
    placements: BTreeMap<(String, i32), Placement>,
    journal: Journal,
    records: usize,
    poisoned: bool,
}

impl Table {
    /// Open or recover the placement journal. The controller epoch is -1 until
    /// the first [`Self::advance_controller`] record.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut table = Self {
            limits,
            controller_epoch: -1,
            placements: BTreeMap::new(),
            journal,
            records: 0,
            poisoned: false,
        };
        let mut offset = table.journal.base_offset();
        loop {
            let batch = table
                .journal
                .fetch(offset, 64, table.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidAssignment);
                }
                table.apply_record(&entry.payload)?;
                table.records = table.records.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(u64::from(entry.record_count))
                    .ok_or(Error::InvalidAssignment)?;
            }
        }
        if table.placements.len() > table.limits.max_partitions
            || table.records > table.limits.max_records
        {
            return Err(Error::Budget);
        }
        Ok(table)
    }

    /// Current controller epoch, or -1 before the first advance.
    pub fn controller_epoch(&self) -> i64 {
        self.controller_epoch
    }

    /// Recovered placement, if this partition has one.
    pub fn placement(&self, topic: &str, partition: i32) -> Option<&Placement> {
        self.placements.get(&(topic.to_owned(), partition))
    }

    /// Move the controller lineage forward. A smaller or equal epoch is stale.
    pub fn advance_controller(&mut self, epoch: i64) -> Result<i64, Error> {
        self.ready()?;
        if epoch < 0 || epoch <= self.controller_epoch {
            return Err(Error::StaleController);
        }
        self.persist(&encode_controller(epoch)?)?;
        self.controller_epoch = epoch;
        Ok(epoch)
    }

    /// Create the first placement. The leader epoch is 0.
    pub fn create(&mut self, controller_epoch: i64, create: &Create) -> Result<Placement, Error> {
        self.ready()?;
        self.require_controller(controller_epoch)?;
        catalog::validate_topic_name(&create.topic).map_err(|_| Error::InvalidAssignment)?;
        if create.partition < 0 {
            return Err(Error::InvalidAssignment);
        }
        validate_set(&create.replicas, Some(create.leader), &create.isr)?;
        let key = (create.topic.clone(), create.partition);
        if self.placements.contains_key(&key) {
            return Err(Error::InvalidAssignment);
        }
        if self.placements.len() >= self.limits.max_partitions {
            return Err(Error::Budget);
        }
        let placement = Placement {
            topic: create.topic.clone(),
            partition: create.partition,
            replicas: create.replicas.clone(),
            leader: create.leader,
            isr: create.isr.clone(),
            leader_epoch: 0,
        };
        self.persist(&encode_placement(controller_epoch, &placement)?)?;
        self.placements.insert(key, placement.clone());
        Ok(placement)
    }

    /// Change the leader. An in-ISR election keeps the ISR. An unclean election
    /// replaces the ISR with the new leader alone.
    pub fn elect(
        &mut self,
        controller_epoch: i64,
        election: &Election,
    ) -> Result<Placement, Error> {
        self.ready()?;
        self.require_controller(controller_epoch)?;
        let key = (election.topic.clone(), election.partition);
        let Some(current) = self.placements.get(&key).cloned() else {
            return Err(Error::UnknownPartition);
        };
        if election.expected_leader_epoch != current.leader_epoch {
            return Err(Error::FencedLeader);
        }
        if !current.replicas.contains(&election.leader) {
            return Err(Error::InvalidAssignment);
        }
        let in_isr = current.isr.contains(&election.leader);
        if !in_isr && !election.unclean {
            return Err(Error::IneligibleReplica);
        }
        if election.leader == current.leader && in_isr {
            return Ok(current);
        }
        let leader_epoch = current.leader_epoch.checked_add(1).ok_or(Error::Budget)?;
        let isr = if in_isr {
            current.isr.clone()
        } else {
            vec![election.leader]
        };
        let placement = Placement {
            leader: election.leader,
            isr,
            leader_epoch,
            ..current
        };
        self.persist(&encode_placement(controller_epoch, &placement)?)?;
        self.placements.insert(key, placement.clone());
        Ok(placement)
    }

    /// Replace the replica set. The leader stays, and the ISR shrinks to the
    /// replicas that remain. Removing the leader is refused.
    pub fn reassign(
        &mut self,
        controller_epoch: i64,
        change: &Reassignment,
    ) -> Result<Placement, Error> {
        self.ready()?;
        self.require_controller(controller_epoch)?;
        let key = (change.topic.clone(), change.partition);
        let Some(current) = self.placements.get(&key).cloned() else {
            return Err(Error::UnknownPartition);
        };
        if change.expected_leader_epoch != current.leader_epoch {
            return Err(Error::FencedLeader);
        }
        validate_replicas(&change.replicas)?;
        if !change.replicas.contains(&current.leader) {
            return Err(Error::InvalidAssignment);
        }
        let isr: Vec<i32> = current
            .isr
            .iter()
            .copied()
            .filter(|id| change.replicas.contains(id))
            .collect();
        if !isr.contains(&current.leader) {
            return Err(Error::InvalidAssignment);
        }
        if change.replicas == current.replicas {
            return Ok(current);
        }
        let placement = Placement {
            replicas: change.replicas.clone(),
            isr,
            ..current
        };
        self.persist(&encode_placement(controller_epoch, &placement)?)?;
        self.placements.insert(key, placement.clone());
        Ok(placement)
    }

    fn require_controller(&self, epoch: i64) -> Result<(), Error> {
        if epoch == self.controller_epoch && epoch >= 0 {
            Ok(())
        } else {
            Err(Error::StaleController)
        }
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn apply_record(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let (kind, rest) = split_kind(bytes)?;
        match kind {
            KIND_CONTROLLER => {
                let epoch = decode_controller(rest)?;
                if epoch < 0 || epoch <= self.controller_epoch {
                    return Err(Error::InvalidAssignment);
                }
                self.controller_epoch = epoch;
            }
            KIND_PLACEMENT => {
                let (controller_epoch, placement) = decode_placement(rest)?;
                if controller_epoch != self.controller_epoch {
                    return Err(Error::InvalidAssignment);
                }
                self.placements
                    .insert((placement.topic.clone(), placement.partition), placement);
            }
            _ => return Err(Error::InvalidAssignment),
        }
        Ok(())
    }

    fn persist(&mut self, payload: &[u8]) -> Result<(), Error> {
        if self.records >= self.limits.max_records {
            return Err(Error::Budget);
        }
        match self.journal.append(1, payload) {
            Ok(_) => {
                self.records = self.records.saturating_add(1);
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(Error::Storage(error))
            }
        }
    }
}

fn validate_set(replicas: &[i32], leader: Option<i32>, isr: &[i32]) -> Result<(), Error> {
    validate_replicas(replicas)?;
    let Some(leader) = leader else {
        return Err(Error::InvalidAssignment);
    };
    if !replicas.contains(&leader) {
        return Err(Error::InvalidAssignment);
    }
    let replica_set: BTreeSet<i32> = replicas.iter().copied().collect();
    let isr_set: BTreeSet<i32> = isr.iter().copied().collect();
    if isr.is_empty()
        || isr.len() != isr_set.len()
        || !isr_set.is_subset(&replica_set)
        || !isr_set.contains(&leader)
    {
        return Err(Error::InvalidAssignment);
    }
    Ok(())
}

fn validate_replicas(replicas: &[i32]) -> Result<(), Error> {
    if replicas.is_empty() || replicas.len() > MAX_REPLICAS {
        return Err(Error::InvalidAssignment);
    }
    let unique: BTreeSet<i32> = replicas.iter().copied().collect();
    if unique.len() != replicas.len() || unique.iter().any(|id| *id < 0) {
        return Err(Error::InvalidAssignment);
    }
    Ok(())
}

fn max_record_bytes() -> usize {
    RECORD_MAGIC.len() + 1 + 8 + 2 + 249 + 4 + 4 + 4 + 2 + MAX_REPLICAS * 4 + 2 + MAX_REPLICAS * 4
}

fn encode_controller(epoch: i64) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(17);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_CONTROLLER);
    bytes.extend_from_slice(&epoch.to_be_bytes());
    Ok(bytes)
}

fn encode_placement(controller_epoch: i64, placement: &Placement) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_PLACEMENT);
    bytes.extend_from_slice(&controller_epoch.to_be_bytes());
    put_str(&mut bytes, &placement.topic)?;
    bytes.extend_from_slice(&placement.partition.to_be_bytes());
    bytes.extend_from_slice(&placement.leader.to_be_bytes());
    bytes.extend_from_slice(&placement.leader_epoch.to_be_bytes());
    put_ids(&mut bytes, &placement.replicas)?;
    put_ids(&mut bytes, &placement.isr)?;
    Ok(bytes)
}

fn split_kind(bytes: &[u8]) -> Result<(u8, &[u8]), Error> {
    if bytes.len() < RECORD_MAGIC.len() + 1 || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidAssignment);
    }
    Ok((bytes[RECORD_MAGIC.len()], &bytes[RECORD_MAGIC.len() + 1..]))
}

fn decode_controller(bytes: &[u8]) -> Result<i64, Error> {
    if bytes.len() != 8 {
        return Err(Error::InvalidAssignment);
    }
    let mut epoch = [0u8; 8];
    epoch.copy_from_slice(bytes);
    Ok(i64::from_be_bytes(epoch))
}

fn decode_placement(mut bytes: &[u8]) -> Result<(i64, Placement), Error> {
    let controller_epoch = i64::from_be_bytes(
        take(&mut bytes, 8)?
            .try_into()
            .map_err(|_| Error::InvalidAssignment)?,
    );
    let topic = take_str(&mut bytes)?;
    let partition = i32::from_be_bytes(
        take(&mut bytes, 4)?
            .try_into()
            .map_err(|_| Error::InvalidAssignment)?,
    );
    let leader = i32::from_be_bytes(
        take(&mut bytes, 4)?
            .try_into()
            .map_err(|_| Error::InvalidAssignment)?,
    );
    let leader_epoch = i32::from_be_bytes(
        take(&mut bytes, 4)?
            .try_into()
            .map_err(|_| Error::InvalidAssignment)?,
    );
    let replicas = take_ids(&mut bytes)?;
    let isr = take_ids(&mut bytes)?;
    if !bytes.is_empty() {
        return Err(Error::InvalidAssignment);
    }
    validate_set(&replicas, Some(leader), &isr)?;
    if partition < 0 || leader_epoch < 0 {
        return Err(Error::InvalidAssignment);
    }
    Ok((
        controller_epoch,
        Placement {
            topic,
            partition,
            replicas,
            leader,
            isr,
            leader_epoch,
        },
    ))
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    let len = u16::try_from(value.len()).map_err(|_| Error::InvalidAssignment)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_ids(bytes: &mut Vec<u8>, ids: &[i32]) -> Result<(), Error> {
    let len = u16::try_from(ids.len()).map_err(|_| Error::InvalidAssignment)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    for id in ids {
        bytes.extend_from_slice(&id.to_be_bytes());
    }
    Ok(())
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if input.len() < len {
        return Err(Error::InvalidAssignment);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn take_str(input: &mut &[u8]) -> Result<String, Error> {
    let len = usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidAssignment)?,
    ));
    let bytes = take(input, len)?;
    std::str::from_utf8(bytes)
        .map(|text| text.to_owned())
        .map_err(|_| Error::InvalidAssignment)
}

fn take_ids(input: &mut &[u8]) -> Result<Vec<i32>, Error> {
    let len = usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidAssignment)?,
    ));
    let mut ids = Vec::with_capacity(len);
    for _ in 0..len {
        ids.push(i32::from_be_bytes(
            take(input, 4)?
                .try_into()
                .map_err(|_| Error::InvalidAssignment)?,
        ));
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(max_record_bytes(), 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(8, 100, journal).unwrap_or_else(|_| panic!("placement limits"))
    }

    fn table(name: &str) -> (Table, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-placement-{name}-{nanos}"));
        let table = Table::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (table, path)
    }

    fn create_orders() -> Create {
        Create {
            topic: "orders".into(),
            partition: 0,
            replicas: vec![1, 2, 3],
            leader: 1,
            isr: vec![1, 2, 3],
        }
    }

    #[test]
    fn placement_and_leader_epoch_replay_after_controller_failover() {
        let (mut table, path) = table("replay");
        table
            .advance_controller(1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let created = table
            .create(1, &create_orders())
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(created.leader_epoch, 0);
        let elected = table
            .elect(
                1,
                &Election {
                    topic: "orders".into(),
                    partition: 0,
                    leader: 2,
                    expected_leader_epoch: 0,
                    unclean: false,
                },
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(elected.leader, 2);
        assert_eq!(elected.leader_epoch, 1);
        assert!(matches!(
            table.advance_controller(1),
            Err(Error::StaleController)
        ));
        table
            .advance_controller(2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            table.reassign(
                1,
                &Reassignment {
                    topic: "orders".into(),
                    partition: 0,
                    replicas: vec![2, 3, 4],
                    expected_leader_epoch: 1,
                },
            ),
            Err(Error::StaleController)
        ));
        drop(table);
        let restored = Table::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.controller_epoch(), 2);
        let placement = restored
            .placement("orders", 0)
            .unwrap_or_else(|| panic!("placement"));
        assert_eq!(placement.leader, 2);
        assert_eq!(placement.leader_epoch, 1);
        assert_eq!(placement.isr, vec![1, 2, 3]);
    }

    #[test]
    fn unclean_election_is_refused_until_explicitly_enabled() {
        let (mut table, _) = table("unclean");
        table
            .advance_controller(4)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut create = create_orders();
        create.isr = vec![1];
        table
            .create(4, &create)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let refused = table.elect(
            4,
            &Election {
                topic: "orders".into(),
                partition: 0,
                leader: 3,
                expected_leader_epoch: 0,
                unclean: false,
            },
        );
        assert!(matches!(refused, Err(Error::IneligibleReplica)));
        assert_eq!(
            table
                .placement("orders", 0)
                .unwrap_or_else(|| panic!("placement"))
                .leader,
            1
        );
        let elected = table
            .elect(
                4,
                &Election {
                    topic: "orders".into(),
                    partition: 0,
                    leader: 3,
                    expected_leader_epoch: 0,
                    unclean: true,
                },
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(elected.leader_epoch, 1);
        assert_eq!(elected.isr, vec![3]);
        assert!(matches!(
            table.elect(
                4,
                &Election {
                    topic: "orders".into(),
                    partition: 0,
                    leader: 1,
                    expected_leader_epoch: 0,
                    unclean: true,
                },
            ),
            Err(Error::FencedLeader)
        ));
    }

    #[test]
    fn reassignment_cannot_drop_the_leader_and_shrinks_the_isr() {
        let (mut table, _) = table("reassign");
        table
            .advance_controller(1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        table
            .create(1, &create_orders())
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            table.reassign(
                1,
                &Reassignment {
                    topic: "orders".into(),
                    partition: 0,
                    replicas: vec![2, 3],
                    expected_leader_epoch: 0,
                },
            ),
            Err(Error::InvalidAssignment)
        ));
        let changed = table
            .reassign(
                1,
                &Reassignment {
                    topic: "orders".into(),
                    partition: 0,
                    replicas: vec![1, 3, 4],
                    expected_leader_epoch: 0,
                },
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(changed.replicas, vec![1, 3, 4]);
        assert_eq!(changed.isr, vec![1, 3]);
        assert_eq!(changed.leader_epoch, 0);
    }

    #[test]
    fn a_negative_partition_is_refused_before_it_is_stored() {
        let (mut table, path) = table("negative");
        table
            .advance_controller(1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut create = create_orders();
        create.partition = -1;
        assert!(matches!(
            table.create(1, &create),
            Err(Error::InvalidAssignment)
        ));
        assert!(table.placement("orders", -1).is_none());
        drop(table);
        let restored = Table::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.controller_epoch(), 1);
        assert!(restored.placement("orders", -1).is_none());
    }
}
