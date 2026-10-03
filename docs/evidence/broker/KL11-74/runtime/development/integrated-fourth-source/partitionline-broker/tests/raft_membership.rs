//! Bounded dynamic configuration identities, strict local bytes and unsafe set jumps.

use partitionline_broker::raft::{
    election::LogPosition,
    membership::{Endpoint, Error, Key, Voter, Voters, MAX_CONFIGURATION_BYTES},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn voter(id: u32) -> Result<Voter> {
    Ok(Voter::new(
        Key::new(id, [(id + 1) as u8; 16])?,
        vec![Endpoint::new(
            "CONTROLLER".into(),
            format!("host{id}"),
            9000 + id as u16,
        )?],
        0,
        1,
    )?)
}
fn initial() -> Result<Voters> {
    Ok(Voters::new(
        0,
        LogPosition::default(),
        1,
        vec![voter(3)?, voter(1)?, voter(2)?],
    )?)
}
fn at(index: u64) -> LogPosition {
    LogPosition { term: 6, index }
}

#[test]
fn one_added_voter_uses_new_majority_and_preserves_exact_existing_descriptors() -> Result {
    let old = initial()?;
    let added = old.add(voter(4)?, at(2))?;
    old.validate_successor(&added)?;
    assert_eq!(old.majority(), 2);
    assert_eq!(added.majority(), 3);
    assert_eq!(added.epoch(), 1);
    assert_eq!(added.position(), at(2));
    for v in old.voters() {
        assert_eq!(added.by_id(v.key().id), Some(v));
    }
    assert_eq!(Voters::decode(&added.encode()?)?, added);
    Ok(())
}

#[test]
fn removed_leader_identity_is_absent_from_new_voters_and_both_survivors_are_needed() -> Result {
    let old = initial()?;
    let new = old.remove(Key::new(1, [2; 16])?, at(3))?;
    old.validate_successor(&new)?;
    assert_eq!(new.majority(), 2);
    assert!(!new.contains(Key::new(1, [2; 16])?));
    assert_eq!(
        new.voters().iter().map(|v| v.key().id).collect::<Vec<_>>(),
        vec![2, 3]
    );
    Ok(())
}

#[test]
fn same_id_with_stale_directory_cannot_remove_or_replace_an_existing_voter() -> Result {
    let old = initial()?;
    assert_eq!(
        old.remove(Key::new(1, [9; 16])?, at(1)),
        Err(Error::VoterNotFound)
    );
    let mut stale = voter(1)?;
    stale = Voter::new(Key::new(1, [9; 16])?, stale.endpoints().to_vec(), 0, 1)?;
    assert_eq!(old.add(stale, at(1)), Err(Error::DuplicateVoter));
    assert!(Key::new(i32::MAX as u32 + 1, [1; 16]).is_err());
    assert!(Key::new(1, [0; 16]).is_err());
    Ok(())
}

#[test]
fn arbitrary_set_jumps_same_size_replacements_and_descriptor_updates_reject() -> Result {
    let old = initial()?;
    for voters in [
        vec![voter(1)?, voter(2)?, voter(3)?, voter(4)?, voter(5)?],
        vec![voter(1)?],
        vec![voter(2)?, voter(3)?, voter(4)?],
        vec![
            Voter::new(
                Key::new(1, [2; 16])?,
                vec![Endpoint::new("CONTROLLER".into(), "changed".into(), 9001)?],
                0,
                1,
            )?,
            voter(2)?,
            voter(3)?,
            voter(4)?,
        ],
    ] {
        let forged = Voters::new(1, at(2), 1, voters)?;
        assert!(old.validate_successor(&forged).is_err());
    }
    let unchanged = Voters::new(1, at(2), 1, old.voters().to_vec())?;
    assert!(old.validate_successor(&unchanged).is_err());
    Ok(())
}

#[test]
fn configuration_epoch_and_authoritative_position_cannot_be_forged() -> Result {
    let old = initial()?;
    let new = old.add(voter(4)?, at(4))?;
    let skipped = Voters::new(3, at(6), 1, new.voters().to_vec())?;
    assert!(old.validate_successor(&skipped).is_err());
    assert!(new.remove(Key::new(4, [5; 16])?, at(4)).is_err());
    assert!(new
        .remove(Key::new(4, [5; 16])?, LogPosition { term: 5, index: 5 })
        .is_err());
    assert!(Voters::new(1, LogPosition::default(), 1, old.voters().to_vec()).is_err());
    assert!(Voters::new(0, at(1), 1, old.voters().to_vec()).is_err());
    Ok(())
}

#[test]
fn last_voter_and_unsupported_features_are_rejected_before_changes() -> Result {
    let one = Voters::new(0, LogPosition::default(), 1, vec![voter(1)?])?;
    assert_eq!(
        one.remove(Key::new(1, [2; 16])?, at(1)),
        Err(Error::LastVoter)
    );
    assert!(Voters::new(0, LogPosition::default(), 0, vec![voter(1)?]).is_err());
    assert!(Voters::new(0, LogPosition::default(), 2, vec![voter(1)?]).is_err());
    let incompatible = Voter::new(Key::new(4, [5; 16])?, voter(4)?.endpoints().to_vec(), 0, 0)?;
    assert_eq!(
        initial()?.add(incompatible, at(1)),
        Err(Error::UnsupportedFeature)
    );
    Ok(())
}

#[test]
fn listener_endpoint_and_feature_range_validation_is_explicit() -> Result {
    for (listener, host, port) in [
        ("", "host", 9000),
        ("controller", "host", 9000),
        ("CONTROLLER", "", 9000),
        ("CONTROLLER", "bad\0host", 9000),
        ("CONTROLLER", "host", 0),
    ] {
        assert!(Endpoint::new(listener.into(), host.into(), port).is_err());
    }
    let v = voter(1)?;
    assert!(v.endpoint("CONTROLLER").is_some());
    assert!(v.endpoint("MISSING").is_none());
    let ep = v.endpoints()[0].clone();
    assert!(Voter::new(v.key(), vec![ep.clone(), ep], 0, 1).is_err());
    assert!(Voter::new(v.key(), vec![], 0, 1).is_err());
    assert!(Voter::new(v.key(), v.endpoints().to_vec(), -1, 1).is_err());
    assert!(Voter::new(v.key(), v.endpoints().to_vec(), 2, 1).is_err());
    Ok(())
}

#[test]
fn all_truncations_and_reserved_count_utf8_identity_mutations_reject() -> Result {
    let bytes = initial()?.encode()?;
    for end in 0..bytes.len() {
        assert!(Voters::decode(&bytes[..end]).is_err(), "truncation{end}");
    }
    for (offset, value) in [(0, 0), (36, 1), (34, 0xff), (64, 0xff), (66, 1)] {
        let mut changed = bytes.clone();
        changed[offset] = value;
        assert!(Voters::decode(&changed).is_err(), "offset{offset}");
    }
    let mut changed = bytes.clone();
    changed[44..60].fill(0);
    assert!(Voters::decode(&changed).is_err());
    let host = bytes
        .windows(5)
        .position(|b| b == b"host1")
        .ok_or("missing host")?;
    let mut changed = bytes.clone();
    changed[host] = 0xff;
    assert!(Voters::decode(&changed).is_err());
    let mut trailing = bytes;
    trailing.push(0);
    assert!(Voters::decode(&trailing).is_err());
    Ok(())
}

#[test]
fn noncanonical_order_and_duplicate_voters_do_not_decode() -> Result {
    let bytes = initial()?.encode()?;
    let mut swapped = bytes.clone();
    // Three fixture voter records are equal length with five-byte hosts.
    let first = 40;
    let size = (bytes.len() - first) / 3;
    swapped[first..first + size].copy_from_slice(&bytes[first + size..first + 2 * size]);
    swapped[first + size..first + 2 * size].copy_from_slice(&bytes[first..first + size]);
    assert!(Voters::decode(&swapped).is_err());
    let mut duplicate = bytes.clone();
    duplicate[first + size..first + 2 * size].copy_from_slice(&bytes[first..first + size]);
    assert!(Voters::decode(&duplicate).is_err());
    Ok(())
}

#[test]
fn complete_maximum_configuration_and_overlimit_counts_remain_bounded() -> Result {
    let mut voters = Vec::new();
    for id in 0..64 {
        let mut endpoints = Vec::new();
        for suffix in ['A', 'B', 'C', 'D'] {
            endpoints.push(Endpoint::new(
                format!("{}{suffix}", "X".repeat(248)),
                "h".repeat(249),
                9000,
            )?);
        }
        voters.push(Voter::new(
            Key::new(id, [(id + 1) as u8; 16])?,
            endpoints,
            1,
            1,
        )?);
    }
    let config = Voters::new(0, LogPosition::default(), 1, voters.clone())?;
    let bytes = config.encode()?;
    assert!(bytes.len() <= MAX_CONFIGURATION_BYTES);
    assert_eq!(Voters::decode(&bytes)?, config);
    voters.push(voter(64)?);
    assert!(Voters::new(0, LogPosition::default(), 1, voters).is_err());
    assert!(Voters::decode(&vec![0; MAX_CONFIGURATION_BYTES + 1]).is_err());
    Ok(())
}

#[test]
fn bootstrap_genesis_is_immutable_and_a_new_directory_starts_as_an_observer() -> Result {
    use partitionline_broker::raft::membership::Bootstrap;
    let genesis = initial()?;
    let new_directory = Voter::new(Key::new(1, [9; 16])?, voter(1)?.endpoints().to_vec(), 0, 1)?;
    let boot = Bootstrap::new(new_directory, genesis.clone(), "CONTROLLER".into())?;
    assert_eq!(boot.genesis(), &genesis);
    assert!(!boot.genesis().contains(boot.local().key()));
    assert!(Bootstrap::new(voter(4)?, genesis.clone(), "MISSING".into()).is_err());
    assert!(Bootstrap::new(
        voter(1)?,
        genesis.add(voter(4)?, at(1))?,
        "CONTROLLER".into()
    )
    .is_err());
    Ok(())
}

#[test]
fn native_recent_catchup_admission_is_not_redefined_as_current_end_equality() -> Result {
    use partitionline_broker::raft::membership::Progress;
    let mut progress = Progress::default();
    progress.observe(2, 2, 100)?;
    progress.observe(2, 3, 200)?;
    assert_eq!(progress.matched, 2);
    assert!(progress.caught_up_for_addition(300));
    assert!(progress.caught_up_for_addition(3_600_199));
    assert!(!progress.caught_up_for_addition(3_600_200));
    assert!(!progress.caught_up_for_addition(199));
    assert!(progress.observe(1, 3, 300).is_err());
    assert!(progress.observe(4, 3, 300).is_err());
    assert!(progress.observe(2, 3, 199).is_err());
    assert_eq!(progress.matched, 2);
    Ok(())
}

#[test]
fn prior_fetch_end_catchup_uses_previous_contact_and_time_zero_is_insufficient() -> Result {
    use partitionline_broker::raft::membership::Progress;
    let mut progress = Progress::default();
    progress.observe(1, 2, 100)?;
    assert!(!progress.caught_up_for_addition(100));
    progress.observe(2, 3, 200)?;
    assert_eq!(progress.last_caught_up_ms, Some(100));
    assert!(progress.caught_up_for_addition(200));
    let mut zero = Progress::default();
    zero.observe(1, 1, 0)?;
    assert!(!zero.caught_up_for_addition(1));
    Ok(())
}

use partitionline_broker::raft::{
    election::{Role, Timeouts},
    membership::Bootstrap,
    protocol,
    replication::{self, Node},
    snapshot,
};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

fn hex_bytes(bytes: &[u8]) -> Result<String> {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}")?;
    }
    Ok(out)
}
struct DynamicGroup {
    root: PathBuf,
    genesis: Voters,
    local: Vec<Voter>,
    nodes: Vec<Option<Node>>,
}
impl DynamicGroup {
    fn new(count: u32, observer: Voter) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "partitionline-membership-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root)?;
        let local: Vec<Voter> = (1..=count).map(voter).collect::<Result<_>>()?;
        let genesis = Voters::new(0, LogPosition::default(), 1, local.clone())?;
        let mut group = Self {
            root,
            genesis,
            local,
            nodes: Vec::new(),
        };
        group.local.push(observer);
        for index in 0..group.local.len() {
            group.nodes.push(Some(group.open(index, 0)?));
        }
        Ok(group)
    }
    fn open(&self, index: usize, now: u64) -> Result<Node> {
        let local = &self.local[index];
        let name = format!("{}-{}", local.key().id, hex_bytes(&local.key().directory)?);
        let mut config = protocol::Config::new(
            local.key().id,
            vec![local.key().id],
            "membership-test".into(),
        )?;
        config.election_timeouts = Timeouts::new(10, 10)?;
        let config = replication::Config::new(config);
        let identity = snapshot::Identity::dynamic(
            "membership-test".into(),
            "__cluster_metadata".into(),
            0,
            self.genesis.clone(),
        )?;
        let store = snapshot::Store::open(
            self.root.join(format!("{name}.images")),
            identity,
            snapshot::Limits::default(),
        )?;
        Ok(Node::open_dynamic(
            self.root.join(format!("{name}.wal")),
            self.root.join(format!("{name}.election")),
            config,
            Bootstrap::new(local.clone(), self.genesis.clone(), "CONTROLLER".into())?,
            Some(store),
            now,
        )?)
    }
    fn node(&mut self, index: usize) -> Result<&mut Node> {
        self.nodes
            .get_mut(index)
            .and_then(Option::as_mut)
            .ok_or_else(|| "test owner is closed".into())
    }
    fn key(&self, index: usize) -> Key {
        self.local[index].key()
    }
    fn elect(&mut self, now: u64) -> Result {
        let requests = self.node(0)?.campaign_dynamic(now)?;
        assert!(!requests.is_empty());
        for request in requests {
            let index = self
                .local
                .iter()
                .position(|v| v.key() == request.context.peer)
                .ok_or("missing voter")?;
            let response = self.node(index)?.receive_dynamic_vote(request, now)?;
            self.node(0)?.acknowledge_dynamic_vote(response, now)?;
        }
        assert_eq!(self.node(0)?.state().election.role, Role::Leader);
        self.node(0)?.activate_leader(now)?;
        Ok(())
    }
    fn exchange(&mut self, index: usize, now: u64) -> Result {
        let key = self.key(index);
        let request = self.node(0)?.prepare_dynamic(key, now)?;
        let response = self.node(index)?.receive_dynamic(&request, now)?;
        self.node(0)?.acknowledge_dynamic(response, now)?;
        Ok(())
    }
    fn catch_up(&mut self, index: usize, now: u64) -> Result {
        for _ in 0..4 {
            self.exchange(index, now)?;
            if self.node(index)?.state().last_position == self.node(0)?.state().last_position {
                return Ok(());
            }
        }
        Err("bounded catch-up did not complete".into())
    }
    fn probe(&mut self, index: usize, now: u64) -> Result {
        let candidate = self.local[index].clone();
        let request = self.node(0)?.probe_addition(candidate, now)?;
        let response = self.node(index)?.receive_feature_probe(request, now)?;
        self.node(0)?.acknowledge_feature_probe(response, now)?;
        Ok(())
    }
    fn reopen(&mut self, index: usize, now: u64) -> Result {
        drop(self.nodes[index].take());
        self.nodes[index] = Some(self.open(index, now)?);
        Ok(())
    }
}
impl Drop for DynamicGroup {
    fn drop(&mut self) {
        self.nodes.clear();
        drop(fs::remove_dir_all(&self.root));
    }
}

#[test]
fn durable_addition_activates_new_set_old_majority_cannot_commit_and_restart_preserves_it() -> Result
{
    let mut group = DynamicGroup::new(3, voter(4)?)?;
    group.elect(10)?;
    group.catch_up(1, 11)?;
    assert_eq!(group.node(0)?.state().committed_end, 1);
    group.probe(3, 12)?;
    group.catch_up(3, 13)?;
    let key = group.key(3);
    let receipt = group.node(0)?.add_voter(key, 14)?;
    assert_eq!(receipt.position.index, 2);
    assert!(!receipt.committed);
    assert_eq!(group.node(0)?.voters()?.voters().len(), 4);
    group.catch_up(1, 15)?;
    assert_eq!(
        group.node(0)?.state().committed_end,
        1,
        "two of four old votes are insufficient"
    );
    assert!(
        group.node(0)?.remove_voter(key, 15).is_err(),
        "uncommitted prior change blocks another"
    );
    group.catch_up(2, 16)?;
    assert_eq!(group.node(0)?.state().committed_end, 2);
    assert!(group.node(0)?.change_status()?.committed);
    group.catch_up(3, 17)?;
    group.exchange(3, 18)?;
    let committed = group.node(0)?.fetch_committed(1, 8, 2 * 1024 * 1024)?;
    assert_eq!(committed[1].kind, replication::RecordKind::Voters);
    for index in [0, 1, 2, 3] {
        let old_term = group.node(index)?.state().election.persistent.term;
        let old_vote = group.node(index)?.voted_directory()?;
        group.reopen(index, 20)?;
        assert_eq!(group.node(index)?.voters()?.epoch(), 1);
        assert_eq!(
            group.node(index)?.state().election.persistent.term,
            old_term
        );
        assert_eq!(group.node(index)?.voted_directory()?, old_vote);
        assert!(group.node(index)?.state().ready);
    }
    assert_eq!(
        group.node(0)?.fetch_committed(1, 8, 2 * 1024 * 1024)?,
        committed
    );
    Ok(())
}

#[test]
fn removed_leader_counts_no_self_vote_and_fences_after_commit_even_without_caller() -> Result {
    let mut group = DynamicGroup::new(3, voter(4)?)?;
    group.elect(10)?;
    group.catch_up(1, 11)?;
    group.catch_up(2, 12)?;
    let leader = group.key(0);
    let receipt = group.node(0)?.remove_voter(leader, 13)?;
    assert!(!receipt.committed);
    group.catch_up(1, 14)?;
    assert_eq!(
        group.node(0)?.state().committed_end,
        1,
        "removed local leader adds no match"
    );
    // There is no retained caller future; durable commitment alone must fence.
    group.catch_up(2, 15)?;
    assert_eq!(group.node(0)?.state().committed_end, 2);
    assert_eq!(group.node(0)?.state().election.role, Role::Follower);
    assert!(group.node(0)?.propose(b"removed writer", 16).is_err());
    group.reopen(0, 20)?;
    assert_eq!(group.node(0)?.state().election.role, Role::Follower);
    assert!(group.node(0)?.campaign_dynamic(40)?.is_empty());
    assert!(!group.node(0)?.voters()?.contains(leader));
    Ok(())
}

#[test]
fn observer_feature_mismatch_directory_forgery_and_unqualified_legacy_paths_reject() -> Result {
    let mut old = voter(4)?;
    old = Voter::new(old.key(), old.endpoints().to_vec(), 0, 0)?;
    let mut group = DynamicGroup::new(3, old)?;
    assert!(group.node(3)?.campaign_dynamic(10)?.is_empty());
    group.elect(10)?;
    group.catch_up(1, 11)?;
    let candidate = group.local[3].clone();
    let request = group.node(0)?.probe_addition(candidate, 12)?;
    let response = group.node(3)?.receive_feature_probe(request, 12)?;
    assert!(group
        .node(0)?
        .acknowledge_feature_probe(response, 12)
        .is_err());
    group.node(0)?.timeout_feature_probe(request)?;
    let key = group.key(1);
    let request = group.node(0)?.prepare_dynamic(key, 13)?;
    let mut forged = request.clone();
    forged.context.peer.directory = [99; 16];
    assert!(group.node(1)?.receive_dynamic(&forged, 13).is_err());
    assert!(group.node(1)?.receive(&request.request, 13).is_err());
    assert!(group.node(0)?.prepare(key.id, 13).is_err());
    let mut response = group.node(1)?.receive_dynamic(&request, 13)?;
    response.context.configuration_epoch += 1;
    assert!(group.node(0)?.acknowledge_dynamic(response, 13).is_err());
    group
        .node(0)?
        .timeout_dynamic(key, request.request.sequence)?;
    assert_eq!(group.node(0)?.voters()?.epoch(), 0);
    Ok(())
}

#[test]
fn dynamic_snapshot_replays_voters_and_suffix_without_importing_local_vote() -> Result {
    let mut group = DynamicGroup::new(3, voter(4)?)?;
    group.elect(10)?;
    group.catch_up(1, 11)?;
    group.probe(3, 12)?;
    group.catch_up(3, 13)?;
    let key = group.key(3);
    group.node(0)?.add_voter(key, 14)?;
    group.catch_up(1, 15)?;
    group.catch_up(2, 16)?;
    let image = group.node(0)?.checkpoint([42; 16], 17)?;
    group.node(0)?.propose(b"later suffix", 18)?;
    group.catch_up(1, 19)?;
    group.catch_up(2, 20)?;
    let old_vote = group.node(3)?.voted_directory()?;
    let offer = group.node(0)?.prepare_dynamic_snapshot(key, 21)?;
    assert_eq!(offer.request.descriptor, image);
    group.node(3)?.begin_dynamic_snapshot(offer, 21)?;
    loop {
        let chunk = group.node(0)?.dynamic_snapshot_chunk(offer, 21)?;
        group
            .node(3)?
            .receive_dynamic_snapshot_chunk(offer, chunk.offset, &chunk.bytes, 21)?;
        if chunk.done {
            break;
        }
    }
    let response = group.node(3)?.finish_dynamic_snapshot(offer, 21)?;
    group.node(0)?.acknowledge_dynamic_snapshot(response, 21)?;
    assert_eq!(group.node(3)?.voters()?.epoch(), 1);
    assert_eq!(group.node(3)?.voted_directory()?, old_vote);
    group.catch_up(3, 22)?;
    group.exchange(3, 23)?;
    let committed = group.node(3)?.fetch_committed(1, 8, 2 * 1024 * 1024)?;
    assert_eq!(committed.len(), 3);
    group.reopen(3, 24)?;
    assert_eq!(group.node(3)?.voters()?.epoch(), 1);
    assert_eq!(group.node(3)?.voted_directory()?, old_vote);
    assert_eq!(
        group.node(3)?.fetch_committed(1, 8, 2 * 1024 * 1024)?,
        committed
    );
    Ok(())
}

#[test]
fn five_voter_campaign_preserves_remaining_correlations_after_denial_and_first_grant() -> Result {
    let mut group = DynamicGroup::new(5, voter(6)?)?;
    let requests = group.node(0)?.campaign_dynamic(10)?;
    assert_eq!(requests.len(), 4);
    // A real prior same-term vote for another candidate makes voter2 deny us.
    let mut other = requests[0];
    other.context.leader = group.key(2);
    other.request.candidate = group.key(2).id;
    let denied_by_prior_vote = group.node(1)?.receive_dynamic_vote(other, 10)?;
    assert!(denied_by_prior_vote.response.granted);
    let denial = group.node(1)?.receive_dynamic_vote(requests[0], 10)?;
    assert!(!denial.response.granted);
    group.node(0)?.acknowledge_dynamic_vote(denial, 10)?;
    assert_eq!(group.node(0)?.state().election.role, Role::Candidate);
    let first = group.node(2)?.receive_dynamic_vote(requests[1], 10)?;
    group.node(0)?.acknowledge_dynamic_vote(first, 10)?;
    assert_eq!(
        group.node(0)?.state().election.role,
        Role::Candidate,
        "self plus one of five is insufficient"
    );
    let second = group.node(3)?.receive_dynamic_vote(requests[2], 10)?;
    group.node(0)?.acknowledge_dynamic_vote(second, 10)?;
    assert_eq!(group.node(0)?.state().election.role, Role::Leader);
    assert!(
        group.node(0)?.acknowledge_dynamic_vote(first, 10).is_err(),
        "consumed correlation cannot count twice"
    );
    Ok(())
}

#[test]
fn malformed_or_uncommitted_multiple_configs_cannot_advance_epoch_or_truncate() -> Result {
    let mut group = DynamicGroup::new(3, voter(4)?)?;
    group.elect(10)?;
    group.catch_up(1, 11)?;
    let key = group.key(1);
    let request = group.node(0)?.prepare_dynamic(key, 12)?;
    let before = group.node(1)?.state();
    let mut forged = request.clone();
    forged.request.term += 1;
    forged.request.entries.push(replication::Record {
        term: forged.request.term,
        index: 2,
        kind: replication::RecordKind::Voters,
        payload: b"invalid".to_vec(),
    });
    assert!(group.node(1)?.receive_dynamic(&forged, 12).is_err());
    assert_eq!(
        group.node(1)?.state().wal_durable_ops,
        before.wal_durable_ops
    );
    assert_eq!(
        group.node(1)?.state().election_durable_states,
        before.election_durable_states
    );
    assert_eq!(
        group.node(1)?.state().election.persistent,
        before.election.persistent
    );
    let first = group.genesis.add(
        voter(4)?,
        LogPosition {
            term: forged.request.term,
            index: 2,
        },
    )?;
    let second = first.remove(
        group.key(2),
        LogPosition {
            term: forged.request.term,
            index: 3,
        },
    )?;
    forged.request.entries = vec![
        replication::Record {
            term: forged.request.term,
            index: 2,
            kind: replication::RecordKind::Voters,
            payload: first.encode()?,
        },
        replication::Record {
            term: forged.request.term,
            index: 3,
            kind: replication::RecordKind::Voters,
            payload: second.encode()?,
        },
    ];
    forged.request.leader_commit = 1;
    assert!(group.node(1)?.receive_dynamic(&forged, 12).is_err());
    assert_eq!(
        group.node(1)?.state().wal_durable_ops,
        before.wal_durable_ops
    );
    assert_eq!(
        group.node(1)?.state().election.persistent,
        before.election.persistent
    );
    group
        .node(0)?
        .timeout_dynamic(key, request.request.sequence)?;
    Ok(())
}

#[test]
fn pending_addition_can_elect_known_directory_across_configuration_epochs_and_commit() -> Result {
    let mut group = DynamicGroup::new(3, voter(4)?)?;
    group.elect(10)?;
    group.catch_up(1, 11)?;
    group.probe(3, 12)?;
    group.catch_up(3, 13)?;
    let added = group.key(3);
    group.node(0)?.add_voter(added, 14)?;
    group.catch_up(1, 15)?;
    assert_eq!(group.node(0)?.state().committed_end, 1);
    assert_eq!(group.node(1)?.voters()?.epoch(), 1);
    assert_eq!(group.node(2)?.voters()?.epoch(), 0);
    group.node(0)?.poll(1100)?;
    let requests = group.node(1)?.campaign_dynamic(1100)?;
    for index in [0, 2] {
        let request = *requests
            .iter()
            .find(|r| r.context.peer == group.key(index))
            .ok_or("missing known voter request")?;
        let response = group.node(index)?.receive_dynamic_vote(request, 1100)?;
        assert!(response.response.granted);
        group.node(1)?.acknowledge_dynamic_vote(response, 1100)?;
    }
    assert_eq!(group.node(1)?.state().election.role, Role::Leader);
    group.node(1)?.activate_leader(1100)?;
    for index in [0, 2] {
        for _ in 0..3 {
            let key = group.key(index);
            let request = group.node(1)?.prepare_dynamic(key, 1101)?;
            let response = group.node(index)?.receive_dynamic(&request, 1101)?;
            group.node(1)?.acknowledge_dynamic(response, 1101)?;
        }
    }
    assert!(group.node(1)?.change_status()?.committed);
    assert_eq!(group.node(1)?.state().committed_end, 3);
    assert_eq!(group.node(2)?.voters()?.epoch(), 1);
    Ok(())
}

#[test]
fn unknown_added_leader_laggard_refuses_before_mutation_then_known_history_allows_it() -> Result {
    let mut group = DynamicGroup::new(3, voter(4)?)?;
    group.elect(10)?;
    group.catch_up(1, 11)?;
    group.probe(3, 12)?;
    group.catch_up(3, 13)?;
    let added = group.key(3);
    group.node(0)?.add_voter(added, 14)?;
    group.catch_up(1, 15)?;
    group.catch_up(3, 16)?;
    assert!(group.node(0)?.change_status()?.committed);
    assert_eq!(group.node(2)?.voters()?.epoch(), 0);
    let laggard = group.key(2);
    let mut offer = group.node(0)?.prepare_dynamic(laggard, 17)?;
    let before = group.node(2)?.state();
    // Declared mutation: self-supplied voter history must not authorize an
    // unknown leader, even when the received payload contains a valid addition.
    let original = offer.clone();
    offer.context.leader = added;
    offer.request.leader = added.id;
    offer.request.term += 1;
    assert!(group.node(2)?.receive_dynamic(&offer, 17).is_err());
    assert_eq!(
        group.node(2)?.state().election.persistent,
        before.election.persistent
    );
    assert_eq!(
        group.node(2)?.state().wal_durable_ops,
        before.wal_durable_ops
    );
    assert_eq!(
        group.node(2)?.state().election_durable_states,
        before.election_durable_states
    );
    let response = group.node(2)?.receive_dynamic(&original, 17)?;
    group.node(0)?.acknowledge_dynamic(response, 17)?;
    // Catch-up can initially return a retry hint; source remains known and
    // eventually sends the actual committed configuration from its journal.
    group.catch_up(2, 18)?;
    group.exchange(2, 19)?;
    assert!(group.node(2)?.voters()?.contains(added));
    group.node(0)?.poll(1100)?;
    let requests = group.node(3)?.campaign_dynamic(1100)?;
    for index in [1, 2] {
        let request = *requests
            .iter()
            .find(|r| r.context.peer == group.key(index))
            .ok_or("missing learned-directory voter")?;
        let response = group.node(index)?.receive_dynamic_vote(request, 1100)?;
        group.node(3)?.acknowledge_dynamic_vote(response, 1100)?;
    }
    assert_eq!(group.node(3)?.state().election.role, Role::Leader);
    group.node(3)?.activate_leader(1100)?;
    let key = group.key(2);
    let request = group.node(3)?.prepare_dynamic(key, 1101)?;
    let response = group.node(2)?.receive_dynamic(&request, 1101)?;
    assert!(response.response.success || response.response.conflict_index > 0);
    group.node(3)?.acknowledge_dynamic(response, 1101)?;
    Ok(())
}

#[test]
fn feature_preparation_has_explicit_cancel_and_deadline_without_reverting_voters() -> Result {
    let mut group = DynamicGroup::new(3, voter(4)?)?;
    group.elect(10)?;
    group.catch_up(1, 11)?;
    group.probe(3, 12)?;
    let added = group.key(3);
    let removed = group.key(2);
    assert!(group.node(0)?.remove_voter(removed, 13).is_err());
    group.node(0)?.cancel_addition(added)?;
    let candidate = group.local[3].clone();
    let pending = group.node(0)?.probe_addition(candidate, 14)?;
    let delayed = group.node(3)?.receive_feature_probe(pending, 14)?;
    // Genuine current-term contact preserves leadership while this preparation
    // alone expires, independently of log commitment or quorum fencing.
    group.exchange(1, 900)?;
    group.node(0)?.poll(1014)?;
    assert!(group
        .node(0)?
        .acknowledge_feature_probe(delayed, 1014)
        .is_err());
    assert_eq!(group.node(0)?.voters()?.epoch(), 0);
    let candidate = group.local[3].clone();
    let next = group.node(0)?.probe_addition(candidate, 1015)?;
    group.node(0)?.timeout_feature_probe(next)?;
    assert_eq!(group.node(0)?.voters()?.epoch(), 0);
    Ok(())
}

#[test]
fn removed_id_rejoins_new_directory_without_old_progress_or_reinterpreting_saved_vote() -> Result {
    let mut group = DynamicGroup::new(5, voter(6)?)?;
    // Candidate5 gets two real votes (itself and voter4), insufficient to lead.
    let minority = group.node(4)?.campaign_dynamic(10)?;
    let request = *minority
        .iter()
        .find(|r| r.context.peer == group.key(3))
        .ok_or("missing minority vote")?;
    let grant = group.node(3)?.receive_dynamic_vote(request, 10)?;
    group.node(4)?.acknowledge_dynamic_vote(grant, 10)?;
    assert_eq!(group.node(4)?.state().election.role, Role::Candidate);
    group.elect(10)?; // self+voters2/3 elect; voters4/5 deny same-term second vote.
    for index in [1, 2, 3] {
        group.catch_up(index, 11)?;
    }
    let old = group.key(4);
    let pending = group.node(0)?.prepare_dynamic(old, 12)?;
    let delayed = group.node(4)?.receive_dynamic(&pending, 12)?;
    group.node(0)?.remove_voter(old, 13)?;
    group.catch_up(1, 14)?;
    group.catch_up(2, 15)?;
    assert!(group.node(0)?.change_status()?.committed);
    assert_eq!(group.node(0)?.voters()?.voters().len(), 4);
    let replacement = Voter::new(
        Key::new(old.id, [77; 16])?,
        voter(old.id)?.endpoints().to_vec(),
        0,
        1,
    )?;
    group.local.push(replacement);
    let new_index = group.local.len() - 1;
    group.nodes.push(Some(group.open(new_index, 16)?));
    group.probe(new_index, 16)?;
    assert!(
        group.node(0)?.acknowledge_dynamic(delayed, 16).is_err(),
        "inactive old directory correlation cannot survive replacement"
    );
    group.catch_up(new_index, 17)?;
    let new_key = group.key(new_index);
    group.node(0)?.add_voter(new_key, 18)?;
    group.catch_up(1, 19)?;
    group.catch_up(2, 20)?;
    group.catch_up(3, 21)?;
    group.exchange(3, 22)?;
    assert!(group.node(0)?.change_status()?.committed);
    assert_eq!(
        group.node(3)?.voters()?.by_id(old.id).map(Voter::key),
        Some(new_key)
    );
    assert_eq!(
        group.node(3)?.voted_directory()?,
        Some(old),
        "the numeric historical vote is not remapped to the new directory"
    );
    group.reopen(3, 23)?;
    assert_eq!(group.node(3)?.voted_directory()?, Some(old));
    assert_eq!(
        group.node(3)?.voters()?.by_id(old.id).map(Voter::key),
        Some(new_key)
    );
    Ok(())
}

fn mj_text(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch < ' ' => out.push_str(&format!("\\u{:04x}", u32::from(ch))),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}
fn mj_key(key: Key) -> Result<String> {
    Ok(format!(
        "{{\"id\":{},\"directory\":{}}}",
        key.id,
        mj_text(&hex_bytes(&key.directory)?)
    ))
}
fn mj_position(p: LogPosition) -> String {
    format!("{{\"term\":{},\"index\":{}}}", p.term, p.index)
}
fn mj_voter(v: &Voter) -> Result<String> {
    Ok(format!(
        "{{\"key\":{},\"kraft_min\":{},\"kraft_max\":{},\"endpoints\":[{}]}}",
        mj_key(v.key())?,
        v.kraft_min(),
        v.kraft_max(),
        v.endpoints()
            .iter()
            .map(|e| format!(
                "{{\"listener\":{},\"host\":{},\"port\":{}}}",
                mj_text(e.listener()),
                mj_text(e.host()),
                e.port()
            ))
            .collect::<Vec<_>>()
            .join(",")
    ))
}
fn mj_voters(v: &Voters) -> Result<String> {
    Ok(format!(
        "{{\"epoch\":{},\"position\":{},\"feature\":{},\"canonical_hex\":{},\"voters\":[{}]}}",
        v.epoch(),
        mj_position(v.position()),
        v.feature(),
        mj_text(&hex_bytes(&v.encode()?)?),
        v.voters()
            .iter()
            .map(mj_voter)
            .collect::<Result<Vec<_>>>()?
            .join(",")
    ))
}
fn mj_context(c: replication::Context) -> Result<String> {
    Ok(format!(
        "{{\"leader\":{},\"peer\":{},\"configuration_epoch\":{}}}",
        mj_key(c.leader)?,
        mj_key(c.peer)?,
        c.configuration_epoch
    ))
}
fn mj_record(r: &replication::Record) -> Result<String> {
    Ok(format!(
        "{{\"term\":{},\"index\":{},\"kind\":{},\"payload_hex\":{}}}",
        r.term,
        r.index,
        match r.kind {
            replication::RecordKind::Data => 0,
            replication::RecordKind::Barrier => 1,
            replication::RecordKind::Voters => 2,
        },
        mj_text(&hex_bytes(&r.payload)?)
    ))
}
fn mj_request(r: &replication::DynamicRequest) -> Result<String> {
    let q = &r.request;
    Ok(format!("{{\"context\":{},\"leader\":{},\"peer\":{},\"term\":{},\"sequence\":{},\"previous\":{},\"leader_commit\":{},\"entries\":[{}]}}",mj_context(r.context)?,q.leader,q.peer,q.term,q.sequence,mj_position(q.previous),q.leader_commit,q.entries.iter().map(mj_record).collect::<Result<Vec<_>>>()?.join(",")))
}
fn mj_response(r: replication::DynamicResponse) -> Result<String> {
    let q = r.response;
    Ok(format!("{{\"context\":{},\"leader\":{},\"peer\":{},\"term\":{},\"sequence\":{},\"success\":{},\"matched\":{},\"conflict_index\":{}}}",mj_context(r.context)?,q.leader,q.peer,q.term,q.sequence,q.success,mj_position(q.matched),q.conflict_index))
}
fn mj_vote(q: replication::DynamicVoteRequest) -> Result<String> {
    Ok(format!(
        "{{\"context\":{},\"sequence\":{},\"term\":{},\"candidate\":{},\"log\":{}}}",
        mj_context(q.context)?,
        q.sequence,
        q.request.term,
        q.request.candidate,
        mj_position(q.request.log)
    ))
}
fn mj_vote_response(q: replication::DynamicVoteResponse) -> Result<String> {
    Ok(format!(
        "{{\"request\":{},\"term\":{},\"voter\":{},\"candidate\":{},\"granted\":{}}}",
        mj_vote(q.request)?,
        q.response.term,
        q.response.voter,
        q.response.candidate,
        q.response.granted
    ))
}
fn mj_feature(q: partitionline_broker::raft::membership::FeatureRequest) -> Result<String> {
    Ok(format!(
        "{{\"leader\":{},\"peer\":{},\"term\":{},\"sequence\":{},\"configuration_epoch\":{}}}",
        mj_key(q.leader)?,
        mj_key(q.peer)?,
        q.term,
        q.sequence,
        q.configuration_epoch
    ))
}
fn mj_descriptor(d: snapshot::Descriptor) -> Result<String> {
    Ok(format!("{{\"generation\":{},\"base\":{},\"records\":{},\"payload_bytes\":{},\"bytes\":{},\"checksum\":{}}}",mj_text(&hex_bytes(&d.generation)?),mj_position(d.base),d.records,d.payload_bytes,d.bytes,d.checksum))
}
fn mj_image_offer(q: replication::DynamicSnapshotRequest) -> Result<String> {
    let r = q.request;
    Ok(format!("{{\"context\":{},\"leader\":{},\"peer\":{},\"term\":{},\"sequence\":{},\"leader_commit\":{},\"descriptor\":{}}}",mj_context(q.context)?,r.leader,r.peer,r.term,r.sequence,r.leader_commit,mj_descriptor(r.descriptor)?))
}
fn mj_state(node: &Node, key: Key) -> Result<String> {
    let s = node.state();
    let records = node.fetch_committed(1, 128, 2 * 1024 * 1024)?;
    Ok(format!("{{\"key\":{},\"open\":true,\"term\":{},\"role\":{},\"leader_id\":{},\"voted_for\":{},\"voters\":{},\"ready\":{},\"poisoned\":{},\"active_term\":{},\"last_position\":{},\"committed_end\":{},\"committed_records\":[{}],\"wal_durable_ops\":{},\"election_durable_states\":{},\"base_position\":{},\"selected_snapshot\":{}}}",mj_key(key)?,s.election.persistent.term,mj_text(&format!("{:?}",s.election.role)),s.election.leader.map_or_else(||"null".into(),|n|n.to_string()),node.voted_directory()?.map_or_else(||Ok("null".into()),mj_key)?,mj_voters(node.voters()?)?,s.ready,s.poisoned,s.active_term.map_or_else(||"null".into(),|n|n.to_string()),mj_position(s.last_position),s.committed_end,records.iter().map(mj_record).collect::<Result<Vec<_>>>()?.join(","),s.wal_durable_ops,s.election_durable_states,mj_position(s.base_position),node.selected_snapshot()?.map_or_else(||Ok("null".into()),mj_descriptor)?))
}
fn mj_write(path: impl AsRef<std::path::Path>, bytes: &[u8]) -> Result {
    use std::io::Write;
    let mut file = fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
fn mj_copy_tree(from: &std::path::Path, to: &std::path::Path) -> Result {
    fs::create_dir_all(to)?;
    let mut entries = fs::read_dir(from)?.collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            return Err("proof path must not be a symlink".into());
        }
        if kind.is_dir() {
            mj_copy_tree(&entry.path(), &to.join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), to.join(entry.file_name()))?;
            fs::File::open(to.join(entry.file_name()))?.sync_all()?;
        }
    }
    Ok(())
}

struct DynamicHistory {
    group: DynamicGroup,
    output: PathBuf,
    events: Vec<String>,
    checkpoints: Vec<String>,
    cached: Vec<String>,
    now: u64,
}
impl DynamicHistory {
    fn create(root: PathBuf, output: PathBuf, count: u32, recovering: bool) -> Result<Self> {
        fs::create_dir_all(&root)?;
        fs::create_dir_all(&output)?;
        let mut local = (1..=count).map(voter).collect::<Result<Vec<_>>>()?;
        let genesis = Voters::new(0, LogPosition::default(), 1, local.clone())?;
        local.push(voter(count + 1)?);
        let mut group = DynamicGroup {
            root,
            genesis,
            local,
            nodes: Vec::new(),
        };
        let mut events = Vec::new();
        let mut checkpoints = Vec::new();
        let mut cached = vec![String::new(); group.local.len()];
        let mut now = 0;
        if recovering {
            events = fs::read_to_string(output.join("events-progress.jsonl"))?
                .lines()
                .map(String::from)
                .collect();
            checkpoints = fs::read_to_string(output.join("checkpoints-progress.jsonl"))?
                .lines()
                .map(String::from)
                .collect();
            cached = fs::read_to_string(output.join("last-states.jsonl"))?
                .lines()
                .map(String::from)
                .collect();
            assert_eq!(cached.len(), group.local.len());
            now = fs::read_to_string(output.join("clock.txt"))?
                .trim()
                .parse::<u64>()?
                .checked_add(1)
                .ok_or("clock overflow")?;
            group.nodes.resize_with(group.local.len(), || None);
        } else {
            for index in 0..group.local.len() {
                group.nodes.push(Some(group.open(index, 0)?));
            }
        }
        Ok(Self {
            group,
            output,
            events,
            checkpoints,
            cached,
            now,
        })
    }
    fn event(&mut self, kind: &str, index: usize, args: String, result: String) -> Result {
        use std::io::Write;
        let mut after = Vec::new();
        for i in 0..self.group.nodes.len() {
            let state = if let Some(node) = self.group.nodes[i].as_ref() {
                mj_state(node, self.group.key(i))?
            } else {
                self.cached[i]
                    .replace("\"open\":true", "\"open\":false")
                    .replace("\"ready\":true", "\"ready\":false")
            };
            self.cached[i] = state.clone();
            after.push(state);
        }
        let event=format!("{{\"ordinal\":{},\"now_ms\":{},\"kind\":{},\"key\":{},\"args\":{},\"result\":{},\"after\":[{}]}}",self.events.len(),self.now,mj_text(kind),mj_key(self.group.key(index))?,args,result,after.join(","));
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.output.join("events-progress.jsonl"))?;
        writeln!(file, "{event}")?;
        file.sync_all()?;
        self.events.push(event);
        mj_write(
            self.output.join("last-states.jsonl"),
            format!("{}\n", self.cached.join("\n")).as_bytes(),
        )?;
        mj_write(
            self.output.join("clock.txt"),
            format!("{}\n", self.now).as_bytes(),
        )
    }
    fn checkpoint(&mut self, index: usize, phase: &str) -> Result {
        use std::io::Write;
        let key = self.group.key(index);
        let name = format!("{}-{}", key.id, hex_bytes(&key.directory)?);
        let folder = format!("journals/{}-{phase}-{}", self.events.len(), key.id);
        let to = self.output.join(&folder);
        fs::create_dir_all(&to)?;
        for (suffix, destination) in [("wal", "metadata.wal"), ("election", "election.wal")] {
            fs::copy(
                self.group.root.join(format!("{name}.{suffix}")),
                to.join(destination),
            )?;
            fs::File::open(to.join(destination))?.sync_all()?;
        }
        mj_copy_tree(
            &self.group.root.join(format!("{name}.images")),
            &to.join("images"),
        )?;
        let s = self.group.node(index)?.state();
        let receipt=format!("{{\"key\":{},\"phase\":{},\"event_ordinal\":{},\"wal_path\":{},\"election_path\":{},\"images_dir\":{},\"wal_confirmed_ops\":{},\"election_confirmed_states\":{}}}",mj_key(key)?,mj_text(phase),self.events.len(),mj_text(&format!("{folder}/metadata.wal")),mj_text(&format!("{folder}/election.wal")),mj_text(&format!("{folder}/images")),s.wal_durable_ops,s.election_durable_states);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.output.join("checkpoints-progress.jsonl"))?;
        writeln!(file, "{receipt}")?;
        file.sync_all()?;
        self.checkpoints.push(receipt.clone());
        self.event(
            "checkpoint",
            index,
            receipt,
            "{\"copied_actual_files\":true}".into(),
        )
    }
    fn checkpoint_all(&mut self, phase: &str) -> Result {
        for index in 0..self.group.local.len() {
            self.checkpoint(index, phase)?;
        }
        Ok(())
    }
    fn elect(&mut self, leader: usize) -> Result {
        let requests = self.group.node(leader)?.campaign_dynamic(self.now)?;
        assert!(!requests.is_empty());
        self.event(
            "campaign",
            leader,
            "{}".into(),
            format!(
                "{{\"requests\":[{}]}}",
                requests
                    .iter()
                    .copied()
                    .map(mj_vote)
                    .collect::<Result<Vec<_>>>()?
                    .join(",")
            ),
        )?;
        for request in requests {
            let index = self
                .group
                .local
                .iter()
                .position(|v| v.key() == request.context.peer)
                .ok_or("missing voter")?;
            let response = self
                .group
                .node(index)?
                .receive_dynamic_vote(request, self.now)?;
            self.event(
                "vote_request",
                index,
                mj_vote(request)?,
                mj_vote_response(response)?,
            )?;
            let tally = self
                .group
                .node(leader)?
                .acknowledge_dynamic_vote(response, self.now)?;
            self.event(
                "vote_response",
                leader,
                mj_vote_response(response)?,
                format!("{{\"tally\":{}}}", mj_text(&format!("{tally:?}"))),
            )?;
        }
        assert_eq!(self.group.node(leader)?.state().election.role, Role::Leader);
        let index = self.group.node(leader)?.activate_leader(self.now)?;
        self.event(
            "activate",
            leader,
            "{}".into(),
            format!("{{\"barrier_index\":{index}}}"),
        )
    }
    fn prepare(&mut self, leader: usize, peer: usize) -> Result<replication::DynamicRequest> {
        let key = self.group.key(peer);
        let request = self.group.node(leader)?.prepare_dynamic(key, self.now)?;
        self.event(
            "prepare",
            leader,
            format!("{{\"peer\":{}}}", mj_key(key)?),
            mj_request(&request)?,
        )?;
        Ok(request)
    }
    fn receive(
        &mut self,
        peer: usize,
        request: &replication::DynamicRequest,
    ) -> Result<replication::DynamicResponse> {
        let response = self.group.node(peer)?.receive_dynamic(request, self.now)?;
        self.event(
            "receive",
            peer,
            mj_request(request)?,
            mj_response(response)?,
        )?;
        Ok(response)
    }
    fn acknowledge(&mut self, leader: usize, response: replication::DynamicResponse) -> Result {
        let committed = self
            .group
            .node(leader)?
            .acknowledge_dynamic(response, self.now)?;
        self.event(
            "acknowledge",
            leader,
            mj_response(response)?,
            format!("{{\"committed_end\":{committed}}}"),
        )
    }
    fn exchange(&mut self, leader: usize, peer: usize) -> Result {
        let request = self.prepare(leader, peer)?;
        let response = self.receive(peer, &request)?;
        self.acknowledge(leader, response)
    }
    fn catch_up(&mut self, leader: usize, peer: usize) -> Result {
        for _ in 0..4 {
            self.exchange(leader, peer)?;
            if self.group.node(leader)?.state().last_position
                == self.group.node(peer)?.state().last_position
            {
                return Ok(());
            }
        }
        Err("bounded history catch-up did not complete".into())
    }
    fn probe(&mut self, leader: usize, peer: usize) -> Result {
        let candidate = self.group.local[peer].clone();
        let request = self
            .group
            .node(leader)?
            .probe_addition(candidate.clone(), self.now)?;
        self.event(
            "feature_prepare",
            leader,
            mj_voter(&candidate)?,
            mj_feature(request)?,
        )?;
        let response = self
            .group
            .node(peer)?
            .receive_feature_probe(request, self.now)?;
        self.event(
            "feature_receive",
            peer,
            mj_feature(request)?,
            format!(
                "{{\"request\":{},\"voter\":{}}}",
                mj_feature(response.request)?,
                mj_voter(&response.voter)?
            ),
        )?;
        self.group
            .node(leader)?
            .acknowledge_feature_probe(response.clone(), self.now)?;
        self.event(
            "feature_acknowledge",
            leader,
            format!(
                "{{\"request\":{},\"voter\":{}}}",
                mj_feature(response.request)?,
                mj_voter(&response.voter)?
            ),
            "{\"accepted\":true}".into(),
        )
    }
    fn propose(&mut self, leader: usize, bytes: &[u8]) -> Result {
        let index = self.group.node(leader)?.propose(bytes, self.now)?;
        self.event(
            "propose",
            leader,
            format!("{{\"payload_hex\":{}}}", mj_text(&hex_bytes(bytes)?)),
            format!("{{\"index\":{index}}}"),
        )
    }
    fn add(&mut self, leader: usize, peer: usize) -> Result {
        let key = self.group.key(peer);
        let receipt = self.group.node(leader)?.add_voter(key, self.now)?;
        self.event(
            "add_voter",
            leader,
            format!("{{\"peer\":{}}}", mj_key(key)?),
            format!(
                "{{\"epoch\":{},\"position\":{},\"committed\":{}}}",
                receipt.epoch,
                mj_position(receipt.position),
                receipt.committed
            ),
        )
    }
    fn remove(&mut self, leader: usize, peer: usize) -> Result {
        let key = self.group.key(peer);
        let receipt = self.group.node(leader)?.remove_voter(key, self.now)?;
        self.event(
            "remove_voter",
            leader,
            format!("{{\"peer\":{}}}", mj_key(key)?),
            format!(
                "{{\"epoch\":{},\"position\":{},\"committed\":{}}}",
                receipt.epoch,
                mj_position(receipt.position),
                receipt.committed
            ),
        )
    }
    fn image(&mut self, leader: usize, generation: [u8; 16]) -> Result {
        let image = self.group.node(leader)?.checkpoint(generation, self.now)?;
        self.event(
            "checkpoint_image",
            leader,
            format!("{{\"generation\":{}}}", mj_text(&hex_bytes(&generation)?)),
            mj_descriptor(image)?,
        )
    }
    fn transfer_image(&mut self, leader: usize, peer: usize) -> Result {
        let key = self.group.key(peer);
        let offer = self
            .group
            .node(leader)?
            .prepare_dynamic_snapshot(key, self.now)?;
        self.event(
            "image_prepare",
            leader,
            format!("{{\"peer\":{}}}", mj_key(key)?),
            mj_image_offer(offer)?,
        )?;
        self.group
            .node(peer)?
            .begin_dynamic_snapshot(offer, self.now)?;
        self.event(
            "image_begin",
            peer,
            mj_image_offer(offer)?,
            "{\"accepted\":true}".into(),
        )?;
        loop {
            let chunk = self
                .group
                .node(leader)?
                .dynamic_snapshot_chunk(offer, self.now)?;
            self.event(
                "image_chunk_prepare",
                leader,
                mj_image_offer(offer)?,
                format!(
                    "{{\"offset\":{},\"done\":{},\"bytes_hex\":{}}}",
                    chunk.offset,
                    chunk.done,
                    mj_text(&hex_bytes(&chunk.bytes)?)
                ),
            )?;
            self.group.node(peer)?.receive_dynamic_snapshot_chunk(
                offer,
                chunk.offset,
                &chunk.bytes,
                self.now,
            )?;
            self.event(
                "image_chunk_receive",
                peer,
                format!(
                    "{{\"offer\":{},\"offset\":{},\"bytes_hex\":{}}}",
                    mj_image_offer(offer)?,
                    chunk.offset,
                    mj_text(&hex_bytes(&chunk.bytes)?)
                ),
                "{\"accepted\":true}".into(),
            )?;
            if chunk.done {
                break;
            }
        }
        let response = self
            .group
            .node(peer)?
            .finish_dynamic_snapshot(offer, self.now)?;
        let value = format!(
            "{{\"context\":{},\"descriptor\":{},\"response\":{}}}",
            mj_context(response.context)?,
            mj_descriptor(response.response.descriptor)?,
            mj_response(replication::DynamicResponse {
                context: response.context,
                response: response.response.response
            })?
        );
        self.event("image_finish", peer, mj_image_offer(offer)?, value.clone())?;
        let end = self
            .group
            .node(leader)?
            .acknowledge_dynamic_snapshot(response, self.now)?;
        self.event(
            "image_acknowledge",
            leader,
            value,
            format!("{{\"committed_end\":{end}}}"),
        )
    }
    fn save(&self) -> Result {
        let source = std::env::var("PL_MEMBERSHIP_SOURCE_SHA")
            .unwrap_or_else(|_| "development-d21-owned-overlay".into());
        let trace=format!("{{\"schema_version\":1,\"profile\":\"caller-driven-durable-directory-membership\",\"source_sha\":{},\"group\":{{\"cluster_id\":\"membership-test\",\"topic\":\"__cluster_metadata\",\"partition\":0,\"genesis\":{}}},\"locals\":[{}],\"events\":[{}],\"checkpoints\":[{}],\"limits\":[\"typed exchange, no autonomous network or native directory wire\",\"opaque fullprefix retained; no application fold/compaction\",\"known directory discovery precondition\"]}}\n",mj_text(&source),mj_voters(&self.group.genesis)?,self.group.local.iter().map(mj_voter).collect::<Result<Vec<_>>>()?.join(","),self.events.join(",\n"),self.checkpoints.join(",\n"));
        mj_write(self.output.join("trace.json"), trace.as_bytes())
    }
}

#[test]
fn dynamic_membership_history_child() -> Result {
    let Ok(root) = std::env::var("PL_MEMBERSHIP_CHILD_ROOT") else {
        return Ok(());
    };
    let output = PathBuf::from(std::env::var("PL_MEMBERSHIP_CHILD_OUTPUT")?);
    let count = std::env::var("PL_MEMBERSHIP_CHILD_VOTERS")?.parse::<u32>()?;
    let mut h = DynamicHistory::create(root.into(), output, count, false)?;
    h.event("open", 0, "{}".into(), "{\"all_owners_open\":true}".into())?;
    h.now = 10;
    h.elect(0)?;
    h.now = 11;
    for index in 1..count as usize {
        h.catch_up(0, index)?;
    }
    h.now = 12;
    h.propose(0, b"immutable committed prefix")?;
    for index in 1..=count as usize / 2 {
        h.catch_up(0, index)?;
    }
    assert_eq!(h.group.node(0)?.state().committed_end, 2);
    h.now = 13;
    h.image(0, [1; 16])?;
    h.now = 14;
    h.probe(0, count as usize)?;
    h.catch_up(0, count as usize)?;
    h.checkpoint_all("before-add")?;
    h.now = 15;
    h.add(0, count as usize)?;
    // Only the old set's minimum majority matches: NEW(n+1) needs one more.
    h.now = 16;
    for index in 1..=count as usize / 2 {
        h.catch_up(0, index)?;
    }
    assert_eq!(h.group.node(0)?.state().committed_end, 2);
    assert!(!h.group.node(0)?.change_status()?.committed);
    let second_peer = h.group.key(1);
    let rejected = h.group.node(0)?.remove_voter(second_peer, 16);
    assert!(rejected.is_err());
    h.event(
        "prior_configuration_guard",
        0,
        format!("{{\"attempted_remove_peer\":{}}}", mj_key(second_peer)?),
        format!(
            "{{\"rejected\":true,\"error\":{}}}",
            mj_text(&format!(
                "{:?}",
                rejected.err().ok_or("expected rejection")?
            ))
        ),
    )?;
    h.checkpoint_all("uncommitted-add-old-majority")?;
    h.event(
        "process_exit",
        0,
        "{\"exit_code\":88,\"phase\":\"uncommitted-add-after-old-majority\"}".into(),
        "{\"phase\":\"before_actual_exit\"}".into(),
    )?;
    h.save()?;
    std::process::exit(88);
}

#[test]
fn actual_three_and_five_voter_membership_fault_restart_histories() -> Result {
    let capture = std::env::var_os("PL_MEMBERSHIP_CAPTURE_DIR").map(PathBuf::from);
    let output = capture.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!(
            "partitionline-membership-proofs-{}",
            std::process::id()
        ))
    });
    fs::create_dir_all(&output)?;
    for count in [3u32, 5u32] {
        let root = std::env::temp_dir().join(format!(
            "partitionline-membership-history-{}-{count}",
            std::process::id()
        ));
        let dir = output.join(format!("history-{count}"));
        fs::create_dir_all(&dir)?;
        let status = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "dynamic_membership_history_child",
                "--test-threads=1",
                "--nocapture",
            ])
            .env("PL_MEMBERSHIP_CHILD_ROOT", &root)
            .env("PL_MEMBERSHIP_CHILD_OUTPUT", &dir)
            .env("PL_MEMBERSHIP_CHILD_VOTERS", count.to_string())
            .status()?;
        assert_eq!(
            status.code(),
            Some(88),
            "child must exit after actual synchronized pending transition"
        );
        let mut h = DynamicHistory::create(root, dir, count, true)?;
        h.event(
            "process_exit_observed",
            0,
            "{\"requested_exit_code\":88}".into(),
            format!(
                "{{\"observed_exit_code\":{},\"all_child_owners_closed\":true}}",
                status.code().ok_or("child signal")?
            ),
        )?;
        for index in 0..h.group.local.len() {
            h.group.nodes[index] = Some(h.group.open(index, h.now)?);
            h.event("reopen", index, "{}".into(), "{\"recovered\":true}".into())?;
            h.checkpoint(index, "reopened-pending-add")?;
        }
        assert_eq!(h.group.node(0)?.voters()?.epoch(), 1);
        assert_eq!(h.group.node(0)?.state().committed_end, 2);
        assert_eq!(h.group.node(0)?.state().election.role, Role::Follower);
        h.now = 40;
        h.elect(0)?;
        let barrier = h.group.node(0)?.state().last_position.index;
        assert_eq!(barrier, 4);
        for index in 1..=count as usize / 2 {
            h.catch_up(0, index)?;
        }
        assert_eq!(
            h.group.node(0)?.state().committed_end,
            2,
            "old majority still insufficient after restart"
        );
        h.catch_up(0, count as usize / 2 + 1)?;
        assert_eq!(h.group.node(0)?.state().committed_end, barrier);
        assert!(h.group.node(0)?.change_status()?.committed);
        h.now = 41;
        h.image(0, [2; 16])?;
        let prior_vote = h.group.node(count as usize)?.voted_directory()?;
        h.transfer_image(0, count as usize)?;
        assert_eq!(h.group.node(count as usize)?.voters()?.epoch(), 1);
        assert_eq!(h.group.node(count as usize)?.voted_directory()?, prior_vote);
        h.checkpoint_all("committed-add-image")?;
        h.now = 42;
        let held = h.prepare(0, 1)?;
        let response = h.receive(1, &held)?;
        let origin = h.events.len() - 1;
        let mut forged = response;
        forged.context.peer.directory = [99; 16];
        let before = h.group.node(0)?.state().wal_durable_ops;
        assert!(h.group.node(0)?.acknowledge_dynamic(forged, h.now).is_err());
        assert_eq!(h.group.node(0)?.state().wal_durable_ops, before);
        h.event("acknowledge_mutated",0,format!("{{\"input_origin\":{{\"type\":\"mutated_actual_response\",\"source_ordinal\":{origin},\"changed_fields\":[\"context.peer.directory\"]}},\"response\":{}}}",mj_response(forged)?),"{\"rejected\":true}".into())?;
        h.acknowledge(0, response)?;
        assert!(h
            .group
            .node(0)?
            .acknowledge_dynamic(response, h.now)
            .is_err());
        h.event(
            "duplicate_acknowledge",
            0,
            mj_response(response)?,
            "{\"rejected\":true}".into(),
        )?;
        h.now = 43;
        h.remove(0, 0)?;
        let removed_index = h.group.node(0)?.state().last_position.index;
        for index in 1..=count as usize / 2 {
            h.catch_up(0, index)?;
        }
        assert_eq!(
            h.group.node(0)?.state().committed_end,
            barrier,
            "removed leader contributes no self match"
        );
        h.catch_up(0, count as usize / 2 + 1)?;
        assert_eq!(h.group.node(0)?.state().committed_end, removed_index);
        assert_eq!(h.group.node(0)?.state().election.role, Role::Follower);
        assert!(h.group.node(0)?.propose(b"removed writer", h.now).is_err());
        h.event(
            "removed_leader_propose",
            0,
            "{\"payload_hex\":\"72656d6f76656420777269746572\"}".into(),
            "{\"rejected\":true}".into(),
        )?;
        h.checkpoint_all("committed-leader-removal")?;
        h.now = 1100;
        h.elect(1)?;
        for index in 2..h.group.local.len() {
            h.catch_up(1, index)?;
        }
        assert!(h.group.node(1)?.state().committed_end > removed_index);
        let removed_key = h.group.key(0);
        assert!(!h.group.node(1)?.voters()?.contains(removed_key));
        h.checkpoint_all("new-leader-new-set")?;
        h.now = 1101;
        for index in 0..h.group.local.len() {
            h.group.reopen(index, h.now)?;
            h.event(
                "reopen_final",
                index,
                "{}".into(),
                "{\"recovered\":true}".into(),
            )?;
            h.checkpoint(index, "final-reopen")?;
        }
        assert!(!h.group.node(0)?.voters()?.contains(removed_key));
        assert!(h.group.node(0)?.campaign_dynamic(1111)?.is_empty());
        h.now = 1111;
        h.event(
            "removed_campaign",
            0,
            "{}".into(),
            "{\"requests\":[]}".into(),
        )?;
        h.save()?;
    }
    if capture.is_none() {
        fs::remove_dir_all(output)?;
    }
    Ok(())
}
