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
            self.root.join(format!("{}.images", local.key().id)),
            identity,
            snapshot::Limits::default(),
        )?;
        Ok(Node::open_dynamic(
            self.root.join(format!("{}.wal", local.key().id)),
            self.root.join(format!("{}.election", local.key().id)),
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
