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
