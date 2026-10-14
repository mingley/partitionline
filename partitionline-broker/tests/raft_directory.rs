//! Directory identity, pre-vote, and mixed-version controller histories.

use partitionline_broker::{
    protocol::ApiVersion,
    raft::{
        election::{self, LogPosition, Role, Timeouts},
        membership::{Bootstrap, Endpoint, Key, Voter, Voters},
        protocol::{
            self, Config, Controller, DirectoryPreVote, Error, PreVoteRefusal, VoteEncode,
        },
        replication::{self, DynamicVoteRequest, Node},
    },
};
use std::{
    fmt::Write as _,
    fs::{self, File},
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const DIR1: [u8; 16] = [1; 16];
const DIR2: [u8; 16] = [2; 16];
const DIR3: [u8; 16] = [3; 16];
const STALE: [u8; 16] = [9; 16];

fn read_file(path: impl AsRef<Path>) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(512 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 512 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "fixture exceeds bound",
        ));
    }
    Ok(bytes)
}
fn write_file(path: impl AsRef<Path>, bytes: impl AsRef<[u8]>) -> std::io::Result<()> {
    File::create(path)?.write_all(bytes.as_ref())
}
struct Temp(PathBuf);
impl Temp {
    fn new() -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "partitionline-directory-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}
fn config(local: u32) -> Result<Config> {
    let mut c = Config::new(local, vec![1, 2, 3], "cluster-fixed".into())?;
    c.election_timeouts = Timeouts::new(5, 5)?;
    c.max_states = 128;
    Ok(c)
}
fn directory_config(local: u32) -> Result<Config> {
    let mut c = config(local)?;
    c.directories = vec![DIR1, DIR2, DIR3];
    Ok(c)
}
fn open_directory(t: &Temp, local: u32) -> Result<Controller> {
    Ok(Controller::open(
        t.path(&format!("node{local}")),
        directory_config(local)?,
        0,
    )?)
}
fn header(key: i16, version: i16, correlation: i32) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&key.to_be_bytes());
    v.extend_from_slice(&version.to_be_bytes());
    v.extend_from_slice(&correlation.to_be_bytes());
    v.extend_from_slice(&(-1_i16).to_be_bytes());
    if key == 52 || version >= 1 {
        v.push(0);
    }
    v
}
fn text(v: &mut Vec<u8>, s: Option<&str>, compact: bool) -> Result {
    if compact {
        v.push(u8::try_from(s.map_or(0, |s| s.len() + 1))?);
    } else {
        v.extend_from_slice(
            &s.map_or(Ok(-1), |s| i16::try_from(s.len()))
                .map_err(Box::<dyn std::error::Error>::from)?
                .to_be_bytes(),
        );
    }
    if let Some(s) = s {
        v.extend_from_slice(s.as_bytes());
    }
    Ok(())
}
fn vote_v0(node: i32, epoch: i32, log_epoch: i32, offset: i64) -> Result<Vec<u8>> {
    vote_at(&VoteEncode {
        vote: election::VoteRequest {
            term: u64::try_from(epoch)? + 1,
            candidate: u32::try_from(node)?,
            log: if offset == 0 {
                LogPosition::default()
            } else {
                LogPosition {
                    term: u64::try_from(log_epoch)? + 1,
                    index: u64::try_from(offset)?,
                }
            },
        },
        correlation: 77,
        version: 0,
        voter_id: 1,
        replica_directory: DIR2,
        voter_directory: DIR1,
        pre_vote: false,
    })
}
fn vote_at(spec: &VoteEncode) -> Result<Vec<u8>> {
    Ok(protocol::encode_vote_request(&directory_config(1)?, spec)?)
}
fn vote_case(
    version: i16,
    voter_id: i32,
    node: i32,
    epoch: i32,
    directories: ([u8; 16], [u8; 16]),
    pre_vote: bool,
) -> Result<Vec<u8>> {
    vote_at(&VoteEncode {
        vote: election::VoteRequest {
            term: u64::try_from(epoch)? + 1,
            candidate: u32::try_from(node)?,
            log: LogPosition::default(),
        },
        correlation: 77,
        version,
        voter_id,
        replica_directory: directories.0,
        voter_directory: directories.1,
        pre_vote,
    })
}
fn begin_v1(voter_id: i32, voter_dir: [u8; 16], leader: i32, epoch: i32) -> Result<Vec<u8>> {
    let mut v = header(53, 1, 77);
    text(&mut v, Some("cluster-fixed"), true)?;
    v.extend_from_slice(&voter_id.to_be_bytes());
    v.push(2);
    text(&mut v, Some("__cluster_metadata"), true)?;
    v.push(2);
    v.extend_from_slice(&0_i32.to_be_bytes());
    v.extend_from_slice(&voter_dir);
    v.extend_from_slice(&leader.to_be_bytes());
    v.extend_from_slice(&epoch.to_be_bytes());
    v.extend_from_slice(&[0, 0, 1, 0]);
    Ok(v)
}
fn end_v1(leader: i32, epoch: i32, candidates: &[(i32, [u8; 16])]) -> Result<Vec<u8>> {
    let mut v = header(54, 1, 77);
    text(&mut v, Some("cluster-fixed"), true)?;
    v.push(2);
    text(&mut v, Some("__cluster_metadata"), true)?;
    v.push(2);
    v.extend_from_slice(&0_i32.to_be_bytes());
    v.extend_from_slice(&leader.to_be_bytes());
    v.extend_from_slice(&epoch.to_be_bytes());
    v.push(u8::try_from(candidates.len() + 1)?);
    for (id, directory) in candidates {
        v.extend_from_slice(&id.to_be_bytes());
        v.extend_from_slice(directory);
        v.push(0);
    }
    v.extend_from_slice(&[0, 0, 1, 0]);
    Ok(v)
}
fn short_at(v: &[u8], offset: usize) -> Result<i16> {
    Ok(i16::from_be_bytes(
        v.get(offset..offset + 2)
            .ok_or("short response")?
            .try_into()?,
    ))
}
fn vote_granted(v: &[u8]) -> Result<u8> {
    Ok(*v.get(v.len().checked_sub(4).ok_or("short vote")?).ok_or("short vote")?)
}
fn vote_partition_error(v: &[u8]) -> Result<i16> {
    short_at(v, "__cluster_metadata".len() + 14)
}
fn hex(bytes: &[u8]) -> Result<String> {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut text, "{byte:02x}")?;
    }
    Ok(text)
}

#[test]
fn directory_profile_advertises_and_falls_back_to_vote0() -> Result {
    let peer_all = [ApiVersion {
        api_key: 52,
        min_version: 0,
        max_version: 2,
    }];
    let peer_v0 = [ApiVersion {
        api_key: 52,
        min_version: 0,
        max_version: 0,
    }];
    let peer_v1 = [ApiVersion {
        api_key: 52,
        min_version: 1,
        max_version: 2,
    }];
    assert_eq!(protocol::negotiate(&peer_all, 52), Some(0));
    assert_eq!(protocol::negotiate(&peer_v1, 52), None);
    assert_eq!(protocol::negotiate_directory(&peer_all, 52), Some(2));
    assert_eq!(protocol::negotiate_directory(&peer_v0, 52), Some(0));
    assert_eq!(protocol::negotiate_directory(&peer_v1, 52), Some(2));
    let begin_end = [
        ApiVersion {
            api_key: 53,
            min_version: 0,
            max_version: 1,
        },
        ApiVersion {
            api_key: 54,
            min_version: 0,
            max_version: 1,
        },
    ];
    assert_eq!(protocol::negotiate_directory(&begin_end, 53), Some(1));
    assert_eq!(protocol::negotiate_directory(&begin_end, 54), Some(1));
    let t = Temp::new()?;
    let mut directory = open_directory(&t, 1)?;
    let mut v0 = Controller::open(t.path("v0"), config(1)?, 0)?;
    assert_eq!(directory_config(1)?.max_controller_version(52), 2);
    assert_eq!(config(1)?.max_controller_version(52), 0);
    let granted = directory.respond(&vote_v0(2, 1, 0, 0)?, 1)?;
    assert_eq!(vote_granted(&granted)?, 1);
    assert_eq!(directory.state().persistent.voted_for, Some(2));
    assert!(matches!(
        v0.respond(&vote_case(2, 1, 2, 1, (DIR2, DIR1), false)?, 1),
        Err(Error::UnsupportedVersion)
    ));
    assert!(matches!(
        directory.respond(&header(52, 3, 77), 1),
        Err(Error::UnsupportedVersion)
    ));
    Ok(())
}

#[test]
fn vote_v1_grants_and_fences_stale_or_zero_directory() -> Result {
    let t = Temp::new()?;
    let mut c = open_directory(&t, 1)?;
    let accepted = c.respond(&vote_case(1, 1, 2, 1, (DIR2, DIR1), false)?, 1)?;
    assert_eq!(vote_partition_error(&accepted)?, 0);
    assert_eq!(vote_granted(&accepted)?, 1);
    assert_eq!(c.state().persistent.voted_for, Some(2));
    assert_eq!(c.state().persistent.term, 2);
    let before = c.state();
    let durable = c.durable_states();
    let stale = c.respond(&vote_case(1, 1, 3, 1, (STALE, DIR1), false)?, 2)?;
    assert_eq!(vote_partition_error(&stale)?, 0);
    assert_eq!(vote_granted(&stale)?, 0);
    assert_eq!(c.state(), before);
    assert_eq!(c.durable_states(), durable);
    let zero = c.respond(&vote_case(1, 1, 3, 1, ([0; 16], DIR1), false)?, 2)?;
    assert_eq!(vote_granted(&zero)?, 0);
    assert_eq!(c.state(), before);
    let wrong_voter = c.respond(&vote_case(1, 2, 3, 1, (DIR3, DIR2), false)?, 2)?;
    assert_eq!(vote_partition_error(&wrong_voter)?, 125);
    assert_eq!(c.state(), before);
    let wrong_local_dir = c.respond(&vote_case(1, 1, 3, 1, (DIR3, STALE), false)?, 2)?;
    assert_eq!(vote_partition_error(&wrong_local_dir)?, 125);
    assert_eq!(c.state(), before);
    Ok(())
}

#[test]
fn vote_v2_prevote_does_not_persist_term_or_vote() -> Result {
    let t = Temp::new()?;
    let mut c = open_directory(&t, 1)?;
    let before = c.state();
    let durable = c.durable_states();
    let granted = c.respond(&vote_case(2, 1, 2, 1, (DIR2, DIR1), true)?, 1)?;
    assert_eq!(vote_partition_error(&granted)?, 0);
    assert_eq!(vote_granted(&granted)?, 1);
    assert_eq!(c.state().persistent, before.persistent);
    assert_eq!(c.state().role, before.role);
    assert_eq!(c.durable_states(), durable);
    let same_epoch = c.respond(&vote_case(2, 1, 2, 0, (DIR2, DIR1), true)?, 1)?;
    assert_eq!(vote_granted(&same_epoch)?, 1);
    assert_eq!(c.state().persistent, before.persistent);
    let stale = c.respond(&vote_case(2, 1, 2, 1, (STALE, DIR1), true)?, 1)?;
    assert_eq!(vote_granted(&stale)?, 0);
    assert_eq!(c.state().persistent, before.persistent);
    let binding = c.respond(&vote_case(2, 1, 2, 1, (DIR2, DIR1), false)?, 1)?;
    assert_eq!(vote_granted(&binding)?, 1);
    assert_eq!(c.state().persistent.voted_for, Some(2));
    assert_eq!(c.state().persistent.term, 2);
    Ok(())
}

#[test]
fn begin_and_end_v1_fence_directories_and_keep_successor_ranks() -> Result {
    let t = Temp::new()?;
    let mut c = open_directory(&t, 1)?;
    let begin = c.respond(&begin_v1(1, DIR1, 2, 1)?, 1)?;
    assert_eq!(c.state().leader, Some(2));
    assert_eq!(c.state().persistent.term, 2);
    assert!(!begin.is_empty());
    let before = c.state();
    let durable = c.durable_states();
    let stale_begin = c.respond(&begin_v1(1, STALE, 3, 2)?, 2)?;
    assert_eq!(vote_partition_error_begin_end(&stale_begin)?, 125);
    assert_eq!(c.state(), before);
    assert_eq!(c.durable_states(), durable);
    let bad_end = c.respond(&end_v1(2, 1, &[(3, STALE)])?, 2)?;
    assert_eq!(vote_partition_error_begin_end(&bad_end)?, 42);
    assert_eq!(c.state(), before);
    let end = c.respond(&end_v1(2, 1, &[(3, DIR3), (1, DIR1)])?, 10)?;
    assert!(!end.is_empty());
    assert_eq!(c.state().leader, Some(2));
    assert_eq!(c.state().deadline_ms, 510);
    assert!(c.tick(509, 88)?.is_none());
    assert!(c.tick(510, 88)?.is_some());
    assert_eq!(c.state().role, Role::Candidate);
    Ok(())
}

fn vote_partition_error_begin_end(v: &[u8]) -> Result<i16> {
    // flexible Begin/End v1: correlation + hdr tags + top + compact topic/partition prefix
    short_at(v, "__cluster_metadata".len() + 14)
}

#[test]
fn leftover_empty_tags_and_truncated_directories_fail_closed() -> Result {
    let t = Temp::new()?;
    let mut c = open_directory(&t, 1)?;
    let before = c.state();
    let mut tagged = vote_case(2, 1, 2, 1, (DIR2, DIR1), true)?;
    tagged.pop();
    tagged.extend_from_slice(&[1, 7, 2, 90, 91]);
    let granted = c.respond(&tagged, 1)?;
    assert_eq!(vote_granted(&granted)?, 1);
    assert_eq!(c.state().persistent, before.persistent);
    for request in [
        vote_case(1, 1, 2, 1, (DIR2, DIR1), false)?,
        begin_v1(1, DIR1, 2, 1)?,
        end_v1(2, 1, &[(3, DIR3)])?,
    ] {
        let durable = c.durable_states();
        let snapshot = c.state();
        for end in 0..request.len() {
            assert!(c.respond(&request[..end], 1).is_err(), "prefix{end}");
            assert_eq!(c.state(), snapshot);
            assert_eq!(c.durable_states(), durable);
        }
    }
    let mut trailing = vote_case(2, 1, 2, 1, (DIR2, DIR1), true)?;
    trailing.push(0);
    assert!(c.respond(&trailing, 1).is_err());
    assert_eq!(c.state().persistent, before.persistent);
    Ok(())
}

#[test]
fn encode_vote_matches_constructed_v0_and_v2_and_counts_grants() -> Result {
    let t = Temp::new()?;
    let mut a = open_directory(&t, 1)?;
    let mut b = open_directory(&t, 2)?;
    let v0 = a.tick(5, 91)?.ok_or("campaign missing")?;
    assert_eq!(v0[2..4], 0_i16.to_be_bytes());
    let encoded = protocol::encode_vote_request(
        &directory_config(1)?,
        &VoteEncode {
            vote: election::VoteRequest {
                term: 2,
                candidate: 1,
                log: LogPosition::default(),
            },
            correlation: 91,
            version: 2,
            voter_id: 2,
            replica_directory: DIR1,
            voter_directory: DIR2,
            pre_vote: false,
        },
    )?;
    assert_eq!(&encoded[..2], &52_i16.to_be_bytes());
    assert_eq!(&encoded[2..4], &2_i16.to_be_bytes());
    let apache = read_file(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/raft-directory/4.3.1/vote-v2-binding.request.bin"),
    )?;
    let rust_binding = protocol::encode_vote_request(
        &directory_config(1)?,
        &VoteEncode {
            vote: election::VoteRequest {
                term: 2,
                candidate: 2,
                log: LogPosition::default(),
            },
            correlation: 77,
            version: 2,
            voter_id: 1,
            replica_directory: DIR2,
            voter_directory: DIR1,
            pre_vote: false,
        },
    )?;
    assert_eq!(
        rust_binding, apache,
        "Vote v2 request must match the Apache 4.3.1 encoder"
    );
    let reply = b.respond(&encoded, 5)?;
    assert_eq!(a.receive_vote(2, 91, &reply, 5)?, election::Tally::Elected);
    assert_eq!(a.state().leader, Some(1));
    Ok(())
}

#[test]
fn directory_config_and_successor_lists_are_bounded() -> Result {
    let t = Temp::new()?;
    let mut c = directory_config(1)?;
    c.directories.pop();
    assert!(matches!(
        Controller::open(t.path("short-dirs"), c, 0),
        Err(Error::InvalidConfig)
    ));
    let mut c = directory_config(1)?;
    c.directories[1] = [0; 16];
    assert!(matches!(
        Controller::open(t.path("zero-dir"), c, 0),
        Err(Error::InvalidConfig)
    ));
    let mut c = directory_config(1)?;
    c.directories[2] = DIR1;
    assert!(matches!(
        Controller::open(t.path("dup-dir"), c, 0),
        Err(Error::InvalidConfig)
    ));
    let mut c = open_directory(&t, 1)?;
    let too_many = end_v1(2, 1, &vec![(1, DIR1); 32])?;
    assert!(c.respond(&too_many, 1).is_err());
    Ok(())
}

#[test]
fn dynamic_prevote_leaves_wal_and_election_journal_unchanged() -> Result {
    let t = Temp::new()?;
    let voters = (1u32..=3)
        .map(|id| {
            Ok(Voter::new(
                Key::new(id, [id as u8; 16])?,
                vec![Endpoint::new(
                    "CONTROLLER".into(),
                    "localhost".into(),
                    9000 + id as u16,
                )?],
                0,
                1,
            )?)
        })
        .collect::<Result<Vec<_>>>()?;
    let local = voters[0].clone();
    let genesis = Voters::new(0, LogPosition::default(), 1, voters)?;
    let node = Node::open_dynamic(
        t.path("dynamic-content"),
        t.path("dynamic-election"),
        replication::Config::new(config(1)?),
        Bootstrap::new(local.clone(), genesis, "CONTROLLER".into())?,
        None,
        0,
    )?;
    let before = node.state();
    let request = DynamicVoteRequest {
        context: replication::Context {
            leader: Key::new(2, DIR2)?,
            peer: local.key(),
            configuration_epoch: 0,
        },
        sequence: 1,
        request: election::VoteRequest {
            term: before.election.persistent.term + 1,
            candidate: 2,
            log: before.election.persistent.log,
        },
    };
    assert!(
        node.receive_dynamic_prevote(request)
            .unwrap_or_else(|error| panic!("{error:?}"))
    );
    let after = node.state();
    assert_eq!(after.election.persistent, before.election.persistent);
    assert_eq!(after.wal_durable_ops, before.wal_durable_ops);
    assert_eq!(
        after.election_durable_states,
        before.election_durable_states
    );
    let stale = DynamicVoteRequest {
        context: replication::Context {
            leader: Key::new(2, STALE)?,
            peer: local.key(),
            configuration_epoch: 0,
        },
        ..request
    };
    assert_eq!(
        node.receive_dynamic_prevote(stale),
        Err(PreVoteRefusal::StaleDirectory)
    );
    assert_eq!(node.state().election.persistent, before.election.persistent);
    let encoded = node.encode_native_vote(&request, 2, 77, true)?;
    assert_eq!(&encoded[2..4], &2_i16.to_be_bytes());
    let _ = DirectoryPreVote {
        term: 1,
        candidate: 2,
        directory: DIR2,
        log: LogPosition::default(),
    };
    Ok(())
}

/// Fixtures are generated by the separately owned Apache oracle.
#[test]
fn independent_apache_directory_goldens() -> Result {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/raft-directory");
    let mut total = 0;
    let mut observations = Vec::new();
    for version in ["4.1.2", "4.2.1", "4.3.1"] {
        let directory = base.join(version);
        let table = String::from_utf8(read_file(directory.join("cases.tsv"))?)?;
        let rows: Vec<Vec<&str>> = table
            .lines()
            .skip(1)
            .filter(|s| !s.is_empty())
            .map(|s| s.split('\t').collect())
            .collect();
        for row in &rows {
            assert!(row.len() >= 6, "{version} fixture schema");
            let t = Temp::new()?;
            let mut c = open_directory(&t, 1)?;
            let request = read_file(directory.join(row[3]))?;
            let before = c.state();
            let durable = c.durable_states();
            match row[4] {
                "response" | "prevote" | "deny" | "unchanged" => {
                    let actual = c
                        .respond(&request, 0)
                        .map_err(|e| format!("{version}/{}: {e}", row[0]))?;
                    if row[5] != "-" {
                        assert_eq!(
                            actual,
                            read_file(directory.join(row[5]))?,
                            "{version}/{}",
                            row[0]
                        );
                    }
                    match row[4] {
                        "prevote" => {
                            assert_eq!(vote_granted(&actual)?, 1, "{version}/{}", row[0]);
                            assert_eq!(c.state().persistent, before.persistent);
                            assert_eq!(c.durable_states(), durable);
                        }
                        "deny" => {
                            assert_eq!(vote_granted(&actual)?, 0, "{version}/{}", row[0]);
                            assert_eq!(c.state().persistent, before.persistent);
                            assert_eq!(c.durable_states(), durable);
                        }
                        "unchanged" => {
                            assert_eq!(c.state().persistent, before.persistent);
                            assert_eq!(c.durable_states(), durable);
                        }
                        _ => {}
                    }
                    observations.push(format!(
                        "{{\"release\":\"{version}\",\"case\":\"{}\",\"response_hex\":\"{}\"}}",
                        row[0],
                        hex(&actual)?
                    ));
                    if let Some(path) = std::env::var_os("PL_DIRECTORY_RESPONSE_DIR") {
                        let output = PathBuf::from(path).join(version);
                        fs::create_dir_all(&output)?;
                        write_file(output.join(format!("{}.actual-response.bin", row[0])), actual)?;
                    }
                }
                "close" => {
                    assert!(c.respond(&request, 0).is_err(), "{version}/{}", row[0]);
                    assert_eq!(c.state(), before);
                    assert_eq!(c.durable_states(), durable);
                }
                other => return Err(format!("unknown disposition {other}").into()),
            }
            total += 1;
        }
    }
    assert_eq!(total, 36, "12 pinned cases must run for each Apache release");
    println!("Independent Apache directory fixture cases passed: {total}");
    Ok(())
}
