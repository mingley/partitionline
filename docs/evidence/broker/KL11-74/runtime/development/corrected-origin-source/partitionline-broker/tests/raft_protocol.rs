//! Behavioral controller state fencing; independent Apache fixtures are separate.

use partitionline_broker::{
    protocol::{ApiVersion, Limits},
    raft::{
        election::{self, Membership, Role, Tally, Timeouts},
        protocol::{self, Config, Controller, ControllerHandler, Error},
    },
    transport::{self, Handler, Transport},
};
use std::{
    fmt::Write as _,
    fs::{self, File},
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
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
fn read_table(path: impl AsRef<Path>) -> std::io::Result<String> {
    String::from_utf8(read_file(path)?)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
fn write_file(path: impl AsRef<Path>, bytes: impl AsRef<[u8]>) -> std::io::Result<()> {
    File::create(path)?.write_all(bytes.as_ref())
}
struct Temp(PathBuf);
impl Temp {
    fn new() -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "partitionline-controller-{}-{}",
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
fn open(t: &Temp, local: u32) -> Result<Controller> {
    Ok(Controller::open(
        t.path(&format!("node{local}")),
        config(local)?,
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
fn vote(
    cluster: Option<&str>,
    node: i32,
    epoch: i32,
    log_epoch: i32,
    offset: i64,
) -> Result<Vec<u8>> {
    let mut v = header(52, 0, 77);
    text(&mut v, cluster, true)?;
    v.push(2);
    text(&mut v, Some("__cluster_metadata"), true)?;
    v.push(2);
    v.extend_from_slice(&0_i32.to_be_bytes());
    for x in [epoch, node, log_epoch] {
        v.extend_from_slice(&x.to_be_bytes());
    }
    v.extend_from_slice(&offset.to_be_bytes());
    v.extend_from_slice(&[0, 0, 0]);
    Ok(v)
}
fn epoch_request(key: i16, node: i32, epoch: i32, successors: &[i32]) -> Result<Vec<u8>> {
    let mut v = header(key, 0, 77);
    text(&mut v, Some("cluster-fixed"), false)?;
    v.extend_from_slice(&1_i32.to_be_bytes());
    text(&mut v, Some("__cluster_metadata"), false)?;
    for x in [1, 0, node, epoch] {
        v.extend_from_slice(&x.to_be_bytes());
    }
    if key == 54 {
        v.extend_from_slice(&i32::try_from(successors.len())?.to_be_bytes());
        for x in successors {
            v.extend_from_slice(&x.to_be_bytes());
        }
    }
    Ok(v)
}
fn short_at(v: &[u8], offset: usize) -> Result<i16> {
    Ok(i16::from_be_bytes(
        v.get(offset..offset + 2)
            .ok_or("short response")?
            .try_into()?,
    ))
}
fn top(v: &[u8], key: i16) -> Result<i16> {
    short_at(v, if key == 52 { 5 } else { 4 })
}
fn partition_error(v: &[u8], key: i16) -> Result<i16> {
    short_at(
        v,
        "__cluster_metadata".len() + if key == 52 { 14 } else { 20 },
    )
}

#[test]
fn initial_epoch_zero_nonempty_log_and_restart_preserve_vote() -> Result {
    let t = Temp::new()?;
    let mut c = open(&t, 1)?;
    assert_eq!(c.state().persistent.term, 1);
    assert_eq!(c.durable_states(), 2);
    c.advance_log(0, 2)?;
    assert_eq!(
        c.state().persistent.log,
        election::LogPosition { term: 1, index: 2 }
    );
    let stale_log = c.respond(&vote(Some("cluster-fixed"), 2, 1, 0, 1)?, 1)?;
    assert_eq!(partition_error(&stale_log, 52)?, 0);
    assert_eq!(stale_log[stale_log.len() - 4], 0);
    let accepted = c.respond(&vote(Some("cluster-fixed"), 2, 1, 0, 2)?, 2)?;
    assert_eq!(accepted[accepted.len() - 4], 1);
    assert_eq!(c.state().persistent.voted_for, Some(2));
    drop(c);
    let mut c = open(&t, 1)?;
    assert_eq!(c.state().persistent.term, 2);
    assert_eq!(c.state().persistent.voted_for, Some(2));
    let other = c.respond(&vote(Some("cluster-fixed"), 3, 1, 0, 2)?, 1)?;
    assert_eq!(other[other.len() - 4], 0);
    assert_eq!(c.state().persistent.voted_for, Some(2));
    Ok(())
}

#[test]
fn semantic_errors_and_stale_epochs_do_not_rewrite_state() -> Result {
    let t = Temp::new()?;
    let mut c = open(&t, 1)?;
    let before = c.state();
    let durable = c.durable_states();
    assert_eq!(
        top(&c.respond(&vote(Some("different"), 2, 1, 0, 0)?, 1)?, 52)?,
        104
    );
    for (node, epoch, log, offset) in [
        (99, 1, 0, 0),
        (-1, 1, 0, 0),
        (2, 1, 1, 0),
        (2, 1, -1, 0),
        (2, 1, 0, -1),
    ] {
        assert_eq!(
            partition_error(&c.respond(&vote(None, node, epoch, log, offset)?, 1)?, 52)?,
            42
        );
    }
    assert_eq!(c.state(), before);
    assert_eq!(c.durable_states(), durable);
    assert_eq!(
        partition_error(&c.respond(&epoch_request(53, 2, 3, &[])?, 1)?, 53)?,
        0
    );
    let current = c.state();
    for key in [52, 53, 54] {
        let v = if key == 52 {
            vote(None, 3, 2, 0, 0)?
        } else {
            epoch_request(key, 3, 2, &[])?
        };
        assert_eq!(partition_error(&c.respond(&v, 2)?, key)?, 74);
    }
    assert_eq!(c.state(), current);
    assert_eq!(
        partition_error(&c.respond(&epoch_request(53, 3, 3, &[])?, 2)?, 53)?,
        42
    );
    assert_eq!(c.state().leader, Some(2));
    Ok(())
}

#[test]
fn end_rank_timers_are_exact_and_invalid_successors_cannot_mutate() -> Result {
    for (count, rank, expected) in [
        (0, None, 0),
        (2, Some(0), 0),
        (2, Some(1), 500),
        (3, Some(1), 250),
        (3, Some(2), 500),
        (31, Some(30), 0),
    ] {
        assert_eq!(protocol::successor_backoff(1000, count, rank)?, expected);
    }
    assert!(protocol::successor_backoff(1000, 32, Some(0)).is_err());
    let t = Temp::new()?;
    let mut c = open(&t, 1)?;
    c.respond(&epoch_request(53, 2, 1, &[])?, 1)?;
    let before = c.state();
    let durable = c.durable_states();
    for s in [&[1, 1][..], &[99][..], &[-1][..]] {
        assert_eq!(
            partition_error(&c.respond(&epoch_request(54, 3, 2, s)?, 2)?, 54)?,
            42
        );
        assert_eq!(c.state(), before);
        assert_eq!(c.durable_states(), durable);
    }
    assert!(c.respond(&epoch_request(54, 3, 2, &[1; 32])?, 2).is_err());
    assert_eq!(c.state(), before);
    c.respond(&epoch_request(54, 2, 1, &[3, 1])?, 10)?;
    assert_eq!(c.state().deadline_ms, 510);
    assert_eq!(c.state().leader, Some(2));
    assert!(c.tick(509, 88)?.is_none());
    assert!(c.tick(510, 88)?.is_some());
    assert_eq!(c.state().role, Role::Candidate);
    assert_eq!(c.state().persistent.term, 3);
    // Exercise the largest supported rank list through actual RPC state, not
    // only the arithmetic helper. Every successor belongs to this fixed quorum.
    let mut wide = config(2)?;
    wide.voters = (1..=31).collect();
    let mut wide = Controller::open(t.path("31-members"), wide, 0)?;
    let successors: Vec<i32> = (1..=31).collect();
    assert_eq!(
        partition_error(
            &wide.respond(&epoch_request(54, 1, 1, &successors)?, 0)?,
            54
        )?,
        0
    );
    assert_eq!(wide.state().deadline_ms, 0);
    assert_eq!(wide.state().leader, Some(1));
    Ok(())
}

#[test]
fn malformed_prefixes_tags_and_counts_are_bounded_before_mutation() -> Result {
    let t = Temp::new()?;
    let mut c = open(&t, 1)?;
    let before = c.state();
    for request in [
        vote(None, 2, 1, 0, 0)?,
        epoch_request(53, 2, 1, &[])?,
        epoch_request(54, 2, 1, &[1])?,
    ] {
        for end in 0..request.len() {
            assert!(c.respond(&request[..end], 1).is_err(), "prefix{end}");
            assert_eq!(c.state(), before);
        }
        let mut trailing = request;
        trailing.push(0);
        assert!(c.respond(&trailing, 1).is_err());
    }
    let mut tagged = vote(None, 2, 1, 0, 0)?;
    tagged.pop();
    tagged.extend_from_slice(&[2, 7, 0, 7, 0]);
    assert!(c.respond(&tagged, 1).is_err());
    assert_eq!(c.state(), before);
    let mut tagged = vote(None, 2, 1, 0, 0)?;
    tagged.pop();
    tagged.extend_from_slice(&[1, 7, 2, 90, 91]);
    assert_eq!(partition_error(&c.respond(&tagged, 1)?, 52)?, 0);
    Ok(())
}

#[test]
fn cross_peer_wire_votes_count_distinct_members_and_fence_correlations() -> Result {
    let t = Temp::new()?;
    let mut a = open(&t, 1)?;
    let mut b = open(&t, 2)?;
    let request = a.tick(5, 91)?.ok_or("campaign missing")?;
    let reply = b.respond(&request, 5)?;
    assert!(a.receive_vote(2, 92, &reply, 5).is_err());
    let before = a.state();
    for end in 0..reply.len() {
        assert!(a.receive_vote(2, 91, &reply[..end], 5).is_err());
        assert_eq!(a.state(), before);
    }
    let mut contradictory = reply.clone();
    let leader_at = "__cluster_metadata".len() + 16;
    contradictory[leader_at..leader_at + 4].copy_from_slice(&3_i32.to_be_bytes());
    assert!(a.receive_vote(2, 91, &contradictory, 5).is_err());
    assert_eq!(a.state(), before);
    assert_eq!(a.receive_vote(2, 91, &reply, 5)?, Tally::Elected);
    assert_eq!(a.receive_vote(2, 91, &reply, 5)?, Tally::Ignored);
    assert_eq!(a.state().leader, Some(1));
    assert_eq!(a.state().granted_votes, 2);
    let peer = [ApiVersion {
        api_key: 52,
        min_version: 0,
        max_version: 2,
    }];
    assert_eq!(protocol::negotiate(&peer, 52), Some(0));
    assert_eq!(
        protocol::negotiate(
            &[ApiVersion {
                min_version: 1,
                ..peer[0]
            }],
            52
        ),
        None
    );
    assert_eq!(protocol::negotiate(&[peer[0], peer[0]], 52), None);
    Ok(())
}

#[test]
fn wire_overflow_and_storage_exhaustion_fail_without_affirmative_reply() -> Result {
    let t = Temp::new()?;
    let mut c = open(&t, 1)?;
    c.respond(&epoch_request(53, 2, i32::MAX, &[])?, 1)?;
    let before = c.state();
    assert!(matches!(c.tick(6, 1), Err(Error::EpochOverflow)));
    assert_eq!(c.state(), before);
    drop(c);
    let mut c = open(&t, 1)?;
    assert_eq!(c.state().persistent.term, i32::MAX as u64 + 1);
    assert!(c.advance_log(-1, 1).is_err());
    assert!(c.advance_log(1, 0).is_err());
    let mut tiny = config(1)?;
    tiny.max_states = 2;
    let mut c = Controller::open(t.path("tiny"), tiny, 0)?;
    assert!(c.respond(&vote(None, 2, 1, 0, 0)?, 1).is_err());
    assert!(c.state().poisoned);
    let path = t.path("outside-wire");
    let mut core = election::Election::open(
        &path,
        Membership::new(1, vec![1, 2, 3])?,
        Timeouts::new(5, 5)?,
        128,
        7,
        0,
    )?;
    core.adopt_term(i32::MAX as u64 + 2, 1)?;
    drop(core);
    assert!(matches!(
        Controller::open(path, config(1)?, 0),
        Err(Error::EpochOverflow)
    ));
    Ok(())
}

#[tokio::test]
async fn actual_transport_dispatch_advertises_controller_profile_and_joins() -> Result {
    let t = Temp::new()?;
    let handler = Arc::new(ControllerHandler::open(t.path("actor"), config(1)?).await?);
    let limits = transport::Config::new(
        2,
        1,
        1024,
        1024,
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )?;
    let mut server = Transport::bind("127.0.0.1:0".parse()?, limits, handler.clone()).await?;
    let mut stream = TcpStream::connect(server.local_addr()).await?;
    for request in [header(18, 0, 77), vote(None, 2, 1, 0, 0)?] {
        stream.write_i32(i32::try_from(request.len())?).await?;
        stream.write_all(&request).await?;
        let n = stream.read_i32().await?;
        let mut response = vec![0; usize::try_from(n)?];
        stream.read_exact(&mut response).await?;
        assert_eq!(&response[..4], &77_i32.to_be_bytes());
        if request[1] == 18 {
            assert_eq!(response.len(), 34);
            assert_eq!(short_at(&response, 4)?, 0);
            for (i, api) in protocol::CONTROLLER_API_VERSIONS.iter().enumerate() {
                assert_eq!(short_at(&response, 10 + 6 * i)?, api.api_key);
            }
        } else {
            assert_eq!(partition_error(&response, 52)?, 0);
        }
    }
    for key in [52, 53, 54] {
        let version = if key == 52 { 2 } else { 1 };
        assert!(matches!(
            handler.handle(header(key, version, 77)).await,
            Err(Error::UnsupportedVersion)
        ));
    }
    assert!(handler.handle(header(0, 0, 77)).await.is_err());
    server.shutdown().await?;
    handler.shutdown().await?;
    handler.shutdown().await?;
    assert!(matches!(
        handler.handle(header(18, 0, 77)).await,
        Err(Error::Stopped)
    ));
    let reopened = Controller::open(t.path("actor"), config(1)?, 0)?;
    assert_eq!(reopened.state().persistent.voted_for, Some(2));
    Ok(())
}

#[tokio::test]
async fn independent_controller_goldens_cross_actual_tcp_transport() -> Result {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/raft-protocol");
    let mut count = 0;
    for version in ["4.1.2", "4.2.1", "4.3.1"] {
        let directory = base.join(version);
        let table_path = directory.join("cases.tsv");
        let table = tokio::task::spawn_blocking(move || read_table(table_path)).await??;
        let rows: Vec<Vec<&str>> = table
            .lines()
            .skip(1)
            .filter(|s| !s.is_empty())
            .map(|s| s.split('\t').collect())
            .collect();
        // Every supported ApiVersions wire layout and one affirmative durable vote.
        for row in rows.iter().filter(|r| {
            r[7] == "response"
                && r[6] == "fresh"
                && (r[0] == "vote-positive"
                    || (r[1] == "18" && matches!(r[2], "0" | "1" | "2" | "3" | "4")))
        }) {
            let t = Temp::new()?;
            let handler =
                Arc::new(ControllerHandler::open(t.path("tcp-oracle"), config(1)?).await?);
            let limits = transport::Config::new(
                1,
                1,
                4096,
                4096,
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(2),
            )?;
            let mut server =
                Transport::bind("127.0.0.1:0".parse()?, limits, handler.clone()).await?;
            let mut stream = TcpStream::connect(server.local_addr()).await?;
            let request_path = directory.join(row[4]);
            let response_path = directory.join(row[5]);
            let (payload, expected) = tokio::task::spawn_blocking(move || -> std::io::Result<_> {
                Ok((read_file(request_path)?, read_file(response_path)?))
            })
            .await??;
            stream.write_i32(i32::try_from(payload.len())?).await?;
            stream.write_all(&payload).await?;
            let size = stream.read_i32().await?;
            assert!((0..=4096).contains(&size));
            let mut actual = vec![0; usize::try_from(size)?];
            stream.read_exact(&mut actual).await?;
            assert_eq!(actual, expected, "{version}/{} TCP", row[0]);
            if let Some(path) = std::env::var_os("PL_CONTROLLER_RESPONSE_DIR") {
                let output = PathBuf::from(path).join("transport").join(version);
                let name = row[0].to_owned();
                let mut frame = size.to_be_bytes().to_vec();
                frame.extend_from_slice(&actual);
                tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                    fs::create_dir_all(&output)?;
                    write_file(output.join(format!("{name}.actual-response.bin")), actual)?;
                    write_file(output.join(format!("{name}.actual-frame.bin")), frame)
                })
                .await??;
            }
            server.shutdown().await?;
            handler.shutdown().await?;
            count += 1;
        }
    }
    assert!(
        count >= 18,
        "all three releases must traverse all five ApiVersions layouts and a Vote"
    );
    Ok(())
}

#[test]
fn configured_request_queue_and_identity_budgets_validate() -> Result {
    let t = Temp::new()?;
    let mut c = config(1)?;
    c.max_queued_requests = 0;
    assert!(Controller::open(t.path("queue"), c, 0).is_err());
    let mut c = config(1)?;
    c.protocol_limits = Limits::new(64 * 1024 * 1024, 1)?;
    c.max_queued_requests = 9;
    assert!(Controller::open(t.path("bytes"), c, 0).is_err());
    let mut c = config(1)?;
    c.voters.push(1);
    assert!(Controller::open(t.path("members"), c, 0).is_err());
    let mut c = config(1)?;
    c.voters = (0..65).collect();
    assert!(matches!(
        Controller::open(t.path("excess-members"), c, 0),
        Err(Error::InvalidConfig)
    ));
    Ok(())
}

fn retain_response(version: &str, name: &str, response: &[u8]) -> Result {
    if let Some(path) = std::env::var_os("PL_CONTROLLER_RESPONSE_DIR") {
        let directory = PathBuf::from(path).join(version);
        fs::create_dir_all(&directory)?;
        write_file(
            directory.join(format!("{name}.actual-response.bin")),
            response,
        )?;
    }
    Ok(())
}

/// Fixtures and expectations are generated by the separately owned Apache oracle.
#[test]
fn independent_apache_controller_goldens_and_registry() -> Result {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/raft-protocol");
    let mut total = 0;
    let mut observations = Vec::new();
    for version in ["4.1.2", "4.2.1", "4.3.1"] {
        let directory = base.join(version);
        let table = read_table(directory.join("cases.tsv"))?;
        let rows: Vec<Vec<&str>> = table
            .lines()
            .skip(1)
            .filter(|s| !s.is_empty())
            .map(|s| s.split('\t').collect())
            .collect();
        for row in &rows {
            assert_eq!(row.len(), 12, "{version} fixture schema");
            let t = Temp::new()?;
            let mut c = open(&t, 1)?;
            if row[6] != "fresh" {
                for action in row[6].split('|') {
                    let action: Vec<&str> = action.split(':').collect();
                    match action.first().copied() {
                        Some("log") => {
                            assert_eq!(action.len(), 3);
                            let epoch = action[1].parse()?;
                            c.adopt_epoch(epoch, 0)?;
                            c.advance_log(epoch, action[2].parse()?)?;
                        }
                        Some("req") => {
                            assert_eq!(action.len(), 2);
                            let setup = rows
                                .iter()
                                .find(|r| r[0] == action[1])
                                .ok_or("unknown independent setup")?;
                            c.respond(&read_file(directory.join(setup[4]))?, 0)?;
                        }
                        _ => return Err("unknown independent setup action".into()),
                    }
                }
            }
            let request = read_file(directory.join(row[4]))?;
            let before = c.state();
            let durable = c.durable_states();
            let result = c.respond(&request, 0);
            match row[7] {
                "response" => {
                    let actual = result.map_err(|e| format!("{version}/{}: {e}", row[0]))?;
                    assert_eq!(
                        actual,
                        read_file(directory.join(row[5]))?,
                        "{version}/{}",
                        row[0]
                    );
                    retain_response(version, row[0], &actual)?;
                    observations.push(format!(
                        "{{\"release\":\"{version}\",\"case\":\"{}\",\"response_hex\":\"{}\"}}",
                        row[0],
                        hex(&actual)?
                    ));
                }
                "close" => {
                    assert!(result.is_err(), "{version}/{}", row[0]);
                    assert_eq!(c.state(), before);
                    assert_eq!(c.durable_states(), durable);
                    observations.push(format!(
                        "{{\"release\":\"{version}\",\"case\":\"{}\",\"response_hex\":null}}",
                        row[0]
                    ));
                }
                _ => return Err("unknown independent disposition".into()),
            }
            let state = c.state();
            if row[8] != "-" {
                assert_eq!(
                    state.persistent.term,
                    row[8].parse::<u64>()?,
                    "{version}/{} term",
                    row[0]
                );
            }
            for (actual, expected) in [
                (state.persistent.voted_for, row[9]),
                (state.leader, row[10]),
            ] {
                if expected != "-" {
                    assert_eq!(
                        actual,
                        if expected == "none" {
                            None
                        } else {
                            Some(expected.parse()?)
                        },
                        "{version}/{} identity",
                        row[0]
                    );
                }
            }
            if row[11] != "-" {
                assert_eq!(
                    state.deadline_ms,
                    row[11].parse::<u64>()?,
                    "{version}/{} deadline",
                    row[0]
                );
            }
            total += 1;
        }
    }
    assert_eq!(
        total, 201,
        "all67 pinned cases must run for each Apache release"
    );
    println!("Independent Apache controller fixture cases passed: {total}");
    if let Some(path) = std::env::var_os("PL_BROKER_CONTROLLER_REPORT") {
        let apis = protocol::CONTROLLER_API_VERSIONS
            .iter()
            .map(|api| {
                format!(
                    "{{\"api_key\":{},\"min_version\":{},\"max_version\":{}}}",
                    api.api_key, api.min_version, api.max_version
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        write_file(path, format!("{{\"schema_version\":1,\"profile\":\"fixed-controller-v0\",\"controller_source_sha256\":\"{}\",\"controller_test_source_sha256\":\"{}\",\"election_source_sha256\":\"{}\",\"raft_module_source_sha256\":\"{}\",\"implemented_api_versions\":[{apis}],\"case_results\":[{}],\"independent_fixture_cases\":{total},\"qualification\":\"not_run\"}}\n", registry_hash("controller_source_sha256")?, registry_hash("controller_test_source_sha256")?, registry_hash("election_source_sha256")?, registry_hash("raft_module_source_sha256")?, observations.join(",")))?;
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> Result<String> {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut text, "{byte:02x}")?;
    }
    Ok(text)
}
fn registry_hash(key: &str) -> Result<&'static str> {
    let registry = include_str!("../../tests/conformance/broker/implemented-api-versions.json");
    let (_, rest) = registry
        .split_once(&format!("\"{key}\""))
        .ok_or("missing controller registry source")?;
    let (_, rest) = rest.split_once(':').ok_or("invalid registry field")?;
    let value = rest.split('"').nth(1).ok_or("invalid registry string")?;
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("invalid registry source hash".into());
    }
    Ok(value)
}

#[test]
fn dynamic_directory_owner_closes_fixed_v0_wire_before_persistent_changes() -> Result {
    use partitionline_broker::raft::{
        membership::{Bootstrap, Endpoint, Key, Voter, Voters},
        replication::{self, Node},
    };
    let t = Temp::new()?;
    let requests = [header(18, 0, 77), vote(None, 2, 1, 0, 0)?];
    let mut fixed = open(&t, 1)?;
    for request in &requests {
        assert!(
            !fixed.respond(request, 0)?.is_empty(),
            "fixed controller keeps its existing supported wire path"
        );
    }
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
    let genesis = Voters::new(0, election::LogPosition::default(), 1, voters)?;
    let mut dynamic = Node::open_dynamic(
        t.path("dynamic-content"),
        t.path("dynamic-election"),
        replication::Config::new(config(1)?),
        Bootstrap::new(local, genesis, "CONTROLLER".into())?,
        None,
        0,
    )?;
    let before = dynamic.state();
    for request in &requests {
        assert!(
            matches!(
                dynamic.respond_controller(request, 0),
                Err(replication::Error::Controller(Error::UnsupportedVersion))
            ),
            "legacy wire must not supply missing directory authority or advertisement"
        );
    }
    let after = dynamic.state();
    assert_eq!(after.election.persistent, before.election.persistent);
    assert_eq!(after.wal_durable_ops, before.wal_durable_ops);
    assert_eq!(
        after.election_durable_states,
        before.election_durable_states
    );
    assert!(after.ready);
    Ok(())
}
