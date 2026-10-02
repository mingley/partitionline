//! Durable election, bounded timers and actual fixed-membership histories.

use partitionline_broker::journal::{Entry, Journal, Limits};
use partitionline_broker::raft::election::{
    Election, Error, LogPosition, Membership, Role, State, Tally, Tick, Timeouts, VoteRequest,
    VoteResponse,
};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Temp(PathBuf);
impl Temp {
    fn new() -> std::io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::var_os("PL_RAFT_TEST_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let path = root.join(format!(
            "partitionline-election-{}-{}",
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
fn members(local: u32) -> std::result::Result<Membership, Error> {
    Membership::new(local, vec![1, 2, 3])
}
fn open(temp: &Temp, name: &str, local: u32, budget: usize) -> Result<Election> {
    Ok(Election::open(
        temp.path(name),
        members(local)?,
        Timeouts::new(5, 5)?,
        budget,
        7,
        0,
    )?)
}
fn request(term: u64, candidate: u32, log_term: u64, index: u64) -> VoteRequest {
    VoteRequest {
        term,
        candidate,
        log: LogPosition {
            term: log_term,
            index,
        },
    }
}

#[test]
fn membership_clock_term_and_timeout_bounds_fail_explicitly() -> Result {
    assert!(Membership::new(1, vec![]).is_err());
    assert!(Membership::new(1, vec![1, 1]).is_err());
    assert!(Membership::new(1, vec![2, 3]).is_err());
    assert!(Membership::new(1, (0..65).collect()).is_err());
    assert!(Membership::new(1, vec![1, u32::MAX]).is_err());
    for (minimum, maximum) in [(0, 1), (5, 4), (1, 600_001)] {
        assert!(Timeouts::new(minimum, maximum).is_err());
    }
    let temp = Temp::new()?;
    assert!(matches!(
        Election::open(
            temp.path("budget"),
            members(1)?,
            Timeouts::new(1, 1)?,
            0,
            0,
            0
        ),
        Err(Error::InvalidStateBudget)
    ));
    assert!(matches!(
        Election::open(
            temp.path("deadline"),
            members(1)?,
            Timeouts::new(1, 1)?,
            8,
            0,
            u64::MAX
        ),
        Err(Error::DeadlineOverflow)
    ));
    let mut node = open(&temp, "clock", 1, 16)?;
    assert_eq!(node.tick(4)?, Tick::Idle);
    assert!(matches!(node.tick(3), Err(Error::ClockRegression)));
    assert!(matches!(
        node.request_vote(request(1, 99, 0, 0), 4),
        Err(Error::UnknownVoter)
    ));
    assert!(matches!(
        node.request_vote(request(0, 2, 0, 0), 4),
        Err(Error::InvalidRequest)
    ));
    assert!(matches!(
        node.request_vote(request(1, 2, 2, 1), 4),
        Err(Error::InvalidRequest)
    ));
    assert!(matches!(
        node.request_vote(request(1, 2, 0, 1), 4),
        Err(Error::InvalidLogPosition)
    ));
    assert!(node.request_vote(request(u64::MAX, 2, 0, 0), 4)?.granted);
    assert!(matches!(node.tick(9), Err(Error::TermOverflow)));
    assert_eq!(node.state().persistent.term, u64::MAX);
    assert!(!node.state().poisoned);
    let mut single = Election::open(
        temp.path("single"),
        Membership::new(0, vec![0])?,
        Timeouts::new(1, 1)?,
        8,
        0,
        0,
    )?;
    assert!(matches!(single.tick(1)?, Tick::Campaign(_)));
    assert_eq!(single.state().role, Role::Leader);
    assert_eq!(single.state().granted_votes, 1);
    assert_eq!(single.state().persistent.voted_for, Some(0));
    Ok(())
}

#[test]
fn synchronized_vote_survives_restart_and_never_changes_in_same_term() -> Result {
    let temp = Temp::new()?;
    let mut node = open(&temp, "votes", 1, 32)?;
    let granted = node.request_vote(request(1, 2, 0, 0), 1)?;
    assert!(granted.granted);
    assert_eq!(node.durable_states(), 2);
    assert!(node.request_vote(request(1, 2, 0, 0), 2)?.granted);
    assert_eq!(node.durable_states(), 2);
    assert!(!node.request_vote(request(1, 3, 0, 0), 3)?.granted);
    drop(node);
    let mut reopened = open(&temp, "votes", 1, 32)?;
    assert_eq!(reopened.state().role, Role::Follower);
    assert_eq!(reopened.state().persistent.voted_for, Some(2));
    assert!(!reopened.request_vote(request(1, 3, 0, 0), 0)?.granted);
    assert!(reopened.request_vote(request(0, 2, 0, 0), 0).is_err());
    assert!(reopened.request_vote(request(2, 3, 0, 0), 1)?.granted);
    assert_eq!(reopened.state().persistent.term, 2);
    assert_eq!(reopened.state().persistent.voted_for, Some(3));
    drop(reopened);
    assert!(Election::open(
        temp.path("votes"),
        Membership::new(1, vec![1, 2, 4])?,
        Timeouts::new(5, 5)?,
        32,
        7,
        0
    )
    .is_err());
    Ok(())
}

#[test]
fn durable_log_freshness_uses_term_then_index_and_denials_persist_higher_term() -> Result {
    let temp = Temp::new()?;
    let mut node = open(&temp, "log", 1, 32)?;
    assert!(matches!(node.tick(5)?, Tick::Campaign(_)));
    node.advance_log(LogPosition::new(1, 5)?)?;
    assert!(matches!(
        node.advance_log(LogPosition::new(1, 4)?),
        Err(Error::LogRegression)
    ));
    assert!(matches!(
        node.advance_log(LogPosition::new(2, 5)?),
        Err(Error::CorruptState)
    ));
    drop(node);
    let mut node = open(&temp, "log", 1, 32)?;
    assert_eq!(node.state().persistent.log, LogPosition::new(1, 5)?);
    let behind = node.request_vote(request(2, 2, 1, 4), 0)?;
    assert!(!behind.granted);
    assert_eq!(behind.term, 2);
    assert_eq!(node.state().persistent.voted_for, None);
    drop(node);
    let mut node = open(&temp, "log", 1, 32)?;
    assert_eq!(node.state().persistent.term, 2);
    assert!(node.request_vote(request(2, 2, 1, 5), 0)?.granted);
    assert!(node.request_vote(request(3, 3, 2, 1), 1)?.granted);
    assert!(node.request_vote(request(2, 2, 9, 9), 2).is_err());
    Ok(())
}

#[test]
fn duplicate_stale_negative_and_misrouted_responses_cannot_create_a_majority() -> Result {
    let temp = Temp::new()?;
    let mut node = Election::open(
        temp.path("five"),
        Membership::new(1, vec![1, 2, 3, 4, 5])?,
        Timeouts::new(5, 5)?,
        32,
        7,
        0,
    )?;
    node.tick(5)?;
    let response = VoteResponse {
        term: 1,
        voter: 2,
        candidate: 1,
        granted: true,
    };
    assert_eq!(node.receive_vote(response, 5)?, Tally::Counted);
    assert_eq!(node.receive_vote(response, 5)?, Tally::Ignored);
    assert_eq!(node.state().role, Role::Candidate);
    assert_eq!(
        node.receive_vote(
            VoteResponse {
                voter: 3,
                granted: false,
                ..response
            },
            5
        )?,
        Tally::Rejected
    );
    assert!(matches!(
        node.receive_vote(
            VoteResponse {
                voter: 99,
                ..response
            },
            5
        ),
        Err(Error::UnknownVoter)
    ));
    assert!(matches!(
        node.receive_vote(
            VoteResponse {
                candidate: 4,
                ..response
            },
            5
        ),
        Err(Error::InvalidRequest)
    ));
    assert_eq!(
        node.receive_vote(
            VoteResponse {
                voter: 4,
                term: 2,
                granted: false,
                ..response
            },
            6
        )?,
        Tally::SteppedDown
    );
    assert_eq!(node.state().persistent.voted_for, None);
    assert_eq!(
        node.receive_vote(
            VoteResponse {
                voter: 3,
                ..response
            },
            6
        )?,
        Tally::Ignored
    );
    assert_eq!(node.state().role, Role::Follower);
    Ok(())
}

#[test]
fn a_real_peer_vote_elects_and_stale_leader_assertions_are_fenced() -> Result {
    let temp = Temp::new()?;
    let mut first = open(&temp, "one", 1, 32)?;
    let mut second = open(&temp, "two", 2, 32)?;
    let Tick::Campaign(campaign) = first.tick(5)? else {
        return Err("expected campaign".into());
    };
    let response = second.request_vote(campaign, 5)?;
    assert_eq!(first.receive_vote(response, 5)?, Tally::Elected);
    assert!(second.observe_leader(1, 1, 5)?);
    assert!(matches!(
        second.observe_leader(3, 1, 6),
        Err(Error::ConflictingLeader)
    ));
    assert!(second.observe_leader(3, 2, 6)?);
    assert!(!second.observe_leader(1, 1, 7)?);
    assert!(matches!(
        first.observe_leader(1, 2, 5),
        Err(Error::NotElected)
    ));
    first.lose_quorum(6)?;
    assert_eq!(first.state().role, Role::Follower);
    assert_eq!(first.state().persistent.voted_for, Some(1));
    assert!(!first.request_vote(request(1, 2, 0, 0), 6)?.granted);
    drop(first);
    let first = open(&temp, "one", 1, 32)?;
    assert_eq!(first.state().role, Role::Follower);
    assert_eq!(first.state().leader, None);
    assert_eq!(first.state().persistent.voted_for, Some(1));
    Ok(())
}

#[test]
fn exhausted_storage_poison_prevents_even_repeat_grants_until_recovery() -> Result {
    let temp = Temp::new()?;
    let mut node = open(&temp, "budget", 1, 2)?;
    assert!(node.request_vote(request(1, 2, 0, 0), 0)?.granted);
    assert!(node.request_vote(request(2, 3, 0, 0), 1).is_err());
    assert!(node.state().poisoned);
    assert_eq!(node.state().persistent.term, 1);
    assert!(matches!(
        node.request_vote(request(1, 2, 0, 0), 1),
        Err(Error::Poisoned)
    ));
    assert!(matches!(node.tick(9), Err(Error::Poisoned)));
    assert!(matches!(node.observe_leader(2, 1, 1), Err(Error::Poisoned)));
    assert!(matches!(
        node.advance_log(LogPosition::default()),
        Err(Error::Poisoned)
    ));
    assert!(matches!(node.lose_quorum(1), Err(Error::Poisoned)));
    drop(node);
    let mut node = open(&temp, "budget", 1, 2)?;
    assert!(!node.state().poisoned);
    assert!(node.request_vote(request(1, 2, 0, 0), 0)?.granted);
    assert!(!node.request_vote(request(1, 3, 0, 0), 0)?.granted);
    assert_eq!(node.durable_states(), 2);
    Ok(())
}

#[test]
fn torn_unconfirmed_tail_repairs_but_checksummed_double_vote_and_corruption_fail_closed() -> Result
{
    let temp = Temp::new()?;
    let path = temp.path("torn");
    let mut node = open(&temp, "torn", 1, 16)?;
    node.request_vote(request(1, 2, 0, 0), 0)?;
    drop(node);
    OpenOptions::new()
        .append(true)
        .open(&path)?
        .write_all(b"PLENTRY1")?;
    let mut node = open(&temp, "torn", 1, 16)?;
    assert_eq!(node.recovery().truncated_bytes, 8);
    assert_eq!(node.state().persistent.voted_for, Some(2));
    assert!(!node.request_vote(request(1, 3, 0, 0), 0)?.granted);
    drop(node);
    let limits = Limits::new(60, 24 + 92 * 16, 16, std::mem::size_of::<Entry>() + 60)?;
    let (mut journal, _) = Journal::open(&path, 0, limits)?;
    let mut payload = journal
        .fetch(1, 1, std::mem::size_of::<Entry>() + 60)?
        .remove(0)
        .payload;
    payload[24..28].copy_from_slice(&3u32.to_be_bytes());
    journal.append(1, &payload)?;
    let bytes_before = journal.file_bytes();
    drop(journal);
    assert!(open(&temp, "torn", 1, 16).is_err());
    assert_eq!(fs::metadata(&path)?.len(), bytes_before);
    let mut good = open(&temp, "crc", 1, 16)?;
    good.request_vote(request(1, 2, 0, 0), 0)?;
    drop(good);
    let mut bytes = Vec::new();
    File::open(temp.path("crc"))?.read_to_end(&mut bytes)?;
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(temp.path("crc"))?
        .write_all(&bytes)?;
    assert!(open(&temp, "crc", 1, 16).is_err());
    Ok(())
}

#[test]
fn seeded_timers_are_reproducible_node_separated_and_always_bounded() -> Result {
    let temp = Temp::new()?;
    let mut first = Election::open(
        temp.path("a"),
        members(1)?,
        Timeouts::new(5, 17)?,
        64,
        41,
        0,
    )?;
    let mut replay = Election::open(
        temp.path("b"),
        members(1)?,
        Timeouts::new(5, 17)?,
        64,
        41,
        0,
    )?;
    let mut other = Election::open(
        temp.path("c"),
        members(2)?,
        Timeouts::new(5, 17)?,
        64,
        41,
        0,
    )?;
    let mut distinct = false;
    for now in 0..24 {
        assert_eq!(first.state().deadline_ms, replay.state().deadline_ms);
        for node in [&mut first, &mut replay, &mut other] {
            node.lose_quorum(now)?;
            assert!((5..=17).contains(&(node.state().deadline_ms - now)));
        }
        distinct |= first.state().deadline_ms != other.state().deadline_ms;
    }
    assert!(distinct);
    Ok(())
}

fn optional(number: Option<u32>) -> String {
    number.map_or_else(|| "null".into(), |value| value.to_string())
}
fn state_json(state: State, states: usize) -> String {
    format!("{{\"term\":{},\"vote\":{},\"log_term\":{},\"log_index\":{},\"role\":\"{:?}\",\"leader\":{},\"deadline\":{},\"grants\":{},\"poisoned\":{},\"states\":{states}}}", state.persistent.term, optional(state.persistent.voted_for), state.persistent.log.term, state.persistent.log.index, state.role, optional(state.leader), state.deadline_ms, state.granted_votes, state.poisoned)
}
fn request_json(value: VoteRequest) -> String {
    format!(
        "{{\"term\":{},\"candidate\":{},\"log_term\":{},\"log_index\":{}}}",
        value.term, value.candidate, value.log.term, value.log.index
    )
}
fn response_json(value: VoteResponse) -> String {
    format!(
        "{{\"term\":{},\"candidate\":{},\"voter\":{},\"granted\":{}}}",
        value.term, value.candidate, value.voter, value.granted
    )
}
struct History {
    directory: Option<PathBuf>,
    writer: Option<BufWriter<File>>,
}
impl History {
    fn new() -> Result<Self> {
        let directory = std::env::var_os("PL_RAFT_HISTORY_DIR").map(PathBuf::from);
        let writer = if let Some(path) = &directory {
            fs::create_dir(path)?;
            Some(BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path.join("history.jsonl"))?,
            ))
        } else {
            None
        };
        Ok(Self { directory, writer })
    }
    fn line(&mut self, line: String) -> std::io::Result<()> {
        if let Some(writer) = &mut self.writer {
            writeln!(writer, "{line}")?;
        }
        Ok(())
    }
    fn event(
        &mut self,
        kind: &str,
        node: u32,
        now: u64,
        fields: &str,
        election: &Election,
    ) -> std::io::Result<()> {
        self.line(format!(
            "{{\"kind\":\"{kind}\",\"node\":{node},\"now\":{now},{fields}\"state\":{}}}",
            state_json(election.state(), election.durable_states())
        ))
    }
}

#[derive(Clone, Copy)]
enum Message {
    Request(u32, VoteRequest),
    Response(u32, VoteResponse),
    Leader(u32, u32, u64),
}
impl Message {
    fn route(self) -> (u32, u32) {
        match self {
            Self::Request(to, req) => (req.candidate, to),
            Self::Response(to, reply) => (reply.voter, to),
            Self::Leader(to, from, _) => (from, to),
        }
    }
}
fn random(value: &mut u64) -> u64 {
    *value ^= *value << 13;
    *value ^= *value >> 7;
    *value ^= *value << 17;
    *value
}

#[test]
fn deterministic_seeded_losing_partition_and_restart_histories() -> Result {
    let mut history = History::new()?;
    let mut campaigns = 0;
    let mut elections = 0;
    let mut denials = 0;
    let mut restarts = 0;
    for seed in 1..=16u64 {
        let temp = Temp::new()?;
        history.line(format!("{{\"kind\":\"config\",\"seed\":{seed},\"members\":[1,2,3],\"timeouts\":[3,9],\"max_states\":512}}"))?;
        let mut nodes = Vec::new();
        for node in 1..=3u32 {
            let election = Election::open(
                temp.path(&format!("node-{node}")),
                members(node)?,
                Timeouts::new(3, 9)?,
                512,
                seed,
                0,
            )?;
            history.event("open", node, 0, "", &election)?;
            nodes.push(Some(election));
        }
        let mut pending = Vec::new();
        let mut generator = seed;
        let mut isolated = 0;
        for now in 1..=112u64 {
            if [8, 36, 60, 96].contains(&now) {
                isolated = match now {
                    8 => 1,
                    60 => 3,
                    _ => 0,
                };
                history.line(format!(
                    "{{\"kind\":\"partition\",\"now\":{now},\"isolated\":{isolated}}}"
                ))?;
                if isolated != 0 {
                    let index = usize::try_from(isolated - 1)?;
                    let node = nodes[index].as_mut().ok_or("missing node")?;
                    if node.state().role == Role::Leader {
                        node.lose_quorum(now)?;
                        history.event("lose_quorum", isolated, now, "", node)?;
                    }
                }
            }
            if now == 48 || now == 84 {
                let old = nodes[1].take().ok_or("missing node")?;
                drop(old);
                let node = Election::open(
                    temp.path("node-2"),
                    members(2)?,
                    Timeouts::new(3, 9)?,
                    512,
                    seed,
                    now,
                )?;
                history.event("restart", 2, now, "", &node)?;
                nodes[1] = Some(node);
                restarts += 1;
            }
            for id in 1..=3u32 {
                let node = nodes[usize::try_from(id - 1)?]
                    .as_mut()
                    .ok_or("missing node")?;
                let result = node.tick(now)?;
                let fields = match result {
                    Tick::Idle => "\"outcome\":\"idle\",".to_string(),
                    Tick::Campaign(req) => {
                        campaigns += 1;
                        for to in 1..=3 {
                            if to != id {
                                pending.push(Message::Request(to, req));
                            }
                        }
                        format!(
                            "\"outcome\":\"campaign\",\"request\":{},",
                            request_json(req)
                        )
                    }
                };
                history.event("tick", id, now, &fields, node)?;
                if node.state().role == Role::Leader && now % 13 == 0 {
                    let log = LogPosition::new(
                        node.state().persistent.term,
                        node.state().persistent.log.index + 1,
                    )?;
                    node.advance_log(log)?;
                    history.event(
                        "advance_log",
                        id,
                        now,
                        &format!("\"log_term\":{},\"log_index\":{},", log.term, log.index),
                        node,
                    )?;
                }
            }
            for _ in 0..8 {
                if pending.is_empty() {
                    break;
                }
                assert!(pending.len() <= 4096);
                let at = usize::try_from(random(&mut generator) % pending.len() as u64)?;
                let message = pending.remove(at);
                let (from, to) = message.route();
                if isolated != 0 && ((from == isolated) != (to == isolated)) {
                    pending.push(message);
                    continue;
                }
                let node = nodes[usize::try_from(to - 1)?]
                    .as_mut()
                    .ok_or("missing node")?;
                match message {
                    Message::Request(_, req) => {
                        let response = node.request_vote(req, now)?;
                        if !response.granted {
                            denials += 1;
                        }
                        history.event(
                            "request_vote",
                            to,
                            now,
                            &format!(
                                "\"request\":{},\"response\":{},",
                                request_json(req),
                                response_json(response)
                            ),
                            node,
                        )?;
                        pending.push(Message::Response(req.candidate, response));
                    }
                    Message::Response(_, response) => {
                        let result = node.receive_vote(response, now)?;
                        history.event(
                            "receive_vote",
                            to,
                            now,
                            &format!(
                                "\"response\":{},\"outcome\":\"{result:?}\",",
                                response_json(response)
                            ),
                            node,
                        )?;
                        if result == Tally::Elected {
                            elections += 1;
                            for peer in 1..=3 {
                                if peer != to {
                                    pending.push(Message::Leader(
                                        peer,
                                        to,
                                        node.state().persistent.term,
                                    ));
                                }
                            }
                        } else if random(&mut generator) % 4 == 0 {
                            // Replay actual stale/duplicate peer replies; no fabricated grants.
                            pending.push(message);
                        }
                    }
                    Message::Leader(_, leader, term) => {
                        let accepted = node.observe_leader(leader, term, now)?;
                        history.event(
                            "observe_leader",
                            to,
                            now,
                            &format!(
                                "\"leader\":{leader},\"term\":{term},\"accepted\":{accepted},"
                            ),
                            node,
                        )?;
                    }
                }
            }
        }
        for id in 1..=3u32 {
            let node = nodes[usize::try_from(id - 1)?]
                .take()
                .ok_or("missing node")?;
            history.event("final", id, 112, "", &node)?;
            drop(node);
            if let Some(directory) = &history.directory {
                let name = format!("seed-{seed}-node-{id}.journal");
                let mut input = File::open(temp.path(&format!("node-{id}")))?;
                let mut output = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(directory.join(&name))?;
                std::io::copy(&mut input, &mut output)?;
                history.line(format!(
                    "{{\"kind\":\"journal\",\"node\":{id},\"path\":\"{name}\"}}"
                ))?;
            }
        }
    }
    if let Some(mut writer) = history.writer {
        writer.flush()?;
    }
    assert!(campaigns > elections && elections > 16 && denials > 0 && restarts == 32);
    println!("seeded histories: seeds=16 campaigns={campaigns} elections={elections} denials={denials} restarts={restarts}");
    Ok(())
}
