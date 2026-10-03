//! Actual bounded durable replication histories; typed caller-driven exchange is not Kafka wire.

use partitionline_broker::raft::{
    election::{LogPosition, Role, Timeouts},
    protocol,
    replication::{
        Config, Error, Limits, Node, Record, RecordKind, ReplicationHandler, Request, Response,
    },
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
fn read_file(path: impl AsRef<Path>) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(256 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 256 * 1024 * 1024 {
        return Err(std::io::Error::other("artifact read exceeds bound"));
    }
    Ok(bytes)
}
fn read_text(path: impl AsRef<Path>) -> Result<String> {
    Ok(String::from_utf8(read_file(path)?)?)
}
fn write_file(path: impl AsRef<Path>, bytes: impl AsRef<[u8]>) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = fs::File::create(path)?;
    file.write_all(bytes.as_ref())?;
    file.sync_all()
}
struct Temp(PathBuf);
impl Temp {
    fn new() -> std::io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::var_os("PL_REPLICATION_TEST_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let path = root.join(format!(
            "partitionline-replication-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
    fn wal(&self, id: u32) -> PathBuf {
        self.0.join(format!("{id}.wal"))
    }
    fn election(&self, id: u32) -> PathBuf {
        self.0.join(format!("{id}.election"))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}
fn config(id: u32, voters: &[u32]) -> Result<Config> {
    let mut c = protocol::Config::new(id, voters.to_vec(), "replication-test".into())?;
    c.election_timeouts = Timeouts::new(5, 5)?;
    c.max_states = 256;
    c.max_queued_requests = 4;
    let mut config = Config::new(c);
    config.max_queued_requests = 4;
    config.quorum_timeout_ms = 100;
    Ok(config)
}
fn open(temp: &Temp, id: u32, voters: &[u32], now: u64) -> Result<Node> {
    Ok(Node::open(
        temp.wal(id),
        temp.election(id),
        config(id, voters)?,
        now,
    )?)
}
fn elect(nodes: &mut [Node], leader: usize, voters: &[usize], now: u64) -> Result {
    let request = nodes[leader].campaign(now, 77)?.ok_or("campaign not due")?;
    for voter in voters {
        let reply = nodes[*voter].respond_controller(&request, now)?;
        let id = nodes[*voter].state().local_id;
        nodes[leader].receive_vote(id, 77, &reply, now)?;
    }
    assert_eq!(nodes[leader].state().election.role, Role::Leader);
    nodes[leader].activate_leader(now)?;
    Ok(())
}
fn exchange(nodes: &mut [Node], leader: usize, follower: usize, now: u64) -> Result<Response> {
    let id = nodes[follower].state().local_id;
    let request = nodes[leader].prepare(id, now)?;
    let response = nodes[follower].receive(&request, now)?;
    nodes[leader].acknowledge(id, response, now)?;
    Ok(response)
}
fn catch_up(nodes: &mut [Node], leader: usize, follower: usize, now: u64) -> Result {
    for _ in 0..8 {
        if exchange(nodes, leader, follower, now)?.success {
            return Ok(());
        }
    }
    Err("catch-up did not converge".into())
}
fn committed(node: &Node) -> Result<Vec<Record>> {
    Ok(node.fetch_committed(1, 4096, 2 * 1024 * 1024)?)
}

#[test]
fn strict_majority_barrier_and_committed_visibility_survive_reopen() -> Result {
    let temp = Temp::new()?;
    let mut nodes = vec![
        open(&temp, 1, &[1, 2, 3], 0)?,
        open(&temp, 2, &[1, 2, 3], 0)?,
        open(&temp, 3, &[1, 2, 3], 0)?,
    ];
    elect(&mut nodes, 0, &[1], 5)?;
    assert_eq!(nodes[0].state().committed_end, 0);
    assert!(committed(&nodes[0])?.is_empty());
    assert_eq!(nodes[0].propose(b"metadata", 5)?, 2);
    catch_up(&mut nodes, 0, 1, 5)?;
    assert_eq!(nodes[0].state().committed_end, 2);
    assert_eq!(nodes[1].state().committed_end, 0);
    exchange(&mut nodes, 0, 1, 6)?;
    assert_eq!(committed(&nodes[0])?, committed(&nodes[1])?);
    drop(nodes);
    let node = open(&temp, 1, &[1, 2, 3], 0)?;
    assert_eq!(node.state().election.role, Role::Follower);
    assert_eq!(node.state().active_term, None);
    assert_eq!(node.state().committed_end, 2);
    assert_eq!(committed(&node)?[0].kind, RecordKind::Barrier);
    assert_eq!(committed(&node)?[1].payload, b"metadata");
    Ok(())
}
#[test]
fn five_voters_need_three_distinct_durable_positions_and_reject_forged_replies() -> Result {
    let temp = Temp::new()?;
    let ids = [1, 2, 3, 4, 5];
    let mut nodes = ids
        .iter()
        .map(|id| open(&temp, *id, &ids, 0))
        .collect::<Result<Vec<_>>>()?;
    elect(&mut nodes, 0, &[1, 2], 5)?;
    nodes[0].propose(b"five", 5)?;
    catch_up(&mut nodes, 0, 1, 5)?;
    assert_eq!(nodes[0].state().committed_end, 0);
    let request = nodes[0].prepare(3, 5)?;
    let response = nodes[2].receive(&request, 5)?;
    // Initial heartbeat rejects: retain the real reply while testing envelope mutants.
    let mut forged = response;
    forged.peer = 2;
    assert!(matches!(
        nodes[0].acknowledge(3, forged, 5),
        Err(Error::InvalidPeer)
    ));
    forged = response;
    forged.sequence += 1;
    assert!(matches!(
        nodes[0].acknowledge(3, forged, 5),
        Err(Error::InvalidPeer)
    ));
    forged = response;
    forged.success = true;
    forged.matched = LogPosition {
        term: request.term,
        index: 100,
    };
    forged.conflict_index = 0;
    assert!(matches!(
        nodes[0].acknowledge(3, forged, 5),
        Err(Error::InvalidPeer)
    ));
    assert_eq!(nodes[0].state().committed_end, 0);
    nodes[0].acknowledge(3, response, 5)?;
    catch_up(&mut nodes, 0, 2, 5)?;
    assert_eq!(nodes[0].state().committed_end, 2);
    assert!(matches!(
        nodes[0].acknowledge(3, response, 5),
        Err(Error::InvalidPeer)
    ));
    Ok(())
}
#[test]
fn minority_partition_expiry_and_lost_ack_cannot_commit() -> Result {
    let temp = Temp::new()?;
    let mut nodes = vec![
        open(&temp, 1, &[1, 2, 3], 0)?,
        open(&temp, 2, &[1, 2, 3], 0)?,
        open(&temp, 3, &[1, 2, 3], 0)?,
    ];
    elect(&mut nodes, 0, &[1], 5)?;
    nodes[0].propose(b"not-committed", 5)?;
    let request = nodes[0].prepare(2, 5)?;
    let response = nodes[1].receive(&request, 5)?;
    nodes[0].acknowledge(2, response, 5)?;
    let request = nodes[0].prepare(2, 5)?;
    let response = nodes[1].receive(&request, 5)?;
    assert!(response.success);
    assert_eq!(nodes[1].state().last_position.index, 2);
    assert_eq!(nodes[0].state().committed_end, 0);
    nodes[0].timeout_peer(2, request.sequence)?;
    assert!(matches!(
        nodes[0].acknowledge(2, response, 5),
        Err(Error::InvalidPeer)
    ));
    assert!(nodes[0].poll(106)?);
    assert!(matches!(
        nodes[0].propose(b"after-expiry", 106),
        Err(Error::NotLeader)
    ));
    assert_eq!(nodes[0].state().committed_end, 0);
    Ok(())
}
#[test]
fn higher_term_repairs_only_uncommitted_suffix_and_preserves_vote_after_restart() -> Result {
    let temp = Temp::new()?;
    let mut nodes = vec![
        open(&temp, 1, &[1, 2, 3], 0)?,
        open(&temp, 2, &[1, 2, 3], 0)?,
        open(&temp, 3, &[1, 2, 3], 0)?,
    ];
    elect(&mut nodes, 0, &[1], 5)?;
    nodes[0].propose(b"committed", 5)?;
    catch_up(&mut nodes, 0, 1, 5)?;
    exchange(&mut nodes, 0, 1, 6)?;
    nodes[0].propose(b"orphan", 6)?;
    assert_eq!(nodes[0].state().last_position.index, 3);
    assert_eq!(nodes[0].state().committed_end, 2);
    elect(&mut nodes, 1, &[2], 11)?;
    nodes[1].propose(b"replacement", 11)?;
    catch_up(&mut nodes, 1, 2, 11)?;
    assert_eq!(nodes[1].state().committed_end, 4);
    catch_up(&mut nodes, 1, 0, 11)?;
    exchange(&mut nodes, 1, 0, 12)?;
    assert_eq!(committed(&nodes[0])?, committed(&nodes[1])?);
    assert_eq!(nodes[0].state().last_position.index, 4);
    assert_eq!(nodes[0].state().election.persistent.voted_for, None);
    let saved = committed(&nodes[0])?;
    drop(nodes);
    let node = open(&temp, 1, &[1, 2, 3], 0)?;
    assert_eq!(committed(&node)?, saved);
    assert!(node.state().ready);
    Ok(())
}
#[test]
fn partial_request_does_not_commit_an_unverified_old_suffix() -> Result {
    let temp = Temp::new()?;
    let mut node = open(&temp, 2, &[1, 2, 3], 0)?;
    let records = vec![
        Record::barrier(2, 1)?,
        Record {
            term: 2,
            index: 2,
            kind: RecordKind::Data,
            payload: b"old".to_vec(),
        },
    ];
    let first = Request {
        leader: 1,
        peer: 2,
        sequence: 1,
        term: 2,
        previous: LogPosition::default(),
        leader_commit: 0,
        entries: records,
    };
    assert!(node.receive(&first, 0)?.success);
    let next = Request {
        leader: 3,
        peer: 2,
        sequence: 2,
        term: 3,
        previous: LogPosition { term: 2, index: 1 },
        leader_commit: 2,
        entries: vec![],
    };
    assert!(node.receive(&next, 1)?.success);
    assert_eq!(node.state().committed_end, 1);
    assert_eq!(node.state().last_position.index, 2);
    Ok(())
}
#[test]
fn committed_prefix_and_same_term_payload_conflicts_fail_before_wal_mutation() -> Result {
    let temp = Temp::new()?;
    let mut node = open(&temp, 2, &[1, 2, 3], 0)?;
    let mut request = Request {
        leader: 1,
        peer: 2,
        sequence: 1,
        term: 2,
        previous: LogPosition::default(),
        leader_commit: 1,
        entries: vec![Record::barrier(2, 1)?],
    };
    node.receive(&request, 0)?;
    let before = node.state().wal_durable_ops;
    request.term = 3;
    request.leader = 3;
    request.sequence = 2;
    request.entries[0].term = 3;
    assert!(matches!(node.receive(&request, 1), Err(Error::InvalidPeer)));
    assert_eq!(node.state().wal_durable_ops, before);
    request.term = 2;
    request.leader = 1;
    request.entries[0].term = 2;
    request.entries[0].kind = RecordKind::Data;
    request.entries[0].payload = b"conflict".to_vec();
    assert!(matches!(node.receive(&request, 1), Err(Error::InvalidPeer)));
    assert_eq!(node.state().wal_durable_ops, before);
    Ok(())
}
#[test]
fn complete_wal_ahead_of_core_is_reconciled_before_vote_or_committed_read() -> Result {
    let temp = Temp::new()?;
    let mut nodes = vec![
        open(&temp, 1, &[1, 2, 3], 0)?,
        open(&temp, 2, &[1, 2, 3], 0)?,
        open(&temp, 3, &[1, 2, 3], 0)?,
    ];
    elect(&mut nodes, 0, &[1], 5)?;
    catch_up(&mut nodes, 0, 1, 5)?;
    assert_eq!(nodes[0].state().committed_end, 1);
    let core = read_file(temp.election(1))?;
    nodes[0].propose(b"ambiguous-complete", 5)?;
    drop(nodes);
    write_file(temp.election(1), core)?;
    // Restoring the previous core models a crash after WAL sync but before summary sync.
    // No majority acknowledged entry2, so the durable commit floor remains1.
    let node = open(&temp, 1, &[1, 2, 3], 0)?;
    assert_eq!(node.state().last_position.index, 2);
    assert!(node.state().ready);
    assert_eq!(node.state().committed_end, 1);
    Ok(())
}
#[test]
fn foreign_identity_corrupt_complete_and_torn_tail_have_distinct_outcomes() -> Result {
    let temp = Temp::new()?;
    let node = open(&temp, 1, &[1, 2, 3], 0)?;
    drop(node);
    assert!(open(&temp, 1, &[1, 2, 4], 0).is_err());
    let mut bytes = read_file(temp.wal(1))?;
    let original = bytes.clone();
    bytes.push(b'P');
    write_file(temp.wal(1), bytes)?;
    let node = open(&temp, 1, &[1, 2, 3], 0)?;
    assert_eq!(node.state().recovery.truncated_bytes, 1);
    drop(node);
    assert_eq!(read_file(temp.wal(1))?, original);
    let mut bytes = original;
    let n = bytes.len();
    bytes[n - 1] ^= 1;
    write_file(temp.wal(1), bytes)?;
    assert!(open(&temp, 1, &[1, 2, 3], 0).is_err());
    Ok(())
}
#[test]
fn bounds_queue_envelope_fetch_and_fixed_membership_are_explicit() -> Result {
    assert!(Limits::new(0, 1024, 8, 1024, 8, 4096, 1024).is_err());
    assert!(Limits::new(
        1024 * 1024,
        2 * 1024 * 1024,
        8,
        2 * 1024 * 1024,
        8,
        4 * 1024 * 1024,
        1024
    )
    .is_err());
    let temp = Temp::new()?;
    for slots in [0, 1024] {
        let mut invalid = config(1, &[1])?;
        invalid.max_queued_requests = slots;
        assert!(matches!(
            Node::open(temp.wal(1), temp.election(1), invalid, 0),
            Err(Error::InvalidConfig)
        ));
        assert!(!temp.wal(1).exists());
        assert!(!temp.election(1).exists());
    }
    let mut c = config(1, &[1])?;
    c.limits = Limits::new(32, 1024, 2, 64, 8, 4096, 1024)?;
    let mut node = Node::open(temp.wal(1), temp.election(1), c, 0)?;
    node.campaign(5, 77)?;
    node.activate_leader(5)?;
    assert!(matches!(node.propose(&[1; 33], 5), Err(Error::Bounds)));
    node.propose(b"bounded", 5)?;
    assert!(matches!(node.propose(b"third", 5), Err(Error::Bounds)));
    assert!(matches!(
        node.fetch_committed(0, 1, 1024),
        Err(Error::Bounds)
    ));
    assert!(matches!(
        node.reconfigure(&[1, 2, 3]),
        Err(Error::MembershipChangeUnsupported)
    ));
    assert_eq!(node.state().committed_end, 2);
    Ok(())
}
#[tokio::test]
async fn actor_serializes_controller_and_typed_storage_and_joins_shutdown() -> Result {
    let temp = Temp::new()?;
    let handler = ReplicationHandler::open(temp.wal(1), temp.election(1), config(1, &[1])?).await?;
    tokio::time::sleep(std::time::Duration::from_millis(6)).await;
    assert!(handler.campaign(77).await?.is_some());
    handler.activate_leader().await?;
    handler.propose(b"actor".to_vec()).await?;
    assert_eq!(handler.state().await?.committed_end, 2);
    assert_eq!(
        handler.fetch_committed(1, 2, 1024).await?[1].payload,
        b"actor"
    );
    let mut oversized = Vec::with_capacity(1024 * 1024 + 1);
    oversized.push(1);
    assert!(matches!(
        handler.propose(oversized).await,
        Err(Error::Bounds)
    ));
    handler.shutdown().await?;
    handler.shutdown().await?;
    assert!(matches!(handler.state().await, Err(Error::Stopped)));
    drop(handler);
    let node = open(&temp, 1, &[1], 0)?;
    assert_eq!(node.state().committed_end, 2);
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").unwrap_or_default();
    }
    out
}
fn text(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => {
                use std::fmt::Write;
                write!(out, "\\u{:04x}", ch as u32).unwrap_or_default();
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}
fn position_json(p: LogPosition) -> String {
    format!("{{\"term\":{},\"index\":{}}}", p.term, p.index)
}
fn record_json(r: &Record) -> String {
    format!(
        "{{\"term\":{},\"index\":{},\"kind\":{},\"payload_hex\":{}}}",
        r.term,
        r.index,
        u8::from(r.kind == RecordKind::Barrier),
        text(&hex(&r.payload))
    )
}
fn request_json(r: &Request) -> String {
    format!("{{\"leader\":{},\"peer\":{},\"sequence\":{},\"term\":{},\"previous\":{},\"leader_commit\":{},\"entries\":[{}]}}",r.leader,r.peer,r.sequence,r.term,position_json(r.previous),r.leader_commit,r.entries.iter().map(record_json).collect::<Vec<_>>().join(","))
}
fn response_json(r: Response) -> String {
    format!("{{\"peer\":{},\"leader\":{},\"sequence\":{},\"term\":{},\"success\":{},\"matched\":{},\"conflict_index\":{}}}",r.peer,r.leader,r.sequence,r.term,r.success,position_json(r.matched),r.conflict_index)
}
fn optional_id(id: Option<u32>) -> String {
    id.map_or_else(|| "null".into(), |id| id.to_string())
}
fn copy_synced(from: &PathBuf, to: &PathBuf) -> Result {
    fs::copy(from, to)?;
    fs::File::open(to)?.sync_all()?;
    Ok(())
}
struct History {
    root: PathBuf,
    output: PathBuf,
    ids: Vec<u32>,
    nodes: Vec<Option<Node>>,
    cached: Vec<String>,
    events: Vec<String>,
    receipts: Vec<String>,
    now: u64,
}
impl History {
    fn open(root: PathBuf, output: PathBuf, count: u32, now: u64) -> Result<Self> {
        fs::create_dir_all(&output)?;
        let ids: Vec<_> = (1..=count).collect();
        let nodes = ids
            .iter()
            .map(|id| {
                Node::open(
                    root.join(format!("{id}.wal")),
                    root.join(format!("{id}.election")),
                    config(*id, &ids)?,
                    now,
                )
                .map(Some)
                .map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            root,
            output,
            ids: ids.clone(),
            nodes,
            cached: vec![String::new(); ids.len()],
            events: Vec::new(),
            receipts: Vec::new(),
            now,
        })
    }
    fn node(&mut self, index: usize) -> Result<&mut Node> {
        self.nodes
            .get_mut(index)
            .and_then(Option::as_mut)
            .ok_or_else(|| "node is crashed".into())
    }
    fn state_json(node: &Node) -> Result<String> {
        let s = node.state();
        let committed = committed(node)?;
        Ok(format!("{{\"node_id\":{},\"open\":true,\"term\":{},\"role\":{},\"leader\":{},\"voted_for\":{},\"poisoned\":{},\"ready\":{},\"active_term\":{},\"last_position\":{},\"committed_end\":{},\"committed_records\":[{}],\"wal_durable_ops\":{},\"election_durable_states\":{}}}",s.local_id,s.election.persistent.term,text(&format!("{:?}",s.election.role)),optional_id(s.election.leader),optional_id(s.election.persistent.voted_for),s.poisoned,s.ready,s.active_term.map_or_else(||"null".into(),|t|t.to_string()),position_json(s.last_position),s.committed_end,committed.iter().map(record_json).collect::<Vec<_>>().join(","),s.wal_durable_ops,s.election_durable_states))
    }
    fn event(&mut self, kind: &str, node: u32, args: String, result: String) -> Result {
        let mut after = Vec::new();
        for (i, entry) in self.nodes.iter().enumerate() {
            let state = if let Some(node) = entry {
                Self::state_json(node)?
            } else {
                self.cached[i]
                    .replace("\"open\":true", "\"open\":false")
                    .replace("\"ready\":true", "\"ready\":false")
            };
            self.cached[i] = state.clone();
            after.push(state);
        }
        self.events.push(format!("{{\"ordinal\":{},\"now_ms\":{},\"kind\":{},\"node_id\":{},\"args\":{},\"result\":{},\"after\":[{}]}}",self.events.len(),self.now,text(kind),node,args,result,after.join(",")));
        use std::io::Write;
        let mut progress = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.output.join("events-progress.jsonl"))?;
        writeln!(
            progress,
            "{}",
            self.events.last().ok_or("missing recorded event")?
        )?;
        Ok(())
    }
    fn copy(&mut self, phase: &str, index: usize) -> Result<String> {
        let id = self.ids[index];
        let folder = format!("journals/{}-{phase}-{id}", self.events.len());
        let dir = self.output.join(&folder);
        fs::create_dir_all(&dir)?;
        copy_synced(
            &self.root.join(format!("{id}.wal")),
            &dir.join("metadata.wal"),
        )?;
        copy_synced(
            &self.root.join(format!("{id}.election")),
            &dir.join("election.wal"),
        )?;
        let node = self.nodes[index]
            .as_ref()
            .ok_or("copy needs a live confirmed owner")?;
        let s = node.state();
        let receipt=format!("{{\"node_id\":{id},\"phase\":{},\"event_ordinal\":{},\"wal_path\":{},\"election_path\":{},\"wal_confirmed_ops\":{},\"election_confirmed_states\":{},\"group\":{{\"cluster_id\":\"replication-test\",\"topic\":\"__cluster_metadata\",\"partition\":0,\"voters\":{:?}}}}}",text(phase),self.events.len(),text(&format!("{folder}/metadata.wal")),text(&format!("{folder}/election.wal")),s.wal_durable_ops,s.election_durable_states,self.ids);
        self.receipts.push(receipt.clone());
        Ok(receipt)
    }
    fn elect(&mut self, leader: usize, voters: &[usize]) -> Result {
        let now = self.now;
        let request = self
            .node(leader)?
            .campaign(now, 77)?
            .ok_or("campaign not due")?;
        self.event(
            "campaign",
            self.ids[leader],
            format!("{{\"candidate\":{},\"correlation\":77}}", self.ids[leader]),
            format!("{{\"encoded_request_hex\":{}}}", text(&hex(&request))),
        )?;
        for voter in voters {
            let reply = self.node(*voter)?.respond_controller(&request, now)?;
            self.event("vote_request",self.ids[*voter],format!("{{\"candidate\":{},\"voter\":{},\"correlation\":77,\"encoded_request_hex\":{}}}",self.ids[leader],self.ids[*voter],text(&hex(&request))),format!("{{\"response_hex\":{}}}",text(&hex(&reply))))?;
            let id = self.ids[*voter];
            let result = self.node(leader)?.receive_vote(id, 77, &reply, now)?;
            self.event(
                "vote_response",
                self.ids[leader],
                format!(
                    "{{\"candidate\":{},\"voter\":{id},\"correlation\":77,\"response_hex\":{}}}",
                    self.ids[leader],
                    text(&hex(&reply))
                ),
                format!("{{\"disposition\":{}}}", text(&format!("{result:?}"))),
            )?;
        }
        let term = self.node(leader)?.state().election.persistent.term;
        let index = self.node(leader)?.activate_leader(now)?;
        self.event("activate",self.ids[leader],format!("{{\"current_term\":{term}}}"),format!("{{\"barrier_index\":{index},\"barrier_kind\":1,\"barrier_payload_empty\":true,\"disposition\":\"accepted\"}}"))
    }
    fn propose(&mut self, leader: usize, payload: &[u8]) -> Result {
        let now = self.now;
        let index = self.node(leader)?.propose(payload, now)?;
        self.event(
            "propose",
            self.ids[leader],
            format!("{{\"payload_hex\":{}}}", text(&hex(payload))),
            format!("{{\"result_index\":{index}}}"),
        )
    }
    fn prepare(&mut self, leader: usize, follower: usize) -> Result<Request> {
        let peer = self.ids[follower];
        let now = self.now;
        let request = self.node(leader)?.prepare(peer, now)?;
        self.event(
            "prepare",
            self.ids[leader],
            format!("{{\"leader\":{},\"peer\":{peer}}}", self.ids[leader]),
            request_json(&request),
        )?;
        Ok(request)
    }
    fn receive(&mut self, follower: usize, request: &Request) -> Result<Response> {
        self.receive_with_origin(follower, request, None)
    }
    fn receive_with_origin(
        &mut self,
        follower: usize,
        request: &Request,
        origin: Option<usize>,
    ) -> Result<Response> {
        let now = self.now;
        let response = self.node(follower)?.receive(request, now)?;
        self.event(
            "receive",
            self.ids[follower],
            if let Some(source_ordinal) = origin {
                let mut args = request_json(request);
                args.pop();
                args.push_str(&format!(",\"input_origin\":{{\"type\":\"mutated_prepared_request\",\"source_ordinal\":{source_ordinal},\"changed_fields\":[\"peer\"],\"purpose\":\"stale epoch rejection\"}}}}"));
                args
            } else { request_json(request) },
            response_json(response),
        )?;
        Ok(response)
    }
    fn ack(
        &mut self,
        leader: usize,
        follower: usize,
        response: Response,
        accepted: bool,
    ) -> Result {
        let now = self.now;
        let peer = self.ids[follower];
        let result = self.node(leader)?.acknowledge(peer, response, now);
        let result_json = match result {
            Ok(commit) => {
                assert!(accepted);
                format!("{{\"disposition\":\"accepted\",\"committed_end\":{commit}}}")
            }
            Err(error) => {
                assert!(!accepted);
                format!(
                    "{{\"disposition\":\"rejected\",\"error\":{}}}",
                    text(&format!("{error:?}"))
                )
            }
        };
        self.event(
            "ack",
            self.ids[leader],
            response_json(response),
            result_json,
        )
    }
    fn exchange(&mut self, leader: usize, follower: usize) -> Result<bool> {
        let request = self.prepare(leader, follower)?;
        let response = self.receive(follower, &request)?;
        self.ack(leader, follower, response, true)?;
        Ok(response.success)
    }
    fn catch_up(&mut self, leader: usize, follower: usize) -> Result {
        for _ in 0..8 {
            if self.exchange(leader, follower)? {
                return Ok(());
            }
        }
        Err("history catch-up failed".into())
    }
    fn timeout(&mut self, leader: usize, follower: usize, sequence: u64) -> Result {
        let peer = self.ids[follower];
        self.node(leader)?.timeout_peer(peer, sequence)?;
        self.event(
            "timeout",
            self.ids[leader],
            format!("{{\"peer\":{peer},\"sequence\":{sequence}}}"),
            "{\"disposition\":\"accepted\"}".into(),
        )
    }
    fn finish_lines(&self) -> Result {
        write_file(
            self.output.join("events.jsonl"),
            self.events.join("\n") + "\n",
        )?;
        write_file(
            self.output.join("journal-receipts.jsonl"),
            self.receipts.join("\n") + "\n",
        )?;
        Ok(())
    }
}
fn run_child_history(root: PathBuf, output: PathBuf, count: u32) -> Result {
    let mut h = History::open(root, output, count, 0)?;
    for index in 0..count as usize {
        h.copy("initial", index)?;
    }
    h.event(
        "initial",
        0,
        "{}".into(),
        "{\"disposition\":\"ready\"}".into(),
    )?;
    h.now = 5;
    let majority = count as usize / 2 + 1;
    let followers: Vec<_> = (1..majority).collect();
    h.elect(0, &followers)?;
    h.propose(0, b"prefix-A")?;
    for follower in &followers {
        h.catch_up(0, *follower)?;
    }
    assert_eq!(h.node(0)?.state().committed_end, 2);
    h.now = 6;
    for follower in 1..count as usize {
        h.catch_up(0, follower)?;
        h.exchange(0, follower)?;
    }
    h.propose(0, b"orphan-metadata")?;
    // Last voter receives and syncs an orphan; its acknowledgment is lost.
    let last = count as usize - 1;
    let old_request_source_ordinal = h.events.len();
    let request = h.prepare(0, last)?;
    let lost = h.receive(last, &request)?;
    assert!(lost.success);
    h.event(
        "drop",
        h.ids[0],
        response_json(lost),
        "{\"reason\":\"lost acknowledgment after follower synchronization\"}".into(),
    )?;
    h.timeout(0, last, request.sequence)?;
    let retry = h.prepare(0, last)?;
    h.ack(0, last, lost, false)?;
    let mut forged = lost;
    forged.sequence = retry.sequence;
    forged.matched.index += 100;
    h.ack(0, last, forged, false)?;
    h.event(
        "drop",
        h.ids[0],
        request_json(&retry),
        "{\"reason\":\"minority partition prevents retry delivery\"}".into(),
    )?;
    h.timeout(0, last, retry.sequence)?;
    assert_eq!(h.node(0)?.state().committed_end, 2);
    h.now = 107;
    let now = h.now;
    let expired = h.node(0)?.poll(now)?;
    assert!(expired);
    h.event(
        "poll",
        h.ids[0],
        "{}".into(),
        "{\"quorumexpired\":true}".into(),
    )?;
    // Candidate2 has the committed prefix; for3 voters the orphan voter grants
    // only after candidate2 adopts its own higher election term (freshness denial
    // prevents a shorter candidate, so use a clean majority before the orphan).
    // A three-voter orphan on voter3 would block this election; discard its
    // uncommitted tail through the new candidate's own durable prefix instead.
    // Therefore candidate3, which has the orphan, wins for3; a new-term barrier
    // still commits that accepted entry rather than pretending it was truncated.
    let new_leader = if count == 3 { last } else { 1 };
    let voters: Vec<_> = (1..count as usize)
        .filter(|index| *index != new_leader)
        .take(majority - 1)
        .collect();
    h.elect(new_leader, &voters)?;
    h.propose(new_leader, b"prefix-B")?;
    for voter in &voters {
        h.catch_up(new_leader, *voter)?;
    }
    let expected = h.node(new_leader)?.state().committed_end;
    assert!(expected >= 4);
    // Stale old leader traffic is actually delivered and fenced by the new epoch.
    let mut stale_request = request.clone();
    stale_request.peer = h.ids[new_leader];
    let origin = (request.peer != stale_request.peer).then_some(old_request_source_ordinal);
    let stale = h.receive_with_origin(new_leader, &stale_request, origin)?;
    assert!(!stale.success);
    h.event(
        "drop",
        h.ids[new_leader],
        response_json(stale),
        "{\"reason\":\"old leader no longer has authority\"}".into(),
    )?;
    for follower in 0..count as usize {
        if follower != new_leader {
            h.catch_up(new_leader, follower)?;
            h.exchange(new_leader, follower)?;
        }
    }
    let expected_records = committed(h.node(new_leader)?)?;
    for index in 0..count as usize {
        assert_eq!(committed(h.node(index)?)?, expected_records);
    }
    // Controlled closed-file torn-tail fault, with before/after immutable copies.
    let receipt = h.copy("before-torn-tail", 0)?;
    h.nodes[0] = None;
    h.event(
        "crash",
        1,
        format!("{{\"checkpoint\":{receipt}}}"),
        "{\"disposition\":\"closed owner for fault injection\"}".into(),
    )?;
    use std::io::Write;
    let wal = h.root.join("1.wal");
    let mut file = fs::OpenOptions::new().append(true).open(&wal)?;
    file.write_all(b"PLENT")?;
    file.sync_all()?;
    drop(file);
    let fault_copy = h.output.join("torn-input.wal");
    copy_synced(&wal, &fault_copy)?;
    h.event("fault",1,"{\"type\":\"recognizable incomplete final Journal header\",\"file\":\"torn-input.wal\",\"bytecount\":5}".into(),"{\"disposition\":\"injected after owner close\"}".into())?;
    let mut reopened = Node::open(&wal, h.root.join("1.election"), config(1, &h.ids)?, h.now)?;
    assert_eq!(reopened.state().recovery.truncated_bytes, 5);
    assert!(matches!(
        reopened.propose(b"after-reopen", h.now),
        Err(Error::NotLeader)
    ));
    h.nodes[0] = Some(reopened);
    h.event(
        "reopen",
        1,
        "{\"input_wal\":\"torn-input.wal\"}".into(),
        "{\"truncated_bytes\":5,\"disposition\":\"ready\"}".into(),
    )?;
    for index in 0..count as usize {
        h.copy("before-abrupt-exit", index)?;
    }
    h.event(
        "process_exit",
        0,
        "{\"destructors\":false}".into(),
        "{\"disposition\":\"all confirmed journals copied before exit\"}".into(),
    )?;
    h.finish_lines()?;
    // The parent performs real reopening after process termination, without Drop.
    std::process::exit(0)
}
fn process_history(name: &str, count: u32) -> Result {
    if std::env::var("PL_REPLICATION_HISTORY_CHILD").as_deref() == Ok(name) {
        let root =
            PathBuf::from(std::env::var_os("PL_REPLICATION_HISTORY_ROOT").ok_or("missing root")?);
        let output = PathBuf::from(
            std::env::var_os("PL_REPLICATION_HISTORY_OUTPUT").ok_or("missing output")?,
        );
        return run_child_history(root, output, count);
    }
    let temp = Temp::new()?;
    let output = std::env::var_os("PL_REPLICATION_RESPONSE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| temp.0.join("artifacts"))
        .join(format!("history-{count}"));
    fs::create_dir_all(&output)?;
    let child = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", name, "--nocapture"])
        .env("PL_REPLICATION_HISTORY_CHILD", name)
        .env("PL_REPLICATION_HISTORY_ROOT", &temp.0)
        .env("PL_REPLICATION_HISTORY_OUTPUT", &output)
        .output()?;
    write_file(
        output.join("child.log"),
        [child.stdout, child.stderr].concat(),
    )?;
    assert!(
        child.status.success(),
        "history child failed: {}",
        read_text(output.join("child.log"))?
    );
    let events = read_text(output.join("events.jsonl"))?;
    let receipts = read_text(output.join("journal-receipts.jsonl"))?;
    let mut h = History::open(temp.0.clone(), output.clone(), count, 108)?;
    h.events = events.lines().map(str::to_owned).collect();
    h.receipts = receipts.lines().map(str::to_owned).collect();
    for index in 0..count as usize {
        assert_eq!(h.node(index)?.state().election.role, Role::Follower);
        assert_eq!(h.node(index)?.state().active_term, None);
        let receipt = h.copy("parent-after-abrupt-exit", index)?;
        h.event(
            "reopen",
            h.ids[index],
            format!("{{\"abrupt_process_exit\":true,\"checkpoint\":{receipt}}}"),
            "{\"truncated_bytes\":0,\"disposition\":\"ready\"}".into(),
        )?;
    }
    let first = committed(h.node(0)?)?;
    for index in 1..count as usize {
        assert_eq!(committed(h.node(index)?)?, first);
    }
    h.finish_lines()?;
    let source =
        std::env::var("PL_REPLICATION_SOURCE_SHA").unwrap_or_else(|_| "development-WORK".into());
    write_file(output.join("trace.json"),format!("{{\"schema_version\":1,\"source_sha\":{},\"seed\":7,\"group\":{{\"cluster_id\":\"replication-test\",\"topic\":\"__cluster_metadata\",\"partition\":0,\"voters\":{:?}}},\"events\":[{}],\"final_journals\":[{}]}}\n",text(&source),h.ids,h.events.join(","),h.receipts.join(",")))?;
    Ok(())
}
#[test]
fn actual_three_node_process_history() -> Result {
    process_history("actual_three_node_process_history", 3)
}
#[test]
fn actual_five_node_process_history() -> Result {
    process_history("actual_five_node_process_history", 5)
}

#[test]
fn wal_success_then_core_budget_failure_is_ambiguous_and_poisoned_until_reconciliation() -> Result {
    let temp = Temp::new()?;
    let mut c = config(1, &[1])?;
    c.controller.max_states = 4;
    let mut node = Node::open(temp.wal(1), temp.election(1), c, 0)?;
    node.campaign(5, 77)?;
    node.activate_leader(5)?;
    assert_eq!(node.state().committed_end, 1);
    assert!(node.propose(b"written-before-core-failure", 5).is_err());
    assert!(node.state().poisoned);
    assert!(!node.state().ready);
    assert_eq!(node.state().last_position.index, 2);
    assert_eq!(node.state().election.persistent.log.index, 1);
    assert!(matches!(
        node.fetch_committed(1, 2, 1024),
        Err(Error::Poisoned)
    ));
    assert!(matches!(node.campaign(10, 77), Err(Error::Poisoned)));
    drop(node);
    let node = open(&temp, 1, &[1], 0)?;
    assert!(node.state().ready);
    assert_eq!(node.state().last_position.index, 2);
    assert_eq!(node.state().committed_end, 1);
    assert_eq!(committed(&node)?.len(), 1);
    Ok(())
}
#[test]
fn changed_external_file_poison_prevents_reply_and_known_torn_tail_can_reopen() -> Result {
    let temp = Temp::new()?;
    let mut node = open(&temp, 1, &[1], 0)?;
    node.campaign(5, 77)?;
    node.activate_leader(5)?;
    use std::io::Write;
    let mut file = fs::OpenOptions::new().append(true).open(temp.wal(1))?;
    file.write_all(b"PLENT")?;
    file.sync_all()?;
    drop(file);
    assert!(matches!(
        node.propose(b"unconfirmed", 5),
        Err(Error::Storage(_))
    ));
    assert!(node.state().poisoned);
    assert!(matches!(node.propose(b"repeat", 5), Err(Error::Poisoned)));
    drop(node);
    let node = open(&temp, 1, &[1], 0)?;
    assert_eq!(node.state().recovery.truncated_bytes, 5);
    assert_eq!(node.state().last_position.index, 1);
    Ok(())
}

#[test]
fn checksummed_follower_commit_cannot_authorize_a_newer_entry_term() -> Result {
    let temp = Temp::new()?;
    let mut node = open(&temp, 2, &[1, 2, 3], 0)?;
    let request = Request {
        leader: 1,
        peer: 2,
        sequence: 1,
        term: 3,
        previous: LogPosition::default(),
        leader_commit: 0,
        entries: vec![Record::barrier(3, 1)?],
    };
    node.receive(&request, 0)?;
    drop(node);
    let (mut journal, _) = partitionline_broker::journal::Journal::open(
        temp.wal(2),
        0,
        partitionline_broker::journal::Limits::default(),
    )?;
    let mut marker = b"PLREPL01\x04\0\0\0\0\0\0\0".to_vec();
    marker.extend_from_slice(&1u64.to_be_bytes());
    marker.extend_from_slice(&2u64.to_be_bytes());
    marker.extend_from_slice(&1u32.to_be_bytes());
    marker.extend_from_slice(&[1, 0, 0, 0]);
    marker.extend_from_slice(&2u64.to_be_bytes());
    journal.append(1, &marker)?;
    drop(journal);
    if let Some(root) = std::env::var_os("PL_REPLICATION_RESPONSE_DIR") {
        let path = PathBuf::from(root).join("invalid-follower-commit");
        fs::create_dir_all(&path)?;
        copy_synced(&temp.wal(2), &path.join("metadata.wal"))?;
        copy_synced(&temp.election(2), &path.join("election.wal"))?;
        write_file(path.join("mutation.json"),"{\"type\":\"checksum-valid follower commit\",\"entry_term\":3,\"authorizing_term\":2,\"index\":1,\"authorizer\":1,\"sequence\":2,\"expected\":\"recovery rejection\"}\n")?;
    }
    assert!(
        open(&temp, 2, &[1, 2, 3], 0).is_err(),
        "lower-term follower commit exposed a newer entry"
    );
    Ok(())
}

#[test]
fn unowned_legacy_log_summary_is_not_silently_replaced_by_an_empty_new_wal() -> Result {
    let temp = Temp::new()?;
    let c = config(1, &[1, 2, 3])?;
    let mut controller = protocol::Controller::open(temp.election(1), c.controller.clone(), 0)?;
    controller.adopt_epoch(1, 0)?;
    controller.advance_log(0, 4)?;
    drop(controller);
    assert!(
        Node::open(temp.wal(1), temp.election(1), c, 0).is_err(),
        "unowned nonempty core summary was silently truncated"
    );
    Ok(())
}

#[test]
fn checksummed_authority_term_regression_fails_recovery_even_for_an_older_entry() -> Result {
    let temp = Temp::new()?;
    let mut node = open(&temp, 2, &[1, 2, 3], 0)?;
    let first = Request {
        leader: 1,
        peer: 2,
        sequence: 1,
        term: 3,
        previous: LogPosition::default(),
        leader_commit: 1,
        entries: vec![Record::barrier(2, 1)?],
    };
    node.receive(&first, 0)?;
    let second = Request {
        leader: 1,
        peer: 2,
        sequence: 2,
        term: 3,
        previous: LogPosition { term: 2, index: 1 },
        leader_commit: 1,
        entries: vec![Record {
            term: 2,
            index: 2,
            kind: RecordKind::Data,
            payload: b"older-entry".to_vec(),
        }],
    };
    node.receive(&second, 0)?;
    drop(node);
    let (mut journal, _) = partitionline_broker::journal::Journal::open(
        temp.wal(2),
        0,
        partitionline_broker::journal::Limits::default(),
    )?;
    let mut marker = b"PLREPL01\x04\0\0\0\0\0\0\0".to_vec();
    marker.extend_from_slice(&2u64.to_be_bytes());
    marker.extend_from_slice(&2u64.to_be_bytes());
    marker.extend_from_slice(&1u32.to_be_bytes());
    marker.extend_from_slice(&[1, 0, 0, 0]);
    marker.extend_from_slice(&3u64.to_be_bytes());
    journal.append(1, &marker)?;
    drop(journal);
    assert!(
        open(&temp, 2, &[1, 2, 3], 0).is_err(),
        "authority term regressed below a previously witnessed WAL term"
    );
    Ok(())
}
