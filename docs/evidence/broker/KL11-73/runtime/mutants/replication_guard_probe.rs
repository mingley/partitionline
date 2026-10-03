//! Evidence-local compiled public-API probes; never installed in the repository tests.
use partitionline_broker::raft::{
    election::{LogPosition, Timeouts},
    protocol,
    replication::{Config, Limits, Node, Record, RecordKind, Request},
};
use std::{fs, path::PathBuf};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
fn nodes(name: &str, count: u32, small: bool) -> Result<Vec<Node>> {
    let root = PathBuf::from(
        std::env::var_os("PL_REPLICATION_GUARD_DIR").ok_or("capture directory required")?,
    )
    .join(name);
    fs::create_dir_all(&root)?;
    let ids: Vec<_> = (1..=count).collect();
    ids.iter()
        .map(|id| {
            let mut controller = protocol::Config::new(*id, ids.clone(), "guard-probe".into())?;
            controller.election_timeouts = Timeouts::new(5, 5)?;
            let mut config = Config::new(controller);
            config.max_queued_requests = 2;
            if small {
                config.limits = Limits::new(256, 1024, 32, 8192, 256, 65536, 1024)?;
            }
            Ok(Node::open(
                root.join(format!("{id}.wal")),
                root.join(format!("{id}.election")),
                config,
                0,
            )?)
        })
        .collect()
}
fn elect(nodes: &mut [Node], leader: usize, voters: &[usize], now: u64) -> Result {
    let request = nodes[leader].campaign(now, 77)?.ok_or("campaign not due")?;
    for voter in voters {
        let reply = nodes[*voter].respond_controller(&request, now)?;
        let id = nodes[*voter].state().local_id;
        nodes[leader].receive_vote(id, 77, &reply, now)?;
    }
    nodes[leader].activate_leader(now)?;
    Ok(())
}
fn round(nodes: &mut [Node], leader: usize, follower: usize, now: u64) -> Result<bool> {
    let peer = nodes[follower].state().local_id;
    let request = nodes[leader].prepare(peer, now)?;
    let reply = nodes[follower].receive(&request, now)?;
    nodes[leader].acknowledge(peer, reply, now)?;
    Ok(reply.success)
}
#[test]
fn exact_sent_target_cannot_be_forged_inside_the_local_durable_tail() -> Result {
    let mut nodes = nodes("exact-target", 3, true)?;
    elect(&mut nodes, 0, &[1], 5)?;
    for _ in 0..5 {
        nodes[0].propose(&[7; 200], 5)?;
    }
    assert!(!round(&mut nodes, 0, 1, 5)?);
    let request = nodes[0].prepare(2, 5)?;
    let mut response = nodes[1].receive(&request, 5)?;
    assert!(response.success);
    assert!(response.matched.index < nodes[0].state().last_position.index);
    let authentic = response;
    response.matched.index += 1;
    println!(
        "actual sent end={}, follower durable end={}, forged end={}, local durable end={}",
        authentic.matched.index,
        nodes[1].state().last_position.index,
        response.matched.index,
        nodes[0].state().last_position.index
    );
    assert!(
        nodes[0].acknowledge(2, response, 5).is_err(),
        "forged unsent end accepted inside local durable tail"
    );
    assert_eq!(nodes[0].state().committed_end, 0);
    nodes[0].acknowledge(2, authentic, 5)?;
    assert_eq!(nodes[0].state().committed_end, authentic.matched.index);
    Ok(())
}
#[test]
fn five_voters_cannot_commit_with_only_one_remote_durable_match() -> Result {
    let mut nodes = nodes("distinct-majority", 5, false)?;
    elect(&mut nodes, 0, &[1, 2], 5)?;
    nodes[0].propose(b"not-a-majority", 5)?;
    assert!(!round(&mut nodes, 0, 1, 5)?);
    assert!(round(&mut nodes, 0, 1, 5)?);
    println!(
        "one remote match; leader commit={}, local tail={}",
        nodes[0].state().committed_end,
        nodes[0].state().last_position.index
    );
    assert_eq!(
        nodes[0].state().committed_end,
        0,
        "two of five durable positions committed"
    );
    Ok(())
}
#[test]
fn a_new_leader_cannot_commit_an_old_term_partial_chunk_before_its_barrier_matches() -> Result {
    let mut nodes = nodes("current-term", 5, true)?;
    elect(&mut nodes, 0, &[1, 2], 5)?;
    for _ in 0..5 {
        nodes[0].propose(&[9; 200], 5)?;
    }
    assert!(!round(&mut nodes, 0, 1, 5)?);
    assert!(round(&mut nodes, 0, 1, 5)?);
    assert!(round(&mut nodes, 0, 1, 5)?);
    assert_eq!(nodes[0].state().committed_end, 0);
    assert_eq!(nodes[1].state().last_position.index, 6);
    elect(&mut nodes, 1, &[2, 3], 11)?;
    assert_eq!(nodes[1].state().last_position.index, 7);
    for follower in [4, 2] {
        assert!(!round(&mut nodes, 1, follower, 11)?);
        assert!(round(&mut nodes, 1, follower, 11)?);
        assert_eq!(
            nodes[follower].state().last_position,
            LogPosition { term: 2, index: 4 }
        );
    }
    println!(
        "new term={}, barrier index7; durable majority matches old term2/index4; leader commit={}",
        nodes[1].state().election.persistent.term,
        nodes[1].state().committed_end
    );
    assert_eq!(
        nodes[1].state().committed_end,
        0,
        "old-term partial majority committed before barrier"
    );
    Ok(())
}
#[test]
fn a_follower_cannot_commit_beyond_this_requests_verified_prefix() -> Result {
    let mut nodes = nodes("matched-end", 3, false)?;
    let first = Request {
        leader: 1,
        peer: 2,
        sequence: 1,
        term: 2,
        previous: LogPosition::default(),
        leader_commit: 0,
        entries: vec![
            Record::barrier(2, 1)?,
            Record {
                term: 2,
                index: 2,
                kind: RecordKind::Data,
                payload: b"unverified-old-suffix".to_vec(),
            },
        ],
    };
    nodes[1].receive(&first, 0)?;
    let next = Request {
        leader: 3,
        peer: 2,
        sequence: 2,
        term: 3,
        previous: LogPosition { term: 2, index: 1 },
        leader_commit: 2,
        entries: vec![],
    };
    nodes[1].receive(&next, 1)?;
    println!(
        "verified end1; old local tail2; claimed leader commit2; follower commit={}",
        nodes[1].state().committed_end
    );
    assert_eq!(
        nodes[1].state().committed_end,
        1,
        "old unmatched suffix became committed"
    );
    Ok(())
}
