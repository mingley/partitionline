// Cooperative and static group histories.
//
// Ownership never overlaps. An offset is stored only after the member has
// been delivered that partition. Failed joins, undelivered commits, and stale
// generations are retained in the history. Reopen is the coordinator crash.
// Session expiry is the member crash.

use partitionline_broker::groups::lifecycle::{self, Lifecycle};
use partitionline_broker::groups::offsets::{self, Offsets};
use partitionline_broker::journal;
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

fn journal_limits() -> journal::Limits {
    journal::Limits::new(8192, 1 << 20, 100, 1 << 20).unwrap_or_else(|_| panic!("journal limits"))
}

fn life_limits() -> lifecycle::Limits {
    lifecycle::Limits::new(8, 1_000, 1_000, 32, journal_limits())
        .unwrap_or_else(|_| panic!("lifecycle limits"))
}

fn offset_limits() -> offsets::Limits {
    offsets::Limits::new(4, 32, 32, journal_limits()).unwrap_or_else(|_| panic!("offset limits"))
}

fn path(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("pl-group-hist-{name}-{nanos}"))
}

fn disjoint(life: &Lifecycle, ids: &[&str]) {
    let mut seen = BTreeSet::new();
    for id in ids {
        if let Some(owned) = life.owned(id) {
            for partition in owned {
                assert!(seen.insert(*partition), "partition {partition} has two owners");
            }
        }
    }
}

fn commit_processed(
    life: &Lifecycle,
    offsets: &mut Offsets,
    member: &str,
    partition: i32,
    offset: i64,
    failures: &mut Vec<&'static str>,
) {
    if life.commit(member, partition).is_err() {
        failures.push("undelivered-commit");
        return;
    }
    offsets
        .commit("orders", member, 1, &[("orders", partition, offset, "")])
        .unwrap_or_else(|error| panic!("{error:?}"));
}

#[test]
fn cooperative_and_static_histories_keep_disjoint_ownership() {
    let life_path = path("life");
    let offset_path = path("offsets");
    let mut life = Lifecycle::open(&life_path, life_limits()).unwrap_or_else(|error| panic!("{error:?}"));
    let mut offsets = Offsets::open(&offset_path, offset_limits()).unwrap_or_else(|error| panic!("{error:?}"));
    let mut failures = Vec::new();

    life.join("alice", None, &[0, 1], 0)
        .unwrap_or_else(|error| panic!("{error:?}"));
    match life.join("bob", None, &[1, 2], 0) {
        Err(lifecycle::Error::InvalidRequest) => failures.push("overlap-join"),
        Ok(()) => panic!("overlapping join was stored"),
        Err(error) => panic!("{error:?}"),
    }
    assert!(life.owned("bob").is_none());
    life.join("bob", None, &[2, 3], 0)
        .unwrap_or_else(|error| panic!("{error:?}"));
    disjoint(&life, &["alice", "bob"]);

    commit_processed(&life, &mut offsets, "alice", 1, 5, &mut failures);
    assert!(offsets.fetch("orders", "orders", 1).is_none());
    life.mark_delivered("alice", 0)
        .unwrap_or_else(|error| panic!("{error:?}"));
    commit_processed(&life, &mut offsets, "alice", 0, 10, &mut failures);
    match offsets.commit("orders", "alice", 9, &[("orders", 0, 99, "")]) {
        Err(offsets::Error::IllegalGeneration) => failures.push("stale-generation"),
        Ok(_) => panic!("stale generation was stored"),
        Err(error) => panic!("{error:?}"),
    }
    assert_eq!(
        offsets
            .fetch("orders", "orders", 0)
            .map(|committed| committed.offset),
        Some(10)
    );

    life.revoke("alice", &[1], 0)
        .unwrap_or_else(|error| panic!("{error:?}"));
    life.leave("bob").unwrap_or_else(|error| panic!("{error:?}"));
    life.join("bob", None, &[1, 2, 3], 10)
        .unwrap_or_else(|error| panic!("{error:?}"));
    disjoint(&life, &["alice", "bob"]);
    life.mark_delivered("bob", 1)
        .unwrap_or_else(|error| panic!("{error:?}"));
    commit_processed(&life, &mut offsets, "bob", 1, 20, &mut failures);

    life.join("carol", Some("i-1"), &[4], 10)
        .unwrap_or_else(|error| panic!("{error:?}"));
    life.join("carol2", Some("i-1"), &[4], 20)
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert!(life.owned("carol").is_none());
    disjoint(&life, &["alice", "bob", "carol2"]);

    drop(life);
    drop(offsets);
    let mut life = Lifecycle::open(&life_path, life_limits()).unwrap_or_else(|error| panic!("{error:?}"));
    let offsets = Offsets::open(&offset_path, offset_limits()).unwrap_or_else(|error| panic!("{error:?}"));
    disjoint(&life, &["alice", "bob", "carol2"]);
    assert_eq!(
        offsets
            .fetch("orders", "orders", 0)
            .map(|committed| committed.offset),
        Some(10)
    );
    assert_eq!(
        offsets
            .fetch("orders", "orders", 1)
            .map(|committed| committed.offset),
        Some(20)
    );
    assert!(offsets.fetch("orders", "orders", 2).is_none());

    life.heartbeat("bob", 500)
        .unwrap_or_else(|error| panic!("{error:?}"));
    life.heartbeat("carol2", 500)
        .unwrap_or_else(|error| panic!("{error:?}"));
    let expired = life.expire(1_001).unwrap_or_else(|error| panic!("{error:?}"));
    assert!(expired.iter().any(|id| id == "alice"));
    failures.push("member-crash");
    assert!(life.owned("alice").is_none());
    disjoint(&life, &["bob", "carol2"]);
    assert_eq!(
        offsets
            .fetch("orders", "orders", 0)
            .map(|committed| committed.offset),
        Some(10)
    );
    assert_eq!(
        failures,
        vec![
            "overlap-join",
            "undelivered-commit",
            "stale-generation",
            "member-crash",
        ]
    );
}
