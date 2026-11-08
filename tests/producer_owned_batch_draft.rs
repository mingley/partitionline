//! Isolated controls for the unwired ownership substrate.
//!
//! These tests do not exercise the public Kafka producer or a broker.

#![expect(clippy::expect_used, reason = "asserting fixture invariants")]

#[expect(dead_code, reason = "qualification-only substrate is intentionally unwired")]
#[expect(clippy::disallowed_types, reason = "isolated draft tests poisoned std mutex behavior and does not hold guards across awaits")]
#[path = "../src/producer/ownership_draft.rs"]
mod ownership;

use ownership::{BatchPhase, Identity, Limits, Owner, OwnerError, Partition};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn identity() -> Identity {
    Identity { producer_id: 17, producer_epoch: 3, generation: 9 }
}

fn partition(name: &str, number: i32) -> Partition {
    Partition { topic: Arc::from(name), incarnation: Some(7), partition: number }
}

fn admit(owner: &Owner, partition: &Partition, value: u8) -> ownership::RecordKey {
    owner
        .try_admit(
            partition.clone(),
            vec![value],
            Instant::now() + Duration::from_secs(60),
            None,
        )
        .expect("admit")
}

#[test]
fn zero_limits_are_unlimited_and_finite_limits_count_empty_records() {
    let p = partition("t", 0);
    let unlimited = Owner::new(identity(), Limits::new(0, 0, 0));
    for _ in 0..20 {
        let _record = unlimited
            .try_admit(p.clone(), vec![], Instant::now() + Duration::from_secs(1), None)
            .expect("zero limits are unlimited");
    }
    assert_eq!(unlimited.counts().expect("counts"), (20, 0));

    let bounded = Owner::new(identity(), Limits::new(1, 1, 1));
    let _record = bounded
        .try_admit(p.clone(), vec![], Instant::now() + Duration::from_secs(1), None)
        .expect("first empty record");
    let error = bounded
        .try_admit(p, vec![42], Instant::now() + Duration::from_secs(1), None)
        .expect_err("record credit is exhausted");
    assert_eq!((error.kind, error.payload), (OwnerError::Full, vec![42]));
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_worker_cannot_drop_or_split_the_rooted_batch() {
    let owner = Owner::new(identity(), Limits::new(8194, 8194, 1));
    let p = partition("t", 0);
    let members: Vec<_> = (0..8193).map(|_| admit(&owner, &p, b'x')).collect();
    let key = owner.seal_batch(&p, 8193, 8193, Instant::now(), 123).expect("seal");
    let snapshot = owner.snapshot(key).expect("snapshot");
    assert_eq!(snapshot.members, members);
    assert_eq!(snapshot.identity, identity());
    let later = admit(&owner, &p, b'y');
    let barrier = owner.flush_barrier().expect("barrier");
    let claim = owner.claim(key).expect("claim");
    let old_generation = claim.generation();
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        for member in snapshot.members.iter().take(8192) {
            let _observed = std::hint::black_box(member);
        }
        reached_tx.send(()).expect("receiver alive");
        std::future::pending::<()>().await;
        drop(claim);
    });
    reached_rx.await.expect("worker reached gate");
    task.abort();
    assert!(task.await.expect_err("worker canceled").is_cancelled());
    assert_eq!(owner.snapshot(key).expect("retained").phase, BatchPhase::Retiring);
    assert_eq!(owner.counts().expect("counts"), (8194, 8194));
    assert!(!owner.drained(barrier).expect("barrier"));
    assert!(matches!(owner.seal_batch(&p, 8193, 8193, Instant::now(), 456), Err(OwnerError::Busy)));
    assert_eq!(
        owner.confirm_stopped_after_join(key, old_generation).expect("stopped"),
        BatchPhase::Ready
    );
    let retry = owner.claim(key).expect("whole batch can be reclaimed");
    assert_ne!(retry.generation(), old_generation);
    let restored = owner.snapshot(key).expect("same batch");
    assert_eq!(restored.members, members);
    assert_eq!(restored.base_sequence, 0);
    assert_eq!(owner.with_record(members.first().copied().expect("first member"), |bytes, _, ts| (bytes.first().copied().expect("byte"), ts)).expect("record"), (b'x', Some(123)));
    assert_eq!(owner.with_record(later, |bytes, _, ts| (bytes.first().copied().expect("byte"), ts)).expect("later"), (b'y', None));
    drop(retry);
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_offered_attempt_remains_unknown_after_join_and_close() {
    let owner = Owner::new(identity(), Limits::new(2, 2, 1));
    let p = partition("t", 0);
    let first = admit(&owner, &p, b'a');
    let second = admit(&owner, &p, b'b');
    let batch = owner.seal_batch(&p, 2, 2, Instant::now(), 7).expect("seal");
    let claim = owner.claim(batch).expect("claim");
    let old_generation = claim.generation();
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _attempt = claim.offer().expect("offer");
        reached_tx.send(()).expect("receiver alive");
        std::future::pending::<()>().await;
        drop(claim);
    });
    reached_rx.await.expect("offered");
    task.abort();
    assert!(task.await.expect_err("worker canceled").is_cancelled());
    assert_eq!(owner.snapshot(batch).expect("snapshot").phase, BatchPhase::Unknown);
    assert_eq!(
        owner.confirm_stopped_after_join(batch, old_generation).expect("stopped"),
        BatchPhase::Unknown
    );
    assert!(matches!(owner.claim(batch), Err(OwnerError::Busy)));
    assert_eq!(owner.snapshot(batch).expect("whole").members, vec![first, second]);
    let close = owner.begin_close().expect("close");
    owner.stop_after_timeout().expect("stop");
    assert!(!owner.drained(close).expect("drained"));
    assert!(matches!(owner.claim(batch), Err(OwnerError::Closed)));
    assert_eq!(owner.counts().expect("retained"), (2, 2));
}

#[test]
fn stale_completed_claim_cannot_clear_a_new_partition_batch() {
    let owner = Owner::new(identity(), Limits::new(3, 3, 1));
    let p = partition("t", 0);
    let first = admit(&owner, &p, b'a');
    let old = owner.seal_batch(&p, 1, 1, Instant::now(), 1).expect("seal");
    let old_claim = owner.claim(old).expect("claim");
    let old_attempt = old_claim.offer().expect("offer");
    old_claim.acknowledge(old_attempt.clone()).expect("ack");
    owner.retire_after_publication(old).expect("retire");
    let second = admit(&owner, &p, b'b');
    let new = owner.seal_batch(&p, 1, 1, Instant::now(), 2).expect("new batch");
    let new_claim = owner.claim(new).expect("new claim");
    let _new_attempt = new_claim.offer().expect("new offer");
    assert_eq!(old_claim.acknowledge(old_attempt), Err(OwnerError::Stale));
    drop(old_claim);
    let third = admit(&owner, &p, b'c');
    assert!(matches!(owner.seal_batch(&p, 1, 1, Instant::now(), 3), Err(OwnerError::Busy)));
    assert_eq!(owner.snapshot(new).expect("new root").members, vec![second]);
    assert!(owner.with_record(first, |_, _, _| ()).is_err());
    assert!(owner.with_record(third, |_, _, _| ()).is_ok());
}

#[test]
fn equal_numeric_sequences_do_not_mix_partition_identities() {
    let owner = Owner::new(identity(), Limits::new(2, 2, 2));
    let a = partition("t", 0);
    let b = partition("t", 1);
    let first = admit(&owner, &a, b'a');
    let second = admit(&owner, &b, b'b');
    let ba = owner.seal_batch(&a, 1, 1, Instant::now(), 1).expect("batch a");
    let bb = owner.seal_batch(&b, 1, 1, Instant::now(), 1).expect("batch b");
    let sa = owner.snapshot(ba).expect("a");
    let sb = owner.snapshot(bb).expect("b");
    assert_ne!(sa.key, sb.key);
    assert_eq!((sa.base_sequence, sb.base_sequence), (0, 0));
    assert_eq!(sa.members, vec![first]);
    assert_eq!(sb.members, vec![second]);
    let ca = owner.claim(ba).expect("claim a");
    let cb = owner.claim(bb).expect("claim b");
    let aa = ca.offer().expect("offer a");
    let ab = cb.offer().expect("offer b");
    assert_eq!(ca.acknowledge(ab), Err(OwnerError::Stale));
    ca.acknowledge(aa).expect("correct a");
    assert_eq!(owner.snapshot(bb).expect("b still active").phase, BatchPhase::Offered);
}

#[test]
fn sequence_wrap_and_timestamp_are_derived_from_the_complete_batch() {
    let owner = Owner::new(identity(), Limits::new(3, 3, 1));
    let p = partition("t", 0);
    let records: Vec<_> = (0..3).map(|_| admit(&owner, &p, b'x')).collect();
    owner.set_next_sequence_for_test(&p, u32::try_from(i32::MAX).expect("positive sequence maximum") - 1).expect("seed");
    let key = owner.seal_batch(&p, 3, 3, Instant::now(), 77).expect("seal");
    let snapshot = owner.snapshot(key).expect("snapshot");
    assert_eq!(snapshot.members, records);
    assert_eq!((snapshot.sequence_at(0), snapshot.sequence_at(1), snapshot.sequence_at(2)),
               (Some(u32::try_from(i32::MAX).expect("positive sequence maximum") - 1), Some(u32::try_from(i32::MAX).expect("positive sequence maximum")), Some(0)));
    for record in records {
        assert_eq!(owner.with_record(record, |_, _, timestamp| timestamp).expect("record"), Some(77));
    }
}

#[test]
fn close_timeout_fences_preexisting_claims_and_keeps_records_rooted() {
    let owner = Owner::new(identity(), Limits::new(2, 2, 1));
    let p = partition("t", 0);
    let first = admit(&owner, &p, b'a');
    let batch = owner.seal_batch(&p, 1, 1, Instant::now(), 1).expect("seal");
    let claim = owner.claim(batch).expect("claim");
    let generation = claim.generation();
    let close = owner.begin_close().expect("close");
    let rejected = owner
        .try_admit(p.clone(), vec![b'b'], Instant::now() + Duration::from_secs(1), None)
        .expect_err("closed");
    assert_eq!((rejected.kind, rejected.payload), (OwnerError::Closed, vec![b'b']));
    owner.stop_after_timeout().expect("stop");
    assert_eq!(claim.offer(), Err(OwnerError::Stale));
    drop(claim);
    assert_eq!(owner.confirm_stopped_after_join(batch, generation).expect("stop"), BatchPhase::Ready);
    assert!(matches!(owner.claim(batch), Err(OwnerError::Closed)));
    assert!(!owner.drained(close).expect("drained"));
    assert_eq!(owner.with_record(first, |bytes, _, _| bytes.first().copied().expect("byte")).expect("first"), b'a');
}

#[test]
fn claim_drop_does_not_wait_for_the_root_mutex() {
    use std::sync::mpsc;
    use std::thread;

    let owner = Owner::new(identity(), Limits::new(1, 1, 1));
    let p = partition("t", 0);
    let record = admit(&owner, &p, b'a');
    let batch = owner.seal_batch(&p, 1, 1, Instant::now(), 1).expect("seal");
    let claim = owner.claim(batch).expect("claim");
    let (locked_tx, locked_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let clone = owner.clone();
    let lock_holder = thread::spawn(move || {
        clone.with_record(record, |_, _, _| {
            locked_tx.send(()).expect("notify lock");
            release_rx.recv().expect("release lock");
        }).expect("held record");
    });
    locked_rx.recv_timeout(Duration::from_secs(2)).expect("lock held");
    let (done_tx, done_rx) = mpsc::channel();
    let dropper = thread::spawn(move || {
        drop(claim);
        done_tx.send(()).expect("drop complete");
    });
    let dropped = done_rx.recv_timeout(Duration::from_secs(2));
    release_tx.send(()).expect("release");
    lock_holder.join().expect("holder");
    dropper.join().expect("dropper");
    dropped.expect("drop did not wait for root lock");
    assert_eq!(owner.snapshot(batch).expect("batch").phase, BatchPhase::Retiring);
}

#[test]
fn admission_and_close_share_a_cutoff_commit_point() {
    use std::sync::Barrier as ThreadBarrier;
    use std::thread;

    let owner = Owner::new(identity(), Limits::new(32, 32, 1));
    let p = partition("t", 0);
    let start = Arc::new(ThreadBarrier::new(9));
    let mut joins = Vec::new();
    for _ in 0..8 {
        let owner = owner.clone();
        let p = p.clone();
        let start = Arc::clone(&start);
        joins.push(thread::spawn(move || {
            let _arrival = start.wait();
            owner.try_admit(p, vec![1], Instant::now() + Duration::from_secs(5), None)
        }));
    }
    let _arrival = start.wait();
    let close = owner.begin_close().expect("close");
    let accepted = joins.into_iter().filter_map(|join| join.join().expect("thread").ok()).count();
    assert_eq!(owner.counts().expect("count").0, accepted);
    assert_eq!(owner.drained(close).expect("barrier"), accepted == 0);
    let error = owner
        .try_admit(p, vec![1], Instant::now() + Duration::from_secs(1), None)
        .expect_err("closed after seal");
    assert_eq!(error.kind, OwnerError::Closed);
}

#[test]
fn expired_member_cannot_be_silently_selected() {
    let owner = Owner::new(identity(), Limits::new(2, 2, 1));
    let p = partition("t", 0);
    let _record = owner.try_admit(p.clone(), vec![1], Instant::now(), None).expect("admit");
    let later = admit(&owner, &p, 2);
    assert!(matches!(owner.seal_batch(&p, 2, 2, Instant::now(), 1), Err(OwnerError::Expired)));
    assert_eq!(owner.counts().expect("retained"), (2, 2));
    assert!(owner.with_record(later, |_, _, _| ()).is_ok());
}

#[test]
fn no_new_offer_is_granted_after_a_member_deadline() {
    let owner = Owner::new(identity(), Limits::new(2, 2, 1));
    let p = partition("deadline", 0);
    let now = Instant::now();
    let deadline = now + Duration::from_millis(10);
    let _record = owner.try_admit(p.clone(), vec![1], deadline, None).expect("admit");
    let batch = owner.seal_batch(&p, 0, 0, now, 123).expect("zero batch limits are unlimited");
    let claim = owner.claim(batch).expect("claim");
    assert_eq!(claim.offer_at(deadline), Err(OwnerError::Expired));
    assert_eq!(owner.snapshot(batch).expect("snapshot").phase, BatchPhase::Claimed);
    drop(claim);
    assert_eq!(owner.snapshot(batch).expect("snapshot").phase, BatchPhase::Retiring);
    assert_eq!(owner.counts().expect("counts"), (1, 1));
}

#[test]
fn current_prototype_treats_late_ack_as_unknown() {
    let owner = Owner::new(identity(), Limits::new(2, 2, 1));
    let p = partition("deadline", 0);
    let now = Instant::now();
    let deadline = now + Duration::from_millis(10);
    let _record = owner.try_admit(p.clone(), vec![1], deadline, None).expect("admit");
    let batch = owner.seal_batch(&p, 1, 1, now, 123).expect("seal");
    let claim = owner.claim(batch).expect("claim");
    let attempt = claim.offer_at(now + Duration::from_millis(1)).expect("offer");
    assert_eq!(claim.acknowledge_at(attempt, deadline), Err(OwnerError::Expired));
    assert_eq!(owner.snapshot(batch).expect("snapshot").phase, BatchPhase::Unknown);
    assert_eq!(owner.counts().expect("counts"), (1, 1));
}

#[test]
fn foreign_owner_attempt_cannot_settle_claim() {
    let p = partition("cross-owner", 0);
    let now = Instant::now();
    let a = Owner::new(identity(), Limits::new(1, 1, 1));
    let mut other_identity = identity();
    other_identity.producer_id += 1;
    let b = Owner::new(other_identity, Limits::new(1, 1, 1));
    let _a_record = a.try_admit(p.clone(), vec![1], now + Duration::from_secs(60), None).expect("a admit");
    let _b_record = b.try_admit(p.clone(), vec![2], now + Duration::from_secs(60), None).expect("b admit");
    let a_batch = a.seal_batch(&p, 1, 1, now, 1).expect("a seal");
    let b_batch = b.seal_batch(&p, 1, 1, now, 1).expect("b seal");
    let a_claim = a.claim(a_batch).expect("a claim");
    let b_claim = b.claim(b_batch).expect("b claim");
    let _a_attempt = a_claim.offer_at(now).expect("a offer");
    let b_attempt = b_claim.offer_at(now).expect("b offer");
    let outcome = a_claim.acknowledge_at(b_attempt, now);
    assert!(matches!(outcome, Err(OwnerError::Stale)), "FOREIGN_ATTEMPT_ACCEPTED: {outcome:?}");
}

#[test]
#[ignore = "Known counterexample: settlement is conflated with user deadline"]
fn counterexample_definitive_late_ack_must_settle_protocol() {
    let p = partition("late-ack", 0);
    let now = Instant::now();
    let deadline = now + Duration::from_millis(10);
    let owner = Owner::new(identity(), Limits::new(1, 1, 1));
    let _record = owner.try_admit(p.clone(), vec![1], deadline, None).expect("admit");
    let batch = owner.seal_batch(&p, 1, 1, now, 1).expect("seal");
    let claim = owner.claim(batch).expect("claim");
    let attempt = claim.offer_at(now).expect("offer");
    let outcome = claim.acknowledge_at(attempt, deadline);
    assert!(outcome.is_ok(), "DEFINITIVE_ACK_LOST: {outcome:?}");
}

#[test]
#[ignore = "Known counterexample: an out-of-budget expired record blocks the fitting prefix"]
fn counterexample_expired_outside_limit_must_not_block_prefix() {
    let p = partition("prefix", 0);
    let now = Instant::now();
    let owner = Owner::new(identity(), Limits::new(2, 2, 1));
    let _live = owner.try_admit(p.clone(), vec![1], now + Duration::from_secs(60), None).expect("live");
    let _expired = owner.try_admit(p.clone(), vec![2], now, None).expect("expired");
    let outcome = owner.seal_batch(&p, 2, 1, now, 1);
    assert!(outcome.is_ok(), "EXPIRED_SUFFIX_BLOCKED_PREFIX: {outcome:?}");
}

#[test]
fn claim_and_attempt_keep_root_alive_until_both_drop() {
    let p = partition("root-lifetime", 0);
    let now = Instant::now();
    let owner = Owner::new(identity(), Limits::new(1, 1, 1));
    let _record = owner.try_admit(p.clone(), vec![1], now + Duration::from_secs(60), None).expect("admit");
    let batch = owner.seal_batch(&p, 1, 1, now, 1).expect("seal");
    let claim = owner.claim(batch).expect("claim");
    let root_alive = owner.root_lifetime_probe();
    let attempt = claim.offer_at(now).expect("offer");
    drop(owner);
    assert!(root_alive(), "claim and attempt retain the root");
    drop(claim);
    assert!(root_alive(), "attempt retains the root after claim is dropped");
    drop(attempt);
    assert!(!root_alive(), "the final attempt releases the root");
}

#[test]
fn successful_retirement_drops_observed_record_after_unlocking_root() {
    use std::sync::mpsc;
    use std::thread;

    let owner = Owner::new(identity(), Limits::new(1, 1, 1));
    let p = partition("reclaim", 0);
    let now = Instant::now();
    let record = owner.try_admit(p.clone(), vec![1], now + Duration::from_secs(60), None).expect("admit");
    let batch = owner.seal_batch(&p, 1, 1, now, 1).expect("seal");
    let claim = owner.claim(batch).expect("claim");
    let attempt = claim.offer_at(now).expect("offer");
    claim.acknowledge_at(attempt, now).expect("ack");

    let (send, receive) = mpsc::channel();
    let observed_owner = owner.clone();
    owner.install_drop_probe(record, move || {
        let counts = observed_owner.counts();
        send.send(counts).expect("probe receiver alive");
    }).expect("install probe");
    let retiring_owner = owner.clone();
    let task = thread::spawn(move || retiring_owner.retire_after_publication(batch));
    let observed = receive.recv_timeout(Duration::from_secs(2)).expect("record destructor must access unlocked root");
    assert_eq!(observed.expect("counts"), (0, 0));
    task.join().expect("retirement thread").expect("retire");
}

#[test]
#[ignore = "Known counterexample: bare barriers are not scoped to their root"]
fn counterexample_foreign_barrier_must_not_report_drained() {
    let p = partition("barrier", 0);
    let now = Instant::now();
    let a = Owner::new(identity(), Limits::new(1, 1, 1));
    let b = Owner::new(identity(), Limits::new(1, 1, 1));
    let _record = a.try_admit(p, vec![1], now + Duration::from_secs(60), None).expect("admit");
    let foreign = b.flush_barrier().expect("empty foreign barrier");
    let result = a.drained(foreign);
    assert!(matches!(result, Err(OwnerError::Stale)), "FOREIGN_BARRIER_ACCEPTED: {result:?}");
}

#[test]
#[ignore = "Known counterexample: unknown writes cannot settle from an exact valid response"]
fn counterexample_valid_ack_after_timeout_must_settle_protocol() {
    let p = partition("unknown-ack", 0);
    let now = Instant::now();
    let owner = Owner::new(identity(), Limits::new(1, 1, 1));
    let _record = owner.try_admit(p.clone(), vec![1], now + Duration::from_secs(60), None).expect("admit");
    let batch = owner.seal_batch(&p, 1, 1, now, 1).expect("seal");
    let claim = owner.claim(batch).expect("claim");
    let attempt = claim.offer_at(now).expect("offer");
    let _close = owner.begin_close().expect("close");
    owner.stop_after_timeout().expect("stop");
    let result = claim.acknowledge_at(attempt, now);
    assert!(result.is_ok(), "UNKNOWN_VALID_ACK_REJECTED: {result:?}");
}

#[test]
#[ignore = "Known counterexample: final Arc drop can synchronously run a record destructor"]
fn counterexample_claim_drop_must_not_run_blocking_final_reclamation() {
    use std::sync::mpsc;
    use std::thread;

    let owner = Owner::new(identity(), Limits::new(1, 1, 1));
    let p = partition("drop", 0);
    let now = Instant::now();
    let record = owner.try_admit(p.clone(), vec![1], now + Duration::from_secs(60), None).expect("admit");
    let batch = owner.seal_batch(&p, 1, 1, now, 1).expect("seal");
    let claim = owner.claim(batch).expect("claim");
    let (started_send, started_receive) = mpsc::channel();
    let (release_send, release_receive) = mpsc::channel();
    owner.install_drop_probe(record, move || {
        started_send.send(()).expect("observer exists");
        release_receive.recv().expect("release destructor");
    }).expect("probe");
    drop(owner);

    let (done_send, done_receive) = mpsc::channel();
    let task = thread::spawn(move || {
        drop(claim);
        done_send.send(()).expect("done receiver exists");
    });
    started_receive.recv_timeout(Duration::from_secs(2)).expect("destructor reached");
    let completed_without_release = done_receive.recv_timeout(Duration::from_millis(100)).is_ok();
    release_send.send(()).expect("release");
    task.join().expect("claim drop thread");
    assert!(completed_without_release, "LAST_ARC_DROP_BLOCKED_WORKER");
}

#[test]
#[ignore = "Known counterexample: reclamation discharges credits and barriers before destructors finish"]
fn counterexample_reclaim_must_keep_credits_and_barrier() {
    use std::sync::mpsc;
    use std::thread;

    let owner = Owner::new(identity(), Limits::new(2, 2, 1));
    let p = partition("reclaim-fence", 0);
    let now = Instant::now();
    let first = owner.try_admit(p.clone(), vec![1], now + Duration::from_secs(60), None).expect("first");
    let second = owner.try_admit(p.clone(), vec![2], now + Duration::from_secs(60), None).expect("second");
    let barrier = owner.flush_barrier().expect("barrier");
    let batch = owner.seal_batch(&p, 2, 2, now, 1).expect("seal");
    let claim = owner.claim(batch).expect("claim");
    let attempt = claim.offer_at(now).expect("offer");
    claim.acknowledge_at(attempt, now).expect("ack");

    let (reached_send, reached_receive) = mpsc::channel();
    let (release_first_send, release_first_receive) = mpsc::channel();
    let (release_second_send, release_second_receive) = mpsc::channel();
    let first_reached = reached_send.clone();
    owner.install_drop_probe(first, move || {
        first_reached.send(()).expect("observer");
        release_first_receive.recv().expect("release first");
    }).expect("first probe");
    owner.install_drop_probe(second, move || {
        reached_send.send(()).expect("observer");
        release_second_receive.recv().expect("release second");
    }).expect("second probe");
    let retiring = owner.clone();
    let task = thread::spawn(move || retiring.retire_after_publication(batch));
    reached_receive.recv_timeout(Duration::from_secs(2)).expect("one destructor started");
    let admitted = owner.try_admit(p, vec![3, 4], now + Duration::from_secs(60), None).is_ok();
    let drained = owner.drained(barrier).expect("barrier state");
    release_first_send.send(()).expect("release first");
    release_second_send.send(()).expect("release second");
    task.join().expect("retirement thread").expect("retirement");
    assert!(!admitted && !drained, "RECLAIM_RELEASED_EARLY: admitted={admitted}, drained={drained}");
}
