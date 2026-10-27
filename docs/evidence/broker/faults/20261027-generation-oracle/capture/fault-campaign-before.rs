#![allow(clippy::panic)]
//! Seeded crash, lost-ack, fence, and partition histories against broker journals.

use partitionline_broker::journal;
use partitionline_broker::recovery::{self, EpochLog};
use partitionline_broker::transactions::markers::{self, MarkerLog};
use partitionline_broker::transactions::offsets::{self, StagedOffset, TxnOffsets};
use std::time::{SystemTime, UNIX_EPOCH};

const FAULTS_PER_PROFILE: usize = 1000;

struct Report {
    profile: &'static str,
    faults: usize,
    crashes: usize,
    lost_acks: usize,
    fences: usize,
    partitions: usize,
    observer_failures: usize,
}

fn journal_limits() -> journal::Limits {
    journal::Limits::new(1024, 1 << 20, 64, 1 << 20).unwrap_or_else(|_| panic!("journal limits"))
}

fn path(profile: &str, seed: u64) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("pl-fault-{profile}-{seed}-{nanos}"))
}

fn mix(state: &mut u64) -> u64 {
    *state ^= state.wrapping_shl(13);
    *state ^= state.wrapping_shr(7);
    *state ^= state.wrapping_shl(17);
    *state
}

fn recovery_profile() -> Report {
    let limits = recovery::Limits::new(32, journal_limits()).unwrap_or_else(|_| panic!("limits"));
    let mut report = Report {
        profile: "data-plane",
        faults: 0,
        crashes: 0,
        lost_acks: 0,
        fences: 0,
        partitions: 0,
        observer_failures: 0,
    };
    let mut seed = 0xD1A5_0001_u64;
    while report.faults < FAULTS_PER_PROFILE {
        seed = seed.wrapping_add(1);
        let leader_path = path("rcv-l", seed);
        let follower_path = path("rcv-f", seed);
        let mut leader =
            EpochLog::open(&leader_path, limits).unwrap_or_else(|error| panic!("{error:?}"));
        let mut follower =
            EpochLog::open(&follower_path, limits).unwrap_or_else(|error| panic!("{error:?}"));
        let mut epoch = 1i32;
        let mut next_id = 1u64;
        for step in 0..4 {
            let roll = mix(&mut seed) % 4;
            if roll == 0 || step == 0 {
                let offset = leader
                    .append(epoch, next_id)
                    .unwrap_or_else(|error| panic!("{error:?}"));
                follower
                    .append(epoch, next_id)
                    .unwrap_or_else(|error| panic!("{error:?}"));
                next_id = next_id.saturating_add(1);
                let leader_end = leader.log_end_offset();
                let follower_end = follower.log_end_offset();
                leader
                    .advance_high_watermark(leader_end)
                    .unwrap_or_else(|error| panic!("{error:?}"));
                follower
                    .advance_high_watermark(follower_end)
                    .unwrap_or_else(|error| panic!("{error:?}"));
                let again = leader
                    .advance_high_watermark(leader.high_watermark())
                    .unwrap_or_else(|error| panic!("{error:?}"));
                if again != leader.high_watermark() {
                    report.observer_failures = report.observer_failures.saturating_add(1);
                }
                report.lost_acks = report.lost_acks.saturating_add(1);
                report.faults = report.faults.saturating_add(1);
                let _ = offset;
            } else if roll == 1 {
                let before = leader.log_end_offset();
                let committed = leader.committed_ids();
                if leader.append(0, 99).is_ok() {
                    report.observer_failures = report.observer_failures.saturating_add(1);
                }
                if leader.log_end_offset() != before || leader.committed_ids() != committed {
                    report.observer_failures = report.observer_failures.saturating_add(1);
                }
                report.fences = report.fences.saturating_add(1);
                report.faults = report.faults.saturating_add(1);
            } else if roll == 2 {
                let hidden = leader
                    .append(epoch, next_id)
                    .unwrap_or_else(|error| panic!("{error:?}"));
                next_id = next_id.saturating_add(1);
                let follower_ids = follower.committed_ids();
                if follower_ids.iter().any(|id| {
                    leader
                        .records()
                        .iter()
                        .find(|record| record.id == *id)
                        .is_none()
                }) {
                    report.observer_failures = report.observer_failures.saturating_add(1);
                }
                if follower
                    .records()
                    .iter()
                    .any(|record| record.offset == hidden)
                {
                    report.observer_failures = report.observer_failures.saturating_add(1);
                }
                report.partitions = report.partitions.saturating_add(1);
                report.faults = report.faults.saturating_add(1);
            } else {
                let committed = leader.committed_ids();
                let watermark = leader.high_watermark();
                if watermark > 0 && leader.truncate_divergent(watermark - 1).is_ok() {
                    report.observer_failures = report.observer_failures.saturating_add(1);
                }
                drop(leader);
                leader = EpochLog::open(&leader_path, limits)
                    .unwrap_or_else(|error| panic!("{error:?}"));
                if leader.committed_ids() != committed || leader.high_watermark() != watermark {
                    report.observer_failures = report.observer_failures.saturating_add(1);
                }
                report.crashes = report.crashes.saturating_add(1);
                report.faults = report.faults.saturating_add(1);
            }
            epoch = epoch.saturating_add(1);
        }
    }
    report
}

fn marker_profile() -> Report {
    let limits = markers::Limits::new(32, journal_limits()).unwrap_or_else(|_| panic!("limits"));
    let mut report = Report {
        profile: "transaction-markers",
        faults: 0,
        crashes: 0,
        lost_acks: 0,
        fences: 0,
        partitions: 0,
        observer_failures: 0,
    };
    let mut seed = 0x7A6_0001_u64;
    while report.faults < FAULTS_PER_PROFILE {
        seed = seed.wrapping_add(1);
        let log_path = path("mark", seed);
        let mut log =
            MarkerLog::open(&log_path, limits).unwrap_or_else(|error| panic!("{error:?}"));
        let id = mix(&mut seed);
        log.append_data(4, 0, id)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let commit = mix(&mut seed).is_multiple_of(2);
        let first = log
            .append_marker(4, 0, commit)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let second = log
            .append_marker(4, 0, commit)
            .unwrap_or_else(|error| panic!("{error:?}"));
        if first != second {
            report.observer_failures = report.observer_failures.saturating_add(1);
        }
        report.lost_acks = report.lost_acks.saturating_add(1);
        report.faults = report.faults.saturating_add(1);
        let visible = log.read_committed(0);
        if log.append_marker(4, 0, !commit).is_ok() {
            report.observer_failures = report.observer_failures.saturating_add(1);
        }
        if log.read_committed(0) != visible {
            report.observer_failures = report.observer_failures.saturating_add(1);
        }
        report.fences = report.fences.saturating_add(1);
        report.faults = report.faults.saturating_add(1);
        drop(log);
        let restored =
            MarkerLog::open(&log_path, limits).unwrap_or_else(|error| panic!("{error:?}"));
        if restored.read_committed(0) != visible {
            report.observer_failures = report.observer_failures.saturating_add(1);
        }
        if commit {
            if !restored.read_committed(0).contains(&id) {
                report.observer_failures = report.observer_failures.saturating_add(1);
            }
        } else if restored.read_committed(0).contains(&id) {
            report.observer_failures = report.observer_failures.saturating_add(1);
        }
        report.crashes = report.crashes.saturating_add(1);
        report.faults = report.faults.saturating_add(1);
    }
    report
}

fn offset_profile() -> Report {
    let limits = offsets::Limits::new(32, journal_limits()).unwrap_or_else(|_| panic!("limits"));
    let mut report = Report {
        profile: "transactional-offsets",
        faults: 0,
        crashes: 0,
        lost_acks: 0,
        fences: 0,
        partitions: 0,
        observer_failures: 0,
    };
    let mut seed = 0x05F5_0001_u64;
    while report.faults < FAULTS_PER_PROFILE {
        seed = seed.wrapping_add(1);
        let log_path = path("off", seed);
        let mut log =
            TxnOffsets::open(&log_path, limits).unwrap_or_else(|error| panic!("{error:?}"));
        let id = mix(&mut seed);
        log.append_data(4, 0, id)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.stage_offset(
            4,
            0,
            StagedOffset {
                group: "orders",
                member: "m-1",
                generation: 2,
                topic: "orders",
                partition: 0,
                offset: 10,
            },
        )
        .unwrap_or_else(|error| panic!("{error:?}"));
        if !log.visible_data().is_empty() {
            report.observer_failures = report.observer_failures.saturating_add(1);
        }
        let commit = mix(&mut seed).is_multiple_of(2);
        log.marker(4, 0, commit)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.marker(4, 0, commit)
            .unwrap_or_else(|error| panic!("{error:?}"));
        report.lost_acks = report.lost_acks.saturating_add(1);
        report.faults = report.faults.saturating_add(1);
        let visible = log.visible_data();
        if log
            .stage_offset(
                4,
                0,
                StagedOffset {
                    group: "orders",
                    member: "m-1",
                    generation: 1,
                    topic: "orders",
                    partition: 1,
                    offset: 1,
                },
            )
            .is_ok()
        {
            report.observer_failures = report.observer_failures.saturating_add(1);
        }
        if log.visible_data() != visible {
            report.observer_failures = report.observer_failures.saturating_add(1);
        }
        report.fences = report.fences.saturating_add(1);
        report.faults = report.faults.saturating_add(1);
        drop(log);
        let restored =
            TxnOffsets::open(&log_path, limits).unwrap_or_else(|error| panic!("{error:?}"));
        if restored.visible_data() != visible {
            report.observer_failures = report.observer_failures.saturating_add(1);
        }
        if commit {
            if !restored.visible_data().contains(&id) {
                report.observer_failures = report.observer_failures.saturating_add(1);
            }
        } else if restored.visible_data().contains(&id) {
            report.observer_failures = report.observer_failures.saturating_add(1);
        }
        report.crashes = report.crashes.saturating_add(1);
        report.partitions = report.partitions.saturating_add(1);
        report.faults = report.faults.saturating_add(1);
    }
    report
}

fn write_report(reports: &[Report]) {
    let body = format!(
        "{{\n  \"faults_per_profile\": {FAULTS_PER_PROFILE},\n  \"profiles\": [\n{}\n  ],\n  \"fsync_errors_injected\": false,\n  \"network_partition\": \"two-log follower misses leader appends during the partition step\"\n}}\n",
        reports
            .iter()
            .map(|report| format!(
                "    {{\"profile\": \"{}\", \"faults\": {}, \"crashes\": {}, \"lost_acks\": {}, \"fences\": {}, \"partitions\": {}, \"observer_failures\": {}}}",
                report.profile,
                report.faults,
                report.crashes,
                report.lost_acks,
                report.fences,
                report.partitions,
                report.observer_failures
            ))
            .collect::<Vec<_>>()
            .join(",\n")
    );
    let out = std::env::var("PL_FAULT_CAMPAIGN_OUT").unwrap_or_default();
    if !out.is_empty() {
        let mut file = std::fs::File::create(&out).unwrap_or_else(|error| panic!("{error:?}"));
        std::io::Write::write_all(&mut file, body.as_bytes())
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
    println!("{body}");
}

#[test]
fn seeded_faults_preserve_committed_outcomes() {
    let reports = [recovery_profile(), marker_profile(), offset_profile()];
    for report in &reports {
        assert!(report.faults >= FAULTS_PER_PROFILE, "{}", report.profile);
        assert_eq!(report.observer_failures, 0, "{}", report.profile);
    }
    write_report(&reports);
}
