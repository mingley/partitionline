// Prepared crate-local fixture consumer. No fixture output exists yet.
// Intended include location: a cfg(test) child of src/partitioner.rs.
use super::*;
use crate::protocol::records::Record;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Observation {
    case: String,
    event: usize,
    before_partition: i32,
    after_partition: i32,
    before_generation: u32,
    after_generation: u32,
}

fn integer<T: std::str::FromStr>(field: &str) -> T
where
    T::Err: std::fmt::Debug,
{
    field.parse().unwrap()
}

fn column<'a>(fields: &[&'a str], index: usize) -> &'a str {
    fields.get(index).copied().unwrap()
}

fn leaders(partitions: usize, mask: u32) -> Vec<i32> {
    (0..partitions)
        .map(|p| if mask & (1u32 << p) == 0 { -1 } else { 0 })
        .collect()
}

fn packed_bytes(kind: &str, value_length: i32) -> usize {
    let value = (value_length >= 0).then(|| {
        bytes::Bytes::from(vec![0; usize::try_from(value_length).unwrap()])
    });
    let record = Record {
        offset: 0,
        timestamp: 0,
        key: (kind == "K").then(bytes::Bytes::new),
        value,
        headers: Vec::new(),
    };
    usize::try_from(record.record_size_upper_bound().unwrap()).unwrap()
}

fn expected(fixture: &str) -> Vec<Observation> {
    assert!(fixture.len() <= 32768);
    let mut observations = Vec::new();
    for line in fixture.lines().filter(|line| !line.starts_with('#') && !line.is_empty()) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 6);
        observations.push(Observation {
            case: column(&fields, 0).to_owned(),
            event: integer(column(&fields, 1)),
            before_partition: integer(column(&fields, 2)),
            after_partition: integer(column(&fields, 3)),
            before_generation: integer(column(&fields, 4)),
            after_generation: integer(column(&fields, 5)),
        });
    }
    assert!(!observations.is_empty() && observations.len() <= 256);
    observations
}

fn actual(input: &str) -> Vec<Observation> {
    assert!(input.len() <= 32768);
    let worker = Arc::new(());
    let topic: Arc<str> = Arc::from("t");
    let mut observations = Vec::new();
    let mut state: Option<StickyState> = None;
    let mut cluster = Cluster::default();
    let mut case = String::new();
    let mut partitions = 0usize;
    let mut event = 0usize;
    let mut generation = 0u32;
    let mut tags = BTreeMap::<String, StickyTag>::new();
    let mut cases = 0usize;
    for line in input.lines().filter(|line| !line.starts_with('#') && !line.is_empty()) {
        let fields: Vec<_> = line.split('\t').collect();
        let kind = column(&fields, 0);
        if kind == "CASE" {
            assert_eq!(fields.len(), 8);
            cases += 1;
            assert!(cases <= 32);
            if let Some(previous) = &state {
                assert_eq!(previous.counts().1, 0, "unreleased reference membership");
            }
            case = column(&fields, 1).to_owned();
            assert!(case.len() <= 64);
            let batch_bytes = integer::<usize>(column(&fields, 2));
            let batch_records = integer::<usize>(column(&fields, 3));
            partitions = integer(column(&fields, 4));
            let mask = integer(column(&fields, 5));
            assert!((1..=65536).contains(&batch_bytes));
            assert!((1..=256).contains(&batch_records));
            assert!((1..=8).contains(&partitions));
            assert!(mask < (1u32 << partitions));
            let seed = u64::from_str_radix(column(&fields, 6), 16).unwrap();
            state = Some(StickyState::new(StickyPartitionerConfig {
                seed: Some(seed), ..StickyPartitionerConfig::default()
            }, batch_bytes, batch_records));
            cluster = Cluster::default();
            let _ = cluster.leaders.insert("t".to_owned(), leaders(partitions, mask));
            tags.clear();
            generation = 0;
            event = 0;
            continue;
        }
        let state = state.as_mut().unwrap();
        let before = state.route("t", &cluster).unwrap();
        let before_generation = generation;
        let before_info = state.topics.get("t").and_then(|row| row.info.as_ref())
            .map(|info| info.generation);
        match kind {
            "U" | "K" | "E" => {
                assert_eq!(fields.len(), 7);
                let id = column(&fields, 1).to_owned();
                let value_length = integer(column(&fields, 3));
                assert!((-1..=65536).contains(&value_length));
                let record_bytes = packed_bytes(kind, value_length);
                assert_eq!(record_bytes, integer::<usize>(column(&fields, 4)));
                let partition = if kind == "U" { before.partition }
                    else { integer(column(&fields, 2)) };
                assert!(partition >= 0 && usize::try_from(partition).unwrap() < partitions);
                let mut plan = state.plan(&topic, partition, &worker, record_bytes, &cluster);
                assert_eq!(plan.delta, integer::<usize>(column(&fields, 5)), "independent packing input");
                plan.next_rng = (kind == "U").then_some(before.rng).flatten();
                let tag = plan.tag;
                state.commit(&topic, partition, &worker, kind == "U", plan, &cluster);
                assert!(tags.insert(id, tag).is_none());
                let lifetime = state.topics.get("t").unwrap().lifetime;
                assert_eq!(state.full("t", lifetime, before.partition), column(&fields, 6) == "1",
                           "independent actual-tail availability input");
            }
            "D" => {
                assert_eq!(fields.len(), 3);
                let tag = *tags.get(column(&fields, 1)).unwrap();
                state.drain(tag, &cluster);
                let lifetime = state.topics.get("t").unwrap().lifetime;
                assert_eq!(state.full("t", lifetime, before.partition), column(&fields, 2) == "1");
            }
            "R" => {
                assert_eq!(fields.len(), 2);
                state.release(tags.remove(column(&fields, 1)).unwrap(), 1, &cluster);
            }
            "P" | "F" => {
                assert_eq!(fields.len(), 1);
                let counts = state.counts();
                let rng = state.rng;
                for _ in 0..8 {
                    let _ = state.plan(&topic, before.partition, &worker, 100, &cluster);
                    assert_eq!(state.route("t", &cluster), Some(before));
                    assert_eq!(state.counts(), counts);
                    assert_eq!(state.rng, rng);
                }
            }
            "M" => {
                assert_eq!(fields.len(), 2);
                let mask = integer(column(&fields, 1));
                assert!(mask < (1u32 << partitions));
                let _ = cluster.leaders.insert("t".to_owned(), leaders(partitions, mask));
            }
            _ => panic!("unknown reference event"),
        }
        let after = state.route("t", &cluster).unwrap();
        let after_info = state.topics.get("t").and_then(|row| row.info.as_ref())
            .map(|info| info.generation);
        // Creating Rust's first admitted info corresponds to Java's initial peek,
        // which already exists before its first update; generation starts at zero.
        let initial_rotation = before_info.is_none() && kind == "U"
            && state.topics.get("t").and_then(|row| row.info.as_ref())
                .is_some_and(|info| info.bytes == 0);
        if (before_info.is_some() && before_info != after_info) || initial_rotation {
            generation += 1;
        }
        observations.push(Observation { case: case.clone(), event,
            before_partition: before.partition, after_partition: after.partition,
            before_generation, after_generation: generation });
        event += 1;
        assert!(observations.len() <= 256 && tags.len() <= 256);
    }
    assert_eq!(state.unwrap().counts().1, 0);
    assert!(tags.is_empty());
    observations
}

#[test]
fn official_java_uniform_transitions_match_actual_rust_admissions_and_drains() {
    let input = include_str!("uniform-input.tsv");
    let fixture = include_str!("uniform-java-4.3.1.tsv");
    assert_eq!(actual(input), expected(fixture));
}

#[test]
fn java_fixture_comparison_rejects_changed_transition_partition_and_missing_rows() {
    let input = include_str!("uniform-input.tsv");
    let fixture = include_str!("uniform-java-4.3.1.tsv");
    let observations = actual(input);
    let original = expected(fixture);
    assert_eq!(observations, original);
    assert_ne!(observations, expected(include_str!("uniform-java-4.3.1-negative-batch.tsv")));
    assert_ne!(observations, expected(include_str!("uniform-java-4.3.1-negative-draw.tsv")));
    let mut changed_generation = original.clone();
    changed_generation.first_mut().unwrap().after_generation += 1;
    assert_ne!(observations, changed_generation);
    let mut changed_partition = original.clone();
    changed_partition.first_mut().unwrap().after_partition = -1;
    assert_ne!(observations, changed_partition);
    let mut missing = original;
    let _ = missing.pop();
    assert_ne!(observations, missing);
}
