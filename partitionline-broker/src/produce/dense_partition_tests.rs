//! Differential controls for bounded Produce partition classification.
use super::{mark_dense_partitions, Part};
use std::collections::BTreeMap;

fn reference(parts: &[Part]) -> (Vec<bool>, Vec<bool>) {
    let mut first = vec![false; parts.len()];
    let mut duplicate = vec![false; parts.len()];
    let mut seen = BTreeMap::new();
    for (position, part) in parts.iter().enumerate() {
        if let Some(earlier) = seen.get(&part.index).copied() {
            duplicate[earlier] = true;
        } else {
            first[position] = true;
            let _ = seen.insert(part.index, position);
        }
    }
    (first, duplicate)
}

fn parts(indexes: &[i32]) -> Vec<Part> {
    indexes.iter().map(|&index| Part { index, bytes: None }).collect()
}

fn check(indexes: &[i32], eligible: bool) {
    let values = parts(indexes);
    let (expected_first, expected_duplicate) = reference(&values);
    let mut first = vec![false; values.len()];
    let mut duplicate = vec![false; values.len()];
    let accepted = mark_dense_partitions(&values, &mut first, &mut duplicate);
    assert_eq!(accepted, eligible, "indexes={indexes:?}");
    if accepted {
        assert_eq!(first, expected_first);
        assert_eq!(duplicate, expected_duplicate);
    } else {
        assert!(first.iter().all(|value| !value));
        assert!(duplicate.iter().all(|value| !value));
    }
}

#[test]
fn dense_partition_classifier_matches_reference_for_order_and_duplicates() {
    for size in [17usize, 18, 31, 32, 63, 64, 255, 256, 1024, 2048, 4096] {
        let ascending: Vec<i32> = (0..size).map(|value| value as i32).collect();
        check(&ascending, true);
        let reversed: Vec<i32> = ascending.iter().rev().copied().collect();
        check(&reversed, true);
        let mut rotated = ascending.clone();
        rotated.rotate_left(size / 3);
        check(&rotated, true);
        let duplicates: Vec<i32> = (0..size).map(|value| (value % 7) as i32).collect();
        check(&duplicates, true);
        let mut edge = reversed;
        edge[0] = ((size * 2).min(4096) - 1) as i32;
        check(&edge, true);
    }
}

#[test]
fn dense_partition_classifier_declines_sparse_negative_and_mismatched_inputs() {
    for size in [17usize, 32, 256, 2048, 4096] {
        let mut indexes: Vec<i32> = (0..size).rev().map(|value| value as i32).collect();
        indexes[size - 1] = ((size * 2).min(4096)) as i32;
        check(&indexes, false);
        indexes[size - 1] = -1;
        check(&indexes, false);
        indexes[size - 1] = i32::MIN;
        check(&indexes, false);
        indexes[size - 1] = i32::MAX;
        check(&indexes, false);
    }
    let values = parts(&[1, 0]);
    let mut first = vec![false];
    let mut duplicate = vec![false; 2];
    assert!(!mark_dense_partitions(&values, &mut first, &mut duplicate));
    assert_eq!(first, [false]);
    assert_eq!(duplicate, [false, false]);
    assert!(!mark_dense_partitions(&[], &mut [], &mut []));
}

#[test]
fn dense_partition_classifier_preserves_last_u16_owner_and_declines_overflow() {
    let mut indexes = vec![0; usize::from(u16::MAX)];
    let last = indexes.len() - 1;
    indexes[last] = 1;
    check(&indexes, true);
    indexes.push(1);
    check(&indexes, false);
}
