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
    indexes
        .iter()
        .map(|&index| Part { index, bytes: None })
        .collect()
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
    indexes[last - 1] = 1;
    indexes[last] = 1;
    check(&indexes, true);
    indexes.push(1);
    check(&indexes, false);
}

#[test]
fn dense_partition_production_path_selection_fallback_and_cancellation(
) -> Result<(), Box<dyn std::error::Error>> {
    use super::{metadata, process, Catalog, Config, Error, Store, TopicId};
    use std::{
        cell::Cell,
        fs,
        time::{Instant, SystemTime, UNIX_EPOCH},
    };

    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            super::DENSE_TEST_FORCE_FALLBACK.with(|force| force.set(false));
            super::DENSE_TEST_FORCED.with(|forced| forced.set(0));
            super::DENSE_TEST_SELECTED.with(|selected| selected.set(0));
        }
    }
    let _reset = Reset;
    super::DENSE_TEST_FORCE_FALLBACK.with(|force| force.set(false));
    super::DENSE_TEST_FORCED.with(|forced| forced.set(0));
    super::DENSE_TEST_SELECTED.with(|selected| selected.set(0));

    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root =
        std::env::temp_dir().join(format!("pl-dense-selected-{}-{nanos}", std::process::id()));
    fs::create_dir(&root)?;
    let common = metadata::Config::new(0, "127.0.0.1".into(), 9092, "dense".into());
    let (mut catalog, _) = Catalog::open(root.join("catalog"), common.catalog_limits)?;
    let mut id = [0; 16];
    id[15] = 2;
    let topic_id = TopicId::new(id)?;
    let _created = catalog.create("alpha", topic_id, 2)?;
    let mut config = Config::new(root.join("data"));
    config.max_partitions = 4096;
    let mut store = Store::open(config, &catalog)?;

    fn request(
        topic: &[u8],
        indexes: &[i32],
        valid: bool,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = include_bytes!("../../tests/fixtures/records/valid-basic.bin");
        let mut out = Vec::new();
        out.extend_from_slice(&0_i16.to_be_bytes());
        out.extend_from_slice(&3_i16.to_be_bytes());
        out.extend_from_slice(&77_i32.to_be_bytes());
        out.extend_from_slice(&(-1_i16).to_be_bytes());
        out.extend_from_slice(&(-1_i16).to_be_bytes());
        out.extend_from_slice(&1_i16.to_be_bytes());
        out.extend_from_slice(&60_000_i32.to_be_bytes());
        out.extend_from_slice(&1_i32.to_be_bytes());
        out.extend_from_slice(&i16::try_from(topic.len())?.to_be_bytes());
        out.extend_from_slice(topic);
        out.extend_from_slice(&i32::try_from(indexes.len())?.to_be_bytes());
        for index in indexes {
            out.extend_from_slice(&index.to_be_bytes());
            if valid {
                out.extend_from_slice(&i32::try_from(batch.len())?.to_be_bytes());
                out.extend_from_slice(batch);
            } else {
                out.extend_from_slice(&(-1_i32).to_be_bytes());
            }
        }
        Ok(out)
    }

    for count in [16_i32, 17, 32, 256, 4096] {
        super::DENSE_TEST_SELECTED.with(|selected| selected.set(0));
        let indexes: Vec<i32> = (0..count).rev().collect();
        let mut input = request(b"omega", &indexes, false)?;
        let response = process(
            &catalog,
            &mut store,
            &mut input,
            &common,
            Instant::now(),
            || Ok(()),
        )?;
        assert!(response.is_some());
        let expected = if count > 16 { 1 } else { 0 };
        assert_eq!(
            super::DENSE_TEST_SELECTED.with(|selected| selected.get()),
            expected
        );
    }

    super::DENSE_TEST_SELECTED.with(|selected| selected.set(0));
    let mut repeated: Vec<i32> = (0..32).rev().collect();
    repeated[29] = 31;
    repeated[30] = 31;
    let mut dense = request(b"omega", &repeated, false)?;
    let original = process(
        &catalog,
        &mut store,
        &mut dense,
        &common,
        Instant::now(),
        || Ok(()),
    )?;
    assert_eq!(
        super::DENSE_TEST_SELECTED.with(|selected| selected.get()),
        1
    );
    let values = parts(&repeated);
    let (first, duplicate) = reference(&values);
    let mut want = Vec::new();
    want.extend_from_slice(&77_i32.to_be_bytes());
    want.extend_from_slice(&1_i32.to_be_bytes());
    want.extend_from_slice(&5_i16.to_be_bytes());
    want.extend_from_slice(b"omega");
    want.extend_from_slice(
        &i32::try_from(first.iter().filter(|value| **value).count())?.to_be_bytes(),
    );
    for (position, part) in values.iter().enumerate() {
        if first[position] {
            want.extend_from_slice(&part.index.to_be_bytes());
            want.extend_from_slice(
                &(if duplicate[position] { 42_i16 } else { 3_i16 }).to_be_bytes(),
            );
            want.extend_from_slice(&(-1_i64).to_be_bytes());
            want.extend_from_slice(&(-1_i64).to_be_bytes());
        }
    }
    want.extend_from_slice(&0_i32.to_be_bytes());
    assert_eq!(original.as_deref(), Some(want.as_slice()));
    super::DENSE_TEST_FORCE_FALLBACK.with(|force| force.set(true));
    let mut fallback = request(b"omega", &repeated, false)?;
    let from_fallback = process(
        &catalog,
        &mut store,
        &mut fallback,
        &common,
        Instant::now(),
        || Ok(()),
    )?;
    assert_eq!(original, from_fallback);
    assert_eq!(super::DENSE_TEST_FORCED.with(|forced| forced.get()), 1);
    assert_eq!(
        super::DENSE_TEST_SELECTED.with(|selected| selected.get()),
        1
    );

    super::DENSE_TEST_FORCE_FALLBACK.with(|force| force.set(false));
    super::DENSE_TEST_SELECTED.with(|selected| selected.set(0));
    let mut sparse: Vec<i32> = (0..256).rev().collect();
    sparse[255] = i32::MAX;
    let mut sparse_input = request(b"omega", &sparse, false)?;
    assert!(process(
        &catalog,
        &mut store,
        &mut sparse_input,
        &common,
        Instant::now(),
        || Ok(())
    )?
    .is_some());
    assert_eq!(
        super::DENSE_TEST_SELECTED.with(|selected| selected.get()),
        0
    );

    let mut indexes = vec![1, 0];
    indexes.extend((2..32).rev());
    let mut valid = request(b"alpha", &indexes, true)?;
    let calls = Cell::new(0usize);
    let canceled = process(
        &catalog,
        &mut store,
        &mut valid,
        &common,
        Instant::now(),
        || {
            let next = calls.get() + 1;
            calls.set(next);
            if next == 3 {
                Err(Error::Canceled)
            } else {
                Ok(())
            }
        },
    );
    assert!(matches!(canceled, Err(Error::Canceled)));
    assert_eq!(calls.get(), 3);
    assert_eq!(
        super::DENSE_TEST_SELECTED.with(|selected| selected.get()),
        1
    );
    let position = store
        .slot(topic_id, 1)
        .map_err(|code| std::io::Error::other(format!("slot error {code}")))?;
    assert!(store.slots[position]
        .partition
        .fetch(0, 1, 4096)?
        .is_empty());
    let mut retry = request(b"alpha", &indexes, true)?;
    assert!(process(
        &catalog,
        &mut store,
        &mut retry,
        &common,
        Instant::now(),
        || Ok(())
    )?
    .is_some());
    assert_eq!(store.log_bounds(topic_id, 0), Ok((0, 1)));
    assert_eq!(store.log_bounds(topic_id, 1), Ok((0, 1)));
    drop(store);
    drop(catalog);
    fs::remove_dir_all(root)?;
    Ok(())
}
