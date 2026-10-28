//! Consumer group coordination.
//!
//! Classic Join/Sync membership lives in [`classic`]. Session, leave, and
//! revocation live in [`lifecycle`]. Modern server-side assignment lives in
//! [`consumer`]. Share groups live in [`share`]. Streams groups live in
//! [`streams`].

use std::collections::BTreeMap;

/// Return the value for `key`, creating it only when the key is absent.
///
/// [`BTreeMap::entry`] would copy `key` before knowing that.
pub(crate) fn occupy<'a, V>(
    map: &'a mut BTreeMap<String, V>,
    key: &str,
    make: impl FnOnce() -> V,
) -> Option<&'a mut V> {
    if !map.contains_key(key) {
        let _ = map.insert(key.to_owned(), make());
    }
    map.get_mut(key)
}

pub mod classic;
pub mod consumer;
pub mod lifecycle;
pub mod offsets;
pub mod share;
pub mod streams;

#[cfg(test)]
fn check_range_assignments(
    mut assign: impl FnMut(
        &BTreeMap<String, i32>,
        Vec<std::collections::BTreeSet<String>>,
    ) -> Vec<BTreeMap<String, Vec<i32>>>,
) {
    for mask in 0u8..32 {
        for count in 0i32..8 {
            let topics = BTreeMap::from([("alpha".to_owned(), count), ("beta".to_owned(), 7)]);
            let subscriptions: Vec<std::collections::BTreeSet<String>> = (0..5)
                .map(|member| {
                    let mut names = std::collections::BTreeSet::new();
                    if mask & (1 << member) != 0 {
                        let _ = names.insert("alpha".to_owned());
                    }
                    if member % 2 == 0 {
                        let _ = names.insert("beta".to_owned());
                    }
                    let _ = names.insert("unknown".to_owned());
                    names
                })
                .collect();
            let actual = assign(&topics, subscriptions.clone());
            assert_eq!(actual.len(), subscriptions.len());
            let mut expected = vec![BTreeMap::new(); subscriptions.len()];
            for (topic, count) in &topics {
                let eligible: Vec<usize> = subscriptions
                    .iter()
                    .enumerate()
                    .filter_map(|(index, names)| names.contains(topic).then_some(index))
                    .collect();
                if eligible.is_empty() {
                    continue;
                }
                let count = usize::try_from(*count).unwrap_or(0);
                for (rank, member) in eligible.iter().copied().enumerate() {
                    let base = count / eligible.len();
                    let remainder = count % eligible.len();
                    let first = rank * base + rank.min(remainder);
                    let end = first + base + usize::from(rank < remainder);
                    if end > first {
                        let parts = (first..end)
                            .map(|part| i32::try_from(part).unwrap_or(0))
                            .collect();
                        let _ = expected[member].insert(topic.clone(), parts);
                    }
                }
            }
            assert_eq!(actual, expected, "member mask {mask}, partitions {count}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::occupy;
    use std::collections::BTreeMap;

    #[test]
    fn an_existing_group_keeps_its_value() {
        let mut groups = BTreeMap::new();
        assert_eq!(occupy(&mut groups, "orders", || 1).copied(), Some(1));
        assert_eq!(occupy(&mut groups, "orders", || 9).copied(), Some(1));
        assert_eq!(occupy(&mut groups, "payments", || 2).copied(), Some(2));
        assert_eq!(groups.get("orders").copied(), Some(1));
        assert_eq!(groups.get("payments").copied(), Some(2));
    }
}
