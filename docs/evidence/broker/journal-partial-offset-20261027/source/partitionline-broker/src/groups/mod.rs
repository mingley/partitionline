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
