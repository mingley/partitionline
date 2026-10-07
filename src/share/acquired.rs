use crate::protocol::share::AcquiredRange;

/// Lookup in sorted, nonoverlapping acquisition ranges validated by ShareFetch.
pub(crate) struct AcquisitionRanges<'a> {
    ranges: &'a [AcquiredRange],
    index: usize,
    previous_offset: Option<i64>,
}

impl<'a> AcquisitionRanges<'a> {
    pub(crate) fn new(ranges: &'a [AcquiredRange]) -> Self {
        Self {
            ranges,
            index: 0,
            previous_offset: None,
        }
    }

    pub(crate) fn delivery_count(&mut self, offset: i64) -> Option<i16> {
        if self
            .previous_offset
            .is_some_and(|previous| offset < previous)
        {
            self.index = self
                .ranges
                .partition_point(|range| range.last_offset < offset);
        } else {
            // Ordered records normally stay in this range or enter its neighbor.
            // Bound cursor work on a sparse jump, then search the remaining tail.
            for _ in 0..4 {
                if self
                    .ranges
                    .get(self.index)
                    .is_none_or(|r| r.last_offset >= offset)
                {
                    break;
                }
                self.index += 1;
            }
            if let Some(tail) = self.ranges.get(self.index..) {
                if tail.first().is_some_and(|range| range.last_offset < offset) {
                    self.index += tail.partition_point(|range| range.last_offset < offset);
                }
            }
        }
        self.previous_offset = Some(offset);
        self.ranges
            .get(self.index)
            .filter(|range| range.first_offset <= offset)
            .map(|range| range.delivery_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn share_ranges_preserve_counts_gaps_and_rewinds() {
        for count in 0..=64 {
            let ranges: Vec<_> = (0..count)
                .map(|i| AcquiredRange {
                    first_offset: i * 7,
                    last_offset: i * 7 + i % 4,
                    delivery_count: i16::try_from(i % 11 + 1).unwrap(),
                })
                .collect();
            let mut offsets: Vec<_> = (-3..count * 7 + 8).collect();
            for order in 0..3 {
                if order == 1 {
                    offsets.reverse();
                } else if order == 2 {
                    let mut state = 0x51_u64;
                    for i in (1..offsets.len()).rev() {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        let modulus = u64::try_from(i + 1).unwrap();
                        let index = usize::try_from(state % modulus).unwrap();
                        offsets.swap(i, index);
                    }
                }
                let mut lookup = AcquisitionRanges::new(&ranges);
                for &offset in &offsets {
                    let expected = ranges
                        .iter()
                        .find(|r| r.first_offset <= offset && offset <= r.last_offset)
                        .map(|r| r.delivery_count);
                    assert_eq!(lookup.delivery_count(offset), expected);
                    assert_eq!(lookup.delivery_count(offset), expected);
                }
            }
        }
    }

    #[test]
    fn share_ranges_handle_offset_limits_without_arithmetic() {
        let ranges = [
            AcquiredRange {
                first_offset: 0,
                last_offset: 0,
                delivery_count: 1,
            },
            AcquiredRange {
                first_offset: i64::MAX - 2,
                last_offset: i64::MAX,
                delivery_count: i16::MAX,
            },
        ];
        let mut lookup = AcquisitionRanges::new(&ranges);
        for offset in [i64::MIN, -1, 0, 1, i64::MAX, i64::MAX - 3, 0, i64::MAX - 1] {
            let expected = ranges
                .iter()
                .find(|r| r.first_offset <= offset && offset <= r.last_offset)
                .map(|r| r.delivery_count);
            assert_eq!(lookup.delivery_count(offset), expected);
        }
    }
}
