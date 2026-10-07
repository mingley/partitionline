use crate::protocol::share::AcquiredRange;

/// Lookup in sorted, nonoverlapping acquisition ranges validated by ShareFetch.
pub(crate) struct AcquisitionRanges<'a> {
    ranges: &'a [AcquiredRange],
}

impl<'a> AcquisitionRanges<'a> {
    pub(crate) fn new(ranges: &'a [AcquiredRange]) -> Self {
        Self { ranges }
    }

    pub(crate) fn delivery_count(&mut self, offset: i64) -> Option<i16> {
        let index = self
            .ranges
            .partition_point(|range| range.last_offset < offset);
        self.ranges
            .get(index)
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
                    delivery_count: (i % 11 + 1) as i16,
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
                        offsets.swap(i, state as usize % (i + 1));
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
