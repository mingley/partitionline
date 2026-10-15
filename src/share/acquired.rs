use crate::error::{Error, Result};
use crate::protocol::share::AcquiredRange;

/// Java `ShareCompletedFetch`: overlapping `AcquiredRecords` log an error
/// and keep the first occurrence. Later ranges contribute only offsets
/// that were not already assigned.
pub(crate) fn keep_first_acquired(ranges: &[AcquiredRange]) -> Result<Vec<AcquiredRange>> {
    let mut assigned: Vec<(i64, i64, i16)> = Vec::new();
    for range in ranges {
        if range.first_offset < 0
            || range.last_offset < range.first_offset
            || range.delivery_count <= 0
        {
            return Err(Error::protocol("invalid ShareFetch acquired range"));
        }
        let mut fragments = vec![(range.first_offset, range.last_offset)];
        for &(start, end, _) in &assigned {
            let mut next = Vec::new();
            for (first, last) in fragments {
                if last < start || first > end {
                    next.push((first, last));
                    continue;
                }
                if first < start {
                    next.push((first, start - 1));
                }
                if last > end {
                    next.push((end + 1, last));
                }
            }
            fragments = next;
            if fragments.is_empty() {
                break;
            }
        }
        for (first, last) in fragments {
            assigned.push((first, last, range.delivery_count));
        }
    }
    assigned.sort_unstable_by_key(|&(first, _, _)| first);
    Ok(assigned
        .into_iter()
        .map(
            |(first_offset, last_offset, delivery_count)| AcquiredRange {
                first_offset,
                last_offset,
                delivery_count,
            },
        )
        .collect())
}

/// Lookup in sorted, nonoverlapping acquisition ranges after keep-first merge.
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
    fn keep_first_acquired_retains_first_occurrence() {
        let ranges = [
            AcquiredRange {
                first_offset: 0,
                last_offset: 9,
                delivery_count: 1,
            },
            AcquiredRange {
                first_offset: 5,
                last_offset: 14,
                delivery_count: 2,
            },
        ];
        let merged = keep_first_acquired(&ranges).unwrap();
        assert_eq!(
            merged,
            vec![
                AcquiredRange {
                    first_offset: 0,
                    last_offset: 9,
                    delivery_count: 1,
                },
                AcquiredRange {
                    first_offset: 10,
                    last_offset: 14,
                    delivery_count: 2,
                },
            ]
        );
    }

    #[test]
    fn keep_first_acquired_uses_list_order() {
        let ranges = [
            AcquiredRange {
                first_offset: 5,
                last_offset: 14,
                delivery_count: 2,
            },
            AcquiredRange {
                first_offset: 0,
                last_offset: 9,
                delivery_count: 1,
            },
        ];
        let merged = keep_first_acquired(&ranges).unwrap();
        assert_eq!(
            merged,
            vec![
                AcquiredRange {
                    first_offset: 0,
                    last_offset: 4,
                    delivery_count: 1,
                },
                AcquiredRange {
                    first_offset: 5,
                    last_offset: 14,
                    delivery_count: 2,
                },
            ]
        );
    }

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
