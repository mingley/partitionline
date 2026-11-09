# Producer retry membership controls

These synthetic unit controls do not change production behavior. They seed
pending records directly and exercise the current readiness and selection
helpers; they do not demonstrate a naturally reached broker history, a public
producer schedule, a wire request, or a fix.

The baseline was main
[`731eb97a08445a18da0b043986ae192397ba176a`](https://github.com/mingley/partitionline/commit/731eb97a08445a18da0b043986ae192397ba176a),
with `src/producer.rs` blob `426dbdb5c5e27fdf655930a76682c5c18fb07e89`.
The [test-only source and workflow](https://github.com/mingley/partitionline/commit/15791b8ff21a6e6c8b0e2a6fc56502bd8a1bc857)
add five `#[cfg(test)]` controls and a workflow. The tested producer file
is blob `baba5ef87bb7ad1fa7cf72f0307b3267ee168bc5`, SHA-256
`4daacfc0832631def64fbb2b0fd26602896b1a9eb409e67794ab8b082368b463`;
`Cargo.lock` blob is `96e364d34121fe90183f87f4f49fd92b883bc447`.
No production helper was modified.

[Run 42685305590](https://github.com/mingley/partitionline/actions/runs/42685305590)
used hosted x86_64 Linux and stable rustc 1.99.1. Both default and
all-feature profiles produced the same outcomes:

| Control | Observed result per profile |
| --- | --- |
| Complete two-record batch on one partition | Passed: the leading count and selection were both 2. |
| Same base, different topic | Expected-negative assertion failed: leading count was 2, expected 1. |
| Same base, different partition | Expected-negative assertion failed: leading count was 2, expected 1. |
| Same base, different identity generation | Expected-negative assertion failed: leading count was 2, expected 1. |
| Two incomplete groups passed to the selector | Expected-negative assertion failed: 2 records selected, expected 0. |

The workflow passed only because it required all four negative assertions to
fail with their markers. Its green status is not producer success. Each
record in the negative cases was directly seeded as the first member of a
two-record batch at base 7; no preceding Kafka identity or broker history was
constructed.

`count_leading_sequence` currently compares only the numeric batch base.
The normal readiness path can therefore count these distinct seeded records
together. The selector independently accepts a prefix containing records from
two incomplete seeded groups. The controls do not call `fire`, do not cover
sticky scheduling or expiration, and do not establish whether this exact
combined state occurred in an uninstrumented producer.

## Implication for an integrated repair

A numeric base cannot be the sole identity of a retry: topic, partition,
producer identity/generation, original ordered membership and a nonreused
batch reservation all matter. Sending a `Vec<Pending>` atomically through
one channel still permits splitting if a worker flattens it into its regular
record queue or expires members individually. Keep the sealed batch as one
owned queue item through routing, selection, encoding, write ambiguity and
completion. An authoritative partition fence must stop later batches from
overtaking an unresolved attempt, including when leaders or workers change.

The current bounded-owner experiment retains individual record slots and
reclaims orphaned records separately; it has no authoritative batch journal
or write/identity fence. These controls are not a basis for merging that
candidate, for claiming rollover correctness, or for production-readiness
or performance claims.
