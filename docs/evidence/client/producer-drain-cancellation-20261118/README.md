# Producer drain cancellation

The worker now computes its remaining queue estimate and deadline before moving
the selected prefix. The scans still yield every 8,192 records. The final
transfer reserves the batch buffer before removing the prefix, then moves the
records and commits the bookkeeping in the same poll. A deadline refresh only
reads the deadlines of the retained records. There is no queue rotation on
cancellation.

This covers the non-sticky `Worker::drain_pending` call. The synchronous
transfer still moves the selected record entries and compacts the remainder.
Later grouping, sticky selection, retry delivery and user callback behavior
remain open.

## Correctness

The [candidate](https://github.com/mingley/partitionline/commit/4923b1b7847dfcaadd6c2eac128be886925364c5)
uses producer blob `47c4fc0c06c57a906d16af32637ebb69c7194511`. Its pinned
[baseline](https://github.com/mingley/partitionline/commit/753a35b19cebad4155d37938a14eee219193d581)
uses producer blob `14bf77b39bba6fadc4e14d151e285f126016f174`. The source
under test and the injected test module were recorded by the
[qualification workflow](https://github.com/mingley/partitionline/blob/4923b1b7847dfcaadd6c2eac128be886925364c5/.github/workflows/producer-drain-cancellation.yml).

Both controls seed 8,193 worker records and poll the drain once before dropping
it. One drain selects every record and yields while scanning that prefix. The
other selects one record, whose deadline is earliest, then yields while scanning
the other 8,192. The tests check that the receipts stay open, the queue and its
order stay intact, the estimate and deadline remain unchanged, and the byte
charge is retained. They then drain again, verify the new queue state, and settle
all seeded records through the existing closed worker cleanup. The controls use
constructed worker state and make no assertion about public Kafka delivery.

The [first run](https://github.com/mingley/partitionline/actions/runs/44410844005)
passed the candidate controls, verified both baseline failures, passed the 122
existing producer unit tests in each arm and feature profile, and passed strict
Clippy. Formatting failed on one assertion in the test code. The successor
wraps that assertion and adjusts the qualification workflow checkout depth. The
production source is byte-identical.

The [formatted run](https://github.com/mingley/partitionline/actions/runs/44411433690)
recorded Rust 1.100.0 on hosted x86_64 runners. Both
candidate profiles passed both controls and all 122 existing producer unit
tests. The same 122 existing tests passed on the baseline, and both new
controls failed at the expected dropped-receipt assertion. Formatting and
strict Clippy passed on the candidate.

The [earlier drain-staging proposal](../producer-drain-staging-20261110/README.md)
and the [grouping ownership review](../producer-grouping-cancellation-design-20261110/README.md)
describe the broader cancellation problem and retain the previous failed
experiments.

## Drain timings

The [timing run](https://github.com/mingley/partitionline/actions/runs/44411332075)
built separate release test binaries and ran eight alternating pairs on one
logical CPU of a hosted x86_64 runner with Rust 1.100.0. Both builds used the
same lockfile and test injection. The first candidate's production prefix is
byte-identical to the formatted source that passed qualification. The
[workflow](https://github.com/mingley/partitionline/blob/802eff6619dac841e342a98d5ae288f245d2e0bd/.github/workflows/producer-drain-timing.yml),
[fixture](https://github.com/mingley/partitionline/blob/802eff6619dac841e342a98d5ae288f245d2e0bd/docs/evidence/client/producer-drain-cancellation-20261118/timing.rs.txt),
and [measurement script](https://github.com/mingley/partitionline/blob/802eff6619dac841e342a98d5ae288f245d2e0bd/docs/evidence/client/producer-drain-cancellation-20261118/timing.py.txt)
are pinned. [Raw samples](timing-samples.txt) record the source and binary hashes,
iteration totals, and all 64 arm measurements.

| Case | Baseline median µs per drain | Candidate median µs per drain | Paired time ratio, median (range) |
| --- | ---: | ---: | ---: |
| Full drain of 64 records | 1.921 | 1.888 | 0.983 (0.946–1.312) |
| Drain 16 of 64 records | 0.930 | 0.677 | 0.728 (0.719–0.847) |
| Full drain of 8,193 records | 241.118 | 241.998 | 1.003 (0.812–1.006) |
| Drain 64 with 8,192 header-bearing records remaining | 292.823 | 68.375 | 0.234 (0.232–0.275) |

Time includes cooperative yields and excludes fixture setup and queue
restoration between iterations. Storage is reused. The 16-of-64 case and
the deep deadline refresh improved in every pair. Both full drains had pairs
on either side of parity. These timings cover the drain method in a synthetic
worker, with no broker traffic or Kafka throughput measurement.
