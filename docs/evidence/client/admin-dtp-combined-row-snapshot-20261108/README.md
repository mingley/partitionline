# Requested-name validation and row checkpoint: snapshot

This record measures an isolated combination of a requested-name validation
draft and an 8,192-row conversion checkpoint. The name-validation draft is
being revised; this snapshot is not a proposed final source. The
[standalone checkpoint evidence](../admin-dtp-row-checkpoint-20261108/README.md)
covers a comparison against main without requested-name validation.

[Run 42482443810](https://github.com/mingley/partitionline/actions/runs/42482443810)
passed 260 interleaved public Admin calls against a TCP mock on one runner.
The baseline is name-validation-only source blob
`eb5a967b89e94a8a3d1a7598919accdfde96f20f`; the candidate adds the
checkpoint in blob `95d95498f07faccba3f1f95b2cab0a20f1d9eaf5`. Both arms use
the identical fixture blob `6944285477b86a8cad7b2e65ef3b55dcebd64f95`,
separate target directories and distinct binary hashes. The fixture checks
all requested and returned names, error codes, empty partition lists and
omitted authorization flags. [All samples](paired.csv) preserve execution
order.

The table shows medians of the longest public-future poll that began after the
mock peer finished writing its response. There were 21 pairs at 1 and 32 rows,
5 pairs for 8,191 through 100,000, and 3 pairs at 500,000.

| Rows | Row kind | Name only | Name plus checkpoint |
| ---: | --- | ---: | ---: |
| 1 | empty | 0.024 ms | 0.023 ms |
| 1 | error | 0.022 ms | 0.022 ms |
| 32 | empty | 0.033 ms | 0.032 ms |
| 32 | error | 0.032 ms | 0.032 ms |
| 8,191 | empty | 4.679 ms | 4.733 ms |
| 8,192 | empty | 3.195 ms | 2.298 ms |
| 8,192 | error | 3.355 ms | 2.494 ms |
| 8,193 | empty | 3.308 ms | 2.460 ms |
| 100,000 | empty | 16.199 ms | 9.290 ms |
| 100,000 | error | 16.218 ms | 9.059 ms |
| 500,000 | empty | 106.870 ms | 68.047 ms |
| 500,000 | error | 104.515 ms | 71.551 ms |

These polls can include reads, decoding, conversion, merging and destruction;
they do not identify the cause of a particular maximum. At 500,000 rows,
whole mock-call medians were 682.7→698.3 ms for empty rows and
679.2→691.3 ms for errors. Whole-call timings include peer work. These data
are neither a Kafka throughput benchmark nor a general production latency
claim.

The runner reported AMD EPYC 7763 on GitHub-hosted Ubuntu 24.04, Rust 1.99.1
(`58cf8f9c3`), LLVM 23.1.1, release thin LTO and one codegen unit.
`RUSTFLAGS` was unset and `cpu.max` was unavailable. Baseline and
candidate executable SHA-256s were
`f987582d780736b71035f17add5aaaac2560fcdad5a12ab83b7ecaeaff1c4dae`
and `e1a5a4c391c27f9e7cf8fa54668a3c8c7f90a6b41e3af268481e94440121e1ea`.
The binaries were not retained. The run artifact retains raw logs and source
pins for 30 days.

The combined snapshot's [focused gate 42482784330](https://github.com/mingley/partitionline/actions/runs/42482784330)
passed 4/4 row, 29/29 name and 61/61 pagination tests in each feature profile;
the public-future `Send` assertion, formatting and strict all-target,
all-feature Clippy passed. Its
[test-instrumented checkpoint controls 42482670005](https://github.com/mingley/partitionline/actions/runs/42482670005)
passed cancellation, deadline and mismatch-scan controls in both profiles,
and detected a mutation replacing the row yield with an immediately ready
future. The initial one-marker control and its failed mutation gate are
retained in the standalone evidence linked above.

The checkpoint adds timeout and cancellation opportunities. It limits
completed conversions between its own yields, not time spent in names,
individual rows, the separate mismatch scan, cleanup or synchronous drop. Any
revised name-validation source needs a new merge and exact-source controls and
timing.
