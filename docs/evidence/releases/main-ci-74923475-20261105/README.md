# Main CI at 74923475

Source: [`74923475a3569041e32ca89a080ebb963a5ccdb1`](https://github.com/mingley/partitionline/commit/74923475a3569041e32ca89a080ebb963a5ccdb1).
The [main CI run](https://github.com/mingley/partitionline/actions/runs/42128570187)
completed with failures.

| Client library job | Passed | Failed |
| --- | ---: | ---: |
| [Linux default](https://github.com/mingley/partitionline/actions/runs/42128570187/job/127599013362) | 2,179 | 17 |
| [Linux all features](https://github.com/mingley/partitionline/actions/runs/42128570187/job/127599013342) | 2,173 | 23 |
| [Windows default](https://github.com/mingley/partitionline/actions/runs/42128570187/job/127599014039) | 2,177 | 19 |
| [macOS arm64 default](https://github.com/mingley/partitionline/actions/runs/42128570187/job/127599013282) | 2,175 | 21 |

Each all-target command stopped at the library failure. Kafka 4.1.2, 4.2.1
and 4.3.1 compatibility jobs logged the classic-join marker, then stack
overflow before the next poll marker. The logs do not identify the failing
frame. Kafka 3.9.1 and 4.1.0 smoke jobs recovered from an initial JoinGroup
error on retry, then failed after six EOS example timeouts; their cause was not
identified. Live conformance was skipped.

Formatting, strict Clippy, documentation, package, audit, schema, fixture and
integrity-smoke jobs passed. The integrity smoke used native Kafka 4.1.0 and
2,000 application records; the then-current history converter inferred
individual acknowledgement labels from aggregate counters. That result does
not qualify per-record acknowledgements or performance.

The separate [broker workflow](https://github.com/mingley/partitionline/actions/runs/42128570165)
passed, reporting 734 default and 882 all-feature tests across its all-target
commands; both doctest commands found zero tests. The
[capabilities workflow](https://github.com/mingley/partitionline/actions/runs/42128570213)
also passed. Its archives were not inspected for this report. These results do
not close the documented broker replication, durability or client runtime gaps.
