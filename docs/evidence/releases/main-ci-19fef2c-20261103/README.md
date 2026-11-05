# Main CI and classic-join witness at 19fef2c

Source commit: [`19fef2cddf912f96af6d4b199c22fda398bca143`](https://github.com/mingley/partitionline/commit/19fef2cddf912f96af6d4b199c22fda398bca143).

## Partial main CI

The [main CI run](https://github.com/mingley/partitionline/actions/runs/41679536975)
was canceled. These jobs finished before cancellation; they do not constitute
a complete release matrix.

| Client library job | Passed | Failed |
| --- | ---: | ---: |
| [Default](https://github.com/mingley/partitionline/actions/runs/41679536975/job/126071124976) | 2,170 | 26 |
| [All features](https://github.com/mingley/partitionline/actions/runs/41679536975/job/126071124654) | 2,173 | 23 |
| [Windows](https://github.com/mingley/partitionline/actions/runs/41679536975/job/126071124794) | 2,184 | 12 |
| [macOS arm64](https://github.com/mingley/partitionline/actions/runs/41679536975/job/126071124484) | 2,179 | 17 |

The Kafka 4.2.1 and 4.3.1 compatibility jobs logged the classic-join marker,
then stack overflow before the next poll marker. This does not identify the
failing frame. The 4.1.2 job was canceled during checkout. Format, Clippy,
documentation and package jobs passed before cancellation.

The separate [broker workflow](https://github.com/mingley/partitionline/actions/runs/41679536760)
passed. Its default and all-feature all-target commands reported 734 and 882
tests passed, respectively; the separate doctest commands found no tests. The
[capabilities workflow](https://github.com/mingley/partitionline/actions/runs/41679536759)
also concluded successfully. Its archives were not inspected for this report.

## Classic join with the closer caller

The [original-caller witness](https://github.com/mingley/partitionline/actions/runs/41679536852)
used client source `45cbff24de76490594970fc962cd04e7be9fafb1` and ran once
per arm on hosted x86_64. The normal and library-box arms each reported
`live_test_stack_overflow` and exit 101 in the supervisor receipt printed in
the job log. Their receipt says the caller and live-test source were unchanged
and the helper would terminate the process immediately after a successful
classic join, before later scenarios. Neither result qualifies the full test.

Both receipts reported matching original and guarded unpolled future sizes and
alignments: caller 71,808 bytes/alignment 16 and helper 7,952 bytes/alignment
8. Equality does not prove identical internal layout, generated code, poll
stack, timing or behavior. The earlier shortened caller's library-box return
therefore does not establish a fix.

The job logs and artifact metadata were inspected; artifact archives were not
downloaded or independently verified. The [baseline artifact](https://github.com/mingley/partitionline/actions/runs/41679536852/artifacts/13252477127)
has reported SHA-256 `5e7dd362162659733a0ad7ee802321c673562832aba9d217c18c202fb4bfe547`;
the [library-box artifact](https://github.com/mingley/partitionline/actions/runs/41679536852/artifacts/13251163961)
has reported SHA-256 `98e6321dbb43d2069bb7ca78251768f7fe5f1e3e55fda7eba5dd78cd3e1a01e2`.
The failure frame, actual stack mapping, full compatibility and a repair remain
unqualified.
