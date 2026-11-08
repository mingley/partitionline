# Main CI at 8b5de2e

The [main CI run](https://github.com/mingley/partitionline/actions/runs/42439941604)
completed on 8 November 2026 for
[`8b5de2e2cfe5f3164a5f89546804ab36c1771ade`](https://github.com/mingley/partitionline/commit/8b5de2e2cfe5f3164a5f89546804ab36c1771ade).
It failed. These results do not cover later changes.

| Client library | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| [Linux default](https://github.com/mingley/partitionline/actions/runs/42439941604/job/128606889257) | 2,181 | 18 | 0 |
| [Linux all features](https://github.com/mingley/partitionline/actions/runs/42439941604/job/128606889618) | 2,181 | 18 | 0 |
| [Windows default](https://github.com/mingley/partitionline/actions/runs/42439941604/job/128606889343) | 2,184 | 15 | 0 |
| [macOS arm64 default](https://github.com/mingley/partitionline/actions/runs/42439941604/job/128606889315) | 2,185 | 14 | 0 |

Each all-target command stopped at the failing library. Failure identities
differ across jobs. The logs include timing-sensitive and socket lifecycle
failures; they do not establish a common cause.

The required Kafka [4.1.2](https://github.com/mingley/partitionline/actions/runs/42439941604/job/128606889387),
[4.2.1](https://github.com/mingley/partitionline/actions/runs/42439941604/job/128606889362)
and [4.3.1](https://github.com/mingley/partitionline/actions/runs/42439941604/job/128606889261)
compatibility jobs overflowed after the classic-join marker. The Kafka
[3.9.1](https://github.com/mingley/partitionline/actions/runs/42439941604/job/128606889265)
and [4.1.0](https://github.com/mingley/partitionline/actions/runs/42439941604/job/128606889391)
smoke jobs failed after repeated EOS example timeouts; their cause is unknown.
The separate live-conformance job was skipped.

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42439941625)
failed in its default Fetch integration suite: 25 tests passed and
`unpolled_snapshot_holds_combined_admission_and_cancel_releases` returned
`waiter completed prematurely`. Its library had passed 450 tests; remaining
targets and the all-feature broker suite did not run. The cause is under
investigation. The separate broker inventory and short fuzz-supervision jobs
passed. The earlier broker run at `f5ba3420e` passed 737 default and 885
all-feature tests, Clippy, docs, inventory and short fuzz supervision; both
doctest commands found zero tests.

Both [client capability profiles](https://github.com/mingley/partitionline/actions/runs/42439941613)
passed their selected checks. Their archives were not independently inspected
for this record. The main run passed format, strict Clippy, docs, package,
audit, dependency policy, schema, fixtures, auth and integrity smoke,
short fuzz, resource-driver, allocation, latency and performance gates. These
do not establish a production release or comparative Kafka performance.
