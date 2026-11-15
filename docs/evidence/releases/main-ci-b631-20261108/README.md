# Main CI at b631809

The [main CI run](https://github.com/mingley/partitionline/actions/runs/42441209189)
completed on 8 November 2026 for
[`b631809f0e03ba6f65f808e51d8e47e3fd9380d1`](https://github.com/mingley/partitionline/commit/b631809f0e03ba6f65f808e51d8e47e3fd9380d1).
It failed. Results do not cover later changes.

| Client check | Result |
| --- | --- |
| [Linux default library](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627561) | 2,190 passed, 17 failed; all-target testing stopped at the library. |
| [Windows default library](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627935) | 2,193 passed, 14 failed. |
| [macOS arm64 default library](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627361) | 2,183 passed, 24 failed. |
| [All-feature lane](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627776) | Stopped at all-target Clippy; its tests did not run. |
| [Strict all-target Clippy](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627588) | Failed on a loop-style lint in a newly added coordinator mock. The later [test-only correction](https://github.com/mingley/partitionline/actions/runs/42441786927) passed strict Clippy and 12 selected tests. |

The default failure identities include timing-sensitive and socket lifecycle
cases; the logs do not establish a shared cause. Required Kafka
[4.1.2](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627844),
[4.2.1](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627762)
and [4.3.1](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627878)
compatibility jobs overflowed after classic join began. The
[3.9.1](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627779)
and [4.1.0](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627767)
smoke jobs failed after repeated EOS timeouts; the cause is unknown. Live
conformance was skipped.

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42441209278)
failed in its default library with 446 passes and four Raft runtime failures.
Three returned `NotLeader` after a leader was selected from observed state;
the fourth failed an assertion with 5 versus 4. Their causes are under
investigation. Later default targets and all-feature broker tests did not run.
The independent broker inventory and short fuzz-supervision jobs passed.

Both [client capability profiles](https://github.com/mingley/partitionline/actions/runs/42441209134)
passed their selected checks; their archives were not independently inspected.
The main run also passed format, docs, package, audit, dependency policy,
schema, fixtures, auth and integrity smoke, short fuzz, resource-driver,
allocation, latency and performance gates. These do not establish production
readiness or comparative Kafka performance.
