# Main CI at 3f45fe6

The [CI run](https://github.com/mingley/partitionline/actions/runs/42132787069) completed on 6 November 2026 for
[source commit 3f45fe6255e72abc6b0c4488bc13117779c50787](https://github.com/mingley/partitionline/commit/3f45fe6255e72abc6b0c4488bc13117779c50787).
The run failed. Results apply to that commit and the hosted runners used in
the linked jobs; they do not validate later changes.

## Client library

| Job | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| [Linux, default](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714003) | 2,180 | 16 | 0 |
| [Linux, all features](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714598) | 2,170 | 26 | 0 |
| [Windows, default](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714542) | 2,170 | 26 | 0 |
| [macOS arm64, default](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613713794) | 2,181 | 15 | 0 |

Each lane stopped after the library failed. Failure identities and assertion
details are in the job logs. They include socket and dial lifecycle tests and
timing-sensitive tests; the logs alone do not establish one common cause.

## Live Kafka

- Required compatibility jobs against [4.1.2](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714666),
  [4.2.1](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714366) and
  [4.3.1](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714485) aborted with stack overflow
  after the classic-join marker. The overflowing frame is unknown.
- Smoke jobs against [3.9.1](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714557) and
  [4.1.0](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714719) logged JoinGroup error 16,
  recovered on retry, then failed after six EOS example timeouts each. The
  timeout cause is unknown.
- The [authentication smoke](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714593) and
  [integrity smoke](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714491) failed to download
  Kafka from archive.apache.org and did not complete their live checks.
  Integrity checker controls passed 45 and 27 tests before the download
  failure. The separate live-conformance job was skipped.

## Passing scopes

- The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42132787053) passed 734 default and
  882 all-feature all-target tests, format, strict Clippy, rustdoc, the
  pinned Apache inventory and 55 negative controls, and short fuzz
  supervision. Both doctest commands found zero tests.
- The two [client capability jobs](https://github.com/mingley/partitionline/actions/runs/42132787088) passed their
  selected default and all-feature checks. Their archives were not
  independently inspected for this record; these results do not qualify the
  complete SDK runtime.
- The [resource-soak job](https://github.com/mingley/partitionline/actions/runs/42132787069/job/127613714356) passed 11
  driver controls. This is not a sustained soak campaign.
- Format, strict Clippy, audit, dependency policy, documentation, package,
  release rehearsal, schema, fixture, fuzz, latency and performance CI gates
  passed in the main run. Those gates do not establish comparative Kafka
  performance or release readiness.

The failed client and live-broker lanes, fault and continuity campaigns,
independent compatibility coverage and matched peer benchmarks remain open.
[Previous CI evidence](../main-ci-74923475-20261105/README.md) records an
earlier source snapshot.
