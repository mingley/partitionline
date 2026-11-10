# Main CI snapshot: 19979b3

This snapshot covers main commit
[`19979b3a22e56b37a1a03d60fe8f67b746a1f047`](https://github.com/mingley/partitionline/commit/19979b3a22e56b37a1a03d60fe8f67b746a1f047).
It is not release qualification.

The [client workflow](https://github.com/mingley/partitionline/actions/runs/42705987742)
completed with 18 successful jobs, nine failures and one skipped job.

| Check | Result |
| --- | --- |
| Linux default client library | 2,317 passed, 11 failed; later targets did not run. |
| Linux all-feature client library | 2,318 passed, 10 failed; later targets and no-default step did not run. |
| Windows default client library | 2,316 passed, 12 failed; later runtime and package steps did not run. |
| macOS arm64 default client library | 2,317 passed, 11 failed; later runtime and package steps did not run. |
| Kafka compatibility | 4.1.2, 4.2.1 and 4.3.1 profiles and the 3.9.1 and 4.1.0 smoke jobs failed. |
| Resource-soak CI job | Passed its finite CI job. No 24-hour campaign was completed. |
| Schema, format, Clippy, audit, dependency policy, docs, package, auth and integrity smoke | Passed. |
| Instruction-count gate | No baseline for `x86_64-1.99.1`; no regression comparison. |

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42705987960)
passed 814 default and 962 all-feature behavioral tests, with four ignored in
each profile, followed by strict Clippy, rustdoc and doctest checks. Inventory
and fuzz supervision passed in separate jobs. Both selected
[client capability profiles](https://github.com/mingley/partitionline/actions/runs/42705987666)
passed; their uploaded archives were not independently inspected.

The [preceding complete snapshot](../main-ci-fd4-20261109/README.md) also
failed client and Kafka checks and passed the broker. The earlier
[669 snapshot](../main-ci-669-20261109/README.md) stopped the broker default
suite after 580 passes, one failure and four ignored, and its resource-soak job
failed setup when the OS denied a `/proc` access. Later passes do not explain
those failures. No sustained fault or continuity campaign is credited.
