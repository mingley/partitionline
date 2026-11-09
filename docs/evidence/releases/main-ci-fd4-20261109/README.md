# Main CI snapshot: fd4fd96

This snapshot covers main commit
[`fd4fd96e3bda329eef213180e4c38a9047d8a361`](https://github.com/mingley/partitionline/commit/fd4fd96e3bda329eef213180e4c38a9047d8a361).
It is not release qualification.

The [client workflow](https://github.com/mingley/partitionline/actions/runs/42703467853)
completed with 18 successful jobs, nine failures and one skipped job.

| Check | Result |
| --- | --- |
| Linux default client library | 2,307 passed, 21 failed; later targets did not run. |
| Linux all-feature client library | 2,307 passed, 21 failed; later targets and no-default step did not run. |
| Windows default client library | 2,307 passed, 21 failed; later runtime and package steps did not run. |
| macOS arm64 default client library | 2,314 passed, 14 failed; later runtime and package steps did not run. |
| Kafka compatibility | 4.1.2, 4.2.1 and 4.3.1 profiles and the 3.9.1 and 4.1.0 smoke jobs failed. |
| Resource-soak CI job | Passed its finite CI job. No 24-hour campaign was completed. |
| Schema, format, Clippy, audit, dependency policy, docs, package, auth and integrity smoke | Passed. |
| Instruction-count gate | No baseline for `x86_64-1.99.1`; no regression comparison. |

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42703467817)
passed 814 default and 962 all-feature behavioral tests, with four ignored in
each profile, followed by strict Clippy, rustdoc and doctest checks. Inventory
and fuzz supervision passed in separate jobs. Both selected
[client capability profiles](https://github.com/mingley/partitionline/actions/runs/42703467992)
passed; their uploaded archives were not independently inspected.

The [preceding complete snapshot](../main-ci-669-20261109/README.md) stopped
the broker default suite after 580 passes, one failure and four ignored, and
the resource-soak job failed setup when the OS denied a `/proc` access. The
later passes do not explain either earlier failure. No sustained fault or
continuity campaign is credited by this snapshot.
