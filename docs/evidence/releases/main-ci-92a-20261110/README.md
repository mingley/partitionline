# Main CI snapshot: 92a365f

This snapshot covers main commit
[`92a365f2829c00438b6c58841dbb0990fca480d6`](https://github.com/mingley/partitionline/commit/92a365f2829c00438b6c58841dbb0990fca480d6).
It is not release qualification.

The [client workflow](https://github.com/mingley/partitionline/actions/runs/42864395852)
completed with 18 successful jobs, nine failures and one skipped job.

| Check | Result |
| --- | --- |
| Linux default client library | 2,314 passed, 14 failed; later targets did not run. |
| Linux all-feature client library | 2,313 passed, 15 failed; later targets and no-default step did not run. |
| Windows default client library | 2,303 passed, 25 failed; later runtime and package steps did not run. |
| macOS arm64 default client library | 2,317 passed, 11 failed; later runtime and package steps did not run. |
| Kafka compatibility | 4.1.2, 4.2.1 and 4.3.1 profiles and the 3.9.1 and 4.1.0 smoke jobs failed. |
| Resource-soak CI job | Passed its finite CI job. No 24-hour campaign was completed. |
| Schema, format, Clippy, audit, dependency policy, docs, package, auth and integrity smoke | Passed. |
| Instruction-count gate | No baseline for `x86_64-1.99.1`; no regression comparison. |

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42864396387)
stopped in the default library after 495 passed, one failed and four ignored.
Later broker targets, all-feature behavior and strict checks did not run.
Inventory and fuzz supervision passed in separate jobs. The failure's cause
is unknown. Both selected [client capability profiles](https://github.com/mingley/partitionline/actions/runs/42864396206)
passed; their uploaded archives were not independently inspected.

The [preceding complete snapshot](../main-ci-199-20261109/README.md) passed
broker checks but also failed client and Kafka jobs. Its broker pass does not
explain this failure. An [earlier snapshot](../main-ci-669-20261109/README.md)
had a broker failure and an OS-denied `/proc` access; later passes do not
explain those failures. No sustained fault or continuity campaign is credited.
