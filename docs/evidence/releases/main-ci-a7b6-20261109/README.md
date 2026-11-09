# Main CI: a7b6edc

9 November 2026

This snapshot covers main commit
`a7b6edc1645e3a4789cf699a3b9e7656c55a3d10`. The complete
[client workflow](https://github.com/mingley/partitionline/actions/runs/42669271449)
had 18 successful jobs, nine failures and one skipped job. These results do
not qualify the project for production or establish a performance ranking.

## Client and Kafka

| Check | Result |
| --- | --- |
| Linux default library | 2,309 passed, 19 failed; later targets did not run. |
| Linux all-feature library | 2,318 passed, ten failed; later targets did not run. |
| Windows default library | 2,314 passed, 14 failed. |
| macOS arm64 default library | 2,320 passed, eight failed. |
| Apache Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Apache Kafka 3.9.1 and 4.1.0 smoke | Both jobs failed. |
| Resource-soak driver | Built; 11 short tests passed. This was not a sustained campaign. |
| Instruction-count gate | Ran without an x86_64 Rust 1.99.1 baseline; no regression comparison. |

Client format, all-target Clippy, audit, dependency policy, documentation,
package, authentication and integrity smoke, allocation, latency, schema,
fuzz supervision, fixtures, zstd, InitProducerId and release rehearsal jobs
passed. The live conformance job was skipped. A passing release rehearsal is
not a publication or a release qualification.

## Broker and capability profiles

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42669271457)
passed 814 default-feature and 962 all-feature tests, with four ignored in
each profile. Formatting, all-feature Clippy and rustdoc, doctests, the pinned
API inventory and fuzz supervision passed.

Both selected [client capability profiles](https://github.com/mingley/partitionline/actions/runs/42669271385)
passed. Their archives were not independently inspected.

A separate [combined-head qualification](../../ci/combined-a7b6-20261109/README.md)
passed focused Admin, producer and transport tests in both feature profiles,
strict client checks and packed documentation checks. It did not run the
broad client suites or live Kafka profiles listed above.

The preceding documentation-only `ba893560` [broker run](https://github.com/mingley/partitionline/actions/runs/42667809972)
stopped in the default library after 492 passes, one failure and four ignored.
The cause is unknown; its later all-feature and strict checks did not run.
The later pass does not explain that failure.
