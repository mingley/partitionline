# Main CI at 731eb97

Recorded 9 November 2026 for commit `731eb97a08445a18da0b043986ae192397ba176a`. Results are limited to the linked workflows; they do not establish production readiness or a performance ranking.

## Client

The [client workflow](https://github.com/mingley/partitionline/actions/runs/42676850879) completed with 18 successful jobs, nine failed jobs, and one skipped job.

| Check | Result |
| --- | --- |
| Linux default library | 2,311 passed, 17 failed; later targets did not run. |
| Linux all-feature library | 2,311 passed, 17 failed; later targets did not run. |
| Windows default library | 2,312 passed, 16 failed; later runtime and package steps did not run. |
| macOS arm64 default library | 2,315 passed, 13 failed; later runtime and package steps did not run. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 | All three current compatibility jobs failed. |
| Kafka 3.9.1 and 4.1.0 | Both smoke jobs failed. |
| Short resource driver controls | Eleven tests passed; this is not a sustained fault or continuity campaign. |
| Instruction-count gate | Passed without a comparison: no baseline existed for x86_64 Rust 1.99.1. |
| Explicit live-conformance lane | Skipped. |

The format, Clippy, dependency policy and audit, schema, package, documentation, allocation-budget, authentication smoke, fixture, integrity smoke, producer-ID, zstd, latency, fuzz-smoke and release-rehearsal jobs passed. A passing job does not establish coverage of unrun or failed cells.

## Broker and capabilities

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42676850904) passed its behavioral, pinned API inventory, and fuzz-supervision jobs. Behavioral checks include default and all-feature broker targets, strict checks and documentation; both profiles still have four ignored tests.

Both [client capability jobs](https://github.com/mingley/partitionline/actions/runs/42676851043) passed. Their uploaded archives were not independently inspected.

The [previous complete record](../main-ci-a7b6-20261109/README.md) and [partial record](../main-ci-0bed-partial-20261109/README.md) retain earlier results. Later passes do not explain earlier failures.
