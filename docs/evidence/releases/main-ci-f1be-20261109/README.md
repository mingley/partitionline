# Main CI at f1be305

Recorded 9 November 2026 for main commit `f1be3052aac5ec70a424ac951fe1189c27a31417`. These results do not qualify production use or establish a performance ranking.

## Client

The [client workflow](https://github.com/mingley/partitionline/actions/runs/42686614975) completed with 18 successful jobs, nine failed jobs, and one skipped job.

| Check | Result |
| --- | --- |
| Linux default library | 2,309 passed, 19 failed; later targets did not run. |
| Linux all-feature library | 2,315 passed, 13 failed; later targets did not run. |
| Windows default library | 2,310 passed, 18 failed; later runtime and package steps did not run. |
| macOS arm64 default library | 2,315 passed, 13 failed; later runtime and package steps did not run. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 | All three current compatibility jobs failed. |
| Kafka 3.9.1 and 4.1.0 | Both smoke jobs failed. |
| Schema, docs and package jobs | Passed, including the shipped Registry HTTP reader. |
| Short resource driver controls | Eleven tests passed; this is not a sustained fault or continuity campaign. |
| Instruction-count gate | Passed without a comparison: no baseline existed for x86_64 Rust 1.99.1. |
| Explicit live-conformance lane | Skipped. |

The format, Clippy, dependency policy and audit, allocation-budget, authentication smoke, fixture, integrity smoke, producer-ID, zstd, latency, fuzz-smoke and release-rehearsal jobs also passed. Passing jobs do not qualify failed or unrun cells.

## Broker and capabilities

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42686615152) failed its default-feature behavioral step after 579 passes, two failures and four ignored tests across the targets reached. The all-feature behavioral suite, strict Clippy and Rustdoc in that job did not run. The separate pinned inventory and fuzz-supervision jobs passed. The cause of the behavioral failure is unknown; the earlier pass at `731eb97` does not explain it.

Both [client capability jobs](https://github.com/mingley/partitionline/actions/runs/42686615097) passed. Their uploaded archives were not independently inspected.

The [previous complete record](../main-ci-731-20261109/README.md) retains the preceding results.
