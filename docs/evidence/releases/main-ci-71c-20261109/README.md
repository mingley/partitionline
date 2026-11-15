# Main CI at 71c847f

Recorded 9 November 2026 for main commit `71c847f74bab5384ab614e1486b0dc4c7c893bef`. These results do not qualify production use or establish a performance ranking.

## Client

The [client workflow](https://github.com/mingley/partitionline/actions/runs/42691334223) completed with 18 successful jobs, nine failed jobs and one skipped job.

| Check | Result |
| --- | --- |
| Linux default library | 2,309 passed, 19 failed; later targets did not run. |
| Linux all-feature library | 2,305 passed, 23 failed; later targets did not run. |
| Windows default library | 2,313 passed, 15 failed; later runtime and package steps did not run. |
| macOS arm64 default library | 2,320 passed, eight failed; later runtime and package steps did not run. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 | All three current compatibility jobs failed. |
| Kafka 3.9.1 and 4.1.0 | Both smoke jobs failed. |
| Schema, docs and package jobs | Passed. The docs job compiled the public Markdown and operator examples from the extracted package. |
| Short resource driver controls | Eleven tests passed; this is not a sustained fault or continuity campaign. |
| Instruction-count gate | Passed without a comparison: no baseline existed for x86_64 Rust 1.99.1. |
| Explicit live-conformance lane | Skipped. |

The format, Clippy, dependency policy and audit, allocation-budget, authentication smoke, fixture, integrity smoke, producer-ID, zstd, latency, fuzz-smoke and release-rehearsal jobs also passed. Passing jobs do not qualify failed or unrun cells.

## Broker and capabilities

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42691334136) passed its behavioral, pinned API inventory and fuzz-supervision jobs. Across the default broker targets, 814 tests passed and four were ignored; the all-feature targets passed 962 and ignored four. Format, strict Clippy, Rustdoc and doctests passed. This does not explain the [preceding default-suite failure](../main-ci-f1be-20261109/README.md).

Both [client capability jobs](https://github.com/mingley/partitionline/actions/runs/42691333891) passed. Their uploaded archives were not independently inspected.

A separate [documentation check](https://github.com/mingley/partitionline/actions/runs/42691570975) checked 14 repository documents and 76 local links, 20 Markdown controls, and the extracted crate's 30 documents, 229 local links, 80 pinned targets, license and exclusions. It did not fetch external links.
