# Main CI at 669f5b1

Recorded 9 November 2026 for main commit `669f5b1e58103530f2a75020444d3148a9735f7e`. These results do not qualify production use or establish a performance ranking.

## Client

The [client workflow](https://github.com/mingley/partitionline/actions/runs/42697106790) completed with 17 successful jobs, ten failed jobs and one skipped job.

| Check | Result |
| --- | --- |
| Linux default library | 2,312 passed, 16 failed; later targets did not run. |
| Linux all-feature library | 2,313 passed, 15 failed; later targets did not run. |
| Windows default library | 2,306 passed, 22 failed; later runtime and package steps did not run. |
| macOS arm64 default library | 2,317 passed, 11 failed; later runtime and package steps did not run. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 | All three current compatibility jobs failed. |
| Kafka 3.9.1 and 4.1.0 | Both smoke jobs failed. |
| Schema, docs and package jobs | Passed, including the extracted-package documentation checks. |
| Resource-soak driver controls | Setup failed before any tests ran because the OS denied a `/proc` access. No soak campaign completed. |
| Instruction-count gate | Passed without a comparison: no baseline existed for x86_64 Rust 1.99.1. |
| Explicit live-conformance lane | Skipped. |

The format, Clippy, dependency policy and audit, allocation-budget, authentication smoke, fixture, integrity smoke, producer-ID, zstd, latency, fuzz-smoke and release-rehearsal jobs also passed. Passing jobs do not qualify failed or unrun cells.

The resource-soak denial was not worked around or retried in a separate targeted run. The earlier 11 short controls at [71c847f](../main-ci-71c-20261109/README.md) passed, but do not explain or erase this failure.

## Broker and capabilities

The [broker workflow](https://github.com/mingley/partitionline/actions/runs/42697106801) failed its default behavioral step after 580 passed, one failed and four ignored across the targets reached. Its all-feature behavioral, strict Clippy, Rustdoc and doctest steps were skipped. Pinned inventory and fuzz supervision passed separately. The cause of the default failure is unknown; the earlier pass at `71c847f` does not explain it.

Both [client capability jobs](https://github.com/mingley/partitionline/actions/runs/42697107675) passed. Their uploaded archives were not independently inspected.

The [previous complete record](../main-ci-71c-20261109/README.md) retains the preceding results.
