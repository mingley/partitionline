# Main CI at 77021f7dc

GitHub Actions results for `77021f7dc6c0d19aa93c7bf0b7fad125ee632e23`,
retrieved on 30 October 2026. The main workflow still had one integrity job
running at retrieval. It had already failed, so this commit does not qualify
for release.

| Scope | Result |
| --- | --- |
| Client default library | 2,145 passed, 24 failed |
| Client all-feature library | 2,152 passed, 17 failed |
| Broker default all targets | 701 passed |
| Broker all features, stopped at `raft_runtime` | 710 passed, two failed |
| Client strict all-target, all-feature Clippy | Passed |
| Audit and dependency policy | Passed |
| Schema, documentation consumers, package consumers and publish dry run | Passed |
| Release recovery rehearsal | Passed; substituted publication commands |

The client commands stopped at the library failures; their later integration
targets did not run. The broker all-feature failures were the three-peer and
five-peer TCP observer histories. Its later Clippy, rustdoc and doctest steps
were skipped. The previous commit's complete broker passes remain in the
[earlier snapshot](../main-ci-20261030/README.md).

The audit reported zero vulnerabilities and zero warnings against advisory
database commit `2295466c4e9df26647fd67d8fa1c705ac2fc7f81`.
This result applies to the checked lockfile and database revision.

Other failures remain visible in the retained logs:

- Both capability lanes stopped on stale expected test counts. The retained
  all-feature artifact discovers 95 cases, including two ignored external
  server tests. [Selection controls](../../client/capability-selection-20261030/README.md)
  describe the gate repair; they do not supply new Rust runtime results.
- The broker inventory has 11 stale source pins. Instrumented fuzz targets
  build and lint, but the supervisor setup requires a cgroup v2 metadata file
  absent from the runner.
- The Java codec lane downloads LZ4 from the wrong Maven group and receives
  HTTP 404 before running its decoder checks.
- A live compatibility test aborts with stack overflow. Both older broker
  smoke lanes fail group join and transaction examples.
- Native macOS and Windows default library runs report respectively 15 and
  32 failures. Their later runtime and packaging steps were skipped.

`files.json` records the stored and reconstructed hashes of complete fetched
job logs, run/job API snapshots, and the selected capability artifact's manifest
and validation report. `summary.json` counts test-result blocks without
crediting skipped targets. The artifact ZIP remains available through GitHub
artifact 13006707295; its identity is also recorded in the selection receipt.

Sources: [client workflow](https://github.com/mingley/partitionline/actions/runs/41117187887),
[broker workflow](https://github.com/mingley/partitionline/actions/runs/41117187877),
[capability workflow](https://github.com/mingley/partitionline/actions/runs/41117187886).
