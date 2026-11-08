# Main CI snapshot: `0c9ab1e`

Snapshot recorded 8 November 2026 at 16:25 UTC for main commit
[`0c9ab1e712a043a2acc5fa56963cff2f3e5ccc91`](https://github.com/mingley/partitionline/commit/0c9ab1e712a043a2acc5fa56963cff2f3e5ccc91).
Changes since `d314151` are documentation and evidence only. This is not
release qualification.

## Client

At the snapshot, [client CI](https://github.com/mingley/partitionline/actions/runs/42498614832)
had 18 successful jobs, eight failed, one running and one skipped. The linked
run has any later result.

| Check | Result when recorded |
| --- | --- |
| Linux default library | 2,307 passed, 10 failed; later targets did not run. |
| Linux all-feature library | 2,299 passed, 18 failed; later targets did not run. |
| Windows default library | 2,303 passed, 14 failed. |
| macOS arm64 default library | 2,308 passed, nine failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 4.1.0 smoke | Failed. |
| Kafka 3.9.1 smoke | Still running. |
| Live conformance | Skipped. |

Successful jobs included resource soak, allocation, instruction-count gate,
format, all-target/all-feature Clippy, audit, deny, docs, package, release
rehearsal, auth and integrity smoke. The instruction counter reported
`BASELINE_MISSING` for the tested Rust version and did not compare against a
baseline. This snapshot does not assign causes to the failed jobs.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42498614854)
stopped in its default-feature `raft_runtime` integration executable at
729 passed, one failed and four ignored across 27 executables. Later default
targets, the all-feature suite and strict checks in that job did not run.
API inventory and fuzz-supervision jobs passed separately. The cause of the
failure is unknown; an [earlier broker pass](../main-ci-a374-20261108/README.md)
does not explain it.

Both [client capability profiles](https://github.com/mingley/partitionline/actions/runs/42498614858)
passed their selected checks. Their retained archives were not independently
inspected.
