# Main CI snapshot: `464d33d5`

This records completed workflows for main commit
[`464d33d5afe668d46274972cd3baa4e870159bf8`](https://github.com/mingley/partitionline/commit/464d33d5afe668d46274972cd3baa4e870159bf8)
on 8 November 2026. It includes the Admin row checkpoint. These results do
not qualify a release.

## Client

[Client CI](https://github.com/mingley/partitionline/actions/runs/42484165402)
finished with 18 successful jobs, nine failed and one skipped.

| Check | Result |
| --- | --- |
| Linux default library | 2,259 passed, 17 failed; later targets did not run. |
| Linux all-feature library | 2,262 passed, 14 failed; later targets did not run. |
| Windows default library | 2,256 passed, 20 failed. |
| macOS arm64 default library | 2,262 passed, 14 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 3.9.1 and 4.1.0 smoke | Both jobs failed. |
| Live conformance | Skipped. |

The successful jobs included resource soak, format, strict all-target and
all-feature Clippy, audit, deny, documentation, package, release rehearsal,
auth and integrity smoke, the configured performance and latency gates, and
fuzz smoke. These successes do not resolve the failed jobs. This snapshot
does not assign a cause to them.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42484165381)
stopped its default-feature library with 489 passed, two failed and four
ignored. The failed tests were
`raft::runtime::tests::actual_five_peer_tcp_observer_multichunk_image_install_reopen_and_join`
and
`raft::runtime::tests::actual_three_peer_tcp_observer_multichunk_image_install_reopen_and_join`.
Later default executables, the all-feature profile and later strict checks in
that job did not run. The separately scheduled API inventory and fuzz
supervision jobs passed. The causes are not established. The previous
[complete broker pass](../main-ci-426-20261108/README.md) and the
[intervening partial failure](../main-ci-536-partial-20261108/README.md)
do not explain these results.

Both selected [capability profiles](https://github.com/mingley/partitionline/actions/runs/42484165423)
passed. Their retained archives were not independently inspected.
