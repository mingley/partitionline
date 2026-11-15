# Main CI snapshot: `536d5e9`

Snapshot taken 8 November 2026 while the Kafka 3.9.1 smoke job was still
running. The tested commit is
[`536d5e938aa2ad123471fe6446b0ba406dabe8cd`](https://github.com/mingley/partitionline/commit/536d5e938aa2ad123471fe6446b0ba406dabe8cd),
which changed documentation after the preceding
[complete snapshot](../main-ci-426-20261108/README.md). These results are
not a release qualification.

## Client

At the snapshot, [client CI](https://github.com/mingley/partitionline/actions/runs/42480984868)
had 18 successful jobs, eight failed, one running and one skipped.

| Check | Result |
| --- | --- |
| Linux default library | 2,255 passed, 17 failed; later targets did not run. |
| Linux all-feature library | 2,255 passed, 17 failed; later targets did not run. |
| Windows default library | 2,257 passed, 15 failed. |
| macOS arm64 default library | 2,261 passed, 11 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 4.1.0 smoke | Failed. |
| Kafka 3.9.1 smoke | Still running when recorded. |
| Live conformance | Skipped. |

The Kafka 3.9.1 smoke was later canceled, so it has no result. The completed
client run had 18 successful jobs, eight failed, one canceled and one skipped.

The successful jobs included resource soak, format, strict all-target and
all-feature Clippy, audit, deny, documentation, package, release rehearsal,
auth and integrity smoke, the configured performance and latency gates, and
fuzz smoke. Their success does not resolve the failed jobs. This snapshot does
not assign a cause to their failures.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42480984874)
passed 781 default-feature tests with four ignored. Its all-feature library
stopped with 534 passed, one failed and four ignored: the failed test was
`raft::runtime::tests::actual_five_node_tcp_minority_partition_late_ack_image_restart_and_removal`.
Later all-feature executables and later strict checks in that job did not run.
The separately scheduled API inventory and fuzz-supervision jobs passed.
The cause of the Raft failure is not established; the preceding passing
broker run does not explain it.

Both selected [capability profiles](https://github.com/mingley/partitionline/actions/runs/42480984872)
passed. Their retained archives were not independently inspected.
