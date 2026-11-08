# Main CI snapshot: `f5edca5`

Snapshot recorded 8 November 2026 at 14:32 UTC for main commit
[`f5edca564b5481eb9d66c105ea4b13b3877e092c`](https://github.com/mingley/partitionline/commit/f5edca564b5481eb9d66c105ea4b13b3877e092c).
It includes the in-memory quota clock fix. Some jobs were still running at the
snapshot; this is not a release qualification.

## Client

At the snapshot, [client CI](https://github.com/mingley/partitionline/actions/runs/42489384525)
had 18 successful jobs, six failed, three running and one skipped.

| Check | Result when recorded |
| --- | --- |
| Linux default library | 2,299 passed, 11 failed; later targets did not run. |
| Linux all-feature library | 2,300 passed, 10 failed; later targets did not run. |
| Windows default library | 2,300 passed, 10 failed. |
| macOS arm64 default library | Still running. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 3.9.1 and 4.1.0 smoke | Both still running. |
| Live conformance | Skipped. |

Successful jobs included resource soak, format, strict all-target/all-feature
Clippy, audit, deny, documentation, package, release rehearsal, auth and
integrity smoke, configured performance and latency gates, and fuzz smoke.
The snapshot does not assign the causes of failed jobs.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42489384442)
passed 794 default and 942 all-feature behavioral tests, with four ignored in
each profile. Strict format, Clippy, Rustdoc and doctest commands passed;
the separate API inventory and fuzz-supervision jobs passed. This pass does
not explain the earlier [Raft failures](../main-ci-464-20261108/README.md).

At the snapshot, the [all-feature capability job](https://github.com/mingley/partitionline/actions/runs/42489384447)
had passed and the default job was still running. Retained archives were not
independently inspected.
