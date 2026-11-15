# Main CI snapshot: `97a0757`

This records completed workflows for main commit
[`97a0757a3e5ab8777b147876305ab854b1faf675`](https://github.com/mingley/partitionline/commit/97a0757a3e5ab8777b147876305ab854b1faf675)
on 8 November 2026. It includes the requested-name Admin validation. These
results do not qualify a release.

## Client

[Client CI](https://github.com/mingley/partitionline/actions/runs/42487682029)
finished with 18 successful jobs, nine failed and one skipped.

| Check | Result |
| --- | --- |
| Linux default library | 2,301 passed, nine failed; later targets did not run. |
| Linux all-feature library | 2,290 passed, 20 failed; later targets did not run. |
| Windows default library | 2,296 passed, 14 failed. |
| macOS arm64 default library | 2,295 passed, 15 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 3.9.1 and 4.1.0 smoke | Both jobs failed. |
| Live conformance | Skipped. |

The successful jobs included resource soak, format, strict all-target and
all-feature Clippy, audit, deny, documentation, package, release rehearsal,
auth and integrity smoke, configured performance and latency gates, and fuzz
smoke. These successes do not resolve the failed jobs; this snapshot does not
assign their causes.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42487682111)
passed 781 default and 929 all-feature behavioral tests, with four ignored in
each profile. Strict format, Clippy, Rustdoc, and doctest commands passed;
the separate API inventory and fuzz-supervision jobs passed. This pass does
not explain the earlier [Raft failures](../main-ci-464-20261108/README.md).

Both selected [capability profiles](https://github.com/mingley/partitionline/actions/runs/42487682042)
passed. Their retained archives were not independently inspected.
