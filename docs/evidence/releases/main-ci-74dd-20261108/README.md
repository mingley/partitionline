# Main CI snapshot: `74dd7f7`

This records completed workflows for main commit
[`74dd7f7c9b2430b308142678bf9534c385f0ff04`](https://github.com/mingley/partitionline/commit/74dd7f7c9b2430b308142678bf9534c385f0ff04)
on 8 November 2026. It has the same production source as
[`464d33d`](../main-ci-464-20261108/README.md) and later documentation.
These results do not qualify a release.

## Client

[Client CI](https://github.com/mingley/partitionline/actions/runs/42485449700)
finished with 18 successful jobs, nine failed and one skipped.

| Check | Result |
| --- | --- |
| Linux default library | 2,261 passed, 15 failed; later targets did not run. |
| Linux all-feature library | 2,265 passed, 11 failed; later targets did not run. |
| Windows default library | 2,262 passed, 14 failed. |
| macOS arm64 default library | 2,266 passed, 10 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 3.9.1 and 4.1.0 smoke | Both jobs failed. |
| Live conformance | Skipped. |

The successful jobs included resource soak, format, strict all-target and
all-feature Clippy, audit, deny, documentation, package, release rehearsal,
auth and integrity smoke, configured performance and latency gates, and fuzz
smoke. These successes do not resolve the failed jobs; this snapshot does not
assign their causes.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42485449664)
passed 781 default and 929 all-feature behavioral tests, with four ignored in
each profile. Strict format, Clippy, Rustdoc, and doctest commands passed;
the separate API inventory and fuzz-supervision jobs passed. The broker pass
does not explain the preceding [two Raft failures](../main-ci-464-20261108/README.md).

Both selected [capability profiles](https://github.com/mingley/partitionline/actions/runs/42485449645)
passed. Their retained archives were not independently inspected.
