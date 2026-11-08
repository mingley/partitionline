# Main CI snapshot: `342e7b9`

The completed runs below tested main commit
[`342e7b90ee63d6824be2652b65d71be483ea6d16`](https://github.com/mingley/partitionline/commit/342e7b90ee63d6824be2652b65d71be483ea6d16).
They include the producer timestamp and in-memory quota clock changes. This is
not release qualification.

## Client

[Client CI](https://github.com/mingley/partitionline/actions/runs/42490443902)
finished with 18 successful jobs, nine failed and one skipped.

| Check | Result |
| --- | --- |
| Linux default library | 2,290 passed, 21 failed; later targets did not run. |
| Linux all-feature library | 2,301 passed, 10 failed; later targets did not run. |
| Windows default library | 2,297 passed, 14 failed. |
| macOS arm64 default library | 2,297 passed, 14 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 3.9.1 and 4.1.0 smoke | Both jobs failed. |
| Live conformance | Skipped. |

Resource soak, format, Clippy, audit, deny, docs, package, release rehearsal,
auth and integrity smoke, configured performance and latency gates, and fuzz
smoke were among the successful jobs. A successful configured gate does not
establish a production-readiness or comparative performance claim. This
snapshot does not assign causes to the failed jobs.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42490443978)
passed 794 default and 942 all-feature behavioral tests, with four ignored in
each profile. Format, strict Clippy, Rustdoc and doctest commands passed;
API inventory and fuzz-supervision jobs passed. This does not explain earlier
[Raft failures](../main-ci-464-20261108/README.md).

Both [client capability profiles](https://github.com/mingley/partitionline/actions/runs/42490443962)
passed their selected checks. Their retained archives were not independently
inspected.
