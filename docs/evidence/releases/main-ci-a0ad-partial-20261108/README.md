# Main CI snapshot: `a0ad79d`

The runs below tested main commit
[`a0ad79df8a5d9872eacd1cd322b59d8d979f04e3`](https://github.com/mingley/partitionline/commit/a0ad79df8a5d9872eacd1cd322b59d8d979f04e3),
which includes the Admin slot index. Main later advanced; canceled jobs have no
result. This is not release qualification.

## Client

[Client CI](https://github.com/mingley/partitionline/actions/runs/42492157950)
finished with 16 successful jobs, six failed, five canceled and one skipped.

| Check | Result |
| --- | --- |
| Linux default library | 2,306 passed, 11 failed; later targets did not run. |
| Linux all-feature, Windows and macOS libraries | Canceled; no result. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 3.9.1 and 4.1.0 smoke | Both canceled; no result. |
| Instruction and allocation gates | Both failed because `benchmarks/codec/Cargo.lock` needed an update with `--locked`; the measurements did not run. |
| Live conformance | Skipped. |

Resource soak, format, Clippy, audit, deny, docs, package, release rehearsal,
auth and integrity smoke, the configured latency gate, and fuzz smoke were
among the successful jobs. The failed allocation and instruction gates are
not measurements of an allocation or speed regression. No cause is assigned
to the other failures.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42492157899)
passed 794 default behavioral tests with four ignored. The all-feature
library stopped with 534 passed, one failed and four ignored. Later broker
targets and strict checks in that job did not run. API inventory and fuzz
supervision passed in separate jobs. The cause of the failure is unknown.

Both [client capability profiles](https://github.com/mingley/partitionline/actions/runs/42492157914)
passed their selected checks. Their retained archives were not independently
inspected.
