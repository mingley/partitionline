# Main CI snapshot: `a3748ca`

The completed runs below tested main commit
[`a3748caecae60a893de96c0700e1e5ab04854443`](https://github.com/mingley/partitionline/commit/a3748caecae60a893de96c0700e1e5ab04854443).
This includes the quota error mapping and interoperability peer changes. It is
not a release qualification.

## Client

[Client CI](https://github.com/mingley/partitionline/actions/runs/42495190892)
finished with 18 successful jobs, nine failed and one skipped.

| Check | Result |
| --- | --- |
| Linux default library | 2,299 passed, 18 failed; later targets did not run. |
| Linux all-feature library | 2,295 passed, 22 failed; later targets did not run. |
| macOS arm64 default library | 2,307 passed, 10 failed. |
| Windows default library | 2,302 passed, 15 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 4.1.0 smoke | Failed. |
| Kafka 3.9.1 smoke | Failed. |
| Live conformance | Skipped. |

The successful jobs included allocation and instruction-count gates, package,
resource soak, format, Clippy, audit, deny, documentation, auth and integrity
smoke. The instruction-count job reported `BASELINE_MISSING` for the tested
Rust version, so it did not compare instruction counts against a baseline.
A successful configured gate does not establish a comparative Kafka performance
claim. The causes of the failed jobs are not assigned here.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42495190842)
passed 806 default and 954 all-feature behavioral tests, with four ignored in
each profile. Format, strict Clippy, Rustdoc and doctest commands passed;
API inventory and fuzz-supervision jobs passed. This pass does not explain the
[earlier full-suite failures](../../broker/quota-wire-boundary-20261108/README.md).

Both [client capability profiles](https://github.com/mingley/partitionline/actions/runs/42495190891)
passed their selected checks. Their retained archives were not independently
inspected.
