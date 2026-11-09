# Main CI snapshot: `133d241`

The completed runs tested main commit
[`133d2413217cee4130bca2267e4e36b7225ff86f`](https://github.com/mingley/partitionline/commit/133d2413217cee4130bca2267e4e36b7225ff86f).
Changes since `0c9ab1e` were documentation and evidence only. This is not a
release qualification.

## Client

[Client CI](https://github.com/mingley/partitionline/actions/runs/42500038137)
finished with 17 successful jobs, 10 failed and one skipped.

| Check | Result |
| --- | --- |
| Linux default library | 2,299 passed, 18 failed; later targets did not run. |
| Linux all-feature library | 2,302 passed, 15 failed; later targets did not run. |
| Windows default library | 2,296 passed, 21 failed. |
| macOS arm64 default library | 2,296 passed, 21 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 3.9.1 and 4.1.0 smoke | Both jobs failed. |
| Resource soak | Failed after a child exited with code 1; the cause is unknown. |
| Live conformance | Skipped. |

Successful jobs included allocation, the instruction-count gate, format,
all-target/all-feature Clippy, audit, deny, docs, package, release rehearsal,
auth and integrity smoke. The instruction counter reported `BASELINE_MISSING`
for the tested Rust version and did not compare against a baseline. This
snapshot does not assign causes to the other failures.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42500038158)
passed 806 default and 954 all-feature behavioral tests, with four ignored in
each profile. Format, strict Clippy, Rustdoc and doctest commands passed; API
inventory and fuzz-supervision jobs passed. This does not explain the
[earlier broker failures](../main-ci-0c9-20261108/README.md).

Both [client capability profiles](https://github.com/mingley/partitionline/actions/runs/42500038248)
passed their selected checks. Their retained archives were not independently
inspected.
