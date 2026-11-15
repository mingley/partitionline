# Main CI snapshot: `d314151`

The completed runs below tested main commit
[`d314151e6718ffb2172bf9297728724216401099`](https://github.com/mingley/partitionline/commit/d314151e6718ffb2172bf9297728724216401099).
The changes since `a3748ca` are documentation and evidence only. This is not
release qualification.

## Client

[Client CI](https://github.com/mingley/partitionline/actions/runs/42497018929)
finished with 18 successful jobs, nine failed and one skipped.

| Check | Result |
| --- | --- |
| Linux default library | 2,301 passed, 16 failed; later targets did not run. |
| Linux all-feature library | 2,303 passed, 14 failed; later targets did not run. |
| Windows default library | 2,306 passed, 11 failed. |
| macOS arm64 default library | 2,298 passed, 19 failed. |
| Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility | All three jobs failed. |
| Kafka 3.9.1 and 4.1.0 smoke | Both jobs failed. |
| Live conformance | Skipped. |

Successful jobs included resource soak, allocation, instruction-count gate,
format, strict all-target/all-feature Clippy, audit, deny, docs, package,
release rehearsal, auth and integrity smoke. The instruction counter reported
`BASELINE_MISSING` for the tested Rust version and did not compare against a
baseline. This snapshot does not assign causes to the failed jobs.

## Broker and capabilities

[Broker CI](https://github.com/mingley/partitionline/actions/runs/42497018937)
stopped in the default-feature library at 490 passed, three failed and four
ignored. Later default targets, the all-feature suite and strict checks in
that job did not run. API inventory and fuzz-supervision jobs passed
separately. The causes of the failures are unknown; the earlier
[broker pass](../main-ci-a374-20261108/README.md) does not explain them.

Both [client capability profiles](https://github.com/mingley/partitionline/actions/runs/42497018876)
passed their selected checks. Their retained archives were not independently
inspected.
