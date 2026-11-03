# Client v16 qualification repeat at e2a04e2

This hosted x86_64 run failed client qualification. It tested the v16
candidate on checkout `e2a04e229514d3a2662a9f765b57145c467de237` with Rust
1.99.1 and LLVM 23.1.1. The default profile passed 2,432 tests and failed 16;
the all-feature profile passed 2,431 and failed 17. Each profile had two
ignored tests. Format, strict Clippy and both builds passed.

- [Workflow run](https://github.com/mingley/partitionline/actions/runs/41591455215)
- [Client v16 job](https://github.com/mingley/partitionline/actions/runs/41591455215/job/125768893922)
- [Compact reports artifact](https://github.com/mingley/partitionline/actions/runs/41591455215/artifacts/13211368195)

The 15 Mock Produce failures were the same in both profiles. Each profile also
failed `cancelled_channel_admission_releases_only_the_unenqueued_reservation`
with `Error: Timeout`; the cause is not established. In all features,
`producer::tests::encoding_many_produce_topics_does_not_hold_the_caller`
reported 80.509933 ms against its unchanged 80 ms assertion. The cause of the
timing overrun is not established. Individual failure excerpts and identities
are in `failure-diagnostics.json` and their full logs are in `reports.zip`.

| Target | Default passed / failed / ignored | All features passed / failed / ignored |
| --- | --- | --- |
| partitionline library | 2293 / 0 / 0 | 2292 / 1 / 0 |
| group_coordinator_startup | 14 / 0 / 0 | 14 / 0 / 0 |
| group_stack | 1 / 0 / 0 | 1 / 0 / 0 |
| produce_cancel | 23 / 0 / 0 | 23 / 0 / 0 |
| buffer_ownership | 12 / 0 / 0 | 12 / 0 / 0 |
| mock_produce | 32 / 15 / 0 | 32 / 15 / 0 |
| producer_initialization | 8 / 0 / 0 | 8 / 0 / 0 |
| producer_retained_backing | 8 / 1 / 1 | 8 / 1 / 1 |
| sticky_partitioner | 29 / 0 / 0 | 29 / 0 / 0 |
| init_producer_id_v6 | 12 / 0 / 1 | 12 / 0 / 1 |

All 21 asserted baseline expectations matched, and two observational cases
passed. **The case-23 match does not prove its intended cancellation phase.**
The test exited with broker error 14 before observing its two-response and
third-request barrier, as recorded in `baseline-case-23.log`. The specification
requires an expected failure but does not constrain its phase; the driver
accepts exit 101 and one failed test. V17 inherits that limitation. A
fail-closed successor needs a witness emitted only after the barrier and
cancellation, with the expected post-barrier assertion attributed to that
same test. The existing fixed test, identities, deadlines and assertions
should remain required; an early error must not count as phase proof.

All 136 command receipts recorded stable source inputs, no runner stop and
clean owned-process cleanup; all 136 before/after source inventories match.
OOM counters were unknown for every receipt, so these counters cannot establish
whether an OOM occurred. Dependency source bytes were not exhaustively pinned.
This selected-target run does not establish whole-main production readiness,
external broker interoperability or a performance ranking.

`reports.zip` is the original compact artifact, and `members.json` records the
SHA256, length and CRC for all 623 members. It was obtained using the GitHub
connector's `github_download_workflow_artifact` and `functions.download_file`;
the downloader's original local path is recorded in `analysis.json` without
any temporary download URL. `job.log` is the connector's decoded job log.
`contract.json`, `summary.json`, `baseline-results.json`, and
`baseline-case-23.log` are exact members extracted from the ZIP. Some inherited
specification and summary text describes preparation before v16 had run; the
retained results here record the later observation. The full 203 MB artifact
and compiled binaries were not downloaded. `SHA256SUMS` covers every other
file in this directory.
