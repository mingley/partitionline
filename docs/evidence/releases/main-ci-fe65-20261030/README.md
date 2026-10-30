# Main CI at fe65ac783

GitHub Actions results for `fe65ac7833fbaaa81331e31a182964c4ef3657a5`,
retrieved on 30 October 2026. All listed jobs have completed. This commit
does not qualify for release.

| Scope | Result |
| --- | --- |
| Client default library | 2,174 passed, 22 failed |
| Client all-feature library | 2,180 passed, 16 failed |
| Broker default all targets | 701 passed |
| Broker all-feature all targets | 849 passed |
| Broker format, strict all-target/all-feature Clippy, rustdoc and both doctest profiles | Passed |
| Instrumented broker fuzz build, lint and 12 supervisor controls | Passed |
| Client strict all-target/all-feature Clippy, audit and dependency policy | Passed |
| Schema, docs, package consumers, publish dry run and release rehearsal | Passed |
| Java/Rust codec lane | Passed; three opt-in Rust cases remained ignored |

The client all-target commands stop at their library failures. Their later
integration targets did not run. Passing broker results do not explain the
two Raft observer failures at `77021f7dc`; both observations remain recorded.
The fuzz supervisor controls exercise actual short-lived workers, interruption
and resume. They do not complete a 24-hour campaign.

Both capability lanes remain failed. The default artifact contains 91 raw
Rust passes and two required Java-fixture cases that were ignored. The old
result parser reports only 67 verified passes because it cannot parse those
two ignored-case lines. Its original failed report is preserved in the
[fixture gate record](../../client/capability-java-20261030/README.md).
The two separate SDK servers also remain unrun. No SDK qualification follows.

The broker inventory stops at stale source bindings. The
[reviewed refresh](../../broker/source-pins-20261030/README.md) updates only
11 hashes; the full negative-control suite still needs CI execution.

All three Kafka 4.1.2/4.2.1/4.3.1 live compatibility jobs abort with stack
overflow. Both older broker smoke jobs fail group join and transaction
examples. Windows and macOS library jobs also fail; their logs are not part
of this x86 qualification snapshot. No new performance comparison is claimed.

`jobs.json.gz` preserves the completed job API snapshots. `summary.json`
indexes the 13 complete retained job logs and their test-result groups;
`files.json` records stored and reconstructed hashes. Successful jobs without
a retained log are reported from their completed GitHub job status only.

Sources: [main workflow](https://github.com/mingley/partitionline/actions/runs/41127419819),
[broker workflow](https://github.com/mingley/partitionline/actions/runs/41127419630),
[capability workflow](https://github.com/mingley/partitionline/actions/runs/41127419768).
