# V17 client qualification at 2645e96

The [qualification run](https://github.com/mingley/partitionline/actions/runs/41593203672) includes the [client v17 job](https://github.com/mingley/partitionline/actions/runs/41593203672/job/125777847165). The trigger and checkout were `2645e96ce693fcdaa9d8c56f6f0c910934fd19a2`. The job failed its qualification step.

The decoded console projections show 2,432 passed, 16 failed and two ignored in each of the default and all-features profiles. Each profile had 15 failures in `mock_produce` and one timeout in `producer_retained_backing`. Format, Clippy and both builds reported exit 0. Each library target reported 2,293 passing tests.

The console showed `Error: Timeout` for both retained-backing failures without a numeric phase line. The compact artifact was inaccessible and its raw logs were not inspected. The absence of a line from the console projection does not identify the timeout phase or establish whether the raw log contains further detail.

Baseline case 23 exited 101 with one failed test. Its expected-failure matcher has no failure regex, and its raw log was unavailable, so this run does not establish whether the third-request barrier was reached or why the test failed. A prior v16 run failed before observing the barrier; that result does not establish the cause in this run.

The driver set `RUST_MIN_STACK` to 8 MiB, used one Cargo build job and one test thread, disabled incremental builds and core dumps, and enforced a 1.5 GiB disk floor. Raw disk and memory receipts were unavailable. These results do not qualify production readiness, default-stack safety or performance rankings.

GitHub listed compact artifact `13211954727` with 15,531,327 bytes and reported SHA-256 `1464d715f3237a674f9e0986a17ab4fd7305cb4df9df3c496ecb39b3ae23a953`. Its bytes were not downloaded or independently verified. `connector-decoded-job.log` is a UTF-8 re-encoding of connector-decoded text, not the original compressed GitHub log. `report.json` records source hashes, projected results and their limits; `connector-metadata.json` records the artifact identity and job steps.
