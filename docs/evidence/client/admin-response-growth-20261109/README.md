# Admin response transform timing observations

9 November 2026

These observations exercise two private Admin response transforms with synthetic
500,000-item inputs in debug test builds. They do not measure a live Kafka
broker, production throughput, or a client/server ranking. No production source
was changed. The transforms also serve other response surfaces, so their
behavior was left unchanged.

The prior broad Admin runs had reported failures in two tests that race each
transform against a 30 ms timer and assert total elapsed time below 80 ms.
The assertion measures the whole `select!` expression, including scheduler
delay and cleanup of the losing Admin future. That measurement alone does not
identify the cause of an overrun.

## Original assertions

[Run 42687676801](https://github.com/mingley/partitionline/actions/runs/42687676801)
used separately built baseline and observation binaries on a hosted x86_64 AMD
EPYC 9V45 runner, Rust 1.99.1 and LLVM 23.1.1, pinned to one CPU. Both arms
added a test-only elapsed-time print to the original assertions; the observation
arm also added the completion observer. Production transform behavior was not
changed. Each profile/case/arm had eight alternating samples.

| Case | Default baseline failures | Default observer failures | All-feature baseline failures | All-feature observer failures |
| --- | ---: | ---: | ---: | ---: |
| Named-result grouping | 0/8 | 0/8 | 0/8 | 0/8 |
| Coordinator-result indexing | 4/8 | 4/8 | 5/8 | 4/8 |

The workflow intentionally retained both successful and failing assertion exit
codes, so its success is not a claim that those assertions all passed.
[Raw timing output](timing-raw.txt) retains the individual elapsed values.

The same run separately let each transform finish instead of dropping it at
the timer. There were four trials per case and feature profile. Each completed
in 62 polls; the longest observed individual poll was 153.34–161.75 ms.
Timer observations were 31.62–53.31 ms. Those wall times include any
descheduling and instrumentation effects.

## Map growth observation

[Run 42689662174](https://github.com/mingley/partitionline/actions/runs/42689662174)
used a separate hosted x86_64 AMD EPYC 9V74 runner, Rust 1.99.1, LLVM 23.1.1,
and one pinned CPU. It alternated four pairs per case and feature profile.
Both arms wrapped every poll with wall and thread-CPU measurements. The
comparison arm had no per-entry growth hooks; it was not an uninstrumented
binary. The growth arm added test-only capacity and timing hooks inside the
two transforms. All observed transforms completed and checked their result
sizes and representative entries.

In all 16 growth-instrumented samples, the longest poll was poll 57 and
included a map capacity change from 458,752 to 917,504 entries. The longest
polls took 234.41–245.44 ms; the individual growth operations accounted for
229.00–238.10 ms. Longest-poll CPU samples were 23–25 ticks and growth
samples were 22–24 ticks. The clock was 100 ticks per second, so those
measurements have 10 ms tick resolution. They indicate substantial thread
CPU time, but do not separate hashing, allocation, copying, or instrumentation
overhead. No thread-CPU read was reported unavailable.

The comparison arm also had its longest poll at poll 57, at
235.95–243.68 ms. Its poll wrapper and thread-CPU reads still affect timing.
In this run the 30 ms timer was observed after only three to five polls;
the measured growth occurred later. Consequently these observations do
**not** establish that the late map growth caused the earlier 80 ms
assertion failures.

[Raw growth output](growth-raw.txt) retains the poll and capacity markers.
The extracts preserve repeated output caused by the workflow's `tee` and
`grep`; repeated lines are not extra trials. Results use synthetic inputs
on shared hosted runners, and do not establish a wall-time or memory bound.

## Source and test pins

Both runs checked out main
[`f1be3052aac5ec70a424ac951fe1189c27a31417`](https://github.com/mingley/partitionline/commit/f1be3052aac5ec70a424ac951fe1189c27a31417).
The original `src/admin.rs` Git blob was
`e6c9899f3c645023782a9a33531665e3ef4b741c`; the pagination test blob was
`d497709daffaa1cae3e6cd328aa0848a41c91567`, and `Cargo.lock` was
`96e364d34121fe90183f87f4f49fd92b883bc447`.

The first run's test-modified Admin source blobs were
`af8fc2c65d21cdd88745cc2a298cd0d1a7a6b8aa` (baseline print) and
`62b73545531a43b822b637758164103ef24dbae5` (observer). Its
[workflow](https://github.com/mingley/partitionline/blob/d96dac6ea5ad499c294e90d96b967e70ca2eb617/.github/workflows/admin-response-timing.yml)
pins fixture blob `7a275a6f7524bf504692cfb56006d520c576854e`.

The growth run's modified source blobs were
`673795583a712a3722becc8c502748dfbb3e6026` (comparison) and
`242eade64c3d9629e17ae27db6e428f4659f3ab7` (growth hooks). Its
[workflow](https://github.com/mingley/partitionline/blob/39a8ddc1f444be73eaa1e8009befa77941749646/.github/workflows/admin-response-growth.yml)
pins fixture blob `64d068e070ffa2c1db4e9bb223837134c556f727`.
The run artifacts retain build and test logs for 30 days:
[timing artifact](https://github.com/mingley/partitionline/actions/runs/42687676801/artifacts/13705902568),
[growth artifact](https://github.com/mingley/partitionline/actions/runs/42689662174/artifacts/13706797416).
No production fix or complete release qualification is claimed.
