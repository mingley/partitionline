# In-flight close and interceptor deadline witness

An instrumented public producer history shows that a blocking error
interceptor can delay `close_timeout` and other tasks on a current-thread
runtime. This is a controlled mock observation, not a latency benchmark or a
general timing bound.

## Setup

[Witness commit 2487e3d][source] is based on main
`60caa746c3654c89d01557e35e1100cdc2a84b74`. It adds a cfg-only marker
immediately after the worker creates its in-flight response guard; the marker
adds no await. The test uses one producer worker, one partition and the
repository's mock broker. After a warmup acknowledgement, the mock appends
the target record and withholds its Produce response for 10 seconds.

The test installs an error interceptor whose release is controlled by an
external OS thread. That watchdog starts before producer setup and warmup and
sends a release even if its bounded entry wait expires. A separate Tokio timer
signals only after its sleep has been polled to Pending. The test then calls
public `close_timeout(150 ms)` on a producer clone. In the held arm, the
watchdog waits 600 ms, records whether the timer ran, and releases the
callback. The test verifies that the timer deadline lay between callback
entry and release, then awaits the timer after release.

## Observations

[Run 42467898651][run] used stable Rust 1.99.1 and LLVM 23.1.1 on hosted
x86_64. Each history ran once with default features and once with all
features.

| Callback release | Default close time | All-feature close time | Timer before release |
| --- | ---: | ---: | --- |
| Immediate | 151 ms | 151 ms | Not yet fired |
| Held for 600 ms | 751 ms | 751 ms | Not fired, although its registered deadline fell inside the hold |

In both histories and profiles close returned `Error::Timeout`, the retained
`send` receipt returned `Error::Timeout`, one error callback ran, and
`bytes_buffered` returned to zero. The mock log contained the warmup and
target values; since the reply was withheld, the target's delivery remains
ambiguous to the caller. The marker, withheld response and deliberately
blocking callback affect scheduling. These measurements do not establish
behavior under every runtime, broker or workload.

The current source's in-flight guard drops its groups through the synchronous
failure path; that path calls the interceptor and sends the receipt inline.
The held history is consistent with that source path. A repair still needs to
preserve record ownership, callback and receipt ordering, bounded cleanup and
shutdown behavior; moving arbitrary callback work into a `Drop` implementation
would retain this deadline problem.

## Earlier fixture runs and source pins

[Run 42467663650][first] observed 151 ms with immediate release and
751–753 ms with the held callback, but it did not prove the timer was
registered before close and started its watchdog after warmup. The next
[run 42467724687][second] waited asynchronously before joining the watchdog
but kept those earlier fixture limitations. [Run 42467834366][third] added
the registered-timer and interval checks but still started the watchdog after
warmup. The final run starts it before setup, so an early error cannot leave
the runtime waiting indefinitely for that fixture release.

The normal producer source Git blob is
`1e7733c136e54b532cdc8bddabd1e0c828e50ed2` (SHA-256
`3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`).
The instrumented producer blob is
`b22b3c1e4aef97f95163be1a98376997175c490f` (SHA-256
`1a08c489f59169833b3452ee196a83a46311fc9fcfe8c3156b39d1fd8e4003a2`).
The test blob is `997e6b888ef0159a108b0ac4e653e6d1c2903a59` (SHA-256
`b6904a19425505d4a269a8e6dc41fd452467ed9338657fbb29c8ddc44eb1e438`);
the mock fixture SHA-256 is
`b3761d62c090d87251c1825e64ec468c5e092d22ca27fd4a690b46b7631fa5b1`,
and the lockfile SHA-256 is
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
The marker is not enabled in ordinary builds.

[source]: https://github.com/mingley/partitionline/commit/2487e3de67c0b2ba60b2561290a7aea0cb8b7248
[run]: https://github.com/mingley/partitionline/actions/runs/42467898651
[first]: https://github.com/mingley/partitionline/actions/runs/42467663650
[second]: https://github.com/mingley/partitionline/actions/runs/42467724687
[third]: https://github.com/mingley/partitionline/actions/runs/42467834366
