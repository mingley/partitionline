# Runtime measurements

`runtime` accepts `--runtime current_thread --workers 0` or
`--runtime multi_thread --workers N` (1–64). Results include the requested flavor,
the flavor observed from Tokio, and Tokio's scheduler worker count.
Current-thread execution has no background workers; Tokio reports one scheduler
worker for that configuration.

`native-produce-runtime` and `native-latency-runtime` are benchmark copies of the
existing examples. `PL_BENCH_RUNTIME_FLAVOR`, `PL_BENCH_RUNTIME_WORKERS`, and
`PL_BENCH_RUNTIME_OBSERVATION` select the executor and a new JSON sidecar path.
The producer additionally requires `PL_BENCH_BULK_LATENCY_PATH`. The latency
driver requires `PL_BENCH_WARMUP_OBSERVATION`. These variables apply to the
benchmark copies. The library and examples keep their existing behavior.

The native copies record a process-wide allocation census, workload CPU time,
10ms RSS samples, and a thread-count snapshot. This interval includes client
setup, warmup, timing, output, close, and the cancellation barrier. It excludes
runtime construction. It differs from the null-broker harness's timed-phase
resource interval. Compare each metric within its workload and scope.

Bulk sampling reserves space before timing. Every 512th accepted record records
its admission-call bounds and the final flush observation. The resulting duration
is an upper bound to final flush, **not** an individual record's acknowledgment
latency. Open-loop samples retain intended arrival, enqueue bounds, observed
acknowledgment, and every rejection. Its unchanged fixed payload allows complete
payload and contiguous-offset readback, but has no unique producer payload IDs.

After explicit client close, a two-second benchmark barrier polls already
cancelled tasks until Tokio reports zero live tasks. The pre-barrier count and
barrier duration remain in the result. This work is outside timing. KL02-12 tracks
the library's shutdown completion contract separately.

`measure-runtime-flavors.py` records a randomized five-repetition matrix and five
fresh current-thread repetitions afterward. It uses a clean, pinned source
checkout, retained executable copies, source checks before and after each owned
command, command deadlines, and parent-bound children. The native controller
starts Kafka 4.3.1, verifies the distribution archive and Java SDK, checks actual
topic policy, and independently reads every acknowledged payload with Java.
Topics are deleted after readback; broker shutdown is waited and ports rebound.

The local matrix compares current-thread with 1, 2, 4, and 5 workers on client
CPUs 2 and 4. Five is the host affinity count; 4 and 5 workers oversubscribe the
same two client CPUs. Native Kafka runs on CPUs 0 and 1. All flavors receive the
same absolute open-loop rates, calculated from five fresh current-thread
sequential calibration runs. Those percentages describe current-thread capacity,
not each runtime's saturation point. Failed capacity runs remain failed.

Build with latest stable Rust:

```sh
cargo +stable build --locked --release --manifest-path benchmarks/runtime/Cargo.toml --bins
```

The controller's `--help` lists source, executable, output, and native dependency
paths. Its output directory must be new. Both families require a JSON map of
source-relative paths to SHA-256 hashes, including the controller, its tools, all
Rust inputs, locks, and the shared recorder/command/report scripts. The native
family also requires the Kafka archive home, pinned Kafka client JAR, and slf4j
JAR. Successful runs retain the frozen order, raw observations, source and input
hashes, command receipts, result-validator output, and closure receipts.

These are local unsigned observations. They do not qualify a production
configuration, compare peers, lift Suite HOLD, or establish a speed ranking.

`canonical-native-views.py` retains the original native result and adds fields
required by the JSON schema: the `kraft` mode, histogram range endpoints, and
attempt metadata. It verifies every referenced raw artifact and executable,
records the formatter revision, and checks both the schema and result CLI.
Timing, resource measurements, outcomes, integrity, and percentile values stay
unchanged. An early result that passed the CLI but lacked schema fields remains
available alongside its corrected view.
