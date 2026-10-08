# Resource soak driver

The driver measures a producer under offered load while a manual consumer
processes records slowly. It retains each attempt, including failures and
interrupted runs. It uses stable Rust and Python's standard library on Linux.

Run the finite local rehearsal from the repository root:

```sh
bash scripts/run-resource-soak.sh --output work/soak-local --duration-ms 2000
```

The local peer has an ephemeral loopback address. It delays produce responses
and generates separate fetch batches. It does not store produced records or
serve as an independent correctness oracle. Use `--peer-delay-ms 60000` to
stall replies and `--slow-ms 100` to retain application batches longer.

Before execution, `frozen.json` records source and binary hashes, workload,
budgets, host and stop instructions. `manifest.json` records every child PID,
wait result, sample check and artifact hash. Each attempt has timestamped client
samples, Linux RSS/thread/socket descriptor samples and stderr. Runtime task
counts include driver, client and local-peer tasks; peer connection counts are
measured only for the owned local peer. Socket descriptors are process-wide.

Client samples include offered, accepted, completed, admission failures,
ambiguous outcomes, pending work, producer reserved payload, consumer buffered
payload and application-held decoded payload. Delivered byte totals describe
returned records; they are not a measure of decoder scratch allocations.
Post-admission errors are conservatively ambiguous. An acknowledgment count
does not replace an independent record-history check.

Stop with SIGINT or SIGTERM to the Python driver, or create the stop file named
in the attempt's environment. The client stops admission, releases application
batches, closes clients and joins owned peer connections. The parent waits for
the process. A missed deadline is a retained failure.

Resume or check without rebuilding:

```sh
bash scripts/run-resource-soak.sh --output work/soak-local --resume
bash scripts/run-resource-soak.sh --output work/soak-local --check
```

Resume preserves the original freeze and all prior attempt files. It runs only
the remaining load duration, with a new record ID range. Source or binary
changes, incomplete manifests, changed artifacts and failed attempts block
resume. Investigate a failed run and start a new output directory; it remains
part of the evidence. One directory can have only one active driver.

For a controlled host, pre-create an isolated single-partition `pl-soak-*`
topic and supply its bootstrap address, name and measured baseline. The driver
never creates or deletes topics or commits offsets. It starts consumption at
the current end offset on each attempt. Use a topic dedicated to this run;
topic naming is not an ownership check against an external service.

The controlled invocation requires `--mode controlled`, `--bootstrap`,
`--topic`, `--baseline` and `--rate`. The baseline JSON must contain the exact
driver `profile` with `idempotent: true`, `binary_sha256`, `sources`,
`sustainable_records_per_second`, `measured_utc`, `raw_artifact` and
`raw_sha256`. The raw artifact path is relative to the baseline file. The
driver checks the receipt's hashes and requires an offered rate at least twice
the stated sustainable rate. The operator must establish that rate from a real
measurement on the same host and broker profile. Retain the calibration and
broker configuration alongside the run; hashes alone do not validate a
throughput measurement.

The fixed profile uses 256-byte values, 4 KiB producer and consumer payload
budgets, one in-flight produce request and one returned record per fetch. The
controlled producer uses idempotence and all-replica acknowledgments. The
consumer's first-batch progress exception can exceed its soft budget up to the
64 MiB decode ceiling. The checker allows this exception and rejects growth
while the pending buffer remains above its soft budget.

Short runs check outcome equations, payload limits, task/socket caps, terminal
release and cleanup receipts. They do not qualify a 24-hour soak, RSS drift,
record integrity, replicated durability or a performance ranking. The separate
24-hour card requires a frozen controlled-host job, measured 2x load and the
predeclared first-hour/final-hour RSS comparison.
