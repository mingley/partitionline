# Resource soak driver

The driver offers load to a producer while a manual consumer processes
records slowly. It keeps every attempt, including failures and interrupted
runs. It uses stable Rust and Python's standard library on Linux.

## Local rehearsal

From the repository root:

```sh
bash scripts/run-resource-soak.sh --output work/soak-local --duration-ms 2000
```

The local peer listens on an ephemeral loopback address. It delays produce
responses and builds fetch batches of its own. It does not store the
produced records, and it is not a correctness oracle. `--peer-delay-ms 60000`
stalls replies. `--slow-ms 100` holds application batches longer.

Before the run, `frozen.json` records source and binary hashes, the
workload, the budgets, the host, and how to stop. `manifest.json` records
every child PID, wait result, sample check, and artifact hash. Each attempt
keeps timestamped client samples, Linux RSS, thread, and socket-descriptor
samples, and stderr. Task counts include the driver, the client, and the
local peer. Connection counts are only the peer this driver owns. Socket
descriptors are for the whole process.

| Client sample | Meaning |
|---|---|
| Offered, accepted, completed | Records at each stage of the producer. |
| Admission failures | Records the client refused. |
| Ambiguous outcomes | Errors after admission. The record may still have been written. |
| Pending work | Work not yet finished. |
| Producer reserved payload | Bytes held in the produce budget. |
| Consumer buffered payload | Bytes held in the fetch budget. |
| Application-held decoded payload | Bytes the slow consumer has not released. |

Delivered byte totals are returned records. They do not measure decoder
scratch. An acknowledgement count is not a record-history check.

Stop with SIGINT or SIGTERM to the Python driver, or create the stop file
named in the attempt's environment. The client stops admission, releases
application batches, closes clients, and joins the peer connections it
owns. The parent waits for the process. A missed deadline is kept as a
failure.

## Resume and check

```sh
bash scripts/run-resource-soak.sh --output work/soak-local --resume
bash scripts/run-resource-soak.sh --output work/soak-local --check
```

Resume keeps the original freeze and every earlier attempt file. It runs
only the remaining duration, with a new record-id range. A source or binary
change, an incomplete manifest, a changed artifact, or a failed attempt
blocks resume. Investigate that run and start a new output directory. The
failed directory stays part of the evidence. One directory has one active
driver.

## Controlled host

Create an isolated single-partition topic named `pl-soak-*` and pass its
bootstrap address, topic name, and a measured baseline. The driver does not
create or delete topics and does not commit offsets. Each attempt starts
consumption at the current end offset. Use a topic that exists only for
this run. The name is not a check that some other service owns it.

The invocation needs `--mode controlled`, `--bootstrap`, `--topic`,
`--baseline`, and `--rate`. The baseline JSON contains the driver
`profile` with `idempotent: true`, `binary_sha256`, `sources`,
`sustainable_records_per_second`, `measured_utc`, `raw_artifact`, and
`raw_sha256`. `raw_artifact` is relative to the baseline file. The driver
checks those hashes and requires an offered rate at least twice the stated
sustainable rate. Measure that rate on the same host and broker profile.
Keep the calibration and the broker configuration with the run. Hashes
alone do not validate a throughput number.

The fixed profile uses 256-byte values, 4 KiB producer and consumer payload
budgets, one in-flight produce request, and one returned record per fetch.
The controlled producer is idempotent and waits for all replicas. The
consumer's first-batch exception may pass its soft budget, up to the 64 MiB
decode ceiling. The checker allows that exception and rejects further
growth while the pending buffer stays above the soft budget.

## What a short run checks

A short run checks outcome equations, payload limits, task and socket caps,
terminal release, and cleanup receipts. It does not qualify a 24-hour soak,
RSS drift, record integrity, replicated durability, or a performance
ranking. The 24-hour card needs a frozen controlled-host job, measured 2×
load, and the predeclared first-hour versus final-hour RSS comparison.
