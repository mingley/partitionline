# Benchmark result files

Install the validator dependencies with `python3 -m pip install -r
benchmarks/requirements.txt`, then run `python3 scripts/benchmark-report.py
RESULT.json --json`. Validation requires the complete JSON schema and the
additional outcome, integrity and unit checks. Missing dependencies or a missing
schema are errors.

Schema version `1.0.0` describes Kafka runs. Its durability, producer settings and
warmup fields remain required. Earlier incomplete null-broker files retain their
original bytes and schema failures; the validator does not repair them.

Schema version `2.0.0` describes exploratory null-broker fixtures. The broker mode
is `null`. Durability and Kafka cluster identity are null because the fixture has
neither replicated storage nor a Kafka cluster ID. Fetch files use null for
producer settings; produce files use null for consumer isolation. A separate
idempotence sequence audit is null because this harness does not record one.

These fixtures have a measured phase and no warmup phase. They report
`phase: "bounded_fixture"`, `warmup_completed: false`, the measured duration, and
null for steady-state duration and total campaign repetitions. Repetition
indices start at one. Pairing order is unreported in the result file; a separate
campaign controller can retain that order.

Each new null result retains its effective configuration in a sidecar and hashes
those exact bytes. Histogram bounds describe the measured integer microsecond
buckets. A valid result file does not qualify a scenario or lift Suite HOLD.
