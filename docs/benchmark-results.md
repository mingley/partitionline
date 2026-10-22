# Benchmark result files

A result file is a JSON document checked against a schema. A file that
validates is still only a record of one run. It does not qualify a
scenario, and it does not lift Suite HOLD.

## Check a file

```sh
python3 -m pip install -r benchmarks/requirements.txt
python3 scripts/benchmark-report.py RESULT.json --json
```

The checker loads the schema and then checks outcomes, integrity, and
units. A missing dependency or a missing schema is an error. The checker
does not rewrite old files.

## Schema 1.0.0

Schema `1.0.0` is a Kafka run. Durability, producer settings, and warmup
fields are required.

Older incomplete null-broker files keep their original bytes and their
schema failures. They are not repaired.

## Schema 2.0.0

Schema `2.0.0` is an exploratory null-broker fixture.

| Field | Value | Why |
|---|---|---|
| Broker mode | `null` | There is no Kafka server |
| Durability | null | There is no replicated log |
| Cluster id | null | There is no Kafka cluster |
| Producer settings on a fetch file | null | The fixture did not produce |
| Consumer isolation on a produce file | null | The fixture did not consume |
| Idempotence sequence audit | null | This harness does not record one |

These fixtures measure one phase and have no warmup. They set
`phase` to `bounded_fixture`, `warmup_completed` to false, and the
measured duration. Steady-state duration and total campaign repetitions
are null. Repetition indexes start at 1. The result file does not record
pairing order. A campaign controller can keep that order separately.

Each new null result stores its effective configuration in a sidecar and
hashes those exact bytes. Histogram bounds are the measured integer
microsecond buckets.
