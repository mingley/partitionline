# Paired benchmark runs

`scripts/run-benchmark-matrix.py` runs two named arms in a seeded, randomized
order. Each repetition uses fresh topics, completes the declared warmup and
checks delivery results before accepting a row. It retains failed, unsupported
and interrupted attempts. These runs provide diagnostics; the
[benchmark contract](../docs/benchmark-contract.md) sets the separate campaign
and performance-claim requirements.

## Manifest

Use a JSON object with `schema_version: 1` and these fields:

| Field | Contents |
|---|---|
| `seed` | Integer controlling cell and A/B order |
| `repetitions` | 1–25 paired repetitions |
| `timeout_seconds` | 0.1–600 seconds per command |
| `broker` | Inspected `bootstrap`, `image`, `version` and `cluster_id` |
| `peers` | Exactly two arms, each with a unique `id`, `emit_config` command, result-producing `command` and SHA256-pinned `inputs` |
| `cells` | 1–32 unique cell IDs, nonsecret settings in `env`, and declared `equal_semantics` |
| `provision` | `identity`, `create` and `delete` argument vectors, plus exact `identity_stdout` |

Commands are argument arrays executed directly. They may use `{bootstrap}`,
`{topic}` and `{result}` placeholders. Create and delete commands must contain
the exact `{topic}` argument. Input pins have absolute `path` and lowercase
`sha256` fields; include the adapter, executable and its runtime dependencies.
Commands and paths are trusted operator inputs.

Each cell needs positive `WARMUP` and should explicitly set `COUNT`,
`PAYLOAD_BYTES`, `PARTITIONS`, `RECORD_SEED`, `ACKS`, `IDEMPOTENT`, `COMPRESSION`
and `ISOLATION`. `equal_semantics` uses the result schema's durability, acks,
idempotence, isolation and security fields. An optional `unsupported` object
maps an arm ID to its reason. The final manifest and raw native examples are
retained in [the orchestration evidence](../docs/evidence/perf/KL04-08/README.md).

## Run and resume

On Linux, with Python and the CI-pinned `jsonschema` dependencies installed:

```sh
python3 scripts/run-benchmark-matrix.py plan --manifest matrix.json --output work/matrix
python3 scripts/run-benchmark-matrix.py resume --manifest matrix.json --output work/matrix --approve-provision
python3 scripts/run-benchmark-matrix.py cleanup --manifest matrix.json --output work/matrix --approve-provision
```

`plan` writes a reviewable schedule without starting a subprocess or touching
Kafka. `run` combines planning and execution in a new output directory. Topic
operations require `--approve-provision`. Use an isolated broker you control.
The runner creates a UUID namespace and deletes only topics with successful
creation receipts in that run. It never resets an existing namespace.

Resume with the original manifest and unchanged source, executable and input
pins. Completed rows are checked and skipped. Interrupted rows use a new
attempt directory and topic; prior artifacts remain intact. Failed rows stay
failed unless `--retry-failed` is supplied. A successful retry retains the old
failure and keeps the summary failed. Exit codes are 0 for completion, 1 for
retained row failures, 2 for a rejected run and 130 for interruption.

Cleanup checks broker identity and the retained plan/history before deleting
owned topics. It can be repeated. Inspect retained create receipts after an
ambiguous provisioning failure; the runner does not delete topics whose
creation was never confirmed.

## Result checks and limits

Both arms must report matching effective workload, batching, queue, connection,
timeout, durability and security settings. Declared settings must match the
actual preflight configuration. Results must pass the shared schema and
validator, verify every timed record, exclude completed warmup and retain
SHA256-checked raw artifacts. Missing comparable settings reject an arm before
its topic is created. The current Java adapter lacks a record-count batch cap
and comparable Nagle setting, so it cannot run these matched cells.

The output contains the frozen manifest and plan, a chained event log, immutable
attempt directories, command receipts and a summary. Normal interruption and
timeouts terminate and join owned process groups, including adopted children.
Linux `/proc` and subreaper support are required. SIGKILL of the orchestrator,
machine failure and children that deliberately escape their process group are
outside this qualification. Credential-bearing manifests are rejected; secure
credential handling needs a separate adapter. The source and manifest hashes
detect changes against the retained record; they are not external signatures.

The native qualification uses the same C driver for both control arms and an
actual Java configuration rejection. It proves orchestration and accounting,
without establishing a peer speed comparison, production readiness or a world
ranking. The frozen scenario registry and Suite HOLD remain unchanged.
