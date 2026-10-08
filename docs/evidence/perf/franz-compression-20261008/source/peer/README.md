# franz-go benchmark peer (KL09-65)

Pinned [franz-go](https://github.com/twmb/franz-go) producer/consumer peer
for the equal-semantics contract
([docs/benchmark-contract.md](../../../docs/benchmark-contract.md)).
Results file under result-schema `peer: "peer-adapter"` with the concrete
peer in provenance, until a driver card promotes franz-go to a named
schema peer, if ever.

## Pins

| Component | Pin | Source |
|---|---|---|
| franz-go (kgo, kmsg, sasl) | v1.22.0 | go.mod/go.sum |
| franz-go kadm (admin) | v1.19.0 | go.mod/go.sum |
| golang.org/x/sys (rusage) | v0.48.0 | go.mod/go.sum |
| Go toolchain | go1.26.0 | go.mod `toolchain` |

No Go code or artifact enters the `partitionline` crate or package:
`Cargo.toml` uses an `include` allowlist without `benchmarks/`.

## Layout

- `cmd/peer/main.go` — driver: `emit-config`, `scenarios`, `produce`,
  `fetch`, `roundtrip` subcommands.
- `config.go` — env knobs, fail-closed validation, resolved effective
  configuration (secrets never emitted, presence flags only).
- `records.go` — deterministic record ID scheme.
- `result.go` — result-schema document builder.

## Build and offline tests (no broker)

```bash
cd benchmarks/peers/franz-go
go build ./...
go vet ./...
go test ./...
go run ./cmd/peer emit-config
go run ./cmd/peer scenarios
```

## Knobs (environment)

Same names as the partitionline bench examples where they overlap:

| Knob | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP` | 127.0.0.1:9092 | comma-separated |
| `KAFKA_TOPIC` | partitionline | recreated fresh per `roundtrip` |
| `PARTITIONS` | 6 | topic partition count |
| `COUNT` | 100000 | steady-state records |
| `WARMUP` | 10000 | warmup records to `<topic>-warmup` |
| `PAYLOAD_BYTES` | 100 | value size |
| `RECORD_SEED` | 0x5EED0001 | contract seed |
| `ACKS` | 1 | -1, 0 or 1 |
| `IDEMPOTENT` | unset | `1` enables; requires `MAX_IN_FLIGHT` ≤ 5 |
| `LINGER_MS` | 5 | producer linger |
| `BATCH_BYTES` | 1048576 | per-partition batch cap |
| `BATCH_RECORDS` | 32768 | informational (client buffers size to the run) |
| `MAX_IN_FLIGHT` | 5 | per-broker cap |
| `COMPRESSION` | none | none\|gzip\|snappy\|lz4\|zstd |
| `ISOLATION` | read_uncommitted | or read_committed |
| `TLS_CA_PEM`, `TLS_SERVER_NAME` | unset | SSL transport |
| `TLS_CLIENT_CERT_PEM`, `TLS_CLIENT_KEY_PEM` | unset | mTLS identity |
| `SASL_MECHANISM` | unset | PLAIN\|SCRAM-SHA-256\|SCRAM-SHA-512 (+ USERNAME/PASSWORD) |
| `RESULT_PATH` | franzgo-result.json | result document output |
| `SCENARIO_ID` / `PROFILE` | peer-roundtrip-plain-6p / bulk | result labeling |

Compression is applied explicitly. `COMPRESSION=none` disables producer
compression; it does not use franz-go's default `snappy, none` preference.
The matrix still rejects this peer's incomplete shared configuration. That
adapter qualification and a comparable Kafka campaign remain open.

## Record IDs and delivery definitions

Same definitions as partitionline (contract §3):

- **Acknowledged** = the broker's Produce response arrived with a nil
  promise error. Enqueue (`Produce` acceptance) is never counted.
- **Record ID**: key = `be64(index) || be64(splitmix64(seed^index))`
  (16 bytes); value = `PAYLOAD_BYTES` from the splitmix64 stream
  chained from `seed ^ index*GOLDEN`, so every record regenerates
  without ordering assumptions. Consume verifies bytes and sha256.
- **Latency**: closed-loop per-record produce-ack time (promise
  resolve minus produce call), microseconds. Open-loop cells are
  explicitly unsupported (see `scenarios`).

## Isolated round trip

```bash
# Boot any isolated KRaft broker, then:
KAFKA_BOOTSTRAP=127.0.0.1:9092 KAFKA_TOPIC=fg-roundtrip \
  COUNT=100000 WARMUP=10000 RESULT_PATH=/tmp/fg-roundtrip.json \
  go run ./cmd/peer roundtrip
python3 scripts/benchmark-report.py /tmp/fg-roundtrip.json
```

`roundtrip` recreates the topic (contract §4.7 fresh-topic rule),
produces warmup to `<topic>-warmup`, produces `COUNT` with per-record
ack latencies, audits high watermarks via kadm, consumes from the
start verifying every ID/hash, and writes the result document plus a
`<result>.effective-config.json` companion. Exit is nonzero on any
integrity failure; the offending document is still written and marked
`failed` (no rerun erasure: failures stay in the file).

Throughput MB/s uses 1 MB = 1e6 bytes. Broker CPU/RSS/disk are
unmeasured by this driver (external process) and recorded as zero
with a note.
