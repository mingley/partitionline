# Native librdkafka C benchmark peer (KL04-04)

The reviewed comparison source is **v2.15.0**, commit
`9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab` from
[confluentinc/librdkafka](https://github.com/confluentinc/librdkafka/tree/9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab).
This preserves the historical Lab A reference. No newer baseline is selected.
`source-pin.json` is the machine-readable pin.

`peer.c` calls the C API directly for both produce and direct-assignment
consumption. It is neither rust-rdkafka nor an FFI dependency of partitionline.
`run.py` only validates configuration and packages C measurements in
`benchmarks/result-schema.json` (contract 1.1.0). Python never sends or consumes
Kafka records. Source/build products and dependencies live in a separate build
directory; no Cargo dependency or package content changes.

## Build

On Linux, install a C compiler, GNU make, pkg-config, Git, Python 3, and development
headers/libraries for OpenSSL, zlib and zstd. LZ4 and snappy use the pinned library's
bundled implementations. GSSAPI and HTTP/OIDC refresh are excluded from this build.

```bash
C_PEER_BUILD_DIR=/tmp/partitionline-c-peer benchmarks/peers/librdkafka/build.sh
C_PEER_BUILD_DIR=/tmp/partitionline-c-peer python3 benchmarks/peers/librdkafka/run.py emit-config
python3 -m unittest discover -s benchmarks/peers/librdkafka -p test_adapter.py -v
```

The build refuses a different commit or tracked source edits, requires its
configured dependencies, and disables configure module downloads. It records the
compiler, make, dependency versions, source commit/tree, peer source hash, native
binary hash and shared-library hash in `build-manifest.json`. Runtime checks the
manifest, hashes, source and library version. The executable uses its adjacent
`lib/` with `$ORIGIN`; the adapter clears `LD_PRELOAD`/`LD_LIBRARY_PATH` for this
process so a different librdkafka cannot shadow that library. Build dependencies
are host libraries reported in the manifest, rather than a claim of a hermetic
OS image. Rebuild when the peer source changes.

## Commands and artifacts

Provision a fresh, isolated topic yourself (the peer never deletes a topic):

```bash
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create \
  --topic c-peer-smoke --partitions 6 --replication-factor 1 \
  --config min.insync.replicas=1
KAFKA_BOOTSTRAP=127.0.0.1:9092 KAFKA_TOPIC=c-peer-smoke \
  COUNT=256 WARMUP=10000 ACKS=-1 IDEMPOTENT=1 \
  BROKER_IMAGE=native:apache-kafka-3.9.1 BROKER_VERSION=3.9.1 \
  C_PEER_BUILD_DIR=/tmp/partitionline-c-peer \
  python3 benchmarks/peers/librdkafka/run.py roundtrip --result /tmp/c-peer-01.json
python3 scripts/benchmark-report.py /tmp/c-peer-01.json
```

`roundtrip` produces distinct warmup records, flushes them, then snapshots each
partition's high watermark. Timed records start a new ID range. The audit consumer
reads only the timed offset range, checks the full deterministic key/value bytes,
partition assignment and ID uniqueness, then reconciles HW delta with callback
acknowledgments. RF is read from broker metadata and `min.insync.replicas` from
DescribeConfigs. Cluster ID and broker node count also come from broker metadata;
image/version must be supplied from the inspected distribution because Kafka's
ApiVersions does not identify its release. This is broker replication evidence,
not evidence of per-record fsync or idempotent sequence correctness.

`produce` performs the same delivery/HW/configuration accounting without consuming
records. Its result intentionally remains `integrity.verified=false` and fails
full receipt validation until independently consumed. `emit-config` prints only
an allowlist of effective library settings plus driver settings; credentials and
private key paths are excluded.

Every result has `.raw.json`, `.samples.csv`, `.effective-config.json`,
`.stderr.log`, and `.build-manifest.json` companions with hashes. Every artifact
uses exclusive creation. An existing result or companion causes an error before
sending; use a fresh result path for each attempt. Delivery, warmup, flush or
receipt failure retains the result and returns nonzero. Final callback failures
are counted separately from queue-full retries. A delivery timeout or an
ambiguous delivery failure is never counted as a broker acknowledgment.

## Effective settings

| Environment | Default | Meaning |
|---|---:|---|
| `COUNT`, `WARMUP` | 100000, 10000 | Timed records and distinct excluded warmup records |
| `PARTITIONS`, `PAYLOAD_BYTES` | 6, 100 | Explicit record-index modulo partition count; value bytes |
| `ACKS`, `IDEMPOTENT` | 1, 0 | Idempotence requires explicitly `ACKS=-1`, in-flight ≤5 |
| `LINGER_MS` | 5 | `linger.ms` |
| `BATCH_BYTES`, `BATCH_RECORDS` | 1000000, 32768 | Effective `batch.size`, `batch.num.messages` |
| `QUEUE_MESSAGES`, `QUEUE_KBYTES` | 1000000, 32768 | Both global queue caps, with blocking poll/retry on full |
| `MAX_IN_FLIGHT` | 5 | Per-broker connection request cap; one connection per broker |
| `COMPRESSION` | none | none/gzip/snappy/lz4/zstd |
| `DELIVERY_TIMEOUT_MS`, `FLUSH_TIMEOUT_MS` | 30000, 35000 | Per-record delivery deadline and flush deadline |
| `RUN_TIMEOUT_MS`, `CONSUME_TIMEOUT_MS` | 120000, 30000 | Enqueue deadline and receipt audit deadline |
| `RECORD_SEED` | 0x5EED0001 | Same splitmix64 record scheme as the franz-go peer |
| `KEY_MODE`, `PAYLOAD_MODE` | id, seeded | 16-byte seeded ID keys and deterministic payloads |
| `LATENCY_SAMPLES` | 1000000000 | First N IDs retain callback latencies; 0 disables sampling |
| `ISOLATION` | read_uncommitted | Direct-assignment audit consumer isolation |
| `TLS_CA_PEM`, `TLS_CLIENT_CERT_PEM`, `TLS_CLIENT_KEY_PEM` | unset | Private CA and optional mTLS identity |
| `SASL_MECHANISM`, `SASL_USERNAME`, `SASL_PASSWORD` | unset | PLAIN/SCRAM-SHA-256/SCRAM-SHA-512; secret presence only emitted |

`KEY_MODE=none PAYLOAD_MODE=constant-x` matches `bench_produce`'s null-key, repeated
`x` workload; it cannot independently verify unique record IDs. The explicit
partition argument bypasses librdkafka's configured fallback partitioner and
sticky partitioning is disabled. Both requested driver settings and effective
post-creation global/topic settings are recorded; inherited topic compression
uses the effective global codec. Broker callbacks under `ACKS=0` are classified
as unknown delivery rather than acknowledgments; the acknowledgment integrity
criterion therefore fails for fire-and-forget runs.

The driver reports closed-loop callback latencies, raw samples/histogram and a
fixed-seed bootstrap interval. Sampling and polling overhead are included. These
are diagnostic values; open-loop latency, transactions, group/share, GSSAPI and
OAuth/OIDC refresh are unsupported. Custom TLS server-name overrides fail closed;
use the broker hostname matching its certificate. Plaintext/idempotent success
and intentional broker delivery failure are checked in under
`docs/evidence/perf/KL04-04/`; secure and compression campaigns are not executed.

## Automated Lab A integrity harness

`scripts/lab-a-produce.sh` defaults to both native partitionline and native C,
five repetitions with persisted seeded randomized client order, a fresh topic
before each client and an independent `kafka-get-offsets.sh` HW audit. Use
`CLIENTS=partitionline` or `CLIENTS=librdkafka` for one client. Build the C peer
once and pass `C_PEER_BINARY=/tmp/partitionline-c-peer/c-peer`; set `ARTIFACT_DIR`
to a new directory for each invocation. All output/error/HW artifacts are retained
on failure, and `acked == COUNT == HW delta` is mandatory.

This is an unsigned integrity harness. `WARMUP_SECS=0` is required because the
existing Rust example does not expose warmup counts for HW subtraction. The C
workload uses null keys and constant values; latency sampling is disabled by
default. Rust batching/buffer caps are fixed in source, and C also has an
independent record-count queue cap. Connection routing differs between clients.
The harness records these limitations and does not promote its rates into an
equal-semantics comparison. A qualification campaign still needs full receipt
IDs, matched effective settings, warmup, randomized repetitions, sample floors,
resource measurement, uncertainty and controlled-host signoff. Suite HOLD stays
active; no adapter smoke or harness run creates a public comparison claim.

## Behavioral conformance reuse (KL01-13)

`build-behavioral.sh` links the selected pinned upstream 0125 immediate-flush
regression against this same isolated native library. The original source,
explicit harness/API adaptations, paired Rust adapter, independent receipt
auditor and failing controls live in
[tests/conformance/librdkafka](../../../tests/conformance/librdkafka/README.md).
This adds no main-crate runtime dependency and makes no whole-suite claim.
