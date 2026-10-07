# Apache Java benchmark peer

This peer uses Apache `kafka-clients` 4.3.1 and JDK 21.0.12.1. The dependency
hashes and JDK version are in `source-pin.json`. It runs outside the Rust
dependency graph. Rust builds continue to use the latest stable toolchain.

Build with a JDK that includes the Java compiler and Python 3:

```bash
python3 benchmarks/peers/java/build.py --output /tmp/java-peer --cache /tmp/java-jars
JAVA_PEER_BUILD_DIR=/tmp/java-peer python3 benchmarks/peers/java/run.py emit-config
JAVA_PEER_BUILD_DIR=/tmp/java-peer python3 -m unittest discover -s benchmarks/peers/java -p test_adapter.py -v
```

Add `--offline` when the cache already contains the pinned jars. The build uses
the pinned JDK's compiler, treats warnings as errors and creates a class archive
with fixed timestamps. Its manifest records the exact peer, adapter, shared
result helper, compiler, archive and dependency hashes. The runtime checks those
hashes and the JDK version before starting a client. JVM injection options and
native library override environment variables are cleared. This pins the peer
and dependencies; it does not describe a hermetic operating-system image.

Provision a fresh, empty topic with six partitions. The peer never creates or
deletes topics. On Linux, run an acknowledged round trip:

```bash
KAFKA_BOOTSTRAP=127.0.0.1:9092 KAFKA_TOPIC=java-peer-smoke PARTITIONS=6 \
  COUNT=256 WARMUP=16 ACKS=-1 IDEMPOTENT=1 \
  BROKER_IMAGE=native:apache-kafka-4.3.1 BROKER_VERSION=4.3.1 \
  JAVA_PEER_BUILD_DIR=/tmp/java-peer \
  python3 benchmarks/peers/java/run.py roundtrip --result /tmp/java-01.json
python3 scripts/benchmark-report.py /tmp/java-01.json
```

Sends are pipelined through the SDK's asynchronous API, bounded by
`QUEUE_MESSAGES` application permits and `QUEUE_KBYTES` SDK buffer memory.
Acceptance means `send` returned. An acknowledgment requires a successful
completion callback and `ACKS!=0`. A timeout or ambiguous callback failure never
counts as an acknowledgment. Latency runs from the first admission attempt to
the callback, including application backpressure and SDK blocking. This is a
closed-loop diagnostic; it cannot qualify an open-loop latency cell.

Warmup completes before the timed phase. Both phases use IDs starting at zero,
with separate partition offset ranges. Keys contain the big-endian ID and seeded
splitmix64 hash; seeded values use the same byte stream as the Rust and C peers.
Partitions are explicit `ID % PARTITIONS`. The consumer uses manual assignment
with auto commits and topic creation disabled. It checks the complete timed
range, full keys and values, partitions, missing IDs, duplicates and headers.
The Admin API checks RF, minISR, cluster identity and the final offset delta.
`BROKER_IMAGE` and `BROKER_VERSION` come from the inspected broker distribution.

| Setting | Default |
|---|---:|
| `COUNT`, `WARMUP`, `PARTITIONS` | 100000, 10000, 6 |
| `PAYLOAD_BYTES`, `PAYLOAD_MODE`, `KEY_MODE` | 100, seeded, id |
| `RECORD_SEED` | 0x5eed0001 |
| `ACKS`, `IDEMPOTENT`, `MAX_IN_FLIGHT` | 1, 0, 5 |
| `LINGER_MS`, `BATCH_BYTES` | 5, 1000000 |
| `QUEUE_MESSAGES`, `QUEUE_KBYTES` | 1000000, 32768 |
| `COMPRESSION`, `ISOLATION` | none, read_uncommitted |
| `DELIVERY_TIMEOUT_MS`, `FLUSH_TIMEOUT_MS` | 30000, 35000 |
| `RUN_TIMEOUT_MS`, `CONSUME_TIMEOUT_MS` | 120000, 30000 |
| `LATENCY_SAMPLES` | 1000000000 |

`emit-config` reports parsed SDK settings and driver settings. Idempotence needs
explicit `ACKS=-1` and at most five in-flight requests. Compression supports
none, gzip, snappy, lz4 and zstd; zstd is explicitly level 3 with the pinned
JNI 1.5.6-10 build of libzstd 1.5.6. Payloads may be seeded or constant `x` bytes.
The ID audit is capped at ten million records; warmup and the application cohort
at one million. This includes the frozen eight-million-record count, but does
not qualify that campaign's other workload settings.

Java has byte-based batching and no equivalent `BATCH_RECORDS` limit. Supplying
that setting fails before connecting. Null keys, TLS/SASL, groups, share,
transactions and open-loop scheduling are unsupported by this driver. A receipt
consumer audit is not a separately timed fetch benchmark. These restrictions are
recorded in the scenario inventory; unsupported cells cannot count as wins.

Results retain raw delivery counts, latency samples, effective config, stderr,
build manifest and the parent's process receipt. Existing files cause failure
before sending. The parent waits for the JVM, enforces a deadline and records
CPU and peak RSS; the JVM closes all clients and checks for surviving Kafka
threads. CPU and RSS cover the entire JVM run, including the audit. Allocations,
average RSS, broker resources and steady-state CPU are unmeasured.

`produce` skips receipt consumption, so its integrity flag remains false.
Broker or delivery failure returns nonzero and preserves the attempt. Diagnostic
runs do not qualify frozen cells or establish a speed ranking. Required zstd
comparisons still need an independently retained compressed corpus and backend
evidence. Suite HOLD remains active.
