# Benchmark vs librdkafka C

## Reproducible native C peer (KL04-04)

The checked-in [librdkafka C peer](https://github.com/mingley/partitionline/blob/8920ad999266cd87a8de94450fc6325063105c6c/benchmarks/peers/librdkafka/README.md)
builds historical **v2.15.0** at source commit
`9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab` in an isolated directory.
No newer baseline is selected. It uses the native C producer and consumer,
emits effective settings and callback failures, and retains successful and
failed result-schema artifacts. It introduces no C/FFI dependency into this
crate. Historical tables below remain historical evidence.

`scripts/lab-a-produce.sh` now automates both clients, randomized order and a
fresh-topic HW audit per run, retaining logs, C artifacts and independent
HW counts. The earlier manual C reproduction commands remain historical
recipes. The automated harness defaults to seeded payloads and 16-byte ID
keys, accepts record-count warmup, and measures CPU and peak RSS with GNU time.
It remains an unsigned integrity check.
See the peer README for pins, settings, standalone receipt verification and
[KL04-04 validation evidence](https://github.com/mingley/partitionline/blob/8920ad999266cd87a8de94450fc6325063105c6c/docs/evidence/perf/KL04-04/README.md). Suite HOLD stays active.

Produce is acked records/second (Lab A vs librdkafka 2.15.0 C). Fetch is
consumed records/second from a topic this crate already filled. The signed
produce tables below are Lab A. The fetch writeup is **this-VM 2026-08-28,
unsigned** (rust-rdkafka 0.39.0, not C 2.15.0). Produce-ack and fetch-request
latency is **this-VM 2026-08-28, unsigned** (same rust-rdkafka 0.39.0, not
C 2.15.0, not Lab A). Suite HOLD: [STATUS.md](STATUS.md).

## Record-history correctness gate (KL04-07)

Run [scripts/lab-a-integrity.sh](https://github.com/mingley/partitionline/blob/c9a2e8c12dd12e056902b8b9410e1407f798fad6/scripts/lab-a-integrity.sh) to require complete deterministic record
histories before accepting a benchmark attempt's correctness. It now retains
producer and independent consumer JSONL journals, stdout/stderr, command exit
statuses, normalized history, a checker verdict, and checksums in an exclusive
`ARTIFACT_DIR`. A failed attempt stays available. Reusing an artifact directory
or journal path fails instead of overwriting it.

```sh
COUNT=128 PARTITIONS=2 RUNS=1 PAYLOAD_BYTES=100 ACKS=1 WARMUP_SECS=0 \
KAFKA_BOOTSTRAP=127.0.0.1:19092 KAFKA_HOME=/path/to/kafka \
BROKER_BACKEND=native TOPIC=pl-history ARTIFACT_DIR=work/history-attempt-1 \
bash scripts/lab-a-integrity.sh
```

The harness uses a fresh empty topic and zero warmup for exact application
record accounting. Invalid or zero `COUNT`, `RUNS`, and `PARTITIONS` fail before
broker operations. `ACKS=0` cannot pass this acknowledged-producer harness.
`BENCH_PRODUCE_BINARY` and `BENCH_FETCH_BINARY` can select existing binaries;
otherwise it builds both release examples. A custom native bootstrap always
uses native topic/offset tools for that endpoint.

The examples enable history instrumentation with `RECORD_HISTORY=<new-path>`.
Producer history mode requires an explicit positive `COUNT` and
`PAYLOAD_BYTES>=24`. Its payload contains `PLBENCH1`, a big-endian 64-bit seed,
a global 64-bit ID, and deterministic SplitMix filler, keeping the requested
payload size. IDs span warmup and measurement without resetting. Records route
round robin to explicit partitions, with one stable seeded key per partition.
The journals record every locally accepted application's ID, actual value
SHA-256, key, partition, phase, and independent consumed offset. Producer
completion and delivery counters establish whether accepted rows can be
normalized as broker acknowledged. A teardown error seals a failed journal.

The consumer reads to an independent snapshot of every partition's end offset,
then checks the complete application ID set, generator bytes, keys, and order.
It detects duplicates or extra records even after `COUNT` records have already
been reached. Record IDs are independent of Kafka offsets: transactional
control markers and aborted records can consume offsets without becoming
application records. The gate accepts legitimate offset gaps and rejects
exposed control markers. The harness's additional `HW delta == acked` audit is
restricted to its explicitly non-transactional producer.

[scripts/bench-record-history.py](https://github.com/mingley/partitionline/blob/c9a2e8c12dd12e056902b8b9410e1407f798fad6/scripts/bench-record-history.py) independently regenerates expected payloads
in Python and invokes the existing [KL03-18 checker](https://github.com/mingley/partitionline/blob/c9a2e8c12dd12e056902b8b9410e1407f798fad6/scripts/check-record-history.py) for ID/hash/key/order,
visibility, and control-record rules. Matching totals cannot hide a
missing/duplicate swap or payload corruption. A failed verdict sets
`performance_claims_invalidated=true`; the raw attempt remains intact.
The adapter retains all records in memory while checking a run.

With `ACKS=0`, `bench_produce` reports `acked=0`, `acked_rec_s=null`, and explicit
local completion counts. Independent receipt can validate such a diagnostic's
records, but never promotes it to acknowledged throughput. Malformed numeric
settings and zero work counts fail explicitly. `IDEMPOTENT=1` requires
`ACKS=-1` rather than silently changing the requested acknowledgment semantics.

Without `RECORD_HISTORY`, the producer defaults to seeded payloads and ID keys.
Its output remains unverified until an independent reader checks the records.
The existing null-broker `VERIFY=1` format remains
separate from the native history format. SHA generation, journaling, and
independent validation add work to history runs; compare only matched workloads
and instrumentation. The retained 128-record isolated Kafka 3.9.1 smoke verifies
every ID/hash across two partitions. A native Java transaction additionally
produced three application records and one commit marker: the Rust consumer
verified three records against an end offset of four. Direct `acks=0` evidence
retains received IDs with zero acknowledged records. These are correctness
checks, with no
throughput qualification, controlled-host signoff, or Suite HOLD lift.

## Producer settings

`bench_produce --print-config` prints the effective settings before connecting.
Unknown benchmark settings, conflicting aliases, and settings the client would
silently clamp fail before network or topic operations.

| Setting | Meaning |
|---|---|
| `COUNT` | Measured records; otherwise run for `MEASURE_SECS` (default 5) |
| `WARMUP` | Warmup records, excluded from measured counts and timing |
| `WARMUP_SECS` | Minimum warmup duration; defaults to 0 with `WARMUP`, otherwise 2 |
| `BATCH_SIZE` / `BATCH_BYTES` | Batch byte limit; aliases must agree when both are set |
| `BATCH_RECORDS` | Batch record limit |
| `CONNECTIONS`, `MAX_IN_FLIGHT` | Connection count and requests per connection |
| `IDEMPOTENT` | Requires `ACKS=-1` and `MAX_IN_FLIGHT<=5` |
| `BUFFER_MEMORY` / `QUEUE_KBYTES` | Producer queue budget, in bytes / KiB |
| `RECORD_SEED` / `SEED` | Decimal or hexadecimal generator seed |
| `KEY_MODE` | `id` (default) or `none` |
| `PAYLOAD_MODE` | `seeded` (default) or explicit legacy `constant-x` |
| `PARTITIONS` | Expected topic partition count; also selects round-robin routing |
| `RUN_TIMEOUT_MS` | Deadline for each warmup or measurement phase, including flush |

Seeded records use the pinned C peer's byte generator. Each phase starts IDs
at zero. A separate Java consumer checked all keys, values, partition offsets,
and counts for a 100,000-record Apache Kafka 4.3.1 run, an idempotent gzip run,
and a legacy run. These checks qualify the settings implementation, not speed.
CPU and RSS come from the harness wrapper; they are not measured by the example.

```sh
COUNT=100000 WARMUP=1000 BATCH_SIZE=1048576 BATCH_RECORDS=32768 \
ACKS=-1 IDEMPOTENT=1 MAX_IN_FLIGHT=5 \
cargo +stable run --locked --release --example bench_produce -- --print-config
```

For the historical constant-value/null-key recipes below, set
`PAYLOAD_MODE=constant-x KEY_MODE=none` explicitly. The historical measurements
retain their original workload and dates.

## Fetch settings

`bench_fetch --print-config` prints its limits and mode before connecting.
`FETCH_MODE=manual` assigns the topic from offset zero. `FETCH_MODE=group`
joins a classic group, polls, commits each successfully processed batch's next
offsets, and leaves on completion. Use a fresh `GROUP_ID` for each benchmark.

`COUNT` is the total number of seeded records to consume. `WARMUP` excludes
the first records from timing and throughput; it must be smaller than `COUNT`.
The driver preserves a poll batch across the warmup boundary. The request
that delivered the boundary batch belongs to warmup; timing starts after the
last warmup record is processed. Output reports total consumed records, warmup
records, measured records, and actual measured value bytes. MB/s uses decimal
megabytes and excludes keys, headers, and wire overhead. Verification work and
group commits after the boundary are included in timing; join and close are
outside it.

`MAX_BYTES` caps a response. `MAX_PARTITION_BYTES` independently caps each
partition (default 1 MiB). `MAX_POLL_RECORDS` defaults to 1,000;
`FETCH_BUFFER_MEMORY` defaults to 32 MiB. `RUN_TIMEOUT_MS` bounds the fetch and
commit loop, with separate bounded setup and close. Invalid limits fail before
connecting.

The fetch harness defaults to complete record-history verification. Set
`VERIFY_HISTORY=0` for a separately labeled raw run. Both modes retain GNU time
CPU/RSS output and a broker offset audit. Direct `VERIFY=1` uses the separate
null-broker fixture format; it does not verify the producer's native history
format.

```sh
COUNT=1000 WARMUP=17 FETCH_MODE=group PARTITIONS=2 \
KAFKA_BOOTSTRAP=127.0.0.1:19092 KAFKA_HOME=/path/to/kafka BROKER_BACKEND=native \
ARTIFACT_DIR=work/fetch-attempt-1 bash scripts/lab-a-fetch.sh
```

## Methodology

Lock these on **both** binaries:

| Knob | Value |
|---|---|
| Messages | 8,000,000 |
| Payload | 100 bytes |
| `acks` | 1 |
| `linger.ms` | 5 |
| `batch.num.messages` / `batch_records` | 32,768 |
| `batch.size` / `batch_bytes` | 1,000,000 |
| Compression | `none` unless a codec section says otherwise |
| Topic | `plbench`, 6 partitions, replication 1 |
| Warmup | none (`WARMUP_SECS=0`) |
| Fresh topic | yes, delete+create before each run |

`acks=0` is not this comparison. The C tool default linger is 1000 ms; it must
be overridden.

## Environment (runs below)

| | |
|---|---|
| Date | 2026-08-24 |
| Host | Apple M4 Pro, macOS 26.6.2, arm64 |
| Broker | Docker `apache/kafka:3.9.1` on `127.0.0.1:9092` |
| C client | `rdkafka_performance` built from librdkafka **2.15.0** against Homebrew `librdkafka` 2.15.0 |
| This crate | `cargo run --release --example bench_produce` (`lto = thin`) |

## Reproduce

partitionline:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 KAFKA_TOPIC=plbench \
  cargo run --release --example bench_produce
```

librdkafka C:

```
rdkafka_performance -P -t plbench -s 100 -c 8000000 -b 127.0.0.1:9092 -a 1 -q \
  -X linger.ms=5 -X compression.codec=none \
  -X batch.num.messages=32768 -X batch.size=1000000 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true
```

Build the C tool from the v2.15.0 tag (`examples/rdkafka_performance.c`) linked
to a 2.15.0 `librdkafka`. Do not use rust-rdkafka as the C bar.

## Profiling a run (KL09-11)

[scripts/perf-profile.sh](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/scripts/perf-profile.sh) captures CPU samples, a
syscall summary, rusage (context switches, peak RSS, faults) and a heap
profile for a named cell or command, using only locally installed tools
detected at runtime (`samply`/`perf`/`sample`, `strace`/`dtruss`,
`python3`, `valgrind`/`heap`). Each capture re-runs the command once; one
run directory holds provenance, artifacts, checksums and `summary.json`.
A requested capture without a usable tool fails closed with `tool missing`
instead of reporting partial success. The script exports
`CARGO_PROFILE_RELEASE_DEBUG=true` so release builds carry symbols without
changing benchmarked codegen.

```bash
# Profile the example binary directly (not `cargo run`, which would profile cargo).
CARGO_PROFILE_RELEASE_DEBUG=true cargo build --locked --release --example bench_produce
KAFKA_BOOTSTRAP=127.0.0.1:19092 KAFKA_TOPIC=prof COUNT=2000000 WARMUP_SECS=0 \
  bash scripts/perf-profile.sh --cell bench_produce -- ./target/release/examples/bench_produce
```

On unprivileged macOS there is no syscall tracer (`dtruss` needs dtrace
privileges), so constrain the run to the available captures; the summary
manifest lists exactly what ran:

```bash
bash scripts/perf-profile.sh --cell bench_produce --only cpu,rusage,heap -- ./target/release/examples/bench_produce
```

## Results

### 2026-08-25, three locked runs, no warmup (HW=8e6 both)

Same knobs as above. Broker Docker `apache/kafka:3.9.1` `pl-kafka-bench`. C tool built from librdkafka **v2.15.0** (`9a94e11`).

| Run | partitionline acked rec/s | partitionline HW | librdkafka 2.15.0 C rec/s | C HW |
|---|---|---|---|---|
| 1 | 6,171,566 | 8,000,000 | 4,887,890 | 8,000,000 |
| 2 | 6,252,064 | 8,000,000 | 4,942,033 | 8,000,000 |
| 3 | 6,030,047 | 8,000,000 | 5,053,529 | 8,000,000 |
| **median** | **6,171,566** | 8,000,000 | **4,942,033** | 8,000,000 |

partitionline was higher on every run (about 1.25× the C median). Fast-route `try_send` is in this tree.

### 2026-08-24, three locked runs, no warmup

| Run | partitionline acked rec/s | librdkafka 2.15.0 C rec/s |
|---|---|---|
| 1 | 7,612,611 | 3,851,551 |
| 2 | 7,280,980 | 3,879,019 |
| 3 | 6,782,422 | 3,999,550 |
| **median** | **7,280,980** | **3,879,019** |

partitionline was higher on every run (about 1.9× the C median).

### 2026-08-24 confirmation after snappy landed

Same knobs, one pair, tree that includes the `snap` crate (unused on this uncompressed path):

| | acked rec/s |
|---|---|
| partitionline | 8,344,128 |
| librdkafka 2.15.0 C | 3,644,236 |

Still strictly higher than C. JSON copies: session scratch `bench-pl.json` / `bench-c.json`.

### 2026-08-24 lz4 produce (gating for the lz4 gap)

Same knobs as above except **both** sides use lz4 (`COMPRESSION=lz4` /
`-z lz4`, C `compression.level=0`). One locked pair, no warmup, fresh topic.

| | acked rec/s | elapsed |
|---|---|---|
| partitionline | **6,810,917** | 1.175 s |
| librdkafka 2.15.0 C | 6,051,203 | 1.322 s |

partitionline is strictly higher. Repeating 100-byte payloads compress well, so
this is more a codec+pipeline race than a network race.

Reproduce:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 \
  COMPRESSION=lz4 KAFKA_TOPIC=plbench \
  cargo run --release --example bench_produce

rdkafka_performance -P -t plbench -s 100 -c 8000000 -b 127.0.0.1:9092 -a 1 -q -z lz4 \
  -X linger.ms=5 -X compression.level=0 \
  -X batch.num.messages=32768 -X batch.size=1000000 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true
```

### 2026-08-24 idempotent produce (gating for InitProducerId)

Same knobs as the uncompressed table except **both** sides enable idempotence
(`IDEMPOTENT=1` / `-X enable.idempotence=true`), which forces `acks=-1` and
caps in-flight requests at 5. Three locked pairs, no warmup, fresh topic
each run. After every run, `kafka-get-offsets` high watermark summed to
**8,000,000** (equals records sent). Broker log had no
`OutOfOrderSequenceException` on these runs.

`try_send` is enqueue, not an ack. The bench only prints if `flush` returns
Ok, and flush fails on a broker produce error.

| Run | partitionline rec/s | partitionline HW | C 2.15.0 rec/s | C HW |
|---|---|---|---|---|
| 1 | 7,849,040 | 8,000,000 | 3,233,429 | 8,000,000 |
| 2 | 7,161,331 | 8,000,000 | 3,134,457 | 8,000,000 |
| 3 | 6,479,326 | 8,000,000 | 2,750,327 | 8,000,000 |
| **median** | **7,161,331** | 8,000,000 | **3,134,457** | 8,000,000 |

partitionline was higher on every run (about 2.3× the C median).

An earlier 7.01M vs 2.18M pair is **withdrawn**. That client sprayed unkeyed
records across 8 connections before partition stickiness, the broker rejected
most batches with error 45 (`OUT_OF_ORDER_SEQUENCE_NUMBER`), and `flush`
still returned Ok, so the bench counted queued records as acked. High
watermark was ~32k, not 8e6.

Reproduce:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=-1 LINGER_MS=5 \
  IDEMPOTENT=1 KAFKA_TOPIC=plbench \
  cargo run --release --example bench_produce

# sum of partition high watermarks must equal 8000000
kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic plbench --time -1

rdkafka_performance -P -t plbench -s 100 -c 8000000 -b 127.0.0.1:9092 -a -1 -q \
  -X enable.idempotence=true \
  -X linger.ms=5 -X compression.codec=none \
  -X batch.num.messages=32768 -X batch.size=1000000 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true
```

### 2026-08-24 TLS produce (gating for rustls)

Same knobs as the uncompressed table except **both** sides speak SSL to a
dedicated Kafka 3.9.1 listener (`localhost:9093`). partitionline uses
`rustls` (`TLS_CA_PEM` / `TLS_SERVER_NAME=localhost`). C uses
`security.protocol=ssl` + `ssl.ca.location`. Broker cert SAN is
`DNS:localhost,IP:127.0.0.1`. Three locked pairs, no warmup, fresh topic
each run. After every run, `kafka-get-offsets` high watermark summed to
**8,000,000**.

| Run | partitionline rec/s | partitionline HW | C 2.15.0 rec/s | C HW |
|---|---|---|---|---|
| 1 | 6,609,251 | 8,000,000 | 1,515,535 | 8,000,000 |
| 2 | 8,167,076 | 8,000,000 | 1,537,953 | 8,000,000 |
| 3 | 7,416,029 | 8,000,000 | 1,486,966 | 8,000,000 |
| **median** | **7,416,029** | 8,000,000 | **1,515,535** | 8,000,000 |

partitionline was higher on every run (about 4.9× the C median).

Reproduce:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 \
  KAFKA_BOOTSTRAP=localhost:9093 KAFKA_TOPIC=plbench \
  TLS_CA_PEM=/path/to/ca.crt TLS_SERVER_NAME=localhost \
  cargo run --release --example bench_produce

kafka-get-offsets.sh --bootstrap-server localhost:9093 \
  --command-config client.properties --topic plbench --time -1

rdkafka_performance -P -t plbench -s 100 -c 8000000 -b localhost:9093 -a 1 -q \
  -X security.protocol=ssl -X ssl.ca.location=/path/to/ca.crt \
  -X ssl.endpoint.identification.algorithm=https \
  -X linger.ms=5 -X compression.codec=none \
  -X batch.num.messages=32768 -X batch.size=1000000 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true
```

### 2026-08-24 SASL SCRAM-SHA-256 produce (gating for RFC 7677)

Same knobs as the uncompressed table except **both** sides authenticate with
SCRAM-SHA-256 to a dedicated Kafka 3.9.1 SASL_PLAINTEXT listener
(`localhost:9095`). Admin/offsets use a second PLAINTEXT listener
(`localhost:9096`). User `alice` / `secret`, broker iterations 4096.
partitionline: `SASL_MECHANISM=SCRAM-SHA-256`. C:
`security.protocol=sasl_plaintext`, `sasl.mechanisms=SCRAM-SHA-256`.
Three locked pairs, no warmup, fresh topic each run. After every run,
`kafka-get-offsets` high watermark summed to **8,000,000**.

| Run | partitionline rec/s | partitionline HW | C 2.15.0 rec/s | C HW |
|---|---|---|---|---|
| 1 | 5,479,841 | 8,000,000 | 3,781,004 | 8,000,000 |
| 2 | 6,811,539 | 8,000,000 | 4,096,461 | 8,000,000 |
| 3 | 7,014,976 | 8,000,000 | 3,982,324 | 8,000,000 |
| **median** | **6,811,539** | 8,000,000 | **3,982,324** | 8,000,000 |

partitionline was higher on every run (about 1.7× the C median). Handshake
is once per TCP connection; the produce path after that is the same
uncompressed pipeline.

Reproduce:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 \
  KAFKA_BOOTSTRAP=localhost:9095 KAFKA_TOPIC=plbench \
  SASL_USERNAME=alice SASL_PASSWORD=secret SASL_MECHANISM=SCRAM-SHA-256 \
  cargo run --release --example bench_produce

kafka-get-offsets.sh --bootstrap-server localhost:9096 --topic plbench --time -1

rdkafka_performance -P -t plbench -s 100 -c 8000000 -b localhost:9095 -a 1 -q \
  -X security.protocol=sasl_plaintext \
  -X sasl.mechanisms=SCRAM-SHA-256 \
  -X sasl.username=alice -X sasl.password=secret \
  -X linger.ms=5 -X compression.codec=none \
  -X batch.num.messages=32768 -X batch.size=1000000 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true
```

### 2026-08-24 SASL SCRAM-SHA-512 produce (gating for RFC 5802 SHA-512)

Same knobs as the uncompressed table except **both** sides authenticate with
SCRAM-SHA-512 to the same Kafka 3.9.1 SASL_PLAINTEXT listener
(`localhost:9095`). Admin/offsets on PLAINTEXT `localhost:9096`. User
`alice` / `secret`, broker iterations 4096. partitionline:
`SASL_MECHANISM=SCRAM-SHA-512`. C: `security.protocol=sasl_plaintext`,
`sasl.mechanisms=SCRAM-SHA-512`. Three locked pairs, no warmup, fresh
topic each run. After every run, `kafka-get-offsets` high watermark
summed to **8,000,000**.

| Run | partitionline rec/s | partitionline HW | C 2.15.0 rec/s | C HW |
|---|---|---|---|---|
| 1 | 6,887,871 | 8,000,000 | 3,434,559 | 8,000,000 |
| 2 | 7,252,795 | 8,000,000 | 3,386,481 | 8,000,000 |
| 3 | 6,552,274 | 8,000,000 | 4,006,697 | 8,000,000 |
| **median** | **6,887,871** | 8,000,000 | **3,434,559** | 8,000,000 |

partitionline was higher on every run (about 2.0× the C median). Handshake
is once per TCP connection.

Reproduce:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 \
  KAFKA_BOOTSTRAP=localhost:9095 KAFKA_TOPIC=plbench \
  SASL_USERNAME=alice SASL_PASSWORD=secret SASL_MECHANISM=SCRAM-SHA-512 \
  cargo run --release --example bench_produce

kafka-get-offsets.sh --bootstrap-server localhost:9096 --topic plbench --time -1

rdkafka_performance -P -t plbench -s 100 -c 8000000 -b localhost:9095 -a 1 -q \
  -X security.protocol=sasl_plaintext \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username=alice -X sasl.password=secret \
  -X linger.ms=5 -X compression.codec=none \
  -X batch.num.messages=32768 -X batch.size=1000000 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true
```

### 2026-08-24 SASL OAUTHBEARER produce (gating for RFC 7628)

Same knobs as the uncompressed table except **both** sides authenticate with
unsecured JWT OAUTHBEARER (`alg=none`) to a dedicated Kafka 3.9.1
SASL_PLAINTEXT listener (`localhost:9097`). Admin/offsets on PLAINTEXT
`localhost:9098`. Principal `alice`. partitionline:
`SASL_MECHANISM=OAUTHBEARER`. C: `security.protocol=sasl_plaintext`,
`sasl.mechanisms=OAUTHBEARER`, `enable.sasl.oauthbearer.unsecure.jwt=true`,
`sasl.oauthbearer.config=principal=alice`. Three locked pairs, no warmup,
fresh topic each run. After every run, `kafka-get-offsets` high watermark
summed to **8,000,000**.

| Run | partitionline rec/s | partitionline HW | C 2.15.0 rec/s | C HW |
|---|---|---|---|---|
| 1 | 6,421,189 | 8,000,000 | 3,581,884 | 8,000,000 |
| 2 | 7,374,455 | 8,000,000 | 3,637,117 | 8,000,000 |
| 3 | 6,822,533 | 8,000,000 | 3,635,411 | 8,000,000 |
| **median** | **6,822,533** | 8,000,000 | **3,635,411** | 8,000,000 |

partitionline was higher on every run (about 1.9× the C median). Handshake
is once per TCP connection.

Reproduce:

```
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 \
  KAFKA_BOOTSTRAP=localhost:9097 KAFKA_TOPIC=plbench \
  SASL_MECHANISM=OAUTHBEARER SASL_OAUTH_PRINCIPAL=alice \
  cargo run --release --example bench_produce

kafka-get-offsets.sh --bootstrap-server localhost:9098 --topic plbench --time -1

rdkafka_performance -P -t plbench -s 100 -c 8000000 -b localhost:9097 -a 1 -q \
  -X security.protocol=sasl_plaintext \
  -X sasl.mechanisms=OAUTHBEARER \
  -X enable.sasl.oauthbearer.unsecure.jwt=true \
  -X sasl.oauthbearer.config=principal=alice \
  -X linger.ms=5 -X compression.codec=none \
  -X batch.num.messages=32768 -X batch.size=1000000 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true
```

### 2026-08-24 uncompressed produce (gating for admin)

Admin is not a produce setting. Same locked uncompressed knobs as the first
table, PLAINTEXT `127.0.0.1:9092`. Three pairs, no warmup, fresh topic each
run. High watermark **8,000,000** after every run.

| Run | partitionline rec/s | partitionline HW | C 2.15.0 rec/s | C HW |
|---|---|---|---|---|
| 1 | 5,693,694 | 8,000,000 | 3,294,347 | 8,000,000 |
| 2 | 4,675,767 | 8,000,000 | 3,598,852 | 8,000,000 |
| 3 | 6,022,825 | 8,000,000 | 4,220,885 | 8,000,000 |
| **median** | **5,693,694** | 8,000,000 | **3,598,852** | 8,000,000 |

partitionline was higher on every run (about 1.6× the C median).

## Fetch

Fetch is consumed records/second from a topic this crate already filled.
Produce-ack / fetch-request latency is a separate writeup later in this
file. Do not copy those microseconds into this throughput table.

A mock-broker e2e is not a fetch vs-C win. This writeup is the run executed
on the recording agent, labeled as that run. It is **unsigned** until
Kernel Integrity signs. Suite HOLD stands. See [STATUS.md](STATUS.md).

### 2026-08-28 this-VM (unsigned)

Same locked knobs as the produce table (8,000,000 × 100 B, `plbench`, 6
partitions). Load with this crate (linger 5 ms, `acks=1`). Both consumers
read from offset 0. Completeness: records consumed **equal** records sent
(8,000,000). High watermark summed to **8,000,000** before and after the
pairs.

This run compares a separate rust-rdkafka **0.39.0** binary (`rdkafka-sys`
4.10.0+2.12.1, `cmake-build`, bundled librdkafka **2.12.1**). It is a separate
experiment from Lab A, which used `rdkafka_performance` 2.15.0.

| | |
|---|---|
| Date | 2026-08-28 |
| Host | Linux 6.12.94+ x86_64, 4 vCPU Intel Xeon, 15 GiB RAM |
| Broker | Apache Kafka **3.9.1** KRaft (`kafka_2.13-3.9.1`, not Docker) on `127.0.0.1:9092` |
| This crate | Historical `bench_fetch` build: thin LTO, rustc 1.85.1 |
| Other client | rust-rdkafka **0.39.0** `BaseConsumer::assign` + `poll` (one record per poll) |
| Integrity | **unsigned** |

Lock these on **both** consumers:

| Knob | Value |
|---|---|
| Messages | 8,000,000 |
| `fetch.wait.max.ms` / `max_wait_ms` | 100 |
| `fetch.min.bytes` / `min_bytes` | 1 |
| `fetch.message.max.bytes` / `max_bytes` | 16,777,216 |
| Start | offset 0 / `Offset::Beginning` |
| Partitions | all 6 (`assign_topic` / assign 0..5) |

Load JSON (this-VM, not a produce claim):

```
{"acked":8000000,"elapsed_s":2.378476,"acked_rec_s":3363498.136,"payload_bytes":100,"acks":1,"linger_ms":5,"compression":"none","idempotent":false,"tls":false,"scram":false,"scram512":false,"oauthbearer":false}
```

| Run | partitionline rec/s | partitionline consumed | rdkafka 0.39.0 rec/s | rdkafka consumed |
|---|---|---|---|---|
| 1 | 5,195,618 | 8,000,000 | 884,539 | 8,000,000 |
| 2 | 5,282,935 | 8,000,000 | 897,080 | 8,000,000 |
| 3 | 5,402,792 | 8,000,000 | 900,952 | 8,000,000 |
| **median** | **5,282,935** | 8,000,000 | **897,080** | 8,000,000 |

Exact JSON from the three pairs (do not invent other digits):

```
{"consumed":8000000,"elapsed_s":1.539759,"consumed_rec_s":5195617.624,"partitions":6,"max_wait_ms":100,"max_bytes":16777216}
{"client":"rdkafka-0.39.0","consumed":8000000,"elapsed_s":9.044263,"consumed_rec_s":884538.645,"partitions":6,"max_wait_ms":100,"max_bytes":16777216}
{"consumed":8000000,"elapsed_s":1.514310,"consumed_rec_s":5282934.557,"partitions":6,"max_wait_ms":100,"max_bytes":16777216}
{"client":"rdkafka-0.39.0","consumed":8000000,"elapsed_s":8.917826,"consumed_rec_s":897079.652,"partitions":6,"max_wait_ms":100,"max_bytes":16777216}
{"consumed":8000000,"elapsed_s":1.480716,"consumed_rec_s":5402792.049,"partitions":6,"max_wait_ms":100,"max_bytes":16777216}
{"client":"rdkafka-0.39.0","consumed":8000000,"elapsed_s":8.879495,"consumed_rec_s":900952.157,"partitions":6,"max_wait_ms":100,"max_bytes":16777216}
```

Table integers are the JSON `consumed_rec_s` values rounded to nearest
record/s. Median is the middle run, not a mean. partitionline was higher
on every pair of **this** run. That is a same-hardware measurement vs
rust-rdkafka 0.39.0 `BaseConsumer::poll` on this VM. It is **not** signed.
It is **not** a vs-C 2.15.0 claim. It is **not** a Suite HOLD lift.

Fetch v11 `RackId` is a non-nullable STRING (Apache JSON / kafka-protocol
0.18.0). This tree encodes an empty string when no rack is set. Kafka
3.9.1 rejects a null `rackId`. No new admin API. ElectLeaders /
DescribeLogDirs v5 / DescribeQuorum / raft voters stay closed.

#### Run again

These commands use the latest stable Rust. Record the toolchain with the new
results; the historical measurements above used rustc 1.85.1.

partitionline:

```
COUNT=8000000 MAX_WAIT_MS=100 MAX_BYTES=16777216 MIN_BYTES=1 KAFKA_TOPIC=plbench \
  cargo +stable run --release --example bench_fetch
```

rust-rdkafka 0.39.0 (standalone crate, **not** a dependency of this
package; `default-features = false`, `features = ["cmake-build"]`):

```
COUNT=8000000 MAX_WAIT_MS=100 MAX_BYTES=16777216 MIN_BYTES=1 PARTITIONS=6 \
  KAFKA_TOPIC=plbench KAFKA_BOOTSTRAP=127.0.0.1:9092 \
  ./rdkafka-fetch-bench
```

`rdkafka_performance` C 2.15.0 was **not** present on this VM and was
**not** run. Do not copy Lab A C numbers into this table.

### Historical Lab A fetch (not this agent, unsigned here)

The 2026-08-24 Apple M4 Pro table vs librdkafka 2.15.0
`rdkafka_performance -C` was **not** reproduced on this agent. It is not
this writeup. Integrity has not signed it as a fetch vs-C win. Left here
only as history. Do not treat it as this-VM.

Load HW was **8,000,000** before each pair. Both consumers read the same log.

| Run | partitionline rec/s | partitionline consumed | C 2.15.0 rec/s | C consumed |
|---|---|---|---|---|
| 1 | 4,381,010 | 8,000,000 | 3,092,983 | 8,000,000 |
| 2 | 4,371,067 | 8,000,000 | 3,119,810 | 8,000,000 |
| 3 | 4,781,168 | 8,000,000 | 3,180,308 | 8,000,000 |
| **median** | **4,381,010** | 8,000,000 | **3,119,810** | 8,000,000 |

## Latency

This is **not** the produce or fetch throughput tables above. It is
sequential produce-ack and already-on-log fetch-request latency, p50/p99
in microseconds. A mock-broker e2e is not a latency win. This writeup is
the run executed on the recording agent, labeled as that run. It is
**unsigned** until Kernel Integrity signs. Suite HOLD stands. See
[STATUS.md](STATUS.md).

`rdkafka_performance` C 2.15.0 was **not** present on this VM and was
**not** run. Do not copy Lab A C numbers into this table. Do not treat
the historical 4.38M vs 3.12M Lab A fetch-vs-C row as this writeup.

### 2026-08-28 this-VM (unsigned)

Sequential `Producer::send` (enqueue to Produce ack) vs rust-rdkafka
**0.39.0** `FutureProducer::send`. Linger **0**, `acks=1`, 100 B payload,
1 partition, RF=1, one in-flight. Warmup 1,000 then 10,000 timed sends.
After each partitionline produce, `Consumer::fetch` from offset 0 until
at least 10,000 records, timing every non-empty fetch (`max_bytes=4096`,
`min_bytes=1`, `max_wait_ms=100`). rust-rdkafka fetch latency was **not**
measured: `BaseConsumer::poll` returns one record from an internal queue
and is not a Fetch RPC.

Percentile is nearest-rank on the sorted sample vector: index
`ceil(n * p / 100) - 1` (same as `examples/bench_latency.rs`).

| | |
|---|---|
| Date | 2026-08-28 |
| Host | Linux 6.12.94+ x86_64, 4 vCPU Intel Xeon, 15 GiB RAM |
| Broker | Apache Kafka **3.9.1** KRaft (`kafka_2.13-3.9.1`, not Docker) on `127.0.0.1:9092` |
| This crate | Historical `bench_latency` build: thin LTO, rustc 1.85.1 |
| Other client | rust-rdkafka **0.39.0** (`rdkafka-sys` 4.10.0+2.12.1, `cmake-build` + `tokio`, bundled librdkafka **2.12.1**) standalone `FutureProducer` |
| Integrity | **unsigned** |

Lock these on **both** producers:

| Knob | Value |
|---|---|
| Timed messages | 10,000 |
| Warmup | 1,000 (not in the percentile set) |
| Payload | 100 bytes |
| `acks` | 1 |
| `linger.ms` | 0 |
| `batch.num.messages` / `batch_records` | 1 |
| In-flight / connections | 1 |
| Topic | `pllat`, 1 partition, replication 1 |
| Fresh topic | yes, delete+create before each client run |

Completeness: every timed `send` returned a Produce ack (10,000 samples).
High watermark after each client run was **11,000** (1,000 warmup + 10,000
timed). Fetch consumed **10,008** on every partitionline run (last fetch
returned 8 extra records past the 10,000 stop).

| Run | partitionline p50 µs | partitionline p99 µs | rdkafka 0.39.0 p50 µs | rdkafka 0.39.0 p99 µs | HW |
|---|---|---|---|---|---|
| 1 | 77 | 216 | 53 | 86 | 11,000 |
| 2 | 56 | 85 | 60 | 90 | 11,000 |
| 3 | 62 | 95 | 58 | 95 | 11,000 |
| **median** | **62** | **95** | **58** | **90** | 11,000 |

Median is the middle run of each column, not a mean. partitionline
produce-ack was **not** lower than rust-rdkafka 0.39.0 on this VM (p50
median 62 vs 58; p99 median 95 vs 90). That is not a same-hardware win.
It is **not** signed. It is **not** a vs-C 2.15.0 claim. It is **not** a
Suite HOLD lift. Do not say “faster than librdkafka”.

Exact JSON from the three pairs (do not invent other digits):

```
{"kind":"produce_ack","samples":10000,"p50_us":77,"p99_us":216,"min_us":44,"max_us":2651,"mean_us":86,"payload_bytes":100,"acks":1,"linger_ms":0,"client":"partitionline"}
{"kind":"fetch_rpc","samples":417,"p50_us":245,"p99_us":1979,"min_us":120,"max_us":3443,"mean_us":323,"consumed":10008,"max_wait_ms":100,"max_bytes":4096,"min_bytes":1,"client":"partitionline"}
{"kind":"produce_ack","samples":10000,"p50_us":53,"p99_us":86,"min_us":46,"max_us":8473,"mean_us":57,"payload_bytes":100,"acks":1,"linger_ms":0,"client":"rdkafka-0.39.0"}
{"kind":"produce_ack","samples":10000,"p50_us":56,"p99_us":85,"min_us":46,"max_us":2490,"mean_us":58,"payload_bytes":100,"acks":1,"linger_ms":0,"client":"partitionline"}
{"kind":"fetch_rpc","samples":417,"p50_us":121,"p99_us":751,"min_us":84,"max_us":4208,"mean_us":159,"consumed":10008,"max_wait_ms":100,"max_bytes":4096,"min_bytes":1,"client":"partitionline"}
{"kind":"produce_ack","samples":10000,"p50_us":60,"p99_us":90,"min_us":47,"max_us":5757,"mean_us":62,"payload_bytes":100,"acks":1,"linger_ms":0,"client":"rdkafka-0.39.0"}
{"kind":"produce_ack","samples":10000,"p50_us":62,"p99_us":95,"min_us":50,"max_us":1767,"mean_us":63,"payload_bytes":100,"acks":1,"linger_ms":0,"client":"partitionline"}
{"kind":"fetch_rpc","samples":417,"p50_us":108,"p99_us":422,"min_us":62,"max_us":4326,"mean_us":132,"consumed":10008,"max_wait_ms":100,"max_bytes":4096,"min_bytes":1,"client":"partitionline"}
{"kind":"produce_ack","samples":10000,"p50_us":58,"p99_us":95,"min_us":48,"max_us":3663,"mean_us":60,"payload_bytes":100,"acks":1,"linger_ms":0,"client":"rdkafka-0.39.0"}
```

Fetch-request (partitionline only; not vs rdkafka):

| Run | samples | p50 µs | p99 µs | consumed |
|---|---|---|---|---|
| 1 | 417 | 245 | 1,979 | 10,008 |
| 2 | 417 | 121 | 751 | 10,008 |
| 3 | 417 | 108 | 422 | 10,008 |
| **median** | 417 | **121** | **751** | 10,008 |

No new admin API. ElectLeaders / DescribeLogDirs v5 / DescribeQuorum /
raft voters stay closed.

#### Run again

These commands use the latest stable Rust. Record the toolchain with the new
results; the historical measurements above used rustc 1.85.1.

partitionline:

```
COUNT=10000 WARMUP=1000 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=0 \
  MAX_WAIT_MS=100 MAX_BYTES=4096 MIN_BYTES=1 MODE=both KAFKA_TOPIC=pllat \
  cargo +stable run --release --example bench_latency
```

rust-rdkafka 0.39.0 (standalone crate, **not** a dependency of this
package; `default-features = false`, `features = ["cmake-build", "tokio"]`):

```
COUNT=10000 WARMUP=1000 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=0 \
  KAFKA_TOPIC=pllat KAFKA_BOOTSTRAP=127.0.0.1:9092 \
  ./rdkafka-latency-bench
```

`rdkafka_performance` C 2.15.0 was **not** present on this VM and was
**not** run. Do not copy Lab A C numbers into this table.

### CI latency policies (unsigned; not Lab A)

Shared-runner smoke (`latency-gate`, `LATENCY_LIMIT_US=5000`) is not the
same bar as local-native relative 750 µs (500 µs baseline + 50% slack) or
Lab A / Kernel Integrity signoff. The nested integrity-job miss on
[CI run 33938039612](https://github.com/mingley/partitionline/actions/runs/33938039612)
(produce-ack p99 **1,344 µs** vs **750 µs**) is **historical**:
`integrity-smoke` now sets `SKIP_LATENCY_GATE=1`. Raising a CI ceiling
does not fix that miss. Suite HOLD stays. Machine-readable budgets:
[https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/latency-ci-policy.json](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/latency-ci-policy.json). Reproduce on a
controlled host before treating any sample as qualification.
