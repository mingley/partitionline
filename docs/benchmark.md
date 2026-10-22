# Benchmarks

This page explains how to run a Partitionline benchmark and how to read the
numbers already in this file.

A throughput or latency number is useful only when both clients do the same
work. Same acknowledgment level, same idempotence setting, same isolation,
same security, and the same warmup and payload. The rules are in the
[measurement contract](benchmark-contract.md). Result files are described in
[benchmark result files](benchmark-results.md).

Suite HOLD is still on. Local tables in this file are engineering samples.
They are not a signed “fastest client” claim. See [STATUS.md](STATUS.md).

## What each number means

| Number | What it counts | What it does not count |
|---|---|---|
| Acknowledged records/second | Produce records the broker acked | Records only queued in the client |
| Consumed records/second | Records an independent consumer read back | A mock broker, or a client that stopped at enqueue |
| Produce-ack latency | Time from send until the Produce response | Fetch time |
| Fetch-request latency | Time for a Fetch RPC that returns records already on the log | `poll` of one record from a local queue |

`acks=0` can be used as a diagnostic. It never counts as acknowledged throughput.

## Which comparison is which

| Comparison | Peer | Status |
|---|---|---|
| Produce throughput | Native librdkafka C 2.15.0 (`rdkafka_performance`) | Historical local samples below |
| Fetch throughput | rust-rdkafka 0.39.0, not the C 2.15.0 tool | Unsigned VM sample, 2026-08-28 |
| Produce-ack and fetch latency | rust-rdkafka 0.39.0 | Unsigned VM sample, 2026-08-28. Not Lab A, and not the C tool |

Do not paste a C produce rate into the fetch table, or a rust-rdkafka latency
into the C produce table.

## Run a produce comparison

The checked-in C peer builds librdkafka **2.15.0** at commit
`9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`. It does not add a C dependency to
this crate. Pins and receipt checks are in
`benchmarks/peers/librdkafka/README.md`.

`scripts/lab-a-produce.sh` runs both clients in randomized order, recreates
the topic, and audits the high watermark. It uses seeded payloads and 16-byte
ID keys, and records CPU and peak RSS with GNU time. The run is still unsigned.

Lock these settings on both binaries:

| Setting | Value |
|---|---|
| Records | 8,000,000 |
| Payload | 100 bytes |
| `acks` | 1 |
| Linger | 5 ms |
| Batch records | 32,768 |
| Batch bytes | 1,000,000 |
| Compression | none, unless the section says otherwise |
| Topic | `plbench`, 6 partitions, replication factor 1 |
| Warmup | none (`WARMUP_SECS=0`) |
| Topic lifecycle | delete and create before each run |

The C tool’s default linger is 1,000 ms. Set it to 5 ms. Do not use
rust-rdkafka as the C bar.

Partitionline:

```sh
COUNT=8000000 WARMUP_SECS=0 PAYLOAD_BYTES=100 ACKS=1 LINGER_MS=5 KAFKA_TOPIC=plbench \
  cargo run --release --example bench_produce
```

librdkafka C:

```sh
rdkafka_performance -P -t plbench -s 100 -c 8000000 -b 127.0.0.1:9092 -a 1 -q \
  -X linger.ms=5 -X compression.codec=none \
  -X batch.num.messages=32768 -X batch.size=1000000 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true
```

Build that tool from the v2.15.0 tag (`examples/rdkafka_performance.c`) and
link it to librdkafka 2.15.0.

The 2026-08-24 samples below used an Apple M4 Pro (macOS 26.6.2, arm64),
Docker `apache/kafka:3.9.1` on `127.0.0.1:9092`, and
`cargo run --release --example bench_produce` with thin LTO.

## Check that the records are really there

`scripts/lab-a-integrity.sh` refuses a benchmark attempt unless the record
history is complete. It keeps producer and consumer journals, logs, exit
codes, a checker verdict, and checksums in a new `ARTIFACT_DIR`. Reusing a
directory or journal path fails. A failed attempt stays on disk.

```sh
COUNT=128 PARTITIONS=2 RUNS=1 PAYLOAD_BYTES=100 ACKS=1 WARMUP_SECS=0 \
KAFKA_BOOTSTRAP=127.0.0.1:19092 KAFKA_HOME=/path/to/kafka \
BROKER_BACKEND=native TOPIC=pl-history ARTIFACT_DIR=work/history-attempt-1 \
bash scripts/lab-a-integrity.sh
```

Use a fresh empty topic and zero warmup when you need exact record counts.
`COUNT`, `RUNS`, and `PARTITIONS` must be positive. `ACKS=0` cannot pass this
harness. `BENCH_PRODUCE_BINARY` and `BENCH_FETCH_BINARY` select existing
binaries; otherwise the script builds both release examples.

History mode (`RECORD_HISTORY=<new path>`) needs a positive `COUNT` and
`PAYLOAD_BYTES` of at least 24. Each payload starts with `PLBENCH1`, a
big-endian seed, a global ID, and SplitMix filler, at the requested size.
IDs continue through warmup and measurement. Records go round robin to
explicit partitions, with one stable seeded key per partition.

The consumer reads each partition to an independent end offset, then checks
IDs, bytes, keys, and order. It still looks for duplicates after `COUNT`
records. Record IDs are not Kafka offsets: a transaction marker can consume
an offset without being an application record. The gate allows those gaps
and rejects exposed control markers. The extra “high watermark equals acked
count” check applies only to the non-transactional producer.

`scripts/bench-record-history.py` rebuilds the expected payloads in Python
and runs `scripts/check-record-history.py`. Matching totals are not enough
if an ID was swapped or a payload was corrupted. A failed verdict sets
`performance_claims_invalidated=true` and leaves the raw attempt in place.

Without `RECORD_HISTORY`, the producer still uses seeded payloads and ID
keys, but the output is unverified until another reader checks it. The
null-broker `VERIFY=1` format is a different fixture. History journaling
adds work, so compare only runs with the same instrumentation.

These checks do not qualify throughput and do not lift Suite HOLD:

- 128 records on isolated Kafka 3.9.1, every ID and hash, two partitions
- a Java transaction with three application records and one commit marker;
  the Rust consumer checked three records against an end offset of four
- an `acks=0` run that kept received IDs and reported zero acks

`IDEMPOTENT=1` requires `ACKS=-1`. The harness does not silently change acks.

## Producer settings

`bench_produce --print-config` prints the settings it will use. Unknown
names, conflicting aliases, and values the client would silently clamp fail
before any network call.

| Setting | Meaning |
|---|---|
| `COUNT` | Measured records. If unset, the run lasts `MEASURE_SECS` (default 5) |
| `WARMUP` | Warmup records, excluded from counts and timing |
| `WARMUP_SECS` | Minimum warmup time. 0 when `WARMUP` is set, otherwise 2 |
| `BATCH_SIZE` or `BATCH_BYTES` | Batch byte limit. If both are set, they must match |
| `BATCH_RECORDS` | Batch record limit |
| `CONNECTIONS`, `MAX_IN_FLIGHT` | Connections, and requests allowed on each connection |
| `IDEMPOTENT` | Requires `ACKS=-1` and `MAX_IN_FLIGHT` of 5 or less |
| `BUFFER_MEMORY` or `QUEUE_KBYTES` | Producer queue budget, in bytes or KiB |
| `RECORD_SEED` or `SEED` | Generator seed, decimal or hexadecimal |
| `KEY_MODE` | `id` (default) or `none` |
| `PAYLOAD_MODE` | `seeded` (default) or legacy `constant-x` |
| `PARTITIONS` | Expected partition count, and round-robin routing |
| `RUN_TIMEOUT_MS` | Deadline for one warmup or measurement phase, including flush |

Seeded bytes match the pinned C peer. Each phase starts IDs at zero. A Java
consumer has checked keys, values, offsets, and counts for a 100,000-record
Kafka 4.3.1 run, an idempotent gzip run, and a legacy run. That checks the
settings, not the speed. CPU and RSS come from the harness, not from the
example.

```sh
COUNT=100000 WARMUP=1000 BATCH_SIZE=1048576 BATCH_RECORDS=32768 \
ACKS=-1 IDEMPOTENT=1 MAX_IN_FLIGHT=5 \
cargo +stable run --locked --release --example bench_produce -- --print-config
```

For the old constant-value, null-key recipes, set
`PAYLOAD_MODE=constant-x KEY_MODE=none`. Those historical rows keep their
original workload and date.

## Fetch settings

`bench_fetch --print-config` prints limits and mode before connecting.

`FETCH_MODE=manual` assigns the topic from offset zero. `FETCH_MODE=group`
joins a classic group, commits the next offsets after each successful batch,
and leaves when it finishes. Use a new `GROUP_ID` for every run.

`COUNT` is how many seeded records to read. `WARMUP` must be smaller than
`COUNT`. The poll that crosses the warmup boundary stays in warmup. Timing
starts after the last warmup record. The report includes total, warmup, and
measured records, plus measured value bytes. MB/s is decimal megabytes of
values only: no keys, headers, or wire overhead. Verification and group
commits after the boundary are inside the timer. Join and close are outside it.

`MAX_BYTES` caps a response. `MAX_PARTITION_BYTES` caps one partition
(default 1 MiB). `MAX_POLL_RECORDS` defaults to 1,000.
`FETCH_BUFFER_MEMORY` defaults to 32 MiB. `RUN_TIMEOUT_MS` bounds the fetch
and commit loop. Invalid limits fail before connecting.

History verification is on unless you set `VERIFY_HISTORY=0`. Label that
run as raw. Both modes keep GNU time output and a broker offset audit.
`VERIFY=1` is the null-broker fixture, not the native history format.

```sh
COUNT=1000 WARMUP=17 FETCH_MODE=group PARTITIONS=2 \
KAFKA_BOOTSTRAP=127.0.0.1:19092 KAFKA_HOME=/path/to/kafka BROKER_BACKEND=native \
ARTIFACT_DIR=work/fetch-attempt-1 bash scripts/lab-a-fetch.sh
```

## Profile a run

`scripts/perf-profile.sh` records CPU samples, a syscall summary, rusage,
and a heap profile. It uses tools already installed (`samply`, `perf`,
`sample`, `strace`, `dtruss`, `python3`, `valgrind`). Each capture runs the
command again. One directory holds provenance, artifacts, checksums, and
`summary.json`. If a requested tool is missing, the script fails. It sets
`CARGO_PROFILE_RELEASE_DEBUG=true` so the release binary has symbols without
changing the benchmarked code.

Profile the example binary, not `cargo run`:

```sh
CARGO_PROFILE_RELEASE_DEBUG=true cargo build --locked --release --example bench_produce
KAFKA_BOOTSTRAP=127.0.0.1:19092 KAFKA_TOPIC=prof COUNT=2000000 WARMUP_SECS=0 \
  bash scripts/perf-profile.sh --cell bench_produce -- ./target/release/examples/bench_produce
```

On macOS without dtrace privileges, limit the captures:

```sh
bash scripts/perf-profile.sh --cell bench_produce --only cpu,rusage,heap -- ./target/release/examples/bench_produce
```

<a id="results"></a>

## Historical produce samples

These rows are dated local runs against librdkafka C 2.15.0. They are
separate days, not one experiment, and they are not a signed result.
Suite HOLD stays on. A produce row is not a fetch row.

The [produce comparison](#run-a-produce-comparison) above uses seeded
payloads. These older rows do not. They use a repeated 100-byte value and
a null key, with no warmup. On today's `bench_produce` that is
`PAYLOAD_MODE=constant-x` and `KEY_MODE=none`. The commands under each
table are the commands from that day. Add those two variables before
treating a new run as a rerun of the row.

Unless a row says otherwise, the shared settings are 8,000,000 records,
`acks=1`, linger 5 ms, 32,768 records per batch, 1,000,000 batch bytes,
compression none, topic `plbench`, 6 partitions, replication factor 1.
The 2026-08-24 rows were taken on an Apple M4 Pro with Docker
`apache/kafka:3.9.1`. The 2026-08-25 row used that image as container
`pl-kafka-bench` and librdkafka commit `9a94e11`.

| Row | What changed | Runs | partitionline median rec/s | C 2.15.0 median rec/s |
|---|---|---|---|---|
| [Uncompressed, 2026-08-25](#uncompressed-2026-08-25) | `acks=1`; both high watermarks 8,000,000 | 3 | 6,171,566 | 4,942,033 |
| [Uncompressed, 2026-08-24](#uncompressed-2026-08-24) | Same settings, earlier day | 3 | 7,280,980 | 3,879,019 |
| [After the snappy crate landed](#uncompressed-pair-after-the-snappy-crate-landed-2026-08-24) | One uncompressed pair. Snappy was not used on the path. | 1 | 8,344,128 | 3,644,236 |
| [lz4](#lz4-on-both-sides-2026-08-24) | Both sides lz4. C tool at compression level 0. | 1 | 6,810,917 | 6,051,203 |
| [Idempotent](#idempotent-produce-2026-08-24) | Both sides idempotent, so `acks=-1` and at most 5 in flight | 3 | 7,161,331 | 3,134,457 |
| [TLS](#tls-produce-2026-08-24) | Both sides SSL to `localhost:9093` | 3 | 7,416,029 | 1,515,535 |
| [SCRAM-SHA-256](#scram-sha-256-produce-2026-08-24) | SASL_PLAINTEXT on `localhost:9095` | 3 | 6,811,539 | 3,982,324 |
| [SCRAM-SHA-512](#scram-sha-512-produce-2026-08-24) | Same listener, SHA-512 | 3 | 6,887,871 | 3,434,559 |
| [OAUTHBEARER](#oauthbearer-produce-2026-08-24) | Unsecured JWT on `localhost:9097` | 3 | 6,822,533 | 3,635,411 |
| [Uncompressed, later that day](#uncompressed-plaintext-sample-2026-08-24) | PLAINTEXT `127.0.0.1:9092`. Recorded while admin work was in progress. Admin is not a produce setting. | 3 | 5,693,694 | 3,598,852 |

The four uncompressed partitionline medians are 8,344,128, 7,280,980,
6,171,566, and 5,693,694 records/s. That spread is why these rows stay
samples. Read the row for the day you mean.

One earlier idempotent pair, about 7.01M vs 2.18M, is withdrawn. The
client spread unkeyed records across 8 connections, the broker rejected
most batches with error 45 (`OUT_OF_ORDER_SEQUENCE_NUMBER`), `flush` still
returned Ok, and the high watermark was about 32,000 records rather than
8,000,000. The idempotent table below is the replacement, with the high
watermark checked.

### Uncompressed, 2026-08-25

Broker Docker `apache/kafka:3.9.1` container `pl-kafka-bench`. C tool built from librdkafka **v2.15.0** (`9a94e11`). No warmup.

| Run | partitionline acked rec/s | partitionline HW | librdkafka 2.15.0 C rec/s | C HW |
|---|---|---|---|---|
| 1 | 6,171,566 | 8,000,000 | 4,887,890 | 8,000,000 |
| 2 | 6,252,064 | 8,000,000 | 4,942,033 | 8,000,000 |
| 3 | 6,030,047 | 8,000,000 | 5,053,529 | 8,000,000 |
| **median** | **6,171,566** | 8,000,000 | **4,942,033** | 8,000,000 |

This row includes the fast-route `try_send` path that was in the tree that day.

### Uncompressed, 2026-08-24

| Run | partitionline acked rec/s | librdkafka 2.15.0 C rec/s |
|---|---|---|
| 1 | 7,612,611 | 3,851,551 |
| 2 | 7,280,980 | 3,879,019 |
| 3 | 6,782,422 | 3,999,550 |
| **median** | **7,280,980** | **3,879,019** |

### Uncompressed pair after the snappy crate landed, 2026-08-24

One pair. The tree that day included the `snap` crate. This uncompressed path did not use it.

| | acked rec/s |
|---|---|
| partitionline | 8,344,128 |
| librdkafka 2.15.0 C | 3,644,236 |

JSON copies from that session: `bench-pl.json` / `bench-c.json`.

### lz4 on both sides, 2026-08-24

Same historical workload as the map, except **both** sides use lz4 (`COMPRESSION=lz4` /
`-z lz4`, C `compression.level=0`). One locked pair, no warmup, fresh topic.

| | acked rec/s | elapsed |
|---|---|---|
| partitionline | **6,810,917** | 1.175 s |
| librdkafka 2.15.0 C | 6,051,203 | 1.322 s |

Repeating 100-byte payloads compress well, so this pair is mostly the
codec and the client pipeline, not the network.

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

### Idempotent produce, 2026-08-24

Same historical workload as the map, except **both** sides enable idempotence
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

The withdrawn pair is the one in the map above. This table is the
replacement: three runs, and the high watermark summed to 8,000,000
after each of them.

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

### TLS produce, 2026-08-24

Same historical workload as the map, except **both** sides speak SSL to a
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

### SCRAM-SHA-256 produce, 2026-08-24

Same historical workload as the map, except **both** sides authenticate with
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

The handshake runs once per TCP connection. After that, the produce path
is the same uncompressed pipeline.

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

### SCRAM-SHA-512 produce, 2026-08-24

Same historical workload as the map, except **both** sides authenticate with
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

The handshake runs once per TCP connection.

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

### OAUTHBEARER produce, 2026-08-24

Same historical workload as the map, except **both** sides authenticate with
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

The handshake runs once per TCP connection.

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

### Uncompressed PLAINTEXT sample, 2026-08-24

Same locked uncompressed settings as the first table, PLAINTEXT
`127.0.0.1:9092`. Three pairs, no warmup, fresh topic each run. High
watermark **8,000,000** after every run. The admin API was being exercised
in the same session. It is not a produce setting, and it is not what this
table measures.

| Run | partitionline rec/s | partitionline HW | C 2.15.0 rec/s | C HW |
|---|---|---|---|---|
| 1 | 5,693,694 | 8,000,000 | 3,294,347 | 8,000,000 |
| 2 | 4,675,767 | 8,000,000 | 3,598,852 | 8,000,000 |
| 3 | 6,022,825 | 8,000,000 | 4,220,885 | 8,000,000 |
| **median** | **5,693,694** | 8,000,000 | **3,598,852** | 8,000,000 |

## Historical fetch and latency samples

Fetch counts records an independent consumer read. Latency counts
microseconds. Neither table is a produce row, and neither table is the
matched cell in the [measurement contract](benchmark-contract.md#one-fully-specified-comparison).
Suite HOLD stays on. See [STATUS.md](STATUS.md).

| Sample | Date | Peer | What the median says |
|---|---|---|---|
| [This VM, fetch](#2026-08-28-this-vm-unsigned) | 2026-08-28 | rust-rdkafka 0.39.0, one `poll` per record | 5,282,935 vs 897,080 consumed rec/s. Not the C 2.15.0 tool. |
| [Lab A fetch](#historical-lab-a-fetch-not-this-agent-unsigned-here) | 2026-08-24 | librdkafka C 2.15.0 `rdkafka_performance -C` | 4,381,010 vs 3,119,810 consumed rec/s. Not reproduced on the VM above. |
| [This VM, produce-ack latency](#2026-08-28-this-vm-latency-unsigned) | 2026-08-28 | rust-rdkafka 0.39.0 `FutureProducer::send` | p50 62 vs 58 µs; p99 95 vs 90 µs. The partitionline side was slower. |

### 2026-08-28 this-VM (unsigned)

8,000,000 records of 100 bytes on `plbench`, 6 partitions. Loaded with this
crate at linger 5 ms and `acks=1`. Both consumers read from offset 0.
Records consumed equal records sent (8,000,000). The high watermark summed
to 8,000,000 before and after the pairs.

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

The topic was filled before either consumer ran. That fill is a load step, not a produce comparison. Compression was `none`. Idempotence, TLS, SCRAM, and OAUTHBEARER were off.

| Acked records | Elapsed | Acked rec/s | Payload | Acks | Linger |
|---|---|---|---|---|---|
| 8,000,000 | 2.378476 s | 3,363,498.136 | 100 bytes | 1 | 5 ms |

Both consumers then read that log from offset 0.

| Run | partitionline rec/s | partitionline consumed | rdkafka 0.39.0 rec/s | rdkafka consumed |
|---|---|---|---|---|
| 1 | 5,195,618 | 8,000,000 | 884,539 | 8,000,000 |
| 2 | 5,282,935 | 8,000,000 | 897,080 | 8,000,000 |
| 3 | 5,402,792 | 8,000,000 | 900,952 | 8,000,000 |
| **median** | **5,282,935** | 8,000,000 | **897,080** | 8,000,000 |

Source rows. The summary table rounds `consumed_rec_s` to the nearest record per second. The median is the middle run. Every row consumed 8,000,000 records on 6 partitions, with `max_wait_ms` 100 and `max_bytes` 16,777,216.

| Run | Client | Elapsed | Exact consumed rec/s |
|---|---|---|---|
| 1 | partitionline | 1.539759 s | 5,195,617.624 |
| 1 | rust-rdkafka 0.39.0 | 9.044263 s | 884,538.645 |
| 2 | partitionline | 1.514310 s | 5,282,934.557 |
| 2 | rust-rdkafka 0.39.0 | 8.917826 s | 897,079.652 |
| 3 | partitionline | 1.480716 s | 5,402,792.049 |
| 3 | rust-rdkafka 0.39.0 | 8.879495 s | 900,952.157 |

The peer called `poll` once per record. On this VM, partitionline’s median was 5,282,935 consumed records/s and rust-rdkafka 0.39.0’s median was 897,080. The sample is unsigned. The C 2.15.0 consumer tool was not on this machine. Suite HOLD stays on.

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

This table is the 2026-08-24 Apple M4 Pro sample against librdkafka C 2.15.0
`rdkafka_performance -C`. It was not reproduced on the VM in the section
above. Integrity has not signed it. Read it as that machine's history.

Load HW was **8,000,000** before each pair. Both consumers read the same log.

| Run | partitionline rec/s | partitionline consumed | C 2.15.0 rec/s | C consumed |
|---|---|---|---|---|
| 1 | 4,381,010 | 8,000,000 | 3,092,983 | 8,000,000 |
| 2 | 4,371,067 | 8,000,000 | 3,119,810 | 8,000,000 |
| 3 | 4,781,168 | 8,000,000 | 3,180,308 | 8,000,000 |
| **median** | **4,381,010** | 8,000,000 | **3,119,810** | 8,000,000 |

## Historical latency sample

This section is the third row of the map above: sequential produce-ack
latency, plus a partitionline-only fetch-request latency. It is not the
produce throughput map and not the Lab A fetch row.

`rdkafka_performance` C 2.15.0 was not present on this VM and was not run.

### 2026-08-28 this-VM latency (unsigned)

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

The median is the middle run of each column. On this unsigned sample, partitionline’s produce-ack median was p50 62 µs and p99 95 µs. rust-rdkafka 0.39.0’s median was p50 58 µs and p99 90 µs. Partitionline was slower. The C 2.15.0 tool was not on this machine. Suite HOLD stays on.

Source rows. Every produce-ack row has 10,000 samples, a 100-byte payload, `acks` 1, and linger 0. Every fetch-request row is partitionline only: 417 samples, 10,008 records consumed, `max_wait_ms` 100, `max_bytes` 4096, `min_bytes` 1.

| Run | Client | Kind | p50 µs | p99 µs | min µs | max µs | mean µs |
|---|---|---|---|---|---|---|---|
| 1 | partitionline | produce-ack | 77 | 216 | 44 | 2,651 | 86 |
| 1 | partitionline | fetch request | 245 | 1,979 | 120 | 3,443 | 323 |
| 1 | rust-rdkafka 0.39.0 | produce-ack | 53 | 86 | 46 | 8,473 | 57 |
| 2 | partitionline | produce-ack | 56 | 85 | 46 | 2,490 | 58 |
| 2 | partitionline | fetch request | 121 | 751 | 84 | 4,208 | 159 |
| 2 | rust-rdkafka 0.39.0 | produce-ack | 60 | 90 | 47 | 5,757 | 62 |
| 3 | partitionline | produce-ack | 62 | 95 | 50 | 1,767 | 63 |
| 3 | partitionline | fetch request | 108 | 422 | 62 | 4,326 | 132 |
| 3 | rust-rdkafka 0.39.0 | produce-ack | 58 | 95 | 48 | 3,663 | 60 |

Fetch-request (partitionline only; not vs rdkafka):

| Run | samples | p50 µs | p99 µs | consumed |
|---|---|---|---|---|
| 1 | 417 | 245 | 1,979 | 10,008 |
| 2 | 417 | 121 | 751 | 10,008 |
| 3 | 417 | 108 | 422 | 10,008 |
| **median** | 417 | **121** | **751** | 10,008 |

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
