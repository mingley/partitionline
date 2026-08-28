# Benchmark vs librdkafka C

Produce is acked records/second (Lab A vs librdkafka 2.15.0 C). Fetch is
consumed records/second from a topic this crate already filled. The signed
produce tables below are Lab A. The fetch writeup is **this-VM 2026-08-28,
unsigned** (rust-rdkafka 0.39.0, not C 2.15.0). End-to-end latency was
**not measured**. Suite HOLD: [STATUS.md](STATUS.md).

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
End-to-end latency was **not measured**. Do not start it.

A mock-broker e2e is not a fetch vs-C win. This writeup is the run executed
on the recording agent, labeled as that run. It is **unsigned** until
Kernel Integrity signs. Suite HOLD stands. See [STATUS.md](STATUS.md).

### 2026-08-28 this-VM (unsigned)

Same locked knobs as the produce table (8,000,000 × 100 B, `plbench`, 6
partitions). Load with this crate (linger 5 ms, `acks=1`). Both consumers
read from offset 0. Completeness: records consumed **equal** records sent
(8,000,000). High watermark summed to **8,000,000** before and after the
pairs.

This is **not** Lab A. This is **not** `rdkafka_performance` C 2.15.0.
Comparison is rust-rdkafka **0.39.0** (`rdkafka-sys` 4.10.0+2.12.1,
`cmake-build`, bundled librdkafka **2.12.1**) as a standalone binary
outside this crate. This crate stays pure Rust (no rdkafka dep, no C/FFI).

| | |
|---|---|
| Date | 2026-08-28 |
| Host | Linux 6.12.94+ x86_64, 4 vCPU Intel Xeon, 15 GiB RAM |
| Broker | Apache Kafka **3.9.1** KRaft (`kafka_2.13-3.9.1`, not Docker) on `127.0.0.1:9092` |
| This crate | `cargo +1.85 run --release --example bench_fetch` (`lto = thin`, rustc 1.85.1) |
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

#### Reproduce

partitionline:

```
COUNT=8000000 MAX_WAIT_MS=100 MAX_BYTES=16777216 MIN_BYTES=1 KAFKA_TOPIC=plbench \
  cargo +1.85 run --release --example bench_fetch
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
