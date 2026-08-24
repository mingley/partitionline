# Produce benchmark vs librdkafka C

Fetch and end-to-end latency were **not measured**. This document is produce
acked records/second only.

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

### Fetch

**Unmeasured.** Do not read the produce table as an e2e or consume win.
