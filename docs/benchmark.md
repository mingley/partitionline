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
| Compression | none |
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

### Fetch

**Unmeasured.** Do not read the produce table as an e2e or consume win.
