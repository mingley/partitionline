# Benchmark measurement rules

These rules decide when two Kafka client runs can be compared, and which
sentences this repository is allowed to publish about the result.

Contract version **1.1.0**. Schema version **1.0.0** for Kafka result files.
This document specifies the scenarios. It does not run them. Every cell is
`specified` or `not_run`. None is `passed`.

Suite HOLD is still on. An unsigned sample, a shared CI runner, or a local
VM does not lift it. Official qualification still needs signed evidence on
controlled hardware.

## Compare only equal work

A comparison is invalid when the clients do not match on every row below.

| Field | They must match | A comparison is invalid when |
|---|---|---|
| Durability | Replication factor, `min.insync.replicas`, and cluster shape | RF=1 is compared with RF=3, or min.isr=1 with min.isr=2 |
| Acks | `0`, `1`, or `-1` / `all` | `acks=1` is compared with `acks=all` |
| Idempotence | On or off. When on, at most 5 in-flight requests per connection | An idempotent producer is compared with a non-idempotent one |
| Isolation | `read_uncommitted` or `read_committed` | One consumer sees aborted records and the other does not |
| Security | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`, and the SASL mechanism | Plain traffic is compared with TLS |

Higher throughput from weaker durability, ignored batch errors, or skipped
transaction markers is a defect, not a win.

### What “sent” is allowed to mean

| Word | Meaning |
|---|---|
| Enqueued | The client accepted the record into memory. `try_send` returning `Ok` is only this. |
| Acknowledged | The broker answered the Produce request. |
| Consumed | An independent reader saw the record, and the high-watermark change matches the acked count. |

A broker ack follows `acks` and `min.insync.replicas`. It is not a per-record
`fsync`. Disk durability depends on the broker flush settings and the OS
page cache.

The same trap exists in every client:

- Partitionline `send` and `try_send` queue a record. Count it only after flush or the per-record future.
- librdkafka `rd_kafka_produce` queues a record. Count it from the delivery callback or after `rd_kafka_flush`.
- Java `producer.send` returns a future. Count it when that future completes.

There is no industry certificate for “fastest Kafka client.” Numbers in
this repository are engineering measurements.

## How a run is measured

### Warmup

Every run has a warmup of at least 15 seconds or 10,000 records. Drop those
samples from percentiles, averages, and sample floors. Warmup covers caches,
connections, and broker threads.

### Repetitions

Use at least five repetitions. When comparing two clients, alternate them in
a random order, or use a Latin square. Do not run every trial of client A
and then every trial of client B. The machine drifts.

### Length

Each repetition stays in steady state for at least 60 seconds, or until the
sample floor:

- latency: 10,000 timed request/response samples
- throughput: 1,000,000 records, or 8,000,000 for the standard bulk run

### Latency without hiding the stalls

Schedule sends on an open loop (fixed rate or Poisson). Record the intended
send time and the time the response arrives. Latency is the difference.

A loop that waits for each `send` before starting the next one hides stalls.
While the broker is slow, the client simply starts fewer requests.

### What to report

Report median, p50, p95, p99, p99.9, mean, min, max, and throughput in
records/second and MB/s. Every point estimate needs a 95% bootstrap
confidence interval or the median absolute deviation.

Publish every cell, including losses. Do not show only the payload size
that won.

### Failures stay in the record

A repetition fails if the broker returns an unhandled error, the connection
drops, a record is missing, or the process panics. A later success does not
delete that failure. A progress line such as `sent 8000000 records` is not
success. Success is an independent check of high watermarks and payloads.

Before each run, delete and recreate the topic so the log starts at zero.
After the producer finishes, sum the high watermarks. That sum must equal
the acked count. A consumer benchmark reads the full range and checks
payloads and sequence IDs.

## Do not move the old latency gates

Thresholds are not relaxed to hide a miss.

On CI run `33938039612` (2026-09-05), a nested integrity job measured
produce-ack p99 of **1,344 µs** against a **750 µs** ceiling on a GitHub
Actions runner. That miss stays in the record. `integrity-smoke` sets
`SKIP_LATENCY_GATE=1` when `REQUIRE_INTEGRITY=1` so a noisy host is not
reported as an integrity failure.

The shared-runner ceiling of **5,000 µs** (`docs/latency-ci-policy.json`)
only catches a catastrophic regression on a noisy VM. The local check is
**750 µs** (500 µs baseline plus 50%). Neither number is a qualification
signoff.

## Profiles

Each profile has a required tier and an exploratory tier.

| Profile | Required | Exploratory |
|---|---|---|
| low-latency | Open-loop p99 produce-ack at `acks=1` and `acks=-1` | Saturation curves, network jitter |
| bulk | 8 million uncompressed records at `acks=1`; 8 million idempotent records at `acks=-1` | 64 KB payloads, 64 partitions |
| fetch | 8 million pre-filled records on 6 partitions; Fetch RPC latency | 64-partition fanout, truncation recovery |
| transactional | 1,000 transactions of 100 records; a read-committed consumer | Aborted-batch filter, epoch fence recovery |
| group/share | KIP-848 steady-state consumer; rebalance assignment latency | KIP-932 share group, dynamic rebalance |
| secure | TLS 1.3 bulk produce; SASL SCRAM-SHA-256 produce | mTLS rehandshake, OAUTHBEARER refresh |

Required cells are the ones a profile claim depends on. Exploratory cells
are stress tests. They do not gate a release claim.

The client-ceiling tier runs against a null broker. It measures client CPU
only. Never quote it as Kafka throughput.

What each profile is for:

1. **low-latency.** Linger 0, one-record batches, scheduled arrivals.
2. **bulk.** Saturated throughput. About 1 MB batches, 32,768 records, linger 5 ms, 6 to 12 partitions.
3. **fetch.** Read a pre-filled topic. Includes batch parsing and buffer use.
4. **transactional.** InitProducerId, AddPartitionsToTxn, epoch batches, end markers, and `read_committed`.
5. **group/share.** Group coordination, assignment, and share-group consumption.
6. **secure.** TLS 1.3, AES-GCM or ChaCha20-Poly1305, and SASL handshakes.

### Codec microbenchmarks

The `benchmarks/codec` crate is outside the workspace and builds from the
shared cargo cache. Its timings are diagnostics. They never rank a client.

- Criterion benches: encode, decode, and crc32c on shapes `f01`–`f04`; compress and decompress for gzip, snappy, and lz4 on random and text at 500×100 bytes; fast and slow transform on `f03`.
- Shapes use seed `0xC0DEC`. `f01` is 8 records, 16-byte key, 100-byte random value. `f02` is 8 records, text value, 3 headers. `f03` is 500 records, 100-byte random value. `f04` is 32 records, 1 KiB text value, 2 headers.
- `fixtures/gen.py` writes `f01`–`f04.bin` and `manifest.json`. Both are checked in. A sha256 mismatch fails the load.
- Preflight decodes every fixture, checks the manifest, re-encodes byte for byte, and round-trips gzip, snappy, and lz4.
- A counting allocator records allocations and requested bytes. It does not count frees.
- The slow transform on `f03` must match the fast transform. An interleaved 10×20 timing must stay within 1.5×.
- `benchmarks/codec/baseline.json` plus `baseline.sha256` stores toolchain, git sha, fixture pins, ns/op, census, and the slowcheck ratio.
- Gates: preflight, census, slowcheck, fixture hashes, no significant Criterion regression, an offline rebuild, and a clean worktree at report time.

## Frozen settings

| Knob | Bulk | Low latency |
|---|---|---|
| Replication factor | 1 in the lab, 3 for HA | same |
| `min.insync.replicas` | 1 in the lab, 2 for HA | same |
| `acks` | 1 or -1 | 1 or -1 |
| Idempotence | on when `acks=-1` | on when `acks=-1` |
| Isolation | `read_uncommitted` | `read_committed` |
| Payload | 100 bytes, uniform pseudorandom | same |
| Key | null or a 16-byte id | same |
| Headers | none | none |
| Routing | uniform round robin | same |
| Partitions | 6 | 1 or 6 |
| Linger | 5 ms | 0 ms |
| Batch bytes | 1,048,576 | 1,000 |
| Batch records | 32,768 | 1 |
| Max in flight | 5 | 1 or 5 |
| Connections per broker | 1 | 1 |
| Nagle | off (`TCP_NODELAY`) | off |
| Network | loopback or a controlled LAN, RTT under 0.2 ms | same |

## One fully specified comparison

Scenario `matched-bulk-acks-all-6p-uncompressed`. Profile `bulk`, required
tier, status `not_run`.

Idempotent produce of 8,000,000 uncompressed records, 6 partitions,
`acks=-1`, at most 5 in-flight requests.

| Knob | Value |
|---|---|
| Timed records | 8,000,000 |
| Warmup records | 10,000 |
| Payload | 100 bytes, seed `0x5EED0001` |
| Key | 16-byte id |
| Topic | `plbench-matched`, 6 partitions, RF=1, min.isr=1 |
| Acks | -1 |
| Idempotence | on |
| Linger | 5 ms |
| Batch bytes | 1,048,576 |
| Batch records | 32,768 |
| Max in flight | 5 |
| TCP_NODELAY | on |
| Broker | Apache Kafka 3.9.1 KRaft on `127.0.0.1:9092` |

Partitionline:

```sh
COUNT=8000000 WARMUP=10000 PAYLOAD_BYTES=100 ACKS=-1 LINGER_MS=5 \
BATCH_SIZE=1048576 BATCH_RECORDS=32768 IDEMPOTENT=1 MAX_IN_FLIGHT=5 \
KAFKA_TOPIC=plbench-matched \
cargo run --release --example bench_produce
```

librdkafka:

```sh
rdkafka_performance -P \
  -t plbench-matched \
  -s 100 \
  -c 8000000 \
  -b 127.0.0.1:9092 \
  -a -1 \
  -q \
  -X enable.idempotence=true \
  -X max.in.flight.requests.per.connection=5 \
  -X linger.ms=5 \
  -X batch.size=1048576 \
  -X batch.num.messages=32768 \
  -X queue.buffering.max.messages=1000000 \
  -X socket.nagle.disable=true \
  -X compression.codec=none
```

Java:

```sh
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance \
  --topic plbench-matched \
  --num-records 8000000 \
  --record-size 100 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=127.0.0.1:9092 \
    acks=all \
    enable.idempotence=true \
    max.in.flight.requests.per.connection=5 \
    linger.ms=5 \
    batch.size=1048576 \
    compression.type=none
```

Three details that otherwise skew the run:

1. librdkafka linger defaults to 1,000 ms. Java defaults to 0. Partitionline uses whatever the builder sets. Freeze linger at 5 ms.
2. Idempotence requires at most 5 in-flight requests. A higher cap is rejected with `OUT_OF_ORDER_SEQUENCE_NUMBER`.
3. librdkafka’s default queue holds 100,000 messages. Size it for this run (`1,000,000`) or the producer blocks. Partitionline must apply backpressure instead of growing memory without a bound.

After the run:

```sh
kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic plbench-matched --time -1
```

The partition end offsets must sum to 8,000,000.

## A missing feature is not a loss

Do not score a peer as slower when it does not implement the feature.

librdkafka 2.15.0 does not implement KIP-932 share groups. A share-group
cell against that client is `unsupported_peer`. It is not a throughput win.
The same rule applies to KIP-848 groups and to a codec the peer was built
without, including zstd.

## Sentences this repository may publish

A sentence below is allowed only when equal semantics, the measurement
protocol, Suite HOLD, and any required independent reproduction are all
met. Until then every cell stays `not_run` and no summary claim exists.

“Superior” is against the best peer that implements the cell and has a
runnable driver. A peer without a driver, or without the feature, is
excluded. It is not counted as a loss.

| Peer | Client | Standing |
|---|---|---|
| partitionline | This repository | Subject under test |
| librdkafka | C client, pinned release, with 2.15.0 as the historical bar | Driver pending |
| kafka-clients | Apache Java, pinned release | Driver pending |
| franz-go | Go client, pinned release | Driver pending |
| rust-rdkafka | Rust bindings over librdkafka, reported separately from the C tool | Adapter pending |
| Another Rust client | Chosen when that comparison is specified | Selection pending |

For throughput, the paired 95% confidence interval’s lower bound on the
partitionline/peer ratio must be above 1.0. For latency, the upper bound
of each percentile ratio must be below 1.0 at every declared load.

| Sentence | Required result |
|---|---|
| “Fastest Kafka client” | Superior on every required cell of all six profiles, plus required lz4 and zstd cells, on both x86_64 and arm64, independently reproduced. Any loss, tie, or missing cell blocks the sentence. |
| “Fastest for this profile on this architecture” | Superior on every required cell of that profile on that architecture, independently reproduced |
| “X% faster” | The throughput ratio’s lower bound is at least 1+X in every covered cell. For latency, the percentile ratio’s upper bound is at most 1−X |
| “Most CPU-efficient” | CPU per record has an upper bound of at most 0.80 against the best peer. This is never worded as “fastest” |
| Roadmap scorecard | Throughput lower bound at least 1.20, or CPU-per-record upper bound at most 0.80, and p99 upper bound at most 1.05 at matched load |
| Anything weaker | Dated per-cell results, with every loss listed. No summary sentence |

| Profile | Primary metric | Also report |
|---|---|---|
| Bulk (`acks=1`, idempotent `acks=all`, none/lz4/zstd) | Acknowledged records/s and MB/s, checked against the high watermark | CPU per record, p99 ack latency, RSS |
| Fetch (6 and 64 partitions, both isolation levels) | Consumed records/s and MB/s, checked by id and hash | CPU per record, RSS |
| Low latency (open loop, `acks=1` and `acks=all`) | p50, p99, and p99.9 at 10%, 50%, and 80% of the weakest peer’s saturation, plus the highest load still inside the p99 budget | Schedule lag, timeouts, rejections |
| Transactional, group, share | The contract cell (commit cycles/s, or steady KIP-848 consumption) | Record histories |
| Secure (TLS 1.3, SCRAM-SHA-256) | Same as bulk | Handshakes and reauthentication, counted separately |
| Client ceiling (null broker) | Records/s per client core | No server-side validation failures. Never quoted as Kafka throughput |

These are internal targets, not claims:

- at most 0.01 steady-state produce allocations per record
- no per-record allocation on the additive fetch batch-view path
- idle producer RSS that follows active partitions, not configured connection slots

Evidence levels:

| Level | What it is | What it can support |
|---|---|---|
| L0 | Microbenchmarks, allocations, instruction counts | Internal notes |
| L1 | Client ceilings | Internal notes |
| L2 | Local broker runs, paired and repeated | Engineering direction. Unsigned |
| L3 | Controlled x86_64 and arm64 campaigns | Dated per-cell comparison, losses included |
| L4 | Independent reproduction and the Suite HOLD process | The strongest sentence above whose requirement is met |

## Version history

- **1.1.0.** Claim wording, peer set, metrics, guardrails, scorecard thresholds, client-ceiling tier, and local-evidence rules. Required lz4 and zstd bulk cells. Suite HOLD unchanged.
- **1.0.0.** Initial equal-semantics contract.
