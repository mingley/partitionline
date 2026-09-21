# Benchmark Contract and Equal-Semantics Scenario Tiers

- **Card ID:** KL04-01
- **Schema Version:** 1.0.0
- **Contract Version:** 1.0.0
- **Priority:** P1
- **Kind:** Specification
- **Deliverable:** Versioned equal-semantics contract for microbenchmarks, live-client benchmarks, and end-to-end comparisons.

---

## 1. Executive Summary and Principles

This document establishes the binding equal-semantics benchmark contract for `partitionline`. Performance numbers published for a Kafka client are meaningless without rigorous, verified equivalence in durability, ordering, delivery guarantees, security, and measurement methodology.

### Core Principles

1. **Equal Semantics is Non-Negotiable:** A comparison between two clients is strictly invalid if their durability, acknowledgment levels, idempotence, isolation levels, or security protocols differ. Higher throughput achieved by weakening durability, silently ignoring batch delivery errors, or omitting transaction markers is a defect, not a performance victory.
2. **Enqueue vs. Acknowledgment vs. Consumption:**
   - **Enqueue Acceptance:** Local buffer acceptance (e.g. `Producer::try_send` returning `Ok`) indicates only that memory was allocated or queued locally. It must never be counted or reported as an acknowledgment.
   - **Broker Acknowledgment:** Only responses returned by the broker for a `Produce` request count as acknowledged (`acked`).
   - **Independent Consumption and Delivery:** High-watermark equality and consumer receipt prove that records actually reached the log. Acknowledged records must be audited against partition high-watermark deltas.
   - **Acknowledgment is Not Fsync:** Broker acknowledgment confirms replication according to `min.insync.replicas` and `acks`; it does not prove per-record disk `fsync`. Fsync guarantees depend entirely on broker log-flush settings and OS page cache writeback.
3. **No Universal Kafka Performance Certification:** No universal, standards-body performance certification exists for Kafka clients. All scorecard targets, comparisons, and claims in this repository are proposed benchmarks and engineering targets, not certified industry standards.
4. **Transparent Reporting of Wins and Losses:** Benchmark publications must report all evaluated cells, including regressions and losses. A selective cherry-pick of favorable payload sizes or uncompressed paths is prohibited.
5. **Specification Status:** The status of every scenario cell defined under this contract is `specified` or `not_run`. This specification card does not execute benchmark campaigns; no cell has disposition `passed`.

---

## 2. Suite HOLD and Historical Signoff Rules

Historical benchmark and latency gate findings govern this contract:

1. **Preserve Suite HOLD:** Suite HOLD remains active. Unsigned samples, shared-runner CI runs, and local-VM samples do not lift Suite HOLD.
2. **Historical Signoff Rules Stay:** Thresholds must never be silently relaxed or moved to greenwash a latency or throughput miss:
   - **Historical Nested Miss:** On CI run `33938039612` (2026-09-05), a nested integrity job measured produce-ack p99 latency of **1,344 µs** against a **750 µs** ceiling under GitHub Actions runner load. That miss is historical fact; `integrity-smoke` now sets `SKIP_LATENCY_GATE=1` under `REQUIRE_INTEGRITY=1` to avoid conflating host load with integrity failures.
   - **CI Latency Ceilings:** The shared-runner CI ceiling of **5,000 µs** (`docs/latency-ci-policy.json`) exists solely as a catastrophic regression detector under noisy-neighbor virtualization. The local-native relative gate of **750 µs** (500 µs baseline + 50% slack) remains the local developer check. Neither gate represents a qualification signoff.
3. **Lab A Controlled-Host Signoff:** Official performance qualification requires signed evidence on controlled bare-metal hardware (historically designated Lab A) signed off by Kernel Integrity.

---

## 3. Equal-Semantics Contract: Required Match Fields

Two client benchmark runs are comparable if and only if all required match fields are identical:

| Required Match Field | Matching Rule | Invalidation Trigger |
|---|---|---|
| `durability` | Replication factor (RF), `min.insync.replicas`, and broker cluster topology must be identical. | Comparing RF=1 against RF=3, or min.isr=1 against min.isr=2. |
| `acks` | Producer acknowledgment level must be identical: `0` (fire-and-forget), `1` (leader ack), or `-1`/`all` (full ISR quorum). | Comparing `acks=1` on Client A against `acks=all` on Client B. |
| `idempotence` | `enable.idempotence` must be identical on both clients. When enabled, `max.in.flight.requests.per.connection` must be $\le 5$. | Comparing idempotent producer (sequence tracking) against non-idempotent producer. |
| `isolation` | Consumer `isolation.level` must match: `read_uncommitted` or `read_committed`. | Comparing consumer reading uncommitted records against consumer evaluating abort index. |
| `security` | Wire protocol (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`) and SASL mechanism (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`) must match. | Comparing plain socket traffic against TLS-encrypted traffic. |

### Enqueue Acceptance vs. Broker Acknowledgment

A critical pitfall in client benchmarking is mistaking queue throughput for network/broker throughput.
- In `partitionline`, calling `producer.try_send()` or `producer.send()` enqueues a record into an active batch partition queue. If the benchmark terminates or counts records at enqueue time without awaiting `flush()` or per-record futures, the benchmark measures memory copy speed, not Kafka produce performance.
- In `librdkafka`, `rd_kafka_produce()` enqueues to the outbound queue (`queue.buffering.max.messages`). The benchmark must await delivery callbacks (`dr_cb`) or `rd_kafka_flush()` before counting records as acked.
- In Java, `producer.send()` returns a `Future<RecordMetadata>`. The benchmark must await the future or count inside the completion callback.

---

## 4. Measurement Protocol and Methodology

All benchmarks executed under this contract must comply with the following protocol.

### 4.1 Warmup Phase
- **Mandatory:** Every test run must include a distinct warmup phase of at least 15 seconds or 10,000 records.
- **Exclusion:** All samples collected during warmup must be discarded from latency percentile calculations, throughput averages, and sample floor totals.
- **Rationale:** Warmup stabilizes runtime JIT compilation, TCP socket buffer expansion, connection pools, metadata caches, and broker thread pools.

### 4.2 Paired Randomized Repetitions
- **Minimum Count:** At least 5 repetitions per scenario cell are required ($N \ge 5$).
- **Pairing and Randomization:** Comparisons between Client A and Client B must alternate execution in randomized order (e.g. A-B-B-A-A-B-B-A-B-A) or follow a Latin square. Running all iterations of Client A followed by all iterations of Client B is prohibited due to thermal throttling and background host drift.

### 4.3 Steady-State Duration and Sample Floors
- **Duration:** Each repetition must sustain steady-state load for at least 60 seconds or until the sample floor is reached.
- **Latency Sample Floor:** A minimum of 10,000 individually timed request/response samples is required for latency percentile evaluation (p50, p95, p99, p99.9).
- **Throughput Sample Floor:** A minimum of 1,000,000 records (or 8,000,000 for standard bulk runs) is required for throughput rate determinations.

### 4.4 Coordinated Omission Avoidance
- Benchmarks measuring latency must employ scheduled open-loop arrival times (e.g. Poisson or fixed-rate schedule).
- Timing must record intended send time $T_{\text{intended}}$ and actual response receipt time $T_{\text{received}}$.
- Latency is defined as $T_{\text{received}} - T_{\text{intended}}$.
- Simply awaiting a synchronous loop (`send()` followed by `.await`) suffers from coordinated omission: during a broker pause, fewer requests are initiated, hiding the tail latency suffered by backlogged requests.

### 4.5 Statistical Uncertainty
- Results must report median, p50, p95, p99, p99.9, mean, min, max, and throughput (records/s and MB/s).
- Point estimates without error margins are prohibited. All reported metrics must include 95% bootstrap confidence intervals or Median Absolute Deviation (MAD).

### 4.6 Failed-Run Handling Policy
- **A Failed Cell Stays Failed:** If any repetition encounters an unhandled broker error (e.g. `NOT_ENOUGH_REPLICAS`, `OUT_OF_ORDER_SEQUENCE_NUMBER`), connection reset, dropped record, or panic, that repetition is marked **FAILED**.
- **No Rerun Erasure:** A subsequent successful rerun does not erase, hide, or overwrite a failed run. The failure must remain recorded in raw artifacts and provenance data.
- **No Progress-String Success:** Outputting log lines or progress strings (e.g. `sent 8000000 records`) does not constitute success. The run is successful only when audited against independent verification (broker high watermarks and receipt checks).

### 4.7 Historical Lab A Verification Rules
- **Topic Lifecycle:** Topics must be deleted and cleanly recreated prior to each run to reset log end offsets to zero.
- **High-Watermark Audit:** Immediately following producer completion, `kafka-get-offsets.sh` must be executed across all partitions. The sum of high-watermark offsets must exactly match the number of records claimed as sent and acknowledged:
  $$\sum_{p=0}^{P-1} \text{HW}_p == \text{Count}_{\text{acked}}$$
- **Payload Verification:** Consumer benchmarks must read the full partition range and verify that message payloads and sequence IDs are uncorrupted.

---

## 5. Scenario Profiles and Tiers

The benchmark contract establishes six named profiles, partitioned into **required** and **exploratory** tiers:

```
+-----------------------------------------------------------------------------+
|                          SCENARIO PROFILES & TIERS                          |
+-------------------+-------------------------------------+-------------------+
| Profile Name      | Required Tier (Core Gates)          | Exploratory Tier  |
+-------------------+-------------------------------------+-------------------+
| 1. low-latency    | Open-loop p99 produce-ack (acks 1)  | Saturation curves |
|                   | Open-loop p99 produce-ack (acks -1) | Network jitter    |
+-------------------+-------------------------------------+-------------------+
| 2. bulk           | 8M records uncompressed (acks 1)    | 64 KB payloads    |
|                   | 8M records idempotent (acks -1)     | 64 partitions     |
+-------------------+-------------------------------------+-------------------+
| 3. fetch          | 8M records pre-filled (6 parts)     | 64-partition fanout|
|                   | Fetch RPC latency profile           | Truncation recovery|
+-------------------+-------------------------------------+-------------------+
| 4. transactional  | 1K transactions (100 recs/batch)    | Aborted batch filter|
|                   | Read-committed consumer             | Epoch fence recovery|
+-------------------+-------------------------------------+-------------------+
| 5. group/share    | KIP-848 steady-state consumer       | KIP-932 share group|
|                   | Rebalance assignment latency        | Dynamic rebalance |
+-------------------+-------------------------------------+-------------------+
| 6. secure         | TLS 1.3 bulk produce                | mTLS rehandshake  |
|                   | SASL SCRAM-SHA-256 produce          | OAUTHBEARER refresh|
+-------------------+-------------------------------------+-------------------+
```

### Definitions of Tiers
- **Required Tier:** Cells that represent fundamental operational workloads. A client cannot claim parity or leadership in a profile without passing all required cells under equal semantics.
- **Exploratory Tier:** Cells designed to stress extreme boundaries (e.g. 64 KB records, 64-partition fanout, high abort ratios, share group concurrency). These cells provide diagnostic and architectural insight but do not gate baseline release qualification.

### Profile Descriptions

1. **`low-latency`:** Evaluates request service time and scheduling latency. Focuses on minimal linger (`linger.ms=0`), single-message batches, and open-loop scheduled arrival rates to eliminate coordinated omission.
2. **`bulk`:** Evaluates raw saturated throughput. Configured with larger batches (`batch.size=1MB`, `batch.num.messages=32768`) and linger (`linger.ms=5`) across 6 to 12 partitions.
3. **`fetch`:** Measures consumer consumption throughput and Fetch RPC latency from pre-populated topic partitions. Assesses batch parsing, zero-copy buffer slicing, and memory allocation.
4. **`transactional`:** Measures the overhead of transactional coordination: `InitProducerId`, `AddPartitionsToTxn`, produce batches with producer epoch, and two-phase commit markers (`EndTxn`). Measures consumer filtering under `read_committed`.
5. **`group/share`:** Measures consumer group coordination, heartbeat efficiency, partition assignment under classic and KIP-848 protocols, and concurrent record consumption under KIP-932 Share Groups.
6. **`secure`:** Measures encryption and authentication overhead. Evaluates TLS 1.3 record throughput, symmetric crypto throughput (AES-GCM / ChaCha20-Poly1305), and SASL handshake costs (SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER).

---

## 6. Frozen Knobs and Parameter Matrix

To guarantee equal semantics, every benchmark run must freeze and declare values for all parameter dimensions:

| Parameter Dimension | Frozen Knob | Standard Bulk Value | Standard Low-Latency Value |
|---|---|---|---|
| **Durability & ISR** | `replication_factor` | 1 (Lab A) / 3 (HA) | 1 (Lab A) / 3 (HA) |
| | `min.insync.replicas` | 1 (Lab A) / 2 (HA) | 1 (Lab A) / 2 (HA) |
| **Acks** | `acks` | 1 or -1 (`all`) | 1 or -1 (`all`) |
| **Idempotence** | `enable.idempotence` | `true` (when acks=-1) | `true` (when acks=-1) |
| **Isolation** | `isolation.level` | `read_uncommitted` | `read_committed` |
| **Payload Size & Entropy** | `payload_size_bytes` | 100 bytes | 100 bytes |
| | `payload_entropy` | Uniform pseudorandom | Uniform pseudorandom |
| **Headers, Keys, Skew** | `key_type` | null or 16-byte UUID | null or 16-byte UUID |
| | `has_headers` | false | false |
| | `partition_skew` | Uniform round-robin | Uniform round-robin |
| **Partitions** | `partition_count` | 6 partitions | 1 or 6 partitions |
| **Batching** | `linger.ms` | 5 ms | 0 ms |
| | `batch.size` (bytes) | 1,048,576 (1 MB) | 1,000 bytes |
| | `batch.num.messages` | 32,768 records | 1 record |
| **In-Flight & Connections**| `max.in.flight` | 5 | 1 or 5 |
| | `connections_per_broker`| 1 | 1 |
| | `socket.nagle.disable` | true (`TCP_NODELAY`) | true (`TCP_NODELAY`) |
| **Network Topology** | `environment` | Loopback / Controlled LAN | Loopback / Controlled LAN |
| | `rtt_ms` | < 0.2 ms | < 0.2 ms |

---

## 7. Fully Specified Three-Way Matched Scenario

To demonstrate the contract in practice, this specification defines a fully specified three-way matched scenario across **Rust (`partitionline`)**, **C (`librdkafka`)**, and **Java (`org.apache.kafka:kafka-clients`)**.

- **Scenario ID:** `matched-bulk-acks-all-6p-uncompressed`
- **Profile:** `bulk`
- **Tier:** `required`
- **Status:** `not_run`
- **Disposition:** `not_run`
- **Description:** Idempotent, high-throughput bulk produce of 8,000,000 records across 6 partitions, `acks=-1`, uncompressed, with in-flight requests capped at 5.

### Frozen Knobs

| Knob | Frozen Value |
|---|---|
| Total Timed Records | 8,000,000 |
| Warmup Records | 10,000 |
| Payload Size | 100 bytes (uniform random bytes, seed `0x5EED0001`) |
| Key | 16-byte UUID string |
| Topic | `plbench-matched` (6 partitions, RF=1, min.isr=1) |
| Acks | `-1` (`all`) |
| Idempotence | `true` |
| Linger | 5 ms |
| Batch Size (Bytes) | 1,048,576 bytes (1 MB) |
| Batch Size (Records) | 32,768 records |
| Max In-Flight Requests | 5 |
| TCP NoDelay | Enabled (`nagle.disable=true`) |
| Broker Version | Apache Kafka 3.9.1 KRaft on `127.0.0.1:9092` |

### Peer Client Invocation and Configuration

#### 1. Rust: `partitionline`
```bash
COUNT=8000000 \
WARMUP=10000 \
PAYLOAD_BYTES=100 \
ACKS=-1 \
LINGER_MS=5 \
BATCH_SIZE=1048576 \
BATCH_RECORDS=32768 \
IDEMPOTENT=1 \
MAX_IN_FLIGHT=5 \
KAFKA_TOPIC=plbench-matched \
cargo run --release --example bench_produce
```

#### 2. C: `librdkafka` (via `rdkafka_performance`)
```bash
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

#### 3. Java: `kafka-clients` (via `ProducerPerformance`)
```bash
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

### Review Notes: Matched Scenario Equivalence Analysis

1. **Linger Default Pitfall:** In `librdkafka`, the default `linger.ms` is 1,000 ms (1 second), whereas in Java `kafka-clients` the default is 0 ms, and in `partitionline` it is configurable via builder. Leaving `linger.ms` unspecified causes severe skew (librdkafka will batch aggressively up to 1s while Java sends immediate tiny batches). Explicitly freezing `linger.ms=5` ensures equal batch accumulation windows.
2. **In-Flight Bound Enforcement:** With `enable.idempotence=true`, Kafka broker protocol requires `max.in.flight.requests.per.connection <= 5`. If set higher, the broker rejects produce requests with `OUT_OF_ORDER_SEQUENCE_NUMBER`. All three clients are explicitly pinned to 5.
3. **Queue Buffering and Backpressure:** `librdkafka` defaults `queue.buffering.max.messages` to 100,000. Under 8M record load, this queue fills instantly and blocks unless sized appropriately (`1,000,000`). Similarly, `partitionline` must enforce backpressure rather than unbounded memory growth (see KL02 resource contract).
4. **Verification Requirement:** Upon completion of any matched run, the post-run audit script must verify:
   ```bash
   kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic plbench-matched --time -1
   ```
   The sum of partition end offsets must strictly equal 8,000,000.

---

## 8. Explicitly Unsupported Peer Cell and Non-Win Scoring Rules

A comparison is invalid if a competitor is scored as "losing" when it completely lacks the underlying protocol feature.

### Unsupported Peer Cell Specification

- **Cell ID:** `unsupported-librdkafka-share-groups`
- **Profile:** `group/share`
- **Target Capability:** KIP-932 Share Groups (`ShareConsume` and `ShareAcknowledge` RPCs for cooperative queue-like consumption)
- **Peer Client:** `librdkafka` (version 2.15.0)
- **Missing Capability:** KIP-932 Share Consumer Protocol
- **Status:** `specified`
- **Disposition:** `unsupported`
- **Mandatory Scoring Rule:** **Must not be scored as a win.**

### Review Notes: Non-Win Scoring Rules for Unsupported Capabilities

1. **Protocol Absence is Not a Performance Defeat:** `librdkafka` 2.15.0 does not implement KIP-932 Share Groups (it supports classic partition-assigned consumer groups via KIP-345/KIP-429). Attempting to run a share group benchmark against librdkafka either fails at initialization or forces fallback to a completely different consumption model (partition-locked consumer groups).
2. **Prohibition Against Misleading Victory Claims:** Claiming "partitionline has 5x the share group throughput of librdkafka" is false and deceptive when librdkafka cannot participate in the protocol. Such cells must be classified strictly as `unsupported_peer` in all published comparison matrices.
3. **Applicability to Other Features:** The same rule applies to other protocol capabilities:
   - Comparing KIP-848 next-gen consumer groups against clients lacking KIP-848 support must not be scored as a rebalance speed win.
   - Comparing zstd decompression against a peer client compiled without zstd support must not be scored as a codec win.

---

## 9. Implementation Roadmap

This specification card (`KL04-01`) completes the equal-semantics contract. Subsequent cards in the KL-04 package build directly upon this foundation:

- **KL04-02:** Add machine-readable raw benchmark result and provenance format (`benchmarks/provenance.json`).
- **KL04-03:** Check in pinned Apache Java benchmark peer driver.
- **KL04-04:** Check in reproducible `librdkafka` C benchmark peer driver.
- **KL04-05:** Add pinned Rust-client comparison adapter.
- **KL04-06:** Measure scheduled open-loop latency through saturation without coordinated omission.
- **KL04-07:** Gate benchmark runs on record histories and payload integrity verification.
- **KL04-08:** Automate paired randomized benchmark executions.
- **KL04-09:** Add wire-codec and buffer allocation microbenchmarks.
- **KL04-10:** Record controlled x86_64 benchmark campaign.
- **KL04-11:** Record controlled arm64 benchmark campaign.
- **KL04-12:** Publish reproducible comparison report from raw artifacts.
- **KL04-13:** Profile measured bottlenecks and generate surgical optimization cards.
- **KL04-14:** Obtain independent benchmark reproduction and Kernel Integrity signoff.
