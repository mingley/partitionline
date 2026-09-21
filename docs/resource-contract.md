# Resource Ownership and Outcome Budget Contract

**Specification ID:** KL02-01  
**Status:** Approved Specification  
**Current HEAD:** `bed63aeb7bf9c4d626ef25ec7d57dec9f5fd2000` (`bed63ae`)  
**Audit Baseline:** `cb7e97d3b92a8555aea34d59266a2990c206395f` (`cb7e97d`)  
**Scope:** Memory ownership, accounting boundaries, outcome classification, and release lifecycles across produce, network, and consume pipelines.

> **Provenance and Line Citation Note:**  
> Audit line numbers in `docs/audits/2026-09-21.md` were frozen at commit `cb7e97d` and may have moved in later commits. Current repository HEAD is `bed63ae`, which incorporates the audited planning framework (`docs/plan/*`). All `file:line` citations in this specification refer to verified source positions in current HEAD (`bed63ae`).
>
> In accordance with repository policy, `#![forbid(unsafe_code)]` / `#![deny(unsafe_code)]` is strictly observed; no unsafe code proposals or memory management tricks are permitted.

---

## 1. Accounting Contract: What Today's Counters Include and Exclude

### 1.1 Current Producer Accounting (`buffer_memory` & `bytes_buffered`)

In the producer, byte reservations are managed by `Shared::try_reserve_buffer` (`src/producer.rs:894`) and released by `Shared::release_buffer` (`src/producer.rs:909`). The reserved quantity is calculated exclusively by `rec_bytes` (`src/producer.rs:918`):

```rust
fn rec_bytes(rec: &ProduceRecord) -> u64 {
    let k = rec.key.as_ref().map(bytes::Bytes::len).unwrap_or(0);
    let v = rec.value.as_ref().map(bytes::Bytes::len).unwrap_or(0);
    u64::try_from(k.saturating_add(v)).unwrap_or(u64::MAX)
}
```

#### What is INCLUDED Today:
1. **Key Payload Length:** The length in bytes of `rec.key` (`Bytes::len`).
2. **Value Payload Length:** The length in bytes of `rec.value` (`Bytes::len`).

#### What is EXCLUDED Today (Gaps against Total Memory):
1. **Record Headers:** `rec.headers: Vec<RecordHeader>`. Both header keys (`String`) and optional header values (`Option<Bytes>`) are completely uncharged against `buffered_bytes`. A record containing 2 MB of headers and an empty key/value reserves 0 bytes in `buffered_bytes`.
2. **Object Overhead:** Heap allocations for `Pending` (`src/producer.rs:742`), `ProduceRecord`, `RecordHeader` vectors, `oneshot::channel` state, and Tokio `mpsc` queue nodes.
3. **Shared Backing Allocations:** If `Bytes` slices reference a large underlying allocation (e.g., a 64 MB slab sliced into 16-byte chunks), only the 16-byte slice length is accounted for, not the retained underlying heap capacity.
4. **Wire Encoding & Batch Header Overhead:** `Worker.write_buf: BytesMut` (`src/producer.rs:2738-2759`), batch headers (`BatchHeader`, magic, CRC32C, timestamps, attributes, producer ID/epoch, base sequence), and variable-length integer encodings.
5. **Compressed / Transformed Buffers:** Intermediate memory allocated during batch compression (gzip, snappy, LZ4 in `src/protocol/records.rs:1620-1705`).
6. **Network Socket Buffers:** The underlying kernel TCP socket send/receive buffers and user-space I/O framing buffers (`read_buf: BytesMut` up to `MAX_FRAME` = 100 MB in `src/net.rs:28, 785`).
7. **Spawned Async Tasks:** Stack and task allocations for worker tasks (`Worker::run`, `src/producer.rs:2590`), retry tasks (`retry_loop`, `src/producer.rs:2422`), metadata refresh tasks, and connection loops.

### 1.2 Current Consumer Accounting

1. **Fetch Buffers:** The consumer requests up to `fetch_max_bytes` (default 50 MB, `src/consumer.rs:242`) and `max_partition_fetch_bytes` (default 16 MB, `src/consumer.rs:243`) in `encode_fetch_request` (`src/consumer.rs:2327`).
2. **Decompression Expansion (Audit Finding A08):** Decompression routines (`gzip_decompress`, `snappy_decompress`, `lz4_decompress` in `src/protocol/records.rs:1620-1705`) decompress the full payload using `read_to_end` or chunk loops into freshly allocated vectors. There is currently no pre-allocation or streaming decoded-byte limit.
3. **Pending Record Buffering:** Records exceeding `max_poll_records` (default `None`, `src/consumer.rs:248`) are placed into `self.pending: VecDeque<FetchedRecord>` (`src/consumer.rs:3166`). This queue is unbounded in total bytes.

### 1.3 RSS Non-Equivalence Statement

> **Process RSS is NOT bounded by `bytes_buffered` or `buffer_memory`.**  
> `bytes_buffered` tracks only the sum of unacknowledged record key and value slice lengths. Real process Resident Set Size (RSS) includes heap fragmentation, allocator page pools (jemalloc/glibc arenas), Tokio runtime worker threads, connection TLS context buffers, socket frame buffers, decompressed batch vectors, and client metadata. Conflating `bytes_buffered` with process memory bounds is explicitly prohibited.

### 1.4 Deferred Budget Gaps

The exclusions above are design gaps in the current implementation. They are recorded as proposed deliverables for subsequent KL cards, not as already enforced:
- **KL02-02 (Producer Retained-Byte Budget):** Expand reservation accounting to cover header overhead and wire serialization upper bound or declared batch reservation.
- **KL02-03 (Decoded Record-Batch Expansion Bound):** Enforce an explicit decoded-byte ceiling during decompression before allocating expanded output.
- **KL02-08 (Consumer Fetch Buffer Bounds):** Enforce an aggregate memory ceiling across pre-fetched partition queues.

---

## 2. Byte Ownership and Lifecycle Table

Every byte entering the client has exactly one unambiguous owner across each lifecycle stage and exactly one release owner on every terminal path.

| Stage | Data / Object Representation | Accounting / Budget Mechanism | Single Release Owner (Terminal Path) | Trigger & Source Citation |
|---|---|---|---|---|
| **1. Enqueue & Validation** | `ProduceRecord` | Java upper-bound validation via `reject_oversized` (`src/producer.rs:961`). | None (no memory reserved if rejected). | Immediate return of `Error::RecordTooLarge` (`src/producer.rs:991`). |
| **2. Buffer Reservation** | Key + Value byte count | Increments `Shared.buffered_bytes` via `try_reserve_buffer` (`src/producer.rs:894`). | `Producer::send_all` or `Producer::try_send` on pre-send fail. | `Shared::release_buffer` (`src/producer.rs:909`). |
| **3. Partition Queue** | `Pending` (`src/producer.rs:742`) | Channel capacity (`ProducerConfig::queue_capacity`, default 1024). | `Producer::send_all` (`src/producer.rs:1359`) or `Producer::try_send` (`src/producer.rs:1468`). | `w.data.send(p).is_err()` triggers `release_buffer(bytes)`. |
| **4. Worker Batch Accumulator** | `Worker.pending: Vec<Pending>` (`src/producer.rs:2535`) | Bounded by `batch_records` / `produce_batch_bytes` / `linger`. | `Worker::fire` via `fail_pendings` (`src/producer.rs:3329`). | Pre-send failure (e.g. partition missing, worker shutdown). |
| **5. Retry Queue & Backoff** | `Pending` in `Shared.retry_tx` (`src/producer.rs:800`) | Channel capacity (`cap.max(1024)`). Reservation remains held in `buffered_bytes`. | `retry_one` via `fail_pendings` (`src/producer.rs:2432, 3329`). | Deadline expiry (`Instant::now() >= p.deadline`) or unresolvable leader. |
| **6. Request Encode & Socket Write** | `Worker.write_buf: BytesMut` (`src/producer.rs:2738-2759`) | Temporary worker scratch buffer. Transmitted via `BrokerConn::write_all_timeout` (`src/net.rs:616`). | `Worker::fire` via `fail_groups` -> `fail_pendings` (`src/producer.rs:3323`). | Non-retriable socket I/O error during write. |
| **7. Acks=0 Immediate Ack** | Network frame transmitted | Wire write complete. | `complete_acks0` (`src/producer.rs:3294`). | `complete_acks0` calls `release_buffer(pendings_bytes(&pendings))` (`src/producer.rs:3296`). |
| **8. In-Flight Response Wait** | `InFlight` (`src/producer.rs:2540`) in `Worker.in_flight: VecDeque<InFlight>` | Bounded by `max_in_flight` (default 5). Reservation remains held in `buffered_bytes`. | `Worker::wait_one` (`src/producer.rs:2763`). | **Success:** `Worker::wait_one` calls `release_buffer` (`src/producer.rs:2867`).<br>**Failure:** calls `fail_pendings` (`src/producer.rs:3329`). |
| **9. Producer Shutdown / Drain** | `Worker::drain_inflight` (`src/producer.rs:2982`) | Drains remaining batches and in-flight responses. | `fail_inflight` (`src/producer.rs:3317`) and `fail_pendings` (`src/producer.rs:3329`). | Worker termination or close timeout cleans up all unacked records. |
| **10. Consumer Fetch Framing** | `FetchRequest` frame | `fetch_max_bytes` / `max_partition_fetch_bytes` in request (`src/consumer.rs:2327`). | Broker framing. | No client pre-allocation permit. |
| **11. Consumer Frame Ingestion** | `BrokerConn.read_buf: BytesMut` (`src/net.rs:785`) | Bounded by `MAX_FRAME` (100 MB, `src/net.rs:28`). | `BrokerConn::read_frame` (`src/net.rs:785`). | Frame frozen into `Bytes` and returned to caller. |
| **12. Decompression & Batch Decode** | `RecordBatch` & `Record` (`src/protocol/records.rs:2031`) | Heap allocation in `gzip_decompress`, `snappy_decompress`, `lz4_decompress`. | Decoder / caller. | Output converted to `FetchedRecord`s; intermediate vectors dropped. |
| **13. Consumer Pending Queue** | `self.pending: VecDeque<FetchedRecord>` (`src/consumer.rs:3166`) | `max_poll_records` limits output batch per `poll()` (`src/consumer.rs:3175`). | Application / Consumer drop. | Application consumes `FetchedRecord`; remaining held until next fetch. |
| **14. Consumer Close / Rebalance** | `self.pending` dropped | Position commit decoupled from queue drop. | `Consumer::close` (`src/consumer.rs:2212`) or rebalance clear. | Pending records dropped from memory on unsubscribe/close. |

---

## 3. Four Concrete Record Traces (Current Source)

### Trace 1: Successful Produce Record

1. **Ingestion & Size Check:**
   - Caller invokes `Producer::send(rec)` (`src/producer.rs:1311`), which delegates to `Producer::send_all` (`src/producer.rs:1321`).
   - Interceptors process record via `on_send` (`src/producer.rs:1345`).
   - `reject_oversized` (`src/producer.rs:961`) computes key + value length via `rec_bytes` (`src/producer.rs:918`) and validates that `serialized` size (`estimate_size_in_bytes_upper_bound`) does not exceed `max_request_size` or `buffer_memory`.
2. **Buffer Permit Reservation:**
   - `Producer::wait_buffer` (`src/producer.rs:1277`) calls `Shared::try_reserve_buffer` (`src/producer.rs:894`).
   - `Shared.buffered_bytes.fetch_add(bytes)` succeeds. Bytes are now officially reserved.
3. **Queueing to Worker:**
   - `Producer::worker_for` (`src/producer.rs:1215`) locates `WorkerHandle` for the partition leader.
   - A `oneshot::channel` `(tx, rx)` is allocated (`src/producer.rs:1344`).
   - `Pending` struct (`src/producer.rs:742`) is sent via `w.data.send(p).await` (`src/producer.rs:1356`).
4. **Worker Batching & Fire:**
   - Worker task running `Worker::run` (`src/producer.rs:2590`) receives record into `self.pending: Vec<Pending>` via `Worker::pull_ready` (`src/producer.rs:2559`).
   - `Worker::can_fire` (`src/producer.rs:2576`) signals readiness based on batch size or `linger` expiration.
   - `Worker::fire` (`src/producer.rs:2674`) drains up to `take_count` records from `self.pending`.
   - Sequences are assigned via `assign_sequences` (`src/producer.rs:2707`).
   - The batch is serialized into `self.write_buf: BytesMut` via `encode_produce_body` (`src/producer.rs:2724`).
   - Batch is transmitted over TCP via `BrokerConn::write_all_timeout` (`src/net.rs:616`).
5. **In-Flight Tracking & Broker Acknowledgment:**
   - The batch is registered in `Worker.in_flight: VecDeque<InFlight>` (`src/producer.rs:2766`).
   - `Worker::wait_one` (`src/producer.rs:2763`) invokes `BrokerConn::read_response` (`src/net.rs:632`) and decodes the response using `decode_produce_response` (`src/producer.rs:2787`).
   - Partition response matches topic and partition with `r.error_code == 0`.
6. **Terminal Release & Completion:**
   - `Worker::wait_one` calls `self.shared.release_buffer(pendings_bytes(&pendings))` (`src/producer.rs:2867`), subtracting the reserved bytes from `Shared.buffered_bytes` and waking waiters via `buffer_nudge.notify_waiters()`.
   - Metrics updated via `note_acked` (`src/producer.rs:2868`) and `note_ack_latency` (`src/producer.rs:2870`).
   - `RecordMetadata` constructed and sent to caller via `tx.send(Ok(md))` (`src/producer.rs:2883`).
   - **Outcome:** `COMPLETED`. **Release Owner:** `Worker::wait_one` (`src/producer.rs:2867`).

---

### Trace 2: Retried Produce Record

1. **Initial Pipeline to Transmission:**
   - Record passes Ingestion, `Shared::try_reserve_buffer` (`src/producer.rs:894`), enqueue, and `Worker::fire` (`src/producer.rs:2674`) exactly as in Trace 1.
2. **Retriable Failure Occurrence:**
   - *Case A (Socket write failure):* `BrokerConn::write_all_timeout` (`src/net.rs:616`) returns a retriable I/O error (`e.is_retriable()`). `Worker::fire` calls `Worker::requeue` (`src/producer.rs:2753`), which invokes `Worker::requeue_pendings` (`src/producer.rs:2962`).
   - *Case B (Broker response error):* In `Worker::wait_one` (`src/producer.rs:2763`), `BrokerConn::read_response` returns a retriable error, or `r.error_code` is retriable (e.g. `NOT_LEADER_OR_FOLLOWER`, `REQUEST_TIMED_OUT`). `Worker::wait_one` invokes `Worker::requeue_pendings` (`src/producer.rs:2908`).
3. **Requeue & Retention Across Retries:**
   - `Worker::requeue_pendings` (`src/producer.rs:2962`):
     - Increments `p.retry = p.retry.saturating_add(1)`.
     - Increments `Shared.retries_out.fetch_add(1, Ordering::SeqCst)`.
     - Sends `Pending` to `Shared.retry_tx` (`src/producer.rs:2966`).
     - **Crucial Invariant:** `Shared::release_buffer` is **NOT** called. The original permit in `Shared.buffered_bytes` remains held across the retry queue.
4. **Retry Loop Processing:**
   - `retry_loop` (`src/producer.rs:2422`) receives `p` from `retry_rx` and calls `retry_one` (`src/producer.rs:2432`).
   - Checks deadline: `if Instant::now() >= p.deadline`, calls `fail_pendings` (`src/producer.rs:2433`) (which releases the buffer and fails).
   - Sleeps for backoff duration via `sleep_retry_backoff` (`src/config.rs:434`, `src/producer.rs:2436`).
   - Checks deadline again.
   - Refreshes partition metadata if required via `partitions_for` (`src/producer.rs:2460`).
   - Discovers new leader node ID, fetches `WorkerHandle` for the new leader node, and dispatches `p` to the target worker's `data` channel (`src/producer.rs:2517`).
   - Decrements `Shared.retries_out`.
5. **Re-transmission & Resolution:**
   - The new worker accumulates `p` in its `pending` queue and re-executes `Worker::fire`.
   - If subsequent attempt succeeds: `Worker::wait_one` calls `Shared::release_buffer` (`src/producer.rs:2867`) -> `COMPLETED`.
   - If retries exhaust or timeout expires: `retry_one` or `Worker::wait_one` calls `fail_pendings` (`src/producer.rs:3329`), releasing the buffer -> `FAILED` or `AMBIGUOUS`.
   - **Outcome:** `COMPLETED` on success; `FAILED` or `AMBIGUOUS` on exhaustion. **Release Owner:** `Worker::wait_one` or `fail_pendings`.

---

### Trace 3: Cancelled Produce Record

1. **Enqueue and Caller Future Cancellation:**
   - Caller initiates `let send_fut = producer.send(rec);`.
   - `Producer::send_all` (`src/producer.rs:1321`) validates record, acquires buffer permit in `Shared.buffered_bytes` via `wait_buffer` (`src/producer.rs:1277`), and enqueues `Pending` with `tx: Some(tx)` into `w.data` (`src/producer.rs:1356`).
   - While record is lingering in `Worker.pending` or already in-flight, the caller drops `send_fut` (e.g., Tokio `timeout`, select drop, or task cancellation).
2. **Internal Lifecycle Continuation:**
   - Dropping `send_fut` drops the receiver `rx: oneshot::Receiver<Result<RecordMetadata>>`.
   - The worker task (`Worker::run`, `src/producer.rs:2590`) **retains full ownership** of the record inside `Worker.pending` or `Worker.in_flight`.
   - The record is **not** dequeued or discarded. The worker fires the batch across the TCP connection via `BrokerConn::write_all_timeout` (`src/net.rs:616`).
3. **Response Handling with Dead Channel:**
   - `Worker::wait_one` (`src/producer.rs:2763`) receives broker response.
   - `Shared::release_buffer(pendings_bytes(&pendings))` is executed at `src/producer.rs:2867`.
   - Worker attempts to notify the caller: `if let Some(tx) = p.tx { drop(tx.send(Ok(md))); }` (`src/producer.rs:2883`).
   - `tx.send(...)` returns `Err(Ok(md))` because the receiver is closed. The error is discarded via `drop(...)`.
   - If the worker instead hit a terminal failure, `fail_pendings` (`src/producer.rs:3329`) executes `Shared::release_buffer` (`src/producer.rs:3330`).
4. **Outcome & Release Guarantee:**
   - The memory permit is released exactly once.
   - **Outcome:** `AMBIGUOUS`. The client cannot prove whether the broker committed the record or not. (Verified empirically in `tests/produce_cancel.rs:43-85`).
   - **Release Owner:** `Worker::wait_one` (`src/producer.rs:2867`) or `fail_pendings` (`src/producer.rs:3330`).

---

### Trace 4: Failed Produce Record

Failures occur at distinct boundaries with distinct ownership rules:

#### Path A: Pre-Enqueue Rejection (Size Limits)
1. In `reject_oversized` (`src/producer.rs:961`), serialized record size exceeds `max_request_size` or `buffer_memory`.
2. Returns `Err(Error::RecordTooLarge(...))` (`src/producer.rs:986, 991`).
3. No buffer memory was acquired; no cleanup required.
4. **Outcome:** `FAILED`. **Release Owner:** None (no allocation).

#### Path B: Buffer Saturation Timeout
1. In `Producer::wait_buffer` (`src/producer.rs:1277`), `try_reserve_buffer` repeatedly returns `false` until `max_block` deadline expires.
2. Returns `Err(Error::Timeout)`.
3. No buffer bytes were reserved.
4. **Outcome:** `FAILED`. **Release Owner:** None.

#### Path C: Immediate Channel Enqueue Failure
1. `wait_buffer` reserved `bytes` in `Shared.buffered_bytes`.
2. Worker channel send `w.data.send(p).await` fails (`src/producer.rs:1357`) because worker shut down or queue disconnected.
3. Caller immediately executes `self.inner.shared.release_buffer(bytes)` (`src/producer.rs:1359`) and returns `Err(Error::Closed)`.
4. In `try_send`, `w.data.try_send(p)` failure similarly triggers `self.inner.shared.release_buffer(bytes)` (`src/producer.rs:1468`).
5. **Outcome:** `FAILED`. **Release Owner:** `Producer::send_all` (`src/producer.rs:1359`) / `Producer::try_send` (`src/producer.rs:1468`).

#### Path D: Non-Retriable Broker Rejection
1. Record is batched and sent via `Worker::fire` (`src/producer.rs:2674`).
2. `Worker::wait_one` (`src/producer.rs:2763`) decodes response.
3. Broker returns non-retriable error code (e.g. `TOPIC_AUTHORIZATION_FAILED`, `INVALID_RECORD`).
4. `fail_pendings(&self.shared, pendings, clone_err(&e))` is called (`src/producer.rs:2861, 3329`).
5. `fail_pendings` executes `shared.release_buffer(pendings_bytes(&pendings))` (`src/producer.rs:3330`), notes error metrics, triggers error interceptors, and notifies `p.tx` with `Err(e)`.
6. **Outcome:** `FAILED` (if broker cleanly rejected without persistence) or `AMBIGUOUS` (if broker state is uncertain). **Release Owner:** `fail_pendings` (`src/producer.rs:3330`).

---

## 4. Outcome Classification: Completed, Failed, and Ambiguous

To prevent silent data corruption, silent data duplication, and incorrect retry loops, outcome semantics are strictly defined:

```
+---------------------------------------------------------------------------------------+
|                                    OUTCOME SPECTRUM                                   |
+---------------------------+-----------------------------------+-----------------------+
|         COMPLETED         |             AMBIGUOUS             |         FAILED        |
+---------------------------+-----------------------------------+-----------------------+
| Broker verified write     | State of write on broker cannot   | Proved that write was |
| persisted (or consumer    | be proven (network timeout, drop, | not persisted, or was |
| delivered record).        | disconnect post-send, shutdown).  | rejected pre-send.    |
+---------------------------+-----------------------------------+-----------------------+
```

### 4.1 Completed
- **Definition:** The operation reached a deterministic successful terminal state verified by the protocol.
- **Producer:**
  - For `acks != 0`: The broker returned a `ProduceResponse` with `error_code == 0` and assigned a valid non-negative `base_offset` (`src/producer.rs:2866-2885`).
  - For `acks == 0`: The complete request frame was successfully flushed to the network socket (`complete_acks0`, `src/producer.rs:3294`).
- **Consumer:**
  - The record batch was successfully read, decompressed, decoded, and yielded to application caller via `ConsumerRecords` in `Consumer::fetch` (`src/consumer.rs:2067`).

### 4.2 Failed
- **Definition:** The client can **prove** that the record was not retained by the broker (or not delivered by the consumer).
- **Producer Conditions:**
  - Rejected prior to network transmission (e.g., oversized record in `reject_oversized`, queue full in `try_send`, buffer wait timeout in `wait_buffer`, metadata resolution failure).
  - Transmission failed on socket write before any bytes were sent, and the worker channel closed.
  - Broker returned an explicit non-retriable authorization or semantic rejection (e.g., `TOPIC_AUTHORIZATION_FAILED`, `CLUSTER_AUTHORIZATION_FAILED`, `RECORD_LIST_TOO_LARGE`) that guarantees the log was not appended.
- **Consumer Conditions:**
  - Broker returned an unrecoverable partition error (`OFFSET_OUT_OF_RANGE`, `TOPIC_AUTHORIZATION_FAILED`).

### 4.3 Ambiguous
- **Definition:** The client **cannot prove** whether the broker retained the write or not.
- **Producer Conditions:**
  1. **Post-Write Disconnect / Timeout:** Request bytes were successfully written to the TCP stream (`BrokerConn::write_all_timeout` succeeded), but reading the response (`BrokerConn::read_response`) timed out or failed due to EOF / connection reset. The broker may have committed the write before dropping the connection.
  2. **Caller Cancellation:** The caller dropped the completion future (`send().await`) while the record was buffered in linger, queued, in-flight, or retrying.
  3. **Delivery Timeout / Retries Exhausted After Send:** The record was sent over the wire at least once, retried due to broker disconnect or `NOT_LEADER_OR_FOLLOWER`, and the overall `delivery_timeout` expired before a definitive acknowledgment arrived.
  4. **Client Shutdown During Flight:** The client was closed (`Producer::close_timeout`) while requests were outstanding on the network.
- **Consumer Conditions:**
  - Socket severed mid-fetch after consumer offset advancement was staged but before records were yielded to application.

### 4.4 Inviolable Contract Rule: Never Collapse Ambiguous into Failed
> **Rule:** Ambiguous outcomes MUST NOT be collapsed into `Error::Fail` or reported as "not written".  
> In Kafka, treating an ambiguous write as definitely failed leads applications to re-produce non-idempotent or non-transactional records, producing silent duplicate writes and corrupting ordering invariants. If the client cannot guarantee non-persistence, it must classify the result as ambiguous.

---

## 5. Configuration, Defaults, and Oversized-First-Batch Progress Rule

### 5.1 Current Configuration Defaults

| Config Parameter | Client | Current Value in Source | Apache Kafka Java Default | Source Citation |
|---|---|---|---|---|
| `buffer_memory` | Producer | `33,554,432` (32 MB) | `33,554,432` (32 MB) | `src/producer.rs:205` |
| `max_request_size` | Producer | `1,048,576` (1 MB) | `1,048,576` (1 MB) | `src/producer.rs:204` |
| `max_block` | Producer | `60s` | `60s` | `src/producer.rs:207` |
| `max_partition_fetch_bytes` | Consumer | `16,777,216` (16 MB) | `1,048,576` (1 MB) | `src/consumer.rs:243` |
| `fetch_max_bytes` | Consumer | `52,428,800` (50 MB) | `52,428,800` (50 MB) | `src/consumer.rs:242` |
| `max_poll_records` | Consumer | `None` (unbounded) | `500` | `src/consumer.rs:248` |

### 5.2 Current Code Behavior on Oversized Records

1. **Producer Side:**
   - In `reject_oversized` (`src/producer.rs:961`):
     - If upper-bound serialized size exceeds `max_request_size` (1 MB), fails immediately with `Error::RecordTooLarge(RecordTooLarge { size, max: max_request_size })`.
     - If upper-bound serialized size exceeds `buffer_memory` (32 MB), fails immediately with `Error::RecordTooLarge(RecordTooLarge { size, max: buffer_memory })`.
     - If `size <= max_request_size`: proceeds to buffer acquisition.
   - If the record size is less than `max_request_size` (e.g. 800 KB) and the buffer is empty, `try_reserve_buffer` reserves 800 KB and allows the batch to proceed.
   - If the buffer is currently partially full such that `buffered_bytes + size > buffer_memory`, `send` blocks in `wait_buffer` up to `max_block` (60s) for previous records to drain.

2. **Consumer Side & Kafka's Oversized-First-Batch Progress Rule:**
   - **The Kafka Protocol Rule (KIP-74):** In Kafka, if the first record batch in the first ready partition is larger than `max.partition.fetch.bytes` (or `fetch.max.bytes`), the broker **still returns that batch** to ensure the consumer makes progress rather than deadlocking indefinitely.
   - **Current Client Behavior:** Current consumer specifies `partition_max_bytes = self.cfg.max_partition_fetch_bytes` in `encode_fetch_request` (`src/consumer.rs:2327`). When the broker returns an oversized first batch, current code decodes and returns it in `apply_fetch_body` (`src/consumer.rs:2580`) without enforcing a client-side discard. Progress is maintained.
   - **Current Defect/Risk:** Because decompression in `src/protocol/records.rs:1620-1705` does not enforce a decoded output ceiling, an adversarial or corrupted compressed batch can expand uncontrollably.

### 5.3 Proposed Configuration Changes (Marked PROPOSED — Not Implemented in KL02-01)

The following changes are formally proposed for implementation in subsequent cards. **No configuration defaults are modified in this specification card.**

1. **PROPOSED for KL02-02 (Producer Budget Accounting):**
   - Retain default `buffer_memory = 32 MB` and `max_request_size = 1 MB`.
   - Update `rec_bytes` to account for header keys and values plus a fixed per-record envelope estimate (e.g. 48 bytes object/header framing).
   - Maintain the rule that any single record with upper bound `<= max_request_size` can acquire buffer permit when `buffered_bytes == 0`.

2. **PROPOSED for KL02-03 (Decompression Guard):**
   - Introduce an internal hard ceiling for decompression expansion: `max_record_batch_decode_bytes: usize` (e.g. 64 MB).
   - If decompressed bytes exceed this hard ceiling, abort decompression with `Error::protocol("Decompressed batch exceeds maximum allowable size")`.
   - This hard ceiling must be strictly greater than `max_partition_fetch_bytes` to preserve the oversized-first-batch progress rule for legitimate large batches.

3. **PROPOSED for KL02-08 (Consumer Buffering Alignment):**
   - Propose reducing default `max_partition_fetch_bytes` from 16 MB to 1 MB and setting default `max_poll_records` to 500, aligning with Apache Kafka Java client standards.
   - Maintain oversized-first-batch progress: a batch larger than 1 MB but smaller than the decompression ceiling must be processed and yielded to the caller.

---

## 6. Test Cross-Check and Empirical Coverage Analysis

The specification has been verified against current integration test suites:

### 6.1 `tests/buffer_ownership.rs`
- `saturating_try_send_never_exceeds_buffer_memory`:
  - **Verdict:** Passes.
  - **Coverage:** Exercises `try_send` saturation, verifying that `buffered_bytes <= cap` and drains to 0 after `flush()`.
  - **Limitation:** Uses empty headers and 100-byte values only. Does not exercise header-heavy records or shared backing allocations.
- `queue_full_under_overload_releases_no_orphan_bytes`:
  - **Verdict:** Passes.
  - **Coverage:** 8 concurrent tasks hammering `try_send` observing `QueueFull`. Verifies zero orphan bytes remain after flush.
  - **Limitation:** Empty headers and identical payload sizes only.
- `send_timeout_when_buffer_full_and_max_block_expires`:
  - **Verdict:** Passes.
  - **Coverage:** Verifies `max_block` timeout when `buffer_memory` is exhausted, confirming that timed-out send leaves no orphan reservations.
  - **Limitation:** Tests single payload boundary; does not test cancellation during wait.

### 6.2 `tests/produce_cancel.rs`
- `send_completes_with_record_metadata`:
  - **Verdict:** Passes. Verifies successful terminal release (`bytes_buffered == 0`).
- `send_fails_when_broker_returns_produce_error`:
  - **Verdict:** Passes. Verifies broker error path release (`INVALID_RECORD` releases to 0).
- `dropping_send_future_while_buffered_is_ambiguous_but_still_delivers`:
  - **Verdict:** Passes.
  - **Coverage:** Drops `send` future while record is in linger buffer. Proves record still reaches broker and releases buffer on flush.
- `send_after_close_returns_closed`:
  - **Verdict:** Passes. Verifies closed producer rejects sends and leaves 0 buffered bytes.

### 6.3 Matrix of Tested vs. Untested Paths (Coverage Limits)

| Path / Condition | Current Suite Status | Missing Coverage Classification |
|---|---|---|
| Key + Value buffer saturation | Tested (`buffer_ownership.rs`) | Covered |
| Clean broker ack release | Tested (`produce_cancel.rs`) | Covered |
| Broker error release | Tested (`produce_cancel.rs`) | Covered |
| Cancel while buffered in linger | Tested (`produce_cancel.rs`) | Covered |
| Header-heavy records (>100 KB headers, 0 KB payload) | **Not Tested** | **Limit** (Addressed in KL02-02) |
| Shared backing slices (`Bytes::slice` on large buffer) | **Not Tested** | **Limit** (Addressed in KL02-02) |
| Compressed records buffer lifecycle | **Not Tested** in buffer tests | **Limit** (Basic codec in `full_surface.rs`, but no ownership test) |
| Malicious / zip-bomb decompression expansion | **Not Tested** | **Limit** (Addressed in KL02-03) |
| Pre-send worker failure (encode / txn error) | **Not Tested** | **Limit** (Addressed in KL02-04) |
| In-flight connection drop under retry | **Not Tested** in cancel suite | **Limit** (Addressed in KL02-06, KL02-07) |

---

## 7. Compliance Checklist

- [x] Ownership table covers accepted record through queue, retry, encode, socket, decode, delivery, and shutdown.
- [x] Key/value counters distinguished from headers, object overhead, shared backing allocations, compressed/decoded bytes, tasks, and RSS.
- [x] Exactly one release owner defined on every terminal path.
- [x] Completed, failed, and ambiguous outcomes defined; ambiguous is not collapsed into failed.
- [x] Proposed configuration/default changes marked PROPOSED and not implemented.
- [x] Kafka's oversized-first-batch progress rule explicitly preserved.
- [x] Four concrete records traced against current source with verified `file:line` citations.
- [x] Audit line numbers noted as frozen at `cb7e97d` and current citations verified against `bed63ae`.
- [x] No unsafe code suggestions (`deny(unsafe_code)` preserved).
- [x] Existing tests named and missing coverage classified as limits.
