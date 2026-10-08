# Scheduled open-loop produce latency

`examples/bench_latency.rs` retains its default **sequential-smoke** mode. Its
`produce_ack` and `fetch_rpc` JSON objects retain `p50_us` and `p99_us` for the
existing CI regression detector and add explicit mode, qualification, and
coordinated-omission labels. Sequential call-to-ack timing remains a smoke
measurement. It does not qualify an open-loop performance claim.

For scheduled load, use:

```sh
LATENCY_MODE=open-loop MODE=produce \
RATE_PER_SECOND=1000 COUNT=10000 WARMUP=10000 \
MAX_PENDING=1024 SAMPLE_FLOOR=10000 \
KAFKA_BOOTSTRAP=127.0.0.1:9092 KAFKA_TOPIC=pllat \
cargo run --release --example bench_latency > run.jsonl
```

The caller supplies the broker and topic. Scheduled records target partition 0,
with one connection, one in-flight request, and one record per batch. `ACKS=1`
and `ACKS=-1` are accepted; `ACKS=0` cannot measure a broker acknowledgment.
`MODE` must be `produce` in open-loop mode. Fetch RPC smoke timing remains
available through sequential mode; independent receive timing is optional and
is represented by `independent_receive_ns: null` here.

The fixed-rate schedule computes arrival `i` as
`origin + floor(i * 1_000_000_000 / RATE_PER_SECOND)` nanoseconds. It never
resets the origin or shifts subsequent appointments after a delayed response,
local backpressure, or a runtime pause. The arrival task launches send tasks
without awaiting delivery. If it wakes late, overdue offers keep their original
intended times. Every one of the `COUNT` offers is retained, including admission
rejections. At capacity, the offer is immediately recorded as rejected; it is
not deferred to a convenient later time.

| Environment setting | Open-loop default | Meaning |
| --- | ---: | --- |
| `COUNT` | 10000 | Number of scheduled offers; positive and representable as a sample-vector layout |
| `RATE_PER_SECOND` | 1000 | Fixed arrival rate; integer in 1..=1000000000 |
| `MAX_PENDING` | 1024 | Bound on outstanding send tasks, including tasks waiting to enqueue |
| `SAMPLE_FLOOR` | 10000 | Minimum samples per distribution for all four percentiles; cannot be lowered below 10000 |
| `WARMUP` | 10000 | Sequential warmup records, excluded from timed samples and outcomes |
| `BUFFER_MEMORY` | 33554432 | Producer payload buffer cap in bytes; zero follows the producer's uncapped-buffer semantics |
| `MAX_BLOCK_MS` | 1000 | Maximum producer enqueue wait |
| `DELIVERY_TIMEOUT_MS` | 30000 | Producer delivery deadline after acceptance |
| `REQUEST_TIMEOUT_MS` | 30000 | Per-RPC deadline |
| `PAYLOAD_BYTES` | 100 | Payload size |
| `ACKS` | 1 | Broker acknowledgment level |
| `LINGER_MS` | 0 | Batch linger |

Malformed load settings, zero counts/rates/capacities/timeouts, an insufficient
declared sample floor, and overflowing schedules fail before timed offers.
Existing smoke environment parsing remains compatible with CI.

## Timing and observation resolution

Each `open_loop_sample` retains its record ID and monotonic times in nanoseconds
relative to the measurement origin: intended arrival, actual offer, actual
enqueue lower/upper bounds, observed acknowledgment, and terminal completion.
It also records schedule lag, enqueue wait, outstanding offers, buffered bytes,
acceptance, outcome, and error. Raw samples are emitted in arrival ID order,
followed by one `open_loop_produce_ack` summary.

The existing producer API exposes a per-record delivery future and aggregate
enqueue counters. It does not expose the internal enqueue clock. The benchmark
therefore serializes each poll of its send futures and brackets the poll that
increments `records_queued`. Only this benchmark task family enqueues after
warmup. Worker tasks cannot increment that counter. An increment of exactly one
proves local acceptance for that record; no increment proves nothing about
acknowledgment. The timestamps bound the actual acceptance observed during that
poll. The summary declares `enqueue_observation: serialized_send_poll_bounds`
and the largest `max_enqueue_observation_span_ns`. These observations involve
counter snapshots and should be included when assessing measurement overhead.

Acknowledgment time is the clock read at successful completion of the record's
send future. It includes delivery-future wakeup and scheduling delay after the
broker response. The summary declares
`acknowledgment_observation: send_future_completion`; it is not a wire-level
response timestamp. Send futures run to their terminal result. The benchmark
does not cancel accepted sends to manufacture a local deadline.

| Distribution | Definition | Samples |
| --- | --- | --- |
| `end_to_end` | Observed acknowledgment minus intended arrival | Acknowledged records |
| `enqueue_to_ack_upper_bound` | Observed acknowledgment minus enqueue lower bound | Acknowledged, accepted records |
| `schedule_lag` | Actual offer minus intended arrival | All offers, including rejected offers |
| `enqueue_wait_upper_bound` | Enqueue upper bound minus actual offer | Locally accepted records |

Enqueue-to-ack includes the producer's internal queue, network, broker, and
future wakeup time. It is distinct from scheduled end-to-end latency and does
not isolate broker service time. Using the enqueue lower bound gives an upper
bound on this measured interval. The raw bounds also let readers compute the
corresponding lower bound.

Each distribution reports its own sample count and `sample_floor_met` flag.
`p50_us`, `p95_us`, `p99_us`, and `p99_9_us` are all `null` below the declared
floor; failed or unacknowledged offers do not pad acknowledgment sample counts.
Above the floor, percentiles use nearest ranks (50%, 95%, 99%, 99.9%). Min, max,
mean, and median absolute deviation (`mad_us`) are diagnostics at any nonzero
sample count. Empty distributions retain null values. Summaries use
microseconds; raw timestamps retain nanosecond observation resolution.

## Outcomes and evidence limits

All seven outcome fields are present. `accepted` means local enqueue,
`acknowledged` means the successful send future received a broker acknowledgment,
and `consumed` is zero because this driver has no independent consumer. Local
admission and broker rejection count under `rejected`; producer timeouts count
under `timed_out`; accepted records ending with an unresolved I/O, closed, or
protocol state count under `unknown`. The terminal accounting is
`offered = acknowledged + rejected + timed_out + unknown`; `accepted` overlaps
terminal categories and is never added to that sum.

Capacity rejection has its own counter and raw error. Any rejected, timed-out,
or unknown offer makes `run_disposition` failed. The main program prints the
retained raw samples and summary before returning a failure exit status. A
saturation experiment is expected to expose failures at excessive offered load;
keep each failed attempt alongside subsequent runs.

These JSONL diagnostics are inputs to a campaign report, not a complete
`benchmarks/result-schema.json` result. They do not supply independent payload
or high-watermark verification, signed provenance, resource measurements,
paired repetitions, or controlled-host qualification. Campaigns must add those
requirements from `docs/benchmark-contract.md`, retain the raw JSONL as a
checksummed artifact, and preserve Suite HOLD. This implementation card reports
scheduler regression evidence only; no scenario cell is marked passed.

## Regression checks

```sh
cargo test --test bench_latency_open_loop -- --nocapture
cargo check --example bench_latency
cargo clippy --example bench_latency --test bench_latency_open_loop -- -D warnings
```

The tests check rational absolute schedules, runtime catch-up, explicit enqueue
bounds, sample-floor behavior, and distinct terminal outcomes. Deterministic
virtual pause evidence demonstrates the delay retained by intended-arrival
latency. A local mock broker additionally stalls a real Produce response and
checks that new offers continue before its first acknowledgment, exercises
bounded-capacity rejections, and retains real delivery timeouts.
