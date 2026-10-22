# Scheduled open-loop produce latency

`examples/bench_latency.rs` defaults to sequential smoke. That mode still
emits `p50_us` and `p99_us` on `produce_ack` and `fetch_rpc` for the CI
regression detector, and it labels mode, qualification, and
coordinated omission. Call-to-ack timing in that mode is a smoke
measurement. It is not an open-loop result.

## Run a scheduled load

```sh
LATENCY_MODE=open-loop MODE=produce \
RATE_PER_SECOND=1000 COUNT=10000 WARMUP=10000 \
MAX_PENDING=1024 SAMPLE_FLOOR=10000 \
KAFKA_BOOTSTRAP=127.0.0.1:9092 KAFKA_TOPIC=pllat \
cargo run --release --example bench_latency > run.jsonl
```

You supply the broker and the topic. Records go to partition 0, on one
connection, with one in-flight request and one record per batch. `ACKS=1`
and `ACKS=-1` can measure a broker acknowledgement. `ACKS=0` cannot.
`MODE` must be `produce`. Fetch RPC smoke stays on sequential mode.
`independent_receive_ns` is null here.

Arrival `i` is `origin + floor(i * 1_000_000_000 / RATE_PER_SECOND)`
nanoseconds. The origin does not reset after a late response, local
backpressure, or a runtime pause, and later appointments do not shift.
The arrival task starts sends without waiting for delivery. A late wake
keeps the original intended time. Every one of the `COUNT` offers is
kept, including rejections. At capacity the offer is recorded as
rejected immediately.

| Setting | Default | Meaning |
| --- | ---: | --- |
| `COUNT` | 10000 | Scheduled offers. Positive, and it must fit the sample vector. |
| `RATE_PER_SECOND` | 1000 | Fixed rate, an integer from 1 through 1,000,000,000. |
| `MAX_PENDING` | 1024 | Outstanding send tasks, including tasks waiting to enqueue. |
| `SAMPLE_FLOOR` | 10000 | Minimum samples for all four percentiles. It cannot be lowered. |
| `WARMUP` | 10000 | Sequential warmup records. Excluded from timed samples and outcomes. |
| `BUFFER_MEMORY` | 33554432 | Produce payload cap in bytes. Zero uses the producer's uncapped buffer. |
| `MAX_BLOCK_MS` | 1000 | Longest enqueue wait. |
| `DELIVERY_TIMEOUT_MS` | 30000 | Delivery deadline after acceptance. |
| `REQUEST_TIMEOUT_MS` | 30000 | Per-RPC deadline. |
| `PAYLOAD_BYTES` | 100 | Payload size. |
| `ACKS` | 1 | Broker acknowledgement level. |
| `LINGER_MS` | 0 | Batch linger. |

A malformed setting, a zero count, rate, capacity, or timeout, a sample
floor below 10,000, or a schedule that overflows fails before any timed
offer. The existing smoke environment variables still parse for CI.

## What each sample records

Each `open_loop_sample` keeps its record id and monotonic nanoseconds from
the measurement origin: intended arrival, actual offer, enqueue lower and
upper bounds, observed acknowledgement, and terminal completion. It also
keeps schedule lag, enqueue wait, outstanding offers, buffered bytes,
acceptance, outcome, and error. Raw samples come out in arrival-id order,
then one `open_loop_produce_ack` summary.

The producer API exposes a per-record delivery future and aggregate
enqueue counters. It does not expose an internal enqueue clock. This
benchmark polls its send futures one at a time and brackets the poll that
increments `records_queued`. Only this benchmark family enqueues after
warmup, so a worker cannot bump that counter. An increment of exactly one
proves local acceptance for that record. No increment says nothing about
acknowledgement. The timestamps bound the acceptance observed during that
poll. The summary sets `enqueue_observation` to
`serialized_send_poll_bounds` and reports
`max_enqueue_observation_span_ns`. Those counter snapshots belong in any
overhead assessment.

Acknowledgement time is the clock read when the record's send future
completes. That includes wakeup and scheduling delay after the broker
response. The summary sets `acknowledgment_observation` to
`send_future_completion`. It is not a timestamp from the wire. Send
futures run to their terminal result. The benchmark does not cancel an
accepted send to invent a local deadline.

| Distribution | Definition | Samples |
| --- | --- | --- |
| `end_to_end` | Observed acknowledgement minus intended arrival | Acknowledged records |
| `enqueue_to_ack_upper_bound` | Observed acknowledgement minus the enqueue lower bound | Acknowledged records that were accepted |
| `schedule_lag` | Actual offer minus intended arrival | Every offer, including rejections |
| `enqueue_wait_upper_bound` | Enqueue upper bound minus actual offer | Locally accepted records |

Enqueue-to-ack includes the producer queue, the network, the broker, and
future wakeup. It is not scheduled end-to-end latency, and it is not
broker service time alone. The enqueue lower bound makes this interval an
upper bound. The raw bounds also let a reader compute the lower bound.

Each distribution reports its own sample count and `sample_floor_met`.
Below the floor, `p50_us`, `p95_us`, `p99_us`, and `p99_9_us` are null.
Failed or unacknowledged offers do not pad acknowledgement counts. At or
above the floor, percentiles use nearest ranks: 50%, 95%, 99%, and 99.9%.
Min, max, mean, and median absolute deviation (`mad_us`) are diagnostics
at any nonzero count. An empty distribution stays null. Summaries are in
microseconds. Raw timestamps stay in nanoseconds.

## Outcomes

All seven outcome fields are present.

| Field | Meaning |
| --- | --- |
| `accepted` | Local enqueue. It overlaps the terminal categories and is not added into the sum below. |
| `acknowledged` | The send future completed with a broker acknowledgement. |
| `consumed` | Always zero. This driver has no independent consumer. |
| `rejected` | Local admission refusal or a broker rejection. |
| `timed_out` | A producer timeout. |
| `unknown` | An accepted record that ended in unresolved I/O, closed, or a protocol state. |

Terminal accounting is `offered = acknowledged + rejected + timed_out + unknown`.

A capacity rejection has its own counter and a raw error. Any rejected,
timed-out, or unknown offer marks `run_disposition` failed. The program
prints the retained samples and the summary, then exits with failure. A
saturation run is expected to fail when the offered load is too high.
Keep each failed attempt with the later runs.

## What this file is not

The JSONL is input to a campaign report. It is not a complete
`benchmarks/result-schema.json` result. It does not verify payloads or
the high watermark, and it does not include signed provenance, resource
measurements, paired repetitions, or a controlled host. A campaign adds
those from [benchmark-contract.md](benchmark-contract.md), keeps this
JSONL as a checksummed artifact, and leaves Suite HOLD in place. This
page reports scheduler regression evidence. No scenario cell is marked
passed.

## Regression checks

```sh
cargo test --test bench_latency_open_loop -- --nocapture
cargo check --example bench_latency
cargo clippy --example bench_latency --test bench_latency_open_loop -- -D warnings
```

The tests check absolute schedules, runtime catch-up, explicit enqueue
bounds, the sample floor, and distinct terminal outcomes. A virtual pause
shows that intended-arrival latency keeps the delay. A local mock broker
stalls one Produce response and checks that new offers continue before
the first acknowledgement, exercises capacity rejections, and keeps real
delivery timeouts.
