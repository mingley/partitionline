# Resource ownership and outcomes

The producer and consumer have payload budgets. Those budgets do not bound
process RSS. RSS also includes record objects, allocator overhead, batch
encoding and decoding, metadata, tasks, TLS state and socket buffers.

## Producer

`ProducerConfig::buffer_memory` limits reserved visible key, value and header
bytes. Header keys count their UTF-8 bytes; nullable values count zero.
`ProducerMetrics::bytes_buffered` reports the current reservation.

Admission also checks the serialized record upper bound against
`max_request_size` and a nonzero `buffer_memory`. A zero budget disables its
byte cap. Record and queue counts still matter when payloads are empty.

Accepted records own payload allocations sized to their visible bytes.
Shared `Bytes` slices and custom backing owners are copied; an exact, unique
allocation can be reused. Header vectors and strings shed spare capacity.
Caller aliases and inputs awaiting admission remain caller-owned.

A reservation follows the record through queueing, batching, transmission and
retry. A failed admission releases its temporary reservation. Acknowledgment,
terminal error and shutdown release accepted reservations once. Cancelling a
caller future after admission does not remove the record from a worker.

`try_send` success means accepted into the client. With `acks=0`, completion
means the socket write finished. Neither establishes replicated durability.
For acknowledged modes, the response describes the broker's acknowledgment
under the configured acknowledgment policy.

## Consumer

`ConsumerConfig::buffer_memory` limits retained key, value and header bytes
across prefetched partitions and brokers. `Consumer::buffered_bytes()` reports
pending payload. Records already returned to the application are outside that
counter. `max_poll_records` limits records returned by a fetch call.

A valid first batch may exceed the soft fetch or retention limit so the
partition can make progress. Batch decoding has a 64 MiB decoded-byte ceiling.
Further batches are refused when they would exceed the retained payload
budget. Wire frames, temporary decoding allocations and object overhead need
separate accounting; a byte gauge does not measure these allocations.

Pause retains pending records. Seek, unassign and close discard the applicable
pending records. Delivered positions and commits do not advance solely because
a record was prefetched. Applications remain responsible for releasing their
own returned batches and for their chosen commit policy.

## Outcomes and shutdown

| Outcome | Meaning |
| --- | --- |
| Offered | The application attempted admission. |
| Accepted | The client queued the record. |
| Completed | The configured acknowledgment condition completed. |
| Failed | Admission failed, or evidence proves rejection without persistence. |
| Ambiguous | Acceptance occurred, but delivery cannot be established. |

Timeouts, lost responses and cancelled completion futures can leave delivery
ambiguous. Do not translate a timeout into proof that no record was written.
A conservative resource driver may classify every post-admission error as
ambiguous when it lacks a per-record broker witness.

Close refuses new sends across clones, drains work up to its deadline, then
stops workers. A close timeout can leave delivery ambiguous even though the
client released its resources. Dropping the last producer handle aborts its
owned tasks. Consumers release pending buffers on close; a driver's stop must
also release application-held records and join its own tasks.

The [resource driver](resource-soak.md) records these counters with RSS and
lifecycle receipts. Short rehearsals test accounting and cleanup. Longer
controlled-host runs and independent record-history checks establish separate
operational evidence.
