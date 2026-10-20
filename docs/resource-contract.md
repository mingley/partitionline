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
Further batches stay in a per-partition queue of framed, still-compressed
batches and are decoded when consumption frees budget. Retained framed bytes
are capped by the larger of `buffer_memory` and `max_bytes` (the response
already requested). One batch that does not fit is still kept so the consumer
does not fetch it again. A zero `buffer_memory` keeps every framed batch.
Decoded key, value and header bytes reported by `buffered_bytes()` stay within
`buffer_memory`. Wire frames, temporary decoding allocations and
object overhead need separate accounting; a byte gauge does not measure these
allocations.

Pause retains pending records and framed batches. Seek, unassign, leader
changes and close discard the applicable pending records and framed batches.
Delivered positions and commits do not advance solely because a record was
prefetched. Applications remain responsible for releasing their own returned
batches and for their chosen commit policy.

## Caller deadlines

A timeout covers the whole call, including preparation. A long list is walked
in slices of 8,192 items. Between slices the runtime can notice the deadline,
so one collection cannot occupy the caller for the entire list.

Finished slices are kept. The next slice starts only after the caller has had
a chance to time out. The work itself does not change: names are still copied,
leaders are still looked up, and a repeated name is still stored once.

These walks include:

- copying topic partitions for delete-records, list-offsets, and describe-producers
- finding, listing, and grouping leaders during an admin retry
- listing missing coordinators, collecting ones already cached, and grouping ids under them
- dropping cached topic metadata before a refresh
- placing assigned fetches and collecting topic names
- checking whether assigned or subscribed topics are fresh
- indexing metadata topic names and recording topic ids
- finding recreated topics
- grouping preferred replicas, and dropping one whose leader epoch changed, before metadata is applied
- clearing and pruning the fetch plan
- checking whether assigned fetches are paused or already finished
- dropping fetched records that fall at or after a recovered cursor
- covering and merging share acknowledgements against stored acquisitions
- rebuilding share acquisition indexes
- filtering pending share records after an assignment or session change
- dropping pending share records for a recreated topic
- sorting share acknowledgements that share a topic
- checking whether share topic ids still match metadata, and rewriting them when one is stale
- refilling stale share topic names before a metadata refresh
- summing stored share acquisition metadata before a poll
- clearing and pruning the share leader plan
- resolving share leader addresses before a dial
- opening a KIP-848 group assignment into topic partitions
- checking a group assignment for an unknown topic id
- copying topic ids before assigning committed offsets
- comparing assigned topic ids with that snapshot
- collecting commits for revoked partitions
- writing list-offset resets into an assignment, and dropping slots that are still waiting
- copying revoked and assigned partitions before a rebalance callback
- refreshing delivered topic ids after a poll or commit
- copying delivered positions before an interval commit, and keeping the ones that are still assigned
- indexing share topic ids before a fetch or acknowledgement names them
- recording an overlapped share acquisition
- dropping share acquisitions that are no longer kept
- dropping covered share acquisitions
- keeping pending fetch runs that are still assigned
- dropping queued fetch runs for partitions that moved
- counting runnable records before a fetch drain
- putting returned records back in front of the fetch buffer
- keeping framed fetches whose partitions are still assigned
- counting bytes in queued records that are leaving the fetch buffer
- releasing queued records for partitions that are no longer assigned
- counting bytes before a fetched batch is put back
- counting returned share records for the poll total and topic metrics
- counting returned fetch records when saved totals do not match the batch
- applying saved per-topic totals after a fetch
- returning a fetched batch when no consume interceptor is installed
- clearing queued records when an assignment is dropped
- dropping framed batches kept for a later poll
- reading the next offset, and the partition list, of a returned batch
- queuing records passed to a multi-record produce
- splitting queued sends that still need topic metadata
- copying topic rows for a fetch metrics snapshot
- copying topic rows for a produce metrics snapshot
- copying an assignment into topic partitions
- copying delivered positions into topic partitions
- copying a changed consumer or share heartbeat subscription or assignment
- opening one result slot, and its index, for each item in an admin batch
- copying transactional ids before fencing producers

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
