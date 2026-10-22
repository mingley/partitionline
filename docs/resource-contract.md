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

### Produce

- Queue records passed to a multi-record produce.
- Split queued sends that still need topic metadata.
- Index many partitions of one topic, then assemble groups when a batch names many topics.
- Encode those topics into one Produce request.
- Write partition batches of a Produce request. The request bytes stay the same.
- Write partition rows of a Produce response. The response bytes stay the same.
- Drop acknowledgement slots from the previous batch.
- Copy topic rows for a produce metrics snapshot.

### Fetch

- Place assigned fetches and collect their topic names.
- Check whether an assigned fetch is paused or already finished.
- Clear and prune the fetch plan.
- Apply many one-record batches from one partition. Batches already applied stay.
- Drop fetched records at or after a recovered cursor.
- Keep pending runs that are still assigned, and drop runs for a partition that moved.
- Count runnable records before a drain, and count bytes in records that are leaving.
- Put a returned batch back in front of the buffer, and count its bytes first.
- Keep framed fetches whose partitions are still assigned. Drop framed batches kept for a later poll.
- Release queued records for partitions that are no longer assigned.
- Count returned records when saved totals do not match the batch, then apply the saved per-topic totals.
- Return a fetched batch without copying each record when no consume interceptor is installed.
- Write aborted transactions into a Fetch response. The response bytes stay the same.
- Write Fetch response partitions. A partition row is still copied.
- Write ShareFetch response partitions. A partition row is still copied.
- Write ShareAcknowledge response partitions. A partition row is still copied.
- Measure and write node endpoints into a Produce or Fetch response tag. A host is still copied.
- Measure and write Metadata response topics. A topic name is still copied.
- Measure and write Metadata response brokers. A broker host is still copied.
- Read the next offset and the partition list of a returned batch. The next offset is the last delivered offset plus one, with its leader epoch.
- Copy topic rows for a fetch metrics snapshot.

### Fetch sessions

- Drop session partitions that were not in this request, and record the forgotten ones.
- Rebuild the topic-id index, and clear prepare generations when they wrap.
- Copy fetch partitions into a session request.

### Groups, offsets, and assignment

- Open a KIP-848 assignment into topic partitions, and reject an unknown topic id.
- Copy topic ids before assigning committed offsets, then compare the assignment with that snapshot.
- Validate topic names and reject a repeated partition.
- Copy a range or sticky assignment onto many members, including partitions they already own.
- Write list-offset resets into an assignment, and drop slots that are still waiting. Seek finds each partition in the cursor index. An unknown partition is an error.
- Copy revoked and assigned partitions before a rebalance callback.
- Collect commits for revoked partitions.
- Fill delivered positions after a poll or commit. A negative position is omitted.
- Refresh delivered topic ids, and drop ids for partitions that are no longer assigned.
- Copy delivered positions before an interval commit, and keep the ones that are still assigned.
- Read delivered offsets before a queued commit.
- Read an offset-commit response, and check an offset-fetch response, when either names many topics.
- Pause or resume assigned partitions, and list the ones that are paused.
- Copy an assignment, or the delivered position of each partition.
- Copy a changed heartbeat subscription or assignment.
- Clear queued records when an assignment is dropped.

### Share groups

- Index topic ids before a fetch or acknowledgement names them. A topic id that belongs to two names is an error. An empty name is not used.
- Check topic ids against metadata, rewrite a stale id, and refill stale names before a refresh.
- Record an overlapped acquisition. Drop acquisitions that are no longer kept, and drop a covered acquisition.
- Cover and merge acknowledgements against stored acquisitions, and sort acknowledgements that share a topic.
- Rebuild acquisition indexes. Sum stored acquisition metadata before a poll.
- Filter pending share records after an assignment or session change, and drop them for a recreated topic.
- Count returned share records for the poll total and topic metrics.
- Clear and prune the share leader plan, and resolve leader addresses before a dial.

### Transactions

- Write transaction-marker topics. A topic name is still copied.
- Write transaction-marker response topics. A topic name is still copied.

### Metadata and admin

- Index metadata topic names and record topic ids.
- Check whether assigned or subscribed topics are fresh, and find recreated topics.
- Drop cached topic metadata before a refresh.
- Group preferred replicas, and drop one whose leader epoch changed, before metadata is applied.
- Copy topic partitions for delete-records, list-offsets, and describe-producers.
- Find, list, and group leaders during an admin retry.
- List missing coordinators, collect ones already cached, and group ids under them.
- Open one result slot, and its index, for each item in an admin batch.
- Copy transactional ids before fencing producers.

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
