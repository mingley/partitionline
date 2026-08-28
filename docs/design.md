# How it works

The library talks Kafka's network protocol itself. There is no C Kafka library in the process.

## Producer

1. You call `try_send` (throughput), `send_all` (many offsets), or `send`
   (one offset future per record).
2. The record is given a partition **before** it is queued (the
   [`Partitioner`](../src/partitioner.rs): murmur2 if there is a key,
   round-robin if not, or `ProducerConfig::partitioner`). Until metadata for
   that topic is cached, `try_send` returns `QueueFull` and `send` /
   `send_all` wait.
3. The record goes onto the queue for **one** TCP connection: `partition % connections`. Idempotent sequences for a partition never share a socket with another worker.
4. That connection's worker waits a few milliseconds (`linger`) or until the batch is big enough, then writes a Produce request.
5. Several Produce requests can be in flight on the same socket.
6. `flush` waits for those responses and returns the first broker error. `try_send` Ok only means queued.

`ProducerConfig`, `ConsumerConfig`, and `AdminConfig` accept chainable
builders (`acks`, `sasl`, `tls`, `isolation`, …). The raw fields remain
writable.

The hot path copies each payload once into the Kafka record batch and checksums it with CRC32-C.

## Consumer

`Consumer` is manual: you say topic, partition, offset, then `fetch`.
`fetch` sends one request per partition leader and waits for all of them
when there is more than one. `seek_to_beginning` / `seek_to_end` call
ListOffsets for every assigned partition. `pause` / `resume` skip
assigned partitions without dropping them; pause survives group rebalance.
`position` is the next fetch offset (`position_of` takes `TopicPartition`).
`partitions_for` / `beginning_offsets` / `end_offsets` wrap Metadata and
ListOffsets and take `TopicPartition`. `assignment` is Java `assignment`
(`assigned_partitions` is the same list; `positions` is next fetch offset).
`max.poll.records` caps
how many records one `fetch` returns; the rest stay buffered.

`ConsumerGroup` joins a group, heartbeats, fetches, and can commit offsets.
`join_topics` (range), `join_sticky_topics`, `join_cooperative_sticky_topics`,
and `join_consumer_topics`
subscribe to several topics. Range and sticky assign each topic independently
among members who subscribed to it. Sticky rebalances load when a member joins.
Cooperative-sticky (KIP-429) keeps owned partitions until the owner revokes
them, then rejoins so the new owner can take them. `ConsumerConfig::group_instance_id` is
Kafka `group.instance.id` (static membership) on JoinGroup, Heartbeat, and
KIP-848 heartbeats. `ConsumerConfig::rack` is also sent on KIP-848.
`ConsumerConfig::auto_offset_reset` runs when OffsetFetch has no committed
offset (`Earliest` by default). `committed` is OffsetFetch for the current
assignment. `commit_offsets` commits caller-chosen offsets
([`TopicPartition`](../src/consumer.rs) plus the next fetch offset).
`commit_with_metadata` sends [`OffsetAndMetadata`](../src/consumer.rs)
(leader epoch and a metadata string). `committed` returns the same type.
`current_lag` is high watermark minus position. `subscription` is the
topic list. `enforce_rebalance` rejoins on the next poll.
`subscribe` / `unsubscribe` change the topic list without dropping the
handle. `group_metadata` is Java `ConsumerGroupMetadata`. `list_topics`
is cluster Metadata. `fetch_timeout` / `poll_timeout` are Java
`poll(Duration)`. [`Producer::send_offsets_to_transaction`](../src/producer.rs)
takes [`TopicPartition`](../src/consumer.rs).
[`Producer::send_offsets_with_metadata`](../src/producer.rs)
commits transactional offsets with epoch and a metadata string.
`enable.auto.commit` is off by default; a zero interval commits after every
`poll`. `ConsumerConfig::session_timeout` / `heartbeat_interval` control classic
JoinGroup and the heartbeat loop. `on_rebalance` is `(revoked, assigned)`.
`max.poll.interval.ms` errors on the next `poll` if exceeded (`Error::MaxPollInterval`)
and the heartbeat thread leaves the group.
`Producer::metrics` / `Consumer::metrics` / `ShareGroup::metrics` are counter snapshots.
`Consumer::wakeup` (and a cloneable [`WakeupHandle`](../src/consumer.rs)) interrupts
fetch. `ProducerConfig::interceptor` / `ConsumerConfig::interceptor` observe or rewrite
records. [`TopicPartition`](../src/consumer.rs) and `offsets_for_times` are Java
`offsetsForTimes`. `FetchedRecord.leader_epoch` is the record-batch partition
leader epoch. `FetchedRecord.serialized_key_size` / `serialized_value_size`
match Java. `assign_many` / `unassign` replace or drop a manual assignment.
`Consumer::close` drops fetch connections.

`ShareGroup` is KIP-932 queue sharing. `join_topics` subscribes to several
topics.

## Wire format notes (for people changing encode/decode)

- Request `ClientId` is always a classic nullable string, even on flexible headers.
- ApiVersions **response** header is never flexible. If you parse it as flexible you eat the error code.
- Produce throttle time comes **after** the topic array. Metadata throttle time comes first.
- Record batch magic 2 CRC is CRC32-C over bytes from attributes to the end.
- Record lengths are zigzag varints. Compact protocol lengths are unsigned varint of `n+1` (`0` means null).
- Without `InitProducerId`, producer id / epoch / sequence must be `-1`. Zero is a real id.
- `acks=0` means the broker sends no Produce response. Do not read one.
- This client uses Produce versions 3–8 (classic record bytes). Version 9+ is compact.
- ListOffsets v4+ has `current_leader_epoch` before timestamp. The v4+ response has `leader_epoch` after offset.

## Compression

gzip uses `flate2` with its Rust backend. snappy uses the `snap` crate
(snappy-java framing on produce, raw snappy accepted on fetch). lz4 uses
`lz4_flex` LZ4 frames (independent 64KiB blocks, proper header checksum for
magic ≥ 1). zstd is not implemented; the Kafka ecosystem codec is typically C
(`zstd-sys`).

## TLS

Set `ProducerConfig.tls` / `ConsumerConfig.tls` to a `TlsConfig`. Handshake is
`rustls` with the `ring` backend (not OpenSSL). Custom CA PEM, or Mozilla
roots if `ca_pem` is omitted. Optional client cert/key for mTLS. SNI defaults
to the bootstrap host. Plain TCP stays a `TcpStream`; TLS is a separate
connection type so the uncompressed hot path does not pay for rustls.

Writes pump reads into the connection buffer. TLS (and a full TCP window)
can otherwise stall `poll_write` until `poll_read` runs, which deadlocks a
pipelined producer that only reads after `max_in_flight` writes.
