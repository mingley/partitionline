# How it works

The library speaks Kafka's network protocol itself. There is no C Kafka
library in the process.

## Produce

1. You call `try_send` (throughput), `send_all` (many offsets), or `send`
   (one offset future per record).
2. The record is given a partition **before** it is queued (murmur2 if
   there is a key, round-robin if not, or `ProducerConfig::partitioner`).
   Until metadata for that topic is cached, `try_send` returns `QueueFull`
   and `send` / `send_all` wait.
3. The record goes onto the queue for **one** TCP connection:
   `partition % connections`. Idempotent sequences for a partition never
   share a socket with another worker.
4. That connection's worker waits `linger` or until the batch is big
   enough, then writes a Produce request.
5. Several Produce requests can be in flight on the same socket.
6. `flush` waits for those responses and returns the first broker error.
   `try_send` Ok only means queued.

The hot path copies each payload once into the Kafka record batch and
checksums it with CRC32-C.

## Fetch

`Consumer` is manual: topic, partition, offset, then `fetch`. `fetch`
sends one request per partition leader and waits for all of them.

`max.poll.records` caps how many records one `fetch` returns; the rest
stay buffered.

Group members heartbeat on a background thread. Share groups use
ShareFetch / ShareAcknowledge instead of Fetch / OffsetCommit.

## TLS

Plain TCP stays a `TcpStream`. TLS is a separate connection type so the
uncompressed hot path does not pay for rustls.

Writes pump reads into the connection buffer. TLS (and a full TCP window)
can otherwise stall `poll_write` until `poll_read` runs, which deadlocks a
pipelined producer that only reads after `max_in_flight` writes.

## Protocol

Version table and encode/decode gotchas: [protocol.md](protocol.md).
Java-shaped helpers are documented on the types in rustdoc.
