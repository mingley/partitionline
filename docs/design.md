# How it works

The library talks Kafka's network protocol itself. There is no C Kafka library in the process.

## Producer

1. You call `try_send` (or `send` if you want one offset future per record).
2. The record goes onto a queue for one TCP connection.
3. That connection's worker waits a few milliseconds (`linger`) or until the batch is big enough, then writes a Produce request.
4. Several Produce requests can be in flight on the same socket.

Partition is chosen when the record is queued (round-robin, or murmur2 if there is a key), once the topic's partition count is known.

The hot path copies each payload once into the Kafka record batch and checksums it with CRC32-C.

## Consumer

`Consumer` is manual: you say topic, partition, offset, then `fetch`.

`ConsumerGroup` joins a group, heartbeats, fetches, and can commit offsets.

## Wire format notes (for people changing encode/decode)

- Request `ClientId` is always a classic nullable string, even on flexible headers.
- ApiVersions **response** header is never flexible. If you parse it as flexible you eat the error code.
- Produce throttle time comes **after** the topic array. Metadata throttle time comes first.
- Record batch magic 2 CRC is CRC32-C over bytes from attributes to the end.
- Record lengths are zigzag varints. Compact protocol lengths are unsigned varint of `n+1` (`0` means null).
- Without `InitProducerId`, producer id / epoch / sequence must be `-1`. Zero is a real id.
- `acks=0` means the broker sends no Produce response. Do not read one.
- This client uses Produce versions 3–8 (classic record bytes). Version 9+ is compact.

## Compression

gzip uses `flate2` with its Rust backend. snappy/lz4/zstd stay out because the usual crates pull in C.
